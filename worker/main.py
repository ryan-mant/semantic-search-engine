import asyncio
import json
import signal
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from confluent_kafka import Consumer, KafkaError, Message, Producer, TopicPartition
from pydantic import BaseModel, Field

from src.domain.entities.document import Document
from src.domain.ports.logger import LoggerPort
from src.domain.ports.document_repository import DocumentRepository
from src.domain.ports.embedding import EmbeddingPort
from src.domain.ports.vector_store import VectorStorePort
from src.infrastructure.adapters.mongodb.repository import MongoDocumentRepository
from src.infrastructure.adapters.embeddings.sentence_transformer import SentenceTransformerEmbeddingAdapter
from src.infrastructure.adapters.chroma.vector_store import ChromaVectorStoreAdapter
from src.infrastructure.config.settings import Settings, KafkaSettings


class WorkerKafkaSettings(KafkaSettings):
    group_id: str = "document-worker-group"
    auto_offset_reset: str = "earliest"
    batch_size: int = 16
    batch_timeout: float = 0.5
    max_poll_interval_ms: int = 300000
    session_timeout_ms: int = 45000
    heartbeat_interval_ms: int = 3000
    fetch_min_bytes: int = 16384
    fetch_wait_max_ms: int = 500
    partition_assignment_strategy: str = "cooperative-sticky"
    group_instance_id: Optional[str] = None


class WorkerSettings(Settings):
    kafka: WorkerKafkaSettings = Field(default_factory=WorkerKafkaSettings)


class DocumentMessageDTO(BaseModel):
    id: Optional[str] = None
    content: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None


class ConsoleJsonLogger(LoggerPort):
    def _log(
        self, level: str, message: str, extra: Optional[Dict[str, Any]] = None
    ) -> None:
        log_payload = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": level,
            "message": message,
            "extra": extra or {},
        }
        sys.stdout.write(json.dumps(log_payload) + "\n")
        sys.stdout.flush()

    def info(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self._log("INFO", message, extra)

    def error(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self._log("ERROR", message, extra)

    def warning(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self._log("WARNING", message, extra)

    def debug(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self._log("DEBUG", message, extra)


class KafkaMessageConsumer:
    def __init__(
        self,
        consumer: Consumer,
        settings: WorkerSettings,
        logger: LoggerPort,
        repository: DocumentRepository,
        embedding_port: EmbeddingPort,
        vector_store: VectorStorePort,
        dlq_producer: Optional[Producer] = None,
    ) -> None:
        self._consumer = consumer
        self._settings = settings
        self._logger = logger
        self._repository = repository
        self._embedding_port = embedding_port
        self._vector_store = vector_store
        self._dlq_producer = dlq_producer
        self._running = False

    def _extract_doc_id_from_raw(self, msg: Message) -> Optional[str]:
        try:
            key_bytes = msg.key()
            if isinstance(key_bytes, bytes) and key_bytes:
                return key_bytes.decode("utf-8")
            if isinstance(key_bytes, str) and key_bytes:
                return key_bytes
        except Exception:
            pass

        try:
            payload_bytes = msg.value()
            if isinstance(payload_bytes, bytes) and payload_bytes:
                data = json.loads(payload_bytes.decode("utf-8"))
                if isinstance(data, dict) and isinstance(data.get("id"), str):
                    return data["id"]
        except Exception:
            pass
        return None

    async def _handle_poison_pill(self, msg: Message, error: Exception) -> None:
        doc_id = self._extract_doc_id_from_raw(msg)
        if doc_id:
            try:
                await self._repository.update_status(
                    doc_id, "FAILED", error=f"Invalid message format: {error}"
                )
            except Exception as repo_err:
                self._logger.warning(
                    f"Failed to update status to FAILED for poison pill {doc_id}: {repo_err}"
                )

        if self._dlq_producer is not None:
            try:
                raw_val = msg.value()
                dlq_payload = json.dumps(
                    {
                        "document_id": doc_id,
                        "error": str(error),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "raw_payload": (
                            raw_val.decode("utf-8", errors="replace")
                            if isinstance(raw_val, bytes)
                            else str(raw_val)
                        ),
                        "failed_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                self._dlq_producer.produce(
                    topic=self._settings.kafka.dlq_topic,
                    key=doc_id,
                    value=dlq_payload,
                )
                self._dlq_producer.poll(0)
            except Exception as dlq_err:
                self._logger.warning(f"Failed to publish poison pill to DLQ: {dlq_err}")

    def _rewind_batch(self, batch: List[Tuple[Message, Document]]) -> None:
        earliest_by_partition: Dict[Tuple[str, int], int] = {}
        for msg, _ in batch:
            topic = msg.topic() if isinstance(msg.topic(), str) else self._settings.kafka.topic
            partition = msg.partition()
            offset = msg.offset()
            if isinstance(partition, int) and isinstance(offset, int):
                key = (topic, partition)
                if key not in earliest_by_partition or offset < earliest_by_partition[key]:
                    earliest_by_partition[key] = offset

        for (topic, partition), offset in earliest_by_partition.items():
            try:
                self._consumer.seek(TopicPartition(topic, partition, offset))
            except Exception:
                pass

    async def start(self) -> None:
        self._logger.info(
            "Starting Kafka consumer",
            {
                "topic": self._settings.kafka.topic,
                "batch_size": self._settings.kafka.batch_size,
            },
        )
        self._consumer.subscribe([self._settings.kafka.topic])
        self._running = True

        loop = asyncio.get_running_loop()
        try:
            while self._running:
                msgs = await loop.run_in_executor(
                    None,
                    lambda: self._consumer.consume(
                        num_messages=self._settings.kafka.batch_size,
                        timeout=self._settings.kafka.batch_timeout,
                    ),
                )
                if not msgs:
                    continue

                valid_batch: List[Tuple[Message, Document]] = []
                last_msg_to_commit: Optional[Message] = None

                for msg in msgs:
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        self._logger.error(
                            f"Kafka error: {msg.error()}",
                            {"code": msg.error().code()},
                        )
                        continue

                    try:
                        doc = self._parse_message(msg)
                        if doc:
                            valid_batch.append((msg, doc))
                            last_msg_to_commit = msg
                    except (json.JSONDecodeError, ValueError, TypeError) as e:
                        self._logger.error(
                            "Invalid message format (poison pill), skipping",
                            {
                                "error": str(e),
                                "partition": msg.partition(),
                                "offset": msg.offset(),
                            },
                        )
                        await self._handle_poison_pill(msg, e)
                        last_msg_to_commit = msg

                if not valid_batch:
                    if last_msg_to_commit is not None:
                        msg_ref = last_msg_to_commit
                        await loop.run_in_executor(
                            None,
                            lambda: self._consumer.commit(
                                message=msg_ref, asynchronous=False
                            ),
                        )
                    continue

                try:
                    await self._process_batch(valid_batch)
                    commit_target = last_msg_to_commit or valid_batch[-1][0]
                    await loop.run_in_executor(
                        None,
                        lambda: self._consumer.commit(
                            message=commit_target, asynchronous=False
                        ),
                    )
                except Exception as e:
                    self._logger.error(
                        "Transient error processing batch (offsets NOT committed)",
                        {
                            "error": str(e),
                            "batch_size": len(valid_batch),
                        },
                    )
                    await loop.run_in_executor(
                        None, lambda: self._rewind_batch(valid_batch)
                    )
                    await asyncio.sleep(1.0)
        finally:
            self.stop()

    def _parse_message(self, msg: Message) -> Optional[Document]:
        payload_bytes = msg.value()
        if not payload_bytes:
            self._logger.warning("Empty payload received")
            return None

        payload_str = payload_bytes.decode("utf-8")
        data = json.loads(payload_str)

        dto = DocumentMessageDTO(**data)

        created_at = datetime.now(timezone.utc)
        if dto.created_at:
            try:
                created_at = datetime.fromisoformat(
                    dto.created_at.replace("Z", "+00:00")
                )
            except ValueError:
                pass

        return Document(
            id=dto.id,
            content=dto.content,
            metadata=dto.metadata,
            status="INDEXED",
            created_at=created_at,
        )

    async def _process_batch(
        self, batch: List[Tuple[Message, Document]]
    ) -> None:
        documents = [doc for _, doc in batch]
        texts = [doc.content for doc in documents]

        vectors = await self._embedding_port.generate_batch(texts)

        doc_ids: List[str] = []
        metadatas: List[Dict[str, Any]] = []

        for doc in documents:
            doc_id = doc.id or ""
            doc_ids.append(doc_id)

            chroma_metadata: Dict[str, Any] = {
                "content": doc.content,
            }
            for k, v in doc.metadata.items():
                if isinstance(v, (str, int, float, bool)):
                    chroma_metadata[k] = v
                else:
                    chroma_metadata[k] = str(v)
            metadatas.append(chroma_metadata)

        await self._vector_store.upsert_batch(
            doc_ids=doc_ids,
            vectors=vectors,
            metadatas=metadatas,
        )

        for doc in documents:
            if doc.id:
                await self._repository.update_status(doc.id, "INDEXED")
            else:
                await self._repository.save(doc)

            self._logger.info(
                "Document processed successfully",
                extra={
                    "document": {
                        "id": doc.id,
                        "content_length": len(doc.content),
                        "metadata": doc.metadata,
                        "created_at": doc.created_at.isoformat(),
                    }
                },
            )

    def stop(self) -> None:
        self._running = False
        try:
            if self._dlq_producer is not None:
                self._dlq_producer.flush(timeout=5)
        except Exception:
            pass
        try:
            self._consumer.close()
        except Exception:
            pass
        self._logger.info("Kafka consumer stopped")


def _build_mongo_uri(settings: Settings) -> str:
    mongo = settings.mongo
    if mongo.username and mongo.password:
        return (
            f"mongodb://{mongo.username}:{mongo.password}"
            f"@{mongo.uri.split('://')[-1]}"
        )
    return mongo.uri


async def main() -> None:
    import chromadb
    from sentence_transformers import SentenceTransformer
    from motor.motor_asyncio import AsyncIOMotorClient

    settings = WorkerSettings()
    logger = ConsoleJsonLogger()

    conf: Dict[str, Any] = {
        "bootstrap.servers": settings.kafka.bootstrap_servers,
        "group.id": settings.kafka.group_id,
        "auto.offset.reset": settings.kafka.auto_offset_reset,
        "enable.auto.commit": False,
        "max.poll.interval.ms": settings.kafka.max_poll_interval_ms,
        "session.timeout.ms": settings.kafka.session_timeout_ms,
        "heartbeat.interval.ms": settings.kafka.heartbeat_interval_ms,
        "fetch.min.bytes": settings.kafka.fetch_min_bytes,
        "fetch.wait.max.ms": settings.kafka.fetch_wait_max_ms,
        "partition.assignment.strategy": settings.kafka.partition_assignment_strategy,
    }
    if settings.kafka.group_instance_id:
        conf["group.instance.id"] = settings.kafka.group_instance_id

    try:
        consumer_instance = Consumer(conf)
        dlq_producer = Producer(
            {
                "bootstrap.servers": settings.kafka.bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
            }
        )
    except Exception as e:
        logger.error(f"Failed to initialize confluent-kafka consumer: {e}")
        sys.exit(1)

    mongo_uri = _build_mongo_uri(settings)
    mongo_client = AsyncIOMotorClient(mongo_uri)
    db = mongo_client[settings.mongo.database]
    repository = MongoDocumentRepository(db)

    embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
    embedding_port = SentenceTransformerEmbeddingAdapter(embedding_model)

    chroma_client = chromadb.HttpClient(
        host=settings.chroma.host,
        port=settings.chroma.port
    )
    vector_store = ChromaVectorStoreAdapter(
        chroma_client,
        settings.chroma.collection_name
    )

    consumer = KafkaMessageConsumer(
        consumer=consumer_instance,
        settings=settings,
        logger=logger,
        repository=repository,
        embedding_port=embedding_port,
        vector_store=vector_store,
        dlq_producer=dlq_producer,
    )

    def handle_shutdown(signum: int, frame: Any) -> None:
        consumer.stop()

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    try:
        await consumer.start()
    finally:
        mongo_client.close()


if __name__ == "__main__":
    asyncio.run(main())

