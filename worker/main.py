import asyncio
import json
import signal
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from confluent_kafka import Consumer, KafkaError, Message
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
    ) -> None:
        self._consumer = consumer
        self._settings = settings
        self._logger = logger
        self._repository = repository
        self._embedding_port = embedding_port
        self._vector_store = vector_store
        self._running = False

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
                    except (json.JSONDecodeError, ValueError, TypeError) as e:
                        self._logger.error(
                            "Invalid message format (poison pill), skipping",
                            {
                                "error": str(e),
                                "partition": msg.partition(),
                                "offset": msg.offset(),
                            },
                        )
                        await loop.run_in_executor(
                            None,
                            lambda: self._consumer.commit(message=msg, asynchronous=False)
                        )

                if not valid_batch:
                    continue

                try:
                    await self._process_batch(valid_batch)
                    last_msg = valid_batch[-1][0]
                    await loop.run_in_executor(
                        None,
                        lambda: self._consumer.commit(message=last_msg, asynchronous=False)
                    )
                except Exception as e:
                    self._logger.error(
                        "Transient error processing batch (offsets NOT committed)",
                        {
                            "error": str(e),
                            "batch_size": len(valid_batch),
                        },
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

    conf = {
        "bootstrap.servers": settings.kafka.bootstrap_servers,
        "group.id": settings.kafka.group_id,
        "auto.offset.reset": settings.kafka.auto_offset_reset,
        "enable.auto.commit": False,
    }

    try:
        consumer_instance = Consumer(conf)
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

