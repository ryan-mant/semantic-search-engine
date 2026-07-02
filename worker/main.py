import asyncio
import json
import os
import signal
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "API")))

os.environ.setdefault("AWS_ACCESS_KEY_ID", "dummy_key")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "dummy_secret")
os.environ.setdefault("AWS_S3_BUCKET", "dummy-bucket")
os.environ.setdefault("AWS_CLOUDWATCH_LOG_GROUP", "dummy_log_group")
os.environ.setdefault("AWS_CLOUDWATCH_LOG_STREAM", "dummy_log_stream")

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
            {"topic": self._settings.kafka.topic},
        )
        self._consumer.subscribe([self._settings.kafka.topic])
        self._running = True

        loop = asyncio.get_running_loop()
        try:
            while self._running:
                msg = await loop.run_in_executor(None, self._consumer.poll, 1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    self._logger.error(
                        f"Kafka error: {msg.error()}",
                        {"code": msg.error().code()},
                    )
                    continue

                try:
                    await self._process_message(msg)
                except Exception as e:
                    self._logger.error(
                        "Error processing message",
                        {
                            "error": str(e),
                            "partition": msg.partition(),
                            "offset": msg.offset(),
                        },
                    )
        finally:
            self.stop()

    async def _process_message(self, msg: Message) -> None:
        payload_bytes = msg.value()
        if not payload_bytes:
            self._logger.warning("Empty payload received")
            return

        payload_str = payload_bytes.decode("utf-8")
        data = json.loads(payload_str)

        dto = DocumentMessageDTO(**data)

        created_at = datetime.now()
        if dto.created_at:
            try:
                created_at = datetime.fromisoformat(
                    dto.created_at.replace("Z", "+00:00")
                )
            except ValueError:
                pass

        document = Document(
            id=dto.id,
            content=dto.content,
            metadata=dto.metadata,
            created_at=created_at,
        )

        await self._repository.save(document)

        vector = await self._embedding_port.generate(document.content)

        chroma_metadata = {
            "content": document.content,
        }
        for k, v in document.metadata.items():
            if isinstance(v, (str, int, float, bool)):
                chroma_metadata[k] = v
            else:
                chroma_metadata[k] = str(v)

        await self._vector_store.upsert(
            doc_id=document.id,
            vector=vector,
            metadata=chroma_metadata,
        )

        self._logger.info(
            "Document processed successfully",
            extra={
                "document": {
                    "id": document.id,
                    "content_length": len(document.content),
                    "metadata": document.metadata,
                    "created_at": document.created_at.isoformat(),
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
        "enable.auto.commit": True,
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
