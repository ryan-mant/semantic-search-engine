from contextlib import asynccontextmanager
from typing import AsyncIterator

from confluent_kafka import Producer
from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient

from src.infrastructure.adapters.fastapi.routes import router
from src.infrastructure.config.settings import Settings


settings = Settings()


def _build_mongo_uri(settings: Settings) -> str:
    mongo = settings.mongo
    if mongo.username and mongo.password:
        return (
            f"mongodb://{mongo.username}:{mongo.password}"
            f"@{mongo.uri.split('://')[-1]}"
        )
    return mongo.uri


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    import chromadb
    from sentence_transformers import SentenceTransformer

    mongo_uri = _build_mongo_uri(settings)
    mongo_client = AsyncIOMotorClient(mongo_uri)
    app.state.db = mongo_client[settings.mongo.database]
    app.state.kafka_producer = Producer(
        {"bootstrap.servers": settings.kafka.bootstrap_servers}
    )
    app.state.settings = settings

    app.state.chroma_client = chromadb.HttpClient(
        host=settings.chroma.host,
        port=settings.chroma.port
    )
    app.state.embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
    from src.infrastructure.adapters.aws.s3 import S3StorageAdapter
    app.state.storage_adapter = S3StorageAdapter(settings)

    yield

    app.state.kafka_producer.flush(timeout=5)
    mongo_client.close()


app = FastAPI(
    title="Document Ingestion & Semantic Search API",
    description="API for document ingestion and semantic search.",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(router)
