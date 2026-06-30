from datetime import datetime
from typing import Any, Dict
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field
from motor.motor_asyncio import AsyncIOMotorDatabase
from confluent_kafka import Producer

from src.application.use_cases.ingest_document import IngestDocumentUseCase
from src.domain.ports.document_repository import DocumentRepository
from src.domain.ports.event_publisher import EventPublisher
from src.domain.ports.storage import StoragePort
from src.infrastructure.adapters.mongodb.repository import MongoDocumentRepository
from src.infrastructure.adapters.kafka.producer import KafkaEventPublisher
from src.infrastructure.adapters.aws.s3 import S3StorageAdapter
from src.infrastructure.config.settings import Settings
from src.domain.exceptions import DocumentIngestionError


router = APIRouter()


class DocumentRequest(BaseModel):
    content: str = Field(..., min_length=1)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DocumentResponse(BaseModel):
    id: str
    content: str
    metadata: Dict[str, Any]
    created_at: datetime


def get_settings() -> Settings:
    return Settings()


def get_mongo_db(request: Request) -> AsyncIOMotorDatabase:
    return request.app.state.db


def get_kafka_producer(request: Request) -> Producer:
    return request.app.state.kafka_producer


def get_document_repository(
    db: AsyncIOMotorDatabase = Depends(get_mongo_db)
) -> DocumentRepository:
    return MongoDocumentRepository(db)


def get_event_publisher(
    producer: Producer = Depends(get_kafka_producer),
    settings: Settings = Depends(get_settings)
) -> EventPublisher:
    return KafkaEventPublisher(producer, settings.kafka.topic)


def get_storage_port(
    settings: Settings = Depends(get_settings)
) -> StoragePort:
    return S3StorageAdapter(settings)


def get_ingest_use_case(
    repository: DocumentRepository = Depends(get_document_repository),
    publisher: EventPublisher = Depends(get_event_publisher),
    storage: StoragePort = Depends(get_storage_port)
) -> IngestDocumentUseCase:
    return IngestDocumentUseCase(repository, publisher, storage)


@router.post(
    "/documents/ingest",
    response_model=DocumentResponse,
    status_code=status.HTTP_201_CREATED
)
async def ingest_document(
    request: DocumentRequest,
    use_case: IngestDocumentUseCase = Depends(get_ingest_use_case)
) -> DocumentResponse:
    try:
        document = await use_case.execute(request.content, request.metadata)
        return DocumentResponse(
            id=document.id,
            content=document.content,
            metadata=document.metadata,
            created_at=document.created_at
        )
    except DocumentIngestionError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
