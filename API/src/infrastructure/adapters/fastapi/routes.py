from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field
from motor.motor_asyncio import AsyncIOMotorDatabase
from confluent_kafka import Producer

from src.application.use_cases.ingest_document import IngestDocumentUseCase
from src.application.use_cases.search_documents import SearchDocumentsUseCase
from src.application.use_cases.get_document import GetDocumentUseCase
from src.domain.ports.document_repository import DocumentRepository
from src.domain.ports.event_publisher import EventPublisher
from src.domain.ports.storage import StoragePort
from src.domain.ports.embedding import EmbeddingPort
from src.domain.ports.vector_store import VectorStorePort
from src.infrastructure.adapters.mongodb.repository import MongoDocumentRepository
from src.infrastructure.adapters.kafka.producer import KafkaEventPublisher
from src.infrastructure.adapters.aws.s3 import S3StorageAdapter
from src.infrastructure.adapters.embeddings.sentence_transformer import SentenceTransformerEmbeddingAdapter
from src.infrastructure.adapters.chroma.vector_store import ChromaVectorStoreAdapter
from src.infrastructure.config.settings import Settings
from src.domain.exceptions import (
    DocumentIngestionError,
    DocumentNotFoundError,
    EmptyQueryError,
    SearchError,
)


router = APIRouter()


class DocumentRequest(BaseModel):
    content: str = Field(..., min_length=1)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DocumentResponse(BaseModel):
    id: str
    content: str
    metadata: Dict[str, Any]
    status: str = "PENDING"
    created_at: datetime


class SearchResultResponse(BaseModel):
    id: str
    content: str
    metadata: Dict[str, Any]
    score: float
    distance: float


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_mongo_db(request: Request) -> AsyncIOMotorDatabase:
    return request.app.state.db


def get_kafka_producer(request: Request) -> Producer:
    return request.app.state.kafka_producer


def get_document_repository(
    db: AsyncIOMotorDatabase = Depends(get_mongo_db),
) -> DocumentRepository:
    return MongoDocumentRepository(db)


def get_event_publisher(
    producer: Producer = Depends(get_kafka_producer),
    settings: Settings = Depends(get_settings),
) -> EventPublisher:
    return KafkaEventPublisher(producer, settings.kafka.topic)


def get_storage_port(request: Request) -> StoragePort:
    return request.app.state.storage_adapter


def get_ingest_use_case(
    repository: DocumentRepository = Depends(get_document_repository),
    publisher: EventPublisher = Depends(get_event_publisher),
    storage: StoragePort = Depends(get_storage_port),
) -> IngestDocumentUseCase:
    return IngestDocumentUseCase(repository, publisher, storage)


def get_get_document_use_case(
    repository: DocumentRepository = Depends(get_document_repository),
) -> GetDocumentUseCase:
    return GetDocumentUseCase(repository)


def get_embedding_port(request: Request) -> EmbeddingPort:
    return SentenceTransformerEmbeddingAdapter(request.app.state.embedding_model)


def get_vector_store_port(request: Request) -> VectorStorePort:
    settings = get_settings(request)
    return ChromaVectorStoreAdapter(
        request.app.state.chroma_client,
        settings.chroma.collection_name
    )


def get_search_use_case(
    embedding_port: EmbeddingPort = Depends(get_embedding_port),
    vector_store: VectorStorePort = Depends(get_vector_store_port),
) -> SearchDocumentsUseCase:
    return SearchDocumentsUseCase(embedding_port, vector_store)


@router.get("/", tags=["Health Check"])
async def health_check() -> Dict[str, str]:
    return {"status": "ok"}


@router.post(
    "/documents/ingest",
    response_model=DocumentResponse,
    status_code=status.HTTP_201_CREATED,
)
async def ingest_document(
    request: DocumentRequest,
    use_case: IngestDocumentUseCase = Depends(get_ingest_use_case),
) -> DocumentResponse:
    try:
        document = await use_case.execute(
            request.content, request.metadata
        )
        return DocumentResponse(
            id=document.id,
            content=document.content,
            metadata=document.metadata,
            status=document.status,
            created_at=document.created_at,
        )
    except DocumentIngestionError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.get(
    "/documents/search",
    response_model=List[SearchResultResponse],
    status_code=status.HTTP_200_OK,
)
async def search_documents(
    q: str,
    limit: int = Query(5, ge=1, le=100, description="Maximum number of search results to return"),
    use_case: SearchDocumentsUseCase = Depends(get_search_use_case),
) -> List[SearchResultResponse]:
    try:
        results = await use_case.execute(q, top_k=limit)
        return [
            SearchResultResponse(
                id=res["id"],
                content=res["content"],
                metadata=res["metadata"],
                score=res["score"],
                distance=res.get("distance", round(1.0 - res["score"], 6)),
            )
            for res in results
        ]
    except (ValueError, EmptyQueryError) as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except (SearchError, Exception) as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Semantic search failed: {e}",
        )


@router.get(
    "/documents/{document_id}",
    response_model=DocumentResponse,
    status_code=status.HTTP_200_OK,
)
async def get_document(
    document_id: str,
    use_case: GetDocumentUseCase = Depends(get_get_document_use_case),
) -> DocumentResponse:
    try:
        document = await use_case.execute(document_id)
        return DocumentResponse(
            id=document.id,
            content=document.content,
            metadata=document.metadata,
            status=document.status,
            created_at=document.created_at,
        )
    except DocumentNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )

