from unittest.mock import Mock, AsyncMock, MagicMock
from fastapi import FastAPI, Depends, Request
from fastapi.testclient import TestClient
import pytest
from datetime import datetime

from src.infrastructure.adapters.fastapi.routes import (
    router,
    get_settings,
    get_mongo_db,
    get_kafka_producer,
    get_document_repository,
    get_event_publisher,
    get_storage_port,
    get_ingest_use_case,
    DocumentResponse
)
from src.application.use_cases.ingest_document import IngestDocumentUseCase
from src.domain.entities.document import Document
from src.domain.exceptions import DocumentIngestionError
from src.infrastructure.config.settings import Settings


def test_routes_dependency_getters() -> None:
    settings = get_settings()
    assert isinstance(settings, Settings)
    
    mock_request = Mock()
    mock_request.app.state.db = "mock_db"
    mock_request.app.state.kafka_producer = "mock_producer"
    
    assert get_mongo_db(mock_request) == "mock_db"
    assert get_kafka_producer(mock_request) == "mock_producer"


def test_routes_dependencies_setup() -> None:
    mock_db = MagicMock()
    repo = get_document_repository(mock_db)

    assert repo is not None
    
    mock_producer = Mock()
    settings = Settings()
    settings.aws.s3_bucket = "dummy-bucket"
    
    pub = get_event_publisher(mock_producer, settings)
    assert pub is not None
    
    storage = get_storage_port(settings)
    assert storage is not None


def test_ingest_document_route_success() -> None:
    app = FastAPI()
    app.include_router(router)
    
    mock_use_case = AsyncMock(spec=IngestDocumentUseCase)
    created_at = datetime.utcnow()
    mock_use_case.execute.return_value = Document(
        id="doc-123",
        content="test content",
        metadata={"storage_url": "s3://url"},
        created_at=created_at
    )
    
    app.dependency_overrides[get_ingest_use_case] = lambda: mock_use_case
    
    client = TestClient(app)
    response = client.post(
        "/documents/ingest",
        json={"content": "test content", "metadata": {"key": "val"}}
    )
    
    assert response.status_code == 201
    data = response.json()
    assert data["id"] == "doc-123"
    assert data["content"] == "test content"
    assert data["metadata"] == {"storage_url": "s3://url"}
    assert data["created_at"] == created_at.isoformat()
    mock_use_case.execute.assert_called_once_with("test content", {"key": "val"})


def test_ingest_document_route_ingestion_error() -> None:
    app = FastAPI()
    app.include_router(router)
    
    mock_use_case = AsyncMock(spec=IngestDocumentUseCase)
    mock_use_case.execute.side_effect = DocumentIngestionError("failed")
    
    app.dependency_overrides[get_ingest_use_case] = lambda: mock_use_case
    
    client = TestClient(app)
    response = client.post(
        "/documents/ingest",
        json={"content": "test content", "metadata": {}}
    )
    
    assert response.status_code == 500
    assert response.json()["detail"] == "failed"


def test_ingest_document_route_value_error() -> None:
    app = FastAPI()
    app.include_router(router)
    
    mock_use_case = AsyncMock(spec=IngestDocumentUseCase)
    mock_use_case.execute.side_effect = ValueError("invalid input")
    
    app.dependency_overrides[get_ingest_use_case] = lambda: mock_use_case
    
    client = TestClient(app)
    response = client.post(
        "/documents/ingest",
        json={"content": "test content", "metadata": {}}
    )
    
    assert response.status_code == 400
    assert response.json()["detail"] == "invalid input"
