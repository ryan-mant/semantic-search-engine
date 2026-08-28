import sys
from unittest.mock import Mock, AsyncMock, MagicMock, patch

sys.modules["sentence_transformers"] = MagicMock()
sys.modules["chromadb"] = MagicMock()

from datetime import datetime, timezone
from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from src.infrastructure.adapters.fastapi.routes import (
    router,
    get_settings,
    get_mongo_db,
    get_kafka_producer,
    get_document_repository,
    get_event_publisher,
    get_storage_port,
    get_ingest_use_case,
    get_get_document_use_case,
    get_embedding_port,
    get_vector_store_port,
    DocumentResponse,
    SearchResultResponse,
)
from src.application.use_cases.ingest_document import IngestDocumentUseCase
from src.application.use_cases.get_document import GetDocumentUseCase
from src.domain.entities.document import Document
from src.domain.ports.embedding import EmbeddingPort
from src.domain.ports.vector_store import VectorStorePort
from src.domain.exceptions import DocumentIngestionError, DocumentNotFoundError
from src.infrastructure.config.settings import Settings


def _create_test_app() -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    app.state.settings = Settings()
    app.state.db = MagicMock()
    app.state.kafka_producer = MagicMock()
    app.state.embedding_model = MagicMock()
    app.state.chroma_client = MagicMock()
    return app


def test_routes_dependency_getters() -> None:
    mock_settings = Settings()
    mock_request = Mock()
    mock_request.app.state.db = "mock_db"
    mock_request.app.state.kafka_producer = "mock_producer"
    mock_request.app.state.settings = mock_settings
    mock_request.app.state.embedding_model = "mock_model"
    mock_request.app.state.chroma_client = "mock_chroma"

    assert get_mongo_db(mock_request) == "mock_db"
    assert get_kafka_producer(mock_request) == "mock_producer"
    assert get_settings(mock_request) is mock_settings
    
    emb = get_embedding_port(mock_request)
    assert emb is not None
    
    vec = get_vector_store_port(mock_request)
    assert vec is not None


def test_routes_dependencies_setup() -> None:
    mock_db = MagicMock()
    repo = get_document_repository(mock_db)
    assert repo is not None

    mock_producer = Mock()
    settings = Settings()
    settings.aws.s3_bucket = "dummy-bucket"

    pub = get_event_publisher(mock_producer, settings)
    assert pub is not None

    with patch("boto3.client"):
        request_mock = Mock()
        request_mock.app.state.storage_adapter = Mock()
        storage = get_storage_port(request_mock)
        assert storage is not None


def test_ingest_document_route_success() -> None:
    app = _create_test_app()

    mock_use_case = AsyncMock(spec=IngestDocumentUseCase)
    created_at = datetime.now(timezone.utc)
    mock_use_case.execute.return_value = Document(
        id="doc-123",
        content="test content",
        metadata={"storage_url": "s3://url"},
        status="PENDING",
        created_at=created_at,
    )

    app.dependency_overrides[get_ingest_use_case] = (
        lambda: mock_use_case
    )

    client = TestClient(app)
    response = client.post(
        "/documents/ingest",
        json={"content": "test content", "metadata": {"key": "val"}},
    )

    assert response.status_code == 201
    data = response.json()
    assert data["id"] == "doc-123"
    assert data["content"] == "test content"
    assert data["metadata"] == {"storage_url": "s3://url"}
    assert data["status"] == "PENDING"
    mock_use_case.execute.assert_called_once_with(
        "test content", {"key": "val"}
    )


def test_ingest_document_route_ingestion_error() -> None:
    app = _create_test_app()

    mock_use_case = AsyncMock(spec=IngestDocumentUseCase)
    mock_use_case.execute.side_effect = DocumentIngestionError("failed")

    app.dependency_overrides[get_ingest_use_case] = (
        lambda: mock_use_case
    )

    client = TestClient(app)
    response = client.post(
        "/documents/ingest",
        json={"content": "test content", "metadata": {}}
    )

    assert response.status_code == 500
    assert response.json()["detail"] == "failed"


def test_ingest_document_route_value_error() -> None:
    app = _create_test_app()

    mock_use_case = AsyncMock(spec=IngestDocumentUseCase)
    mock_use_case.execute.side_effect = ValueError("invalid input")

    app.dependency_overrides[get_ingest_use_case] = (
        lambda: mock_use_case
    )

    client = TestClient(app)
    response = client.post(
        "/documents/ingest",
        json={"content": "test content", "metadata": {}}
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "invalid input"


def test_search_documents_route_success() -> None:
    app = _create_test_app()

    mock_embedding = AsyncMock(spec=EmbeddingPort)
    mock_embedding.generate.return_value = [0.1, 0.2, 0.3]

    mock_vector_store = AsyncMock(spec=VectorStorePort)
    mock_vector_store.search.return_value = [
        {
            "id": "doc-1",
            "metadata": {"content": "matching document content", "source": "web"},
            "score": 0.95,
            "distance": 0.05,
        }
    ]

    app.dependency_overrides[get_embedding_port] = lambda: mock_embedding
    app.dependency_overrides[get_vector_store_port] = lambda: mock_vector_store

    client = TestClient(app)
    response = client.get("/documents/search?q=test&limit=10")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["id"] == "doc-1"
    assert data[0]["content"] == "matching document content"
    assert data[0]["metadata"] == {"source": "web"}
    assert data[0]["score"] == 0.95
    assert data[0]["distance"] == 0.05
    mock_embedding.generate.assert_called_once_with("test")
    mock_vector_store.search.assert_called_once_with([0.1, 0.2, 0.3], top_k=10)


def test_search_documents_route_empty_query() -> None:
    app = _create_test_app()

    client = TestClient(app)
    response = client.get("/documents/search?q= ")
    assert response.status_code == 400
    assert "Search query cannot be empty" in response.json()["detail"]


def test_search_documents_route_exception() -> None:
    app = _create_test_app()

    mock_embedding = AsyncMock(spec=EmbeddingPort)
    mock_embedding.generate.side_effect = Exception("Model error")

    app.dependency_overrides[get_embedding_port] = lambda: mock_embedding

    client = TestClient(app)
    response = client.get("/documents/search?q=test")
    assert response.status_code == 500
    assert "Semantic search failed" in response.json()["detail"]


def test_get_document_route_success() -> None:
    app = _create_test_app()

    mock_use_case = AsyncMock(spec=GetDocumentUseCase)
    created_at = datetime.now(timezone.utc)
    mock_use_case.execute.return_value = Document(
        id="doc-999",
        content="some document",
        metadata={"category": "tech"},
        status="INDEXED",
        created_at=created_at,
    )

    app.dependency_overrides[get_get_document_use_case] = (
        lambda: mock_use_case
    )

    client = TestClient(app)
    response = client.get("/documents/doc-999")

    assert response.status_code == 200
    data = response.json()
    assert data["id"] == "doc-999"
    assert data["content"] == "some document"
    assert data["metadata"] == {"category": "tech"}
    assert data["status"] == "INDEXED"
    mock_use_case.execute.assert_called_once_with("doc-999")


def test_get_document_route_not_found() -> None:
    app = _create_test_app()

    mock_use_case = AsyncMock(spec=GetDocumentUseCase)
    mock_use_case.execute.side_effect = DocumentNotFoundError("Document not found")

    app.dependency_overrides[get_get_document_use_case] = (
        lambda: mock_use_case
    )

    client = TestClient(app)
    response = client.get("/documents/non-existent")

    assert response.status_code == 404
    assert "Document not found" in response.json()["detail"]

