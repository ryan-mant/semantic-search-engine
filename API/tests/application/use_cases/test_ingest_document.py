from unittest.mock import Mock, AsyncMock, patch, ANY
import uuid
import pytest

from src.application.use_cases.ingest_document import IngestDocumentUseCase
from src.domain.ports.document_repository import DocumentRepository
from src.domain.ports.event_publisher import EventPublisher
from src.domain.ports.storage import StoragePort
from src.domain.exceptions import DocumentIngestionError, DatabaseConnectionError


@pytest.mark.asyncio
async def test_ingest_document_success() -> None:
    content = "Test content"
    metadata = {"source": "test"}
    fixed_uuid_str = "a5871f00-bd63-4b73-bafc-4e77975c7438"
    fixed_uuid = uuid.UUID(fixed_uuid_str)

    mock_repo = AsyncMock(spec=DocumentRepository)
    mock_storage = Mock(spec=StoragePort)
    mock_storage.upload_stream.return_value = f"s3://test-bucket/raw/{fixed_uuid_str}.txt"
    mock_publisher = AsyncMock(spec=EventPublisher)

    use_case = IngestDocumentUseCase(
        document_repository=mock_repo,
        event_publisher=mock_publisher,
        storage_port=mock_storage,
    )

    with patch("uuid.uuid4", return_value=fixed_uuid):
        result = await use_case.execute(content, metadata)

    assert result.id == fixed_uuid_str
    assert result.content == content
    assert result.metadata["storage_url"] == f"s3://test-bucket/raw/{fixed_uuid_str}.txt"
    
    mock_repo.save.assert_called_once_with(result)
    mock_storage.upload_stream.assert_called_once_with(ANY, f"raw/{fixed_uuid_str}.txt")
    mock_publisher.publish_document_created.assert_called_once_with(result)


@pytest.mark.asyncio
async def test_ingest_document_failure_repo_error() -> None:
    content = "Test content"
    metadata = {"source": "test"}
    fixed_uuid_str = "a5871f00-bd63-4b73-bafc-4e77975c7438"
    fixed_uuid = uuid.UUID(fixed_uuid_str)

    mock_repo = AsyncMock(spec=DocumentRepository)
    mock_repo.save.side_effect = DatabaseConnectionError("Connection timeout")

    mock_storage = Mock(spec=StoragePort)
    mock_storage.upload_stream.return_value = f"s3://test-bucket/raw/{fixed_uuid_str}.txt"
    
    mock_publisher = AsyncMock(spec=EventPublisher)

    use_case = IngestDocumentUseCase(
        document_repository=mock_repo,
        event_publisher=mock_publisher,
        storage_port=mock_storage,
    )

    with patch("uuid.uuid4", return_value=fixed_uuid):
        with pytest.raises(DocumentIngestionError) as exc_info:
            await use_case.execute(content, metadata)

    assert "Document ingestion failed" in str(exc_info.value)
    mock_storage.upload_stream.assert_called_once_with(ANY, f"raw/{fixed_uuid_str}.txt")
    mock_publisher.publish_document_created.assert_not_called()


@pytest.mark.asyncio
async def test_ingest_document_failure_generic_error() -> None:
    content = "Test content"
    metadata = {"source": "test"}
    fixed_uuid_str = "a5871f00-bd63-4b73-bafc-4e77975c7438"
    fixed_uuid = uuid.UUID(fixed_uuid_str)

    mock_repo = AsyncMock(spec=DocumentRepository)
    mock_repo.save.side_effect = ValueError("Unexpected value error")

    mock_storage = Mock(spec=StoragePort)
    mock_storage.upload_stream.return_value = f"s3://test-bucket/raw/{fixed_uuid_str}.txt"
    
    mock_publisher = AsyncMock(spec=EventPublisher)

    use_case = IngestDocumentUseCase(
        document_repository=mock_repo,
        event_publisher=mock_publisher,
        storage_port=mock_storage,
    )

    with patch("uuid.uuid4", return_value=fixed_uuid):
        with pytest.raises(DocumentIngestionError) as exc_info:
            await use_case.execute(content, metadata)

    assert "An unexpected error occurred during document ingestion" in str(exc_info.value)
    mock_storage.upload_stream.assert_called_once_with(ANY, f"raw/{fixed_uuid_str}.txt")
    mock_publisher.publish_document_created.assert_not_called()