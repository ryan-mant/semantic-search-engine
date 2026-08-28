import pytest
from unittest.mock import AsyncMock

from src.application.use_cases.get_document import GetDocumentUseCase
from src.domain.entities.document import Document
from src.domain.ports.document_repository import DocumentRepository
from src.domain.exceptions import DocumentNotFoundError


@pytest.mark.asyncio
async def test_get_document_success() -> None:
    repo_mock = AsyncMock(spec=DocumentRepository)
    doc = Document(
        id="doc-123",
        content="Test content",
        metadata={"category": "news"},
        status="INDEXED",
    )
    repo_mock.get_by_id.return_value = doc

    use_case = GetDocumentUseCase(repo_mock)
    result = await use_case.execute("doc-123")

    assert result.id == "doc-123"
    assert result.content == "Test content"
    assert result.status == "INDEXED"
    repo_mock.get_by_id.assert_called_once_with("doc-123")


@pytest.mark.asyncio
async def test_get_document_not_found() -> None:
    repo_mock = AsyncMock(spec=DocumentRepository)
    repo_mock.get_by_id.return_value = None

    use_case = GetDocumentUseCase(repo_mock)

    with pytest.raises(DocumentNotFoundError):
        await use_case.execute("doc-999")


@pytest.mark.asyncio
async def test_get_document_empty_id() -> None:
    repo_mock = AsyncMock(spec=DocumentRepository)
    use_case = GetDocumentUseCase(repo_mock)

    with pytest.raises(ValueError):
        await use_case.execute("")
