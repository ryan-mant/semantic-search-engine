import pytest
from unittest.mock import AsyncMock

from src.application.use_cases.search_documents import SearchDocumentsUseCase
from src.domain.ports.embedding import EmbeddingPort
from src.domain.ports.vector_store import VectorStorePort
from src.domain.exceptions import EmptyQueryError, SearchError


@pytest.mark.asyncio
async def test_search_documents_success() -> None:
    embedding_mock = AsyncMock(spec=EmbeddingPort)
    embedding_mock.generate.return_value = [0.1, 0.2, 0.3]

    vector_store_mock = AsyncMock(spec=VectorStorePort)
    vector_store_mock.search.return_value = [
        {
            "id": "doc-1",
            "metadata": {"content": "Doc 1 content", "category": "books"},
            "distance": 0.12,
            "score": 0.88,
        }
    ]

    use_case = SearchDocumentsUseCase(
        embedding_port=embedding_mock,
        vector_store_port=vector_store_mock,
    )

    results = await use_case.execute("machine learning", top_k=3)

    assert len(results) == 1
    assert results[0]["id"] == "doc-1"
    assert results[0]["content"] == "Doc 1 content"
    assert results[0]["metadata"] == {"category": "books"}
    assert results[0]["score"] == 0.88
    assert results[0]["distance"] == 0.12

    embedding_mock.generate.assert_called_once_with("machine learning")
    vector_store_mock.search.assert_called_once_with([0.1, 0.2, 0.3], top_k=3)


@pytest.mark.asyncio
async def test_search_documents_empty_query() -> None:
    embedding_mock = AsyncMock(spec=EmbeddingPort)
    vector_store_mock = AsyncMock(spec=VectorStorePort)

    use_case = SearchDocumentsUseCase(
        embedding_port=embedding_mock,
        vector_store_port=vector_store_mock,
    )

    with pytest.raises(EmptyQueryError):
        await use_case.execute("   ")


@pytest.mark.asyncio
async def test_search_documents_invalid_top_k() -> None:
    embedding_mock = AsyncMock(spec=EmbeddingPort)
    vector_store_mock = AsyncMock(spec=VectorStorePort)

    use_case = SearchDocumentsUseCase(
        embedding_port=embedding_mock,
        vector_store_port=vector_store_mock,
    )

    with pytest.raises(ValueError):
        await use_case.execute("query", top_k=0)


@pytest.mark.asyncio
async def test_search_documents_error_wrapping() -> None:
    embedding_mock = AsyncMock(spec=EmbeddingPort)
    embedding_mock.generate.side_effect = RuntimeError("Chroma connection refused")

    vector_store_mock = AsyncMock(spec=VectorStorePort)

    use_case = SearchDocumentsUseCase(
        embedding_port=embedding_mock,
        vector_store_port=vector_store_mock,
    )

    with pytest.raises(SearchError) as exc_info:
        await use_case.execute("query")
    assert "Search operation failed" in str(exc_info.value)
