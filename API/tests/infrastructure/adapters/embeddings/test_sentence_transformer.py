import sys
from unittest.mock import Mock, MagicMock

sys.modules["sentence_transformers"] = MagicMock()
sys.modules["chromadb"] = MagicMock()

import numpy as np
import pytest

from src.infrastructure.adapters.embeddings.sentence_transformer import SentenceTransformerEmbeddingAdapter


@pytest.mark.asyncio
async def test_sentence_transformer_caching() -> None:
    mock_model = Mock()
    mock_model.encode.return_value = np.array([0.1, 0.2, 0.3])

    adapter = SentenceTransformerEmbeddingAdapter(model=mock_model, cache_size=2)

    # First call - cache miss
    res1 = await adapter.generate("hello")
    assert res1 == [0.1, 0.2, 0.3]
    assert mock_model.encode.call_count == 1

    # Second call with same text - cache hit (encode not called again)
    res2 = await adapter.generate("hello")
    assert res2 == [0.1, 0.2, 0.3]
    assert mock_model.encode.call_count == 1

    # New call - cache miss
    mock_model.encode.return_value = np.array([0.4, 0.5, 0.6])
    res3 = await adapter.generate("world")
    assert res3 == [0.4, 0.5, 0.6]
    assert mock_model.encode.call_count == 2


@pytest.mark.asyncio
async def test_sentence_transformer_generate_batch() -> None:
    mock_model = Mock()
    mock_model.encode.return_value = [
        np.array([0.1, 0.2]),
        np.array([0.3, 0.4])
    ]

    adapter = SentenceTransformerEmbeddingAdapter(model=mock_model)
    results = await adapter.generate_batch(["first", "second"])

    assert len(results) == 2
    assert results[0] == [0.1, 0.2]
    assert results[1] == [0.3, 0.4]


@pytest.mark.asyncio
async def test_sentence_transformer_generate_batch_empty() -> None:
    mock_model = Mock()
    adapter = SentenceTransformerEmbeddingAdapter(model=mock_model)
    results = await adapter.generate_batch([])
    assert results == []
    mock_model.encode.assert_not_called()
