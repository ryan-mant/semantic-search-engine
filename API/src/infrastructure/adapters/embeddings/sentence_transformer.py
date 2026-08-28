import asyncio
from collections import OrderedDict
from typing import Any, List, Optional

from src.domain.ports.embedding import EmbeddingPort


class SentenceTransformerEmbeddingAdapter(EmbeddingPort):

    def __init__(self, model: Any, cache_size: int = 1000) -> None:
        self._model = model
        self._cache_size = cache_size
        self._cache: OrderedDict[str, list[float]] = OrderedDict()

    def _get_from_cache(self, text: str) -> Optional[list[float]]:
        if text in self._cache:
            self._cache.move_to_end(text)
            return self._cache[text]
        return None

    def _add_to_cache(self, text: str, embedding: list[float]) -> None:
        self._cache[text] = embedding
        if len(self._cache) > self._cache_size:
            self._cache.popitem(last=False)

    async def generate(self, text: str) -> list[float]:
        cached = self._get_from_cache(text)
        if cached is not None:
            return cached

        loop = asyncio.get_running_loop()
        embedding = await loop.run_in_executor(
            None, self._model.encode, text
        )
        result = embedding.tolist() if hasattr(embedding, "tolist") else list(embedding)
        self._add_to_cache(text, result)
        return result

    async def generate_batch(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        loop = asyncio.get_running_loop()
        embeddings = await loop.run_in_executor(
            None, lambda: self._model.encode(texts, batch_size=len(texts), show_progress_bar=False)
        )
        results = []
        for text, emb in zip(texts, embeddings):
            emb_list = emb.tolist() if hasattr(emb, "tolist") else list(emb)
            self._add_to_cache(text, emb_list)
            results.append(emb_list)
        return results

