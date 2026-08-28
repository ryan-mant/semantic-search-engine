import asyncio
from typing import Any, List

from src.domain.ports.embedding import EmbeddingPort


class SentenceTransformerEmbeddingAdapter(EmbeddingPort):

    def __init__(self, model: Any) -> None:
        self._model = model

    async def generate(self, text: str) -> list[float]:
        results = await self.generate_batch([text])
        return results[0]

    async def generate_batch(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        loop = asyncio.get_running_loop()
        embeddings = await loop.run_in_executor(
            None, lambda: self._model.encode(texts, batch_size=len(texts), show_progress_bar=False)
        )
        return [
            emb.tolist() if hasattr(emb, "tolist") else list(emb)
            for emb in embeddings
        ]

