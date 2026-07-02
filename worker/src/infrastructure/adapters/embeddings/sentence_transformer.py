import asyncio
from sentence_transformers import SentenceTransformer

from src.domain.ports.embedding import EmbeddingPort


class SentenceTransformerEmbeddingAdapter(EmbeddingPort):

    def __init__(self, model: SentenceTransformer) -> None:
        self._model = model

    async def generate(self, text: str) -> list[float]:
        loop = asyncio.get_running_loop()
        embedding = await loop.run_in_executor(
            None, self._model.encode, text
        )
        return embedding.tolist()
