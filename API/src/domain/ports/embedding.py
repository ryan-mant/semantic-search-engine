from abc import ABC, abstractmethod


class EmbeddingPort(ABC):
    """Port interface for text embedding generation."""

    @abstractmethod
    async def generate(self, text: str) -> list[float]:
        """Generates a dense vector embedding for the given text."""
        pass
