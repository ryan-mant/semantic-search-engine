from abc import ABC, abstractmethod


class EmbeddingPort(ABC):

    @abstractmethod
    async def generate(self, text: str) -> list[float]:
        pass
