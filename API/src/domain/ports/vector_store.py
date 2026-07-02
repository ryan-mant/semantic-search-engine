from abc import ABC, abstractmethod


class VectorStorePort(ABC):

    @abstractmethod
    async def search(
        self, vector: list[float], top_k: int
    ) -> list[dict]:
        pass
