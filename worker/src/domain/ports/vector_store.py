from abc import ABC, abstractmethod


class VectorStorePort(ABC):

    @abstractmethod
    async def upsert(
        self, doc_id: str, vector: list[float], metadata: dict
    ) -> None:
        pass
