from abc import ABC, abstractmethod


class VectorStorePort(ABC):

    @abstractmethod
    async def upsert(
        self, doc_id: str, vector: list[float], metadata: dict
    ) -> None:
        pass

    @abstractmethod
    async def upsert_batch(
        self, doc_ids: list[str], vectors: list[list[float]], metadatas: list[dict]
    ) -> None:
        pass

