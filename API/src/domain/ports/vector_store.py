from abc import ABC, abstractmethod


class VectorStorePort(ABC):
    """Port interface for vector database storage operations."""

    @abstractmethod
    async def upsert(
        self, doc_id: str, vector: list[float], metadata: dict
    ) -> None:
        """Upserts a document vector and its metadata into the vector store."""
        pass

    @abstractmethod
    async def search(
        self, vector: list[float], top_k: int
    ) -> list[dict]:
        """Searches for similar vectors in the vector store and returns metadata."""
        pass
