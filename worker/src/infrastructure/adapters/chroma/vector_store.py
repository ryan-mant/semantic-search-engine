import asyncio
from typing import Any, Dict, List
import chromadb

from src.domain.ports.vector_store import VectorStorePort


class ChromaVectorStoreAdapter(VectorStorePort):

    def __init__(
        self, client: chromadb.ClientAPI, collection_name: str = "documents"
    ) -> None:
        self._client = client
        self._collection_name = collection_name
        self._collection = None

    def _get_collection(self) -> Any:
        if self._collection is None:
            self._collection = self._client.get_or_create_collection(
                name=self._collection_name
            )
        return self._collection

    async def upsert(
        self, doc_id: str, vector: List[float], metadata: Dict[str, Any]
    ) -> None:
        loop = asyncio.get_running_loop()
        collection = self._get_collection()
        await loop.run_in_executor(
            None,
            lambda: collection.upsert(
                ids=[doc_id],
                embeddings=[vector],
                metadatas=[metadata]
            )
        )
