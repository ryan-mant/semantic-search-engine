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
                name=self._collection_name,
                metadata={"hnsw:space": "cosine"}
            )
        return self._collection

    async def upsert(
        self, doc_id: str, vector: List[float], metadata: Dict[str, Any]
    ) -> None:
        await self.upsert_batch([doc_id], [vector], [metadata])

    async def upsert_batch(
        self, doc_ids: List[str], vectors: List[List[float]], metadatas: List[Dict[str, Any]]
    ) -> None:
        if not doc_ids:
            return
        loop = asyncio.get_running_loop()
        collection = self._get_collection()
        await loop.run_in_executor(
            None,
            lambda: collection.upsert(
                ids=doc_ids,
                embeddings=vectors,
                metadatas=metadatas
            )
        )

