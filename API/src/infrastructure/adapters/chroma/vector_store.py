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

    async def search(
        self, vector: List[float], top_k: int
    ) -> List[Dict[str, Any]]:
        loop = asyncio.get_running_loop()
        collection = self._get_collection()
        results = await loop.run_in_executor(
            None,
            lambda: collection.query(
                query_embeddings=[vector],
                n_results=top_k
            )
        )
        formatted_results = []
        if results and "ids" in results and results["ids"]:
            ids = results["ids"][0]
            metadatas = results.get("metadatas", [[]])[0] or [{}] * len(ids)
            distances = results.get("distances", [[]])[0] or [0.0] * len(ids)
            
            for idx, doc_id in enumerate(ids):
                dist = round(float(distances[idx]), 6)
                score = round(max(0.0, min(1.0, 1.0 - dist)), 6)
                formatted_results.append({
                    "id": doc_id,
                    "metadata": metadatas[idx],
                    "distance": dist,
                    "score": score
                })
        return formatted_results

