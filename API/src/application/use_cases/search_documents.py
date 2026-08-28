from typing import Any, Dict, List
from src.domain.ports.embedding import EmbeddingPort
from src.domain.ports.vector_store import VectorStorePort
from src.domain.exceptions import EmptyQueryError, SearchError, DomainError


class SearchDocumentsUseCase:

    def __init__(
        self,
        embedding_port: EmbeddingPort,
        vector_store_port: VectorStorePort,
    ) -> None:
        self._embedding_port = embedding_port
        self._vector_store_port = vector_store_port

    async def execute(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        if not query or not query.strip():
            raise EmptyQueryError("Search query cannot be empty")

        if top_k <= 0:
            raise ValueError("top_k must be greater than 0")

        try:
            query_vector = await self._embedding_port.generate(query.strip())
            results = await self._vector_store_port.search(query_vector, top_k=top_k)

            search_results = []
            for res in results:
                metadata = res.get("metadata", {}).copy()
                content = metadata.pop("content", "")
                search_results.append({
                    "id": res["id"],
                    "content": content,
                    "metadata": metadata,
                    "score": res["score"],
                    "distance": res.get("distance", round(1.0 - res["score"], 6)),
                })
            return search_results
        except DomainError:
            raise
        except Exception as e:
            raise SearchError(f"Search operation failed: {e}") from e

