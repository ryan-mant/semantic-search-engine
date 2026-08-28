from datetime import datetime, timezone
from typing import Any, Dict, Optional

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import ConnectionFailure, PyMongoError, ServerSelectionTimeoutError

from src.domain.entities.document import Document
from src.domain.exceptions import DatabaseConnectionError
from src.domain.ports.document_repository import DocumentRepository


class MongoDocumentRepository(DocumentRepository):

    def __init__(self, db: AsyncIOMotorDatabase, collection_name: str = "documents") -> None:
        self._db = db
        self._collection = db[collection_name]

    async def save(self, document: Document) -> None:
        try:
            doc_id = document.id
            query = {"_id": ObjectId(doc_id) if (doc_id and ObjectId.is_valid(doc_id)) else doc_id} if doc_id else {}
            data = {
                "content": document.content,
                "metadata": document.metadata,
                "status": document.status,
                "created_at": document.created_at,
            }
            if doc_id:
                await self._collection.update_one(query, {"$set": data}, upsert=True)
            else:
                await self._collection.insert_one(data)
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            raise DatabaseConnectionError(f"Database connection or timeout failure: {e}") from e
        except PyMongoError as e:
            raise DatabaseConnectionError(f"MongoDB write operation failed: {e}") from e

    async def get_by_id(self, document_id: str) -> Optional[Document]:
        try:
            query = {"_id": ObjectId(document_id) if ObjectId.is_valid(document_id) else document_id}
            doc = await self._collection.find_one(query)
            if not doc:
                return None
            return Document(
                id=str(doc["_id"]),
                content=doc.get("content", ""),
                metadata=doc.get("metadata", {}),
                status=doc.get("status", "INDEXED"),
                created_at=doc.get("created_at") or datetime.now(timezone.utc),
            )
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            raise DatabaseConnectionError(f"Database connection or timeout failure: {e}") from e
        except PyMongoError as e:
            raise DatabaseConnectionError(f"MongoDB read operation failed: {e}") from e

    async def update_status(
        self, document_id: str, status: str, error: Optional[str] = None
    ) -> None:
        try:
            query = {"_id": ObjectId(document_id) if ObjectId.is_valid(document_id) else document_id}
            update_data: Dict[str, Any] = {"status": status}
            if error:
                update_data["error"] = error
            await self._collection.update_one(query, {"$set": update_data})
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            raise DatabaseConnectionError(f"Database connection or timeout failure: {e}") from e
        except PyMongoError as e:
            raise DatabaseConnectionError(f"MongoDB update operation failed: {e}") from e
