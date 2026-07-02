from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, PyMongoError

from src.domain.entities.document import Document
from src.domain.ports.document_repository import DocumentRepository
from src.domain.exceptions import DatabaseConnectionError


class MongoDocumentRepository(DocumentRepository):

    def __init__(self, db: AsyncIOMotorDatabase, collection_name: str = "documents") -> None:
        self._db = db
        self._collection = db[collection_name]

    async def save(self, document: Document) -> None:
        try:
            if document.id:
                query = {"_id": ObjectId(document.id) if ObjectId.is_valid(document.id) else document.id}
                update_data = {
                    "content": document.content,
                    "metadata": document.metadata,
                    "created_at": document.created_at,
                }
                await self._collection.update_one(query, {"$set": update_data}, upsert=True)
            else:
                insert_data = {
                    "content": document.content,
                    "metadata": document.metadata,
                    "created_at": document.created_at,
                }
                result = await self._collection.insert_one(insert_data)
                object.__setattr__(document, "id", str(result.inserted_id))
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            raise DatabaseConnectionError(f"Database connection or timeout failure: {e}") from e
        except PyMongoError as e:
            raise DatabaseConnectionError(f"MongoDB write operation failed: {e}") from e