from typing import Optional
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, PyMongoError

from src.domain.entities.document import Document
from src.domain.ports.document_repository import DocumentRepository
from src.domain.exceptions import DatabaseConnectionError


class MongoDocumentRepository(DocumentRepository):
    """
    MongoDB implementation of the DocumentRepository port using Motor for async operations.
    """

    def __init__(self, db: AsyncIOMotorDatabase, collection_name: str = "documents") -> None:
        """
        Initializes the repository with a Motor database instance and collection name.
        """
        self._db = db
        self._collection = db[collection_name]

    async def save(self, document: Document) -> None:
        """
        Saves a Document entity to MongoDB.
        If the document already has an id, it is upserted. Otherwise, a new record is inserted
        and the generated id is assigned to the document entity.
        """
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

    async def get_by_id(self, document_id: str) -> Optional[Document]:
        """
        Retrieves a Document entity from MongoDB by its ID.
        Returns None if no matching document is found.
        """
        try:
            query = {"_id": ObjectId(document_id) if ObjectId.is_valid(document_id) else document_id}
            doc_dict = await self._collection.find_one(query)
            if not doc_dict:
                return None

            document_data = {
                "id": str(doc_dict["_id"]),
                "content": doc_dict["content"],
                "metadata": doc_dict.get("metadata", {}),
            }
            if "created_at" in doc_dict and doc_dict["created_at"]:
                document_data["created_at"] = doc_dict["created_at"]

            return Document(**document_data)
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            raise DatabaseConnectionError(f"Database connection or timeout failure: {e}") from e
        except PyMongoError as e:
            raise DatabaseConnectionError(f"MongoDB read operation failed: {e}") from e