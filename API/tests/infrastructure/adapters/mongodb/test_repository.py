from unittest.mock import Mock, AsyncMock, MagicMock
import pytest
from bson import ObjectId
from pymongo.errors import ConnectionFailure, PyMongoError

from src.infrastructure.adapters.mongodb.repository import MongoDocumentRepository
from src.domain.entities.document import Document
from src.domain.exceptions import DatabaseConnectionError


@pytest.mark.asyncio
async def test_mongo_save_insert_success() -> None:
    mock_db = MagicMock()
    mock_collection = AsyncMock()
    mock_db.__getitem__.return_value = mock_collection
    
    mock_result = Mock()
    mock_result.inserted_id = ObjectId("64b8f52ef713cd1086a9f4e5")
    mock_collection.insert_one.return_value = mock_result
    
    repo = MongoDocumentRepository(mock_db)
    doc = Document(content="hello", metadata={"a": 1})
    
    await repo.save(doc)
    
    assert doc.id == "64b8f52ef713cd1086a9f4e5"
    mock_collection.insert_one.assert_called_once()
    args, kwargs = mock_collection.insert_one.call_args
    assert args[0]["content"] == "hello"
    assert args[0]["metadata"] == {"a": 1}


@pytest.mark.asyncio
async def test_mongo_save_update_success() -> None:
    mock_db = MagicMock()
    mock_collection = AsyncMock()
    mock_db.__getitem__.return_value = mock_collection
    
    repo = MongoDocumentRepository(mock_db)
    
    doc = Document(id="doc-123", content="hello update", metadata={"a": 2})
    await repo.save(doc)
    
    mock_collection.update_one.assert_called_once()
    args, kwargs = mock_collection.update_one.call_args
    assert args[0]["_id"] == "doc-123"
    assert args[1]["$set"]["content"] == "hello update"
    assert kwargs["upsert"] is True

    mock_collection.update_one.reset_mock()
    valid_id = "64b8f52ef713cd1086a9f4e5"
    doc2 = Document(id=valid_id, content="hello update 2", metadata={"a": 3})
    await repo.save(doc2)
    
    mock_collection.update_one.assert_called_once()
    args, kwargs = mock_collection.update_one.call_args
    assert args[0]["_id"] == ObjectId(valid_id)


@pytest.mark.asyncio
async def test_mongo_save_connection_failure() -> None:
    mock_db = MagicMock()
    mock_collection = AsyncMock()
    mock_db.__getitem__.return_value = mock_collection
    mock_collection.insert_one.side_effect = ConnectionFailure("Conn lost")
    
    repo = MongoDocumentRepository(mock_db)
    doc = Document(content="hello", metadata={})
    
    with pytest.raises(DatabaseConnectionError) as exc_info:
        await repo.save(doc)
    assert "Database connection or timeout failure" in str(exc_info.value)


@pytest.mark.asyncio
async def test_mongo_save_pymongo_error() -> None:
    mock_db = MagicMock()
    mock_collection = AsyncMock()
    mock_db.__getitem__.return_value = mock_collection
    mock_collection.insert_one.side_effect = PyMongoError("Write failed")
    
    repo = MongoDocumentRepository(mock_db)
    doc = Document(content="hello", metadata={})
    
    with pytest.raises(DatabaseConnectionError) as exc_info:
        await repo.save(doc)
    assert "MongoDB write operation failed" in str(exc_info.value)
