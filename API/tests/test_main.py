from unittest.mock import Mock, patch
import pytest
from fastapi.testclient import TestClient

mock_producer = Mock()

with patch("confluent_kafka.Producer", return_value=mock_producer):
    from src.main import app


def test_health_check() -> None:
    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_create_document() -> None:
    mock_producer.reset_mock()
    client = TestClient(app)
    payload = {"content": "main doc", "metadata": {"test": True}}
    
    response = client.post("/documents", json=payload)
    assert response.status_code == 202
    assert response.json() == {"message": "Document received for processing."}
    
    mock_producer.produce.assert_called_once()
    args, kwargs = mock_producer.produce.call_args
    assert kwargs["topic"] == "documents"
    assert "main doc" in kwargs["value"]
    mock_producer.flush.assert_called_once()
