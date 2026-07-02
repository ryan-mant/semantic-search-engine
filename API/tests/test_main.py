import sys
from unittest.mock import MagicMock

# Mock external libraries not available in the host environment
sys.modules["sentence_transformers"] = MagicMock()
sys.modules["chromadb"] = MagicMock()

from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client() -> TestClient:
    with patch("src.main.AsyncIOMotorClient") as mock_motor, \
         patch("src.main.Producer") as mock_producer_cls:
        mock_mongo_client = MagicMock()
        mock_mongo_client.__getitem__ = MagicMock(
            return_value="mock_db"
        )
        mock_motor.return_value = mock_mongo_client

        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer

        from src.main import app

        with TestClient(app) as tc:
            yield tc


def test_health_check(client: TestClient) -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
