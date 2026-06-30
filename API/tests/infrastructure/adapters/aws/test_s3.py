from unittest.mock import Mock, patch
import pytest
from botocore.exceptions import BotoCoreError, ClientError

from src.infrastructure.adapters.aws.s3 import S3StorageAdapter
from src.infrastructure.config.settings import Settings
from src.domain.exceptions import StorageError


def test_s3_adapter_initialization_success() -> None:
    settings = Settings()
    settings.aws.s3_bucket = "test-bucket"
    settings.aws.access_key_id = "test-key"
    settings.aws.secret_access_key = "test-secret"
    settings.aws.session_token = "test-token"
    
    with patch("boto3.client") as mock_client_factory:
        adapter = S3StorageAdapter(settings)
        assert adapter._bucket_name == "test-bucket"
        mock_client_factory.assert_called_once_with(
            "s3",
            region_name="us-east-1",
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            aws_session_token="test-token"
        )


def test_s3_adapter_initialization_missing_bucket() -> None:
    settings = Settings()
    settings.aws.s3_bucket = None
    with pytest.raises(StorageError) as exc_info:
        S3StorageAdapter(settings)
    assert "S3 bucket name is not configured" in str(exc_info.value)


def test_s3_adapter_initialization_failure() -> None:
    settings = Settings()
    settings.aws.s3_bucket = "test-bucket"
    with patch("boto3.client", side_effect=BotoCoreError()) as mock_client_factory:
        with pytest.raises(StorageError) as exc_info:
            S3StorageAdapter(settings)
        assert "Failed to initialize S3 client" in str(exc_info.value)


def test_s3_adapter_upload_success() -> None:
    settings = Settings()
    settings.aws.s3_bucket = "test-bucket"
    mock_s3_client = Mock()
    with patch("boto3.client", return_value=mock_s3_client):
        adapter = S3StorageAdapter(settings)
        file_obj = Mock()
        result = adapter.upload_stream(file_obj, "test-key.txt")
        assert result == "s3://test-bucket/test-key.txt"
        mock_s3_client.upload_fileobj.assert_called_once_with(file_obj, "test-bucket", "test-key.txt")


def test_s3_adapter_upload_failure() -> None:
    settings = Settings()
    settings.aws.s3_bucket = "test-bucket"
    mock_s3_client = Mock()
    mock_s3_client.upload_fileobj.side_effect = ClientError({"Error": {"Code": "500", "Message": "Internal Server Error"}}, "upload_fileobj")
    with patch("boto3.client", return_value=mock_s3_client):
        adapter = S3StorageAdapter(settings)
        file_obj = Mock()
        with pytest.raises(StorageError) as exc_info:
            adapter.upload_stream(file_obj, "test-key.txt")
        assert "S3 upload failed for key 'test-key.txt'" in str(exc_info.value)
