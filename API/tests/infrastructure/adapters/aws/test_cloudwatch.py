import pytest
from unittest.mock import Mock, patch
from botocore.exceptions import BotoCoreError, ClientError

from src.infrastructure.adapters.aws.cloudwatch import CloudWatchLoggerAdapter
from src.infrastructure.config.settings import Settings
from src.domain.exceptions import LoggingError


def test_cloudwatch_adapter_missing_config() -> None:
    settings = Settings()
    settings.aws.cloudwatch_log_group = None
    with pytest.raises(LoggingError) as exc_info:
        CloudWatchLoggerAdapter(settings)
    assert "CloudWatch log group and stream must be configured" in str(exc_info.value)


def test_cloudwatch_adapter_initialization_success() -> None:
    settings = Settings()
    settings.aws.cloudwatch_log_group = "my-group"
    settings.aws.cloudwatch_log_stream = "my-stream"

    mock_cw_client = Mock()
    mock_cw_client.exceptions.ResourceAlreadyExistsException = ClientError
    mock_cw_client.describe_log_streams.return_value = {
        "logStreams": [
            {"logStreamName": "my-stream", "uploadSequenceToken": "token-123"}
        ]
    }

    with patch("boto3.client", return_value=mock_cw_client):
        adapter = CloudWatchLoggerAdapter(settings)
        assert adapter._log_group == "my-group"
        assert adapter._log_stream == "my-stream"
        assert adapter._sequence_token == "token-123"


def test_cloudwatch_adapter_initialization_resource_exists() -> None:
    settings = Settings()
    settings.aws.cloudwatch_log_group = "my-group"
    settings.aws.cloudwatch_log_stream = "my-stream"

    mock_cw_client = Mock()
    # Mock exceptions.ResourceAlreadyExistsException
    class DummyResourceAlreadyExists(Exception):
        pass
    mock_cw_client.exceptions.ResourceAlreadyExistsException = DummyResourceAlreadyExists
    mock_cw_client.create_log_group.side_effect = DummyResourceAlreadyExists()
    mock_cw_client.create_log_stream.side_effect = DummyResourceAlreadyExists()
    
    mock_cw_client.describe_log_streams.return_value = {}

    with patch("boto3.client", return_value=mock_cw_client):
        adapter = CloudWatchLoggerAdapter(settings)
        assert adapter._sequence_token is None


def test_cloudwatch_adapter_initialization_client_error() -> None:
    settings = Settings()
    settings.aws.cloudwatch_log_group = "my-group"
    settings.aws.cloudwatch_log_stream = "my-stream"

    with patch("boto3.client", side_effect=BotoCoreError()):
        with pytest.raises(LoggingError) as exc_info:
            CloudWatchLoggerAdapter(settings)
        assert "Failed to initialize CloudWatch Logs" in str(exc_info.value)


def test_cloudwatch_adapter_log_methods_success() -> None:
    settings = Settings()
    settings.aws.cloudwatch_log_group = "my-group"
    settings.aws.cloudwatch_log_stream = "my-stream"

    mock_cw_client = Mock()
    mock_cw_client.exceptions.ResourceAlreadyExistsException = Exception
    mock_cw_client.describe_log_streams.return_value = {
        "logStreams": [
            {"logStreamName": "my-stream", "uploadSequenceToken": "token-123"}
        ]
    }
    
    mock_cw_client.put_log_events.return_value = {"nextSequenceToken": "token-456"}

    with patch("boto3.client", return_value=mock_cw_client):
        adapter = CloudWatchLoggerAdapter(settings)
        
        # Test info log
        adapter.info("info message", {"key": "val"})
        mock_cw_client.put_log_events.assert_called_once()
        args, kwargs = mock_cw_client.put_log_events.call_args
        assert kwargs["logGroupName"] == "my-group"
        assert kwargs["logStreamName"] == "my-stream"
        assert kwargs["sequenceToken"] == "token-123"
        assert len(kwargs["logEvents"]) == 1
        assert "info message" in kwargs["logEvents"][0]["message"]
        assert adapter._sequence_token == "token-456"

        # Test other level logging
        mock_cw_client.put_log_events.reset_mock()
        adapter.error("error message")
        args, kwargs = mock_cw_client.put_log_events.call_args
        assert "error message" in kwargs["logEvents"][0]["message"]

        mock_cw_client.put_log_events.reset_mock()
        adapter.warning("warning message")
        args, kwargs = mock_cw_client.put_log_events.call_args
        assert "warning message" in kwargs["logEvents"][0]["message"]

        mock_cw_client.put_log_events.reset_mock()
        adapter.debug("debug message")
        args, kwargs = mock_cw_client.put_log_events.call_args
        assert "debug message" in kwargs["logEvents"][0]["message"]


def test_cloudwatch_adapter_invalid_sequence_token_exception() -> None:
    settings = Settings()
    settings.aws.cloudwatch_log_group = "my-group"
    settings.aws.cloudwatch_log_stream = "my-stream"

    mock_cw_client = Mock()
    mock_cw_client.exceptions.ResourceAlreadyExistsException = Exception
    mock_cw_client.describe_log_streams.return_value = {
        "logStreams": [
            {"logStreamName": "my-stream", "uploadSequenceToken": "token-123"}
        ]
    }
    
    # Define custom InvalidSequenceTokenException
    class DummyInvalidSequenceToken(Exception):
        def __init__(self, response):
            self.response = response
            super().__init__()

    mock_cw_client.exceptions.InvalidSequenceTokenException = DummyInvalidSequenceToken
    
    # First call to put_log_events raises DummyInvalidSequenceToken
    exc_response = {"expectedSequenceToken": "expected-token-999"}
    mock_cw_client.put_log_events.side_effect = [
        DummyInvalidSequenceToken(exc_response),
        {"nextSequenceToken": "token-next"}
    ]

    with patch("boto3.client", return_value=mock_cw_client):
        adapter = CloudWatchLoggerAdapter(settings)
        adapter.info("retry test")
        
        assert mock_cw_client.put_log_events.call_count == 2
        # Check that the second call used "expected-token-999"
        first_call, second_call = mock_cw_client.put_log_events.call_args_list
        assert second_call.kwargs["sequenceToken"] == "expected-token-999"
        assert adapter._sequence_token == "token-next"


def test_cloudwatch_adapter_put_log_events_failure() -> None:
    settings = Settings()
    settings.aws.cloudwatch_log_group = "my-group"
    settings.aws.cloudwatch_log_stream = "my-stream"

    mock_cw_client = Mock()
    class DummyResourceAlreadyExists(Exception):
        pass
    class DummyInvalidSequenceToken(Exception):
        pass
    mock_cw_client.exceptions.ResourceAlreadyExistsException = DummyResourceAlreadyExists
    mock_cw_client.exceptions.InvalidSequenceTokenException = DummyInvalidSequenceToken
    mock_cw_client.describe_log_streams.return_value = {}
    mock_cw_client.put_log_events.side_effect = BotoCoreError()

    with patch("boto3.client", return_value=mock_cw_client):
        adapter = CloudWatchLoggerAdapter(settings)
        with pytest.raises(LoggingError) as exc_info:
            adapter.info("fail log")
        assert "CloudWatch logging failed" in str(exc_info.value)
