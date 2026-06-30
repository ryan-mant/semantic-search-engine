import json
import logging
from unittest.mock import Mock, patch
import pytest
from confluent_kafka import Message

from processor import MessageProcessor
from src.domain.entities.document import Document


def test_process_message_success() -> None:
    processor = MessageProcessor()
    
    msg = Mock(spec=Message)
    payload = {
        "id": "doc-123",
        "content": "hello world",
        "metadata": {"test": True},
        "created_at": "2023-01-01T12:00:00"
    }
    msg.value.return_value = json.dumps(payload).encode("utf-8")
    
    with patch("logging.info") as mock_log_info:
        processor.process_message(msg)
        mock_log_info.assert_called_once()
        args, kwargs = mock_log_info.call_args
        assert "Successfully processed document" in args[0]
        assert kwargs["extra"]["document_id"] == "doc-123"


def test_process_message_json_decode_error() -> None:
    processor = MessageProcessor()
    msg = Mock(spec=Message)
    msg.value.return_value = b"invalid json"
    
    with patch("logging.error") as mock_log_error:
        processor.process_message(msg)
        mock_log_error.assert_called_once()
        assert "Failed to decode JSON message" in mock_log_error.call_args[0][0]


def test_process_message_creation_error() -> None:
    processor = MessageProcessor()
    msg = Mock(spec=Message)
    payload = {
        "id": "doc-123",
        "metadata": {"test": True}
    }
    msg.value.return_value = json.dumps(payload).encode("utf-8")
    
    with patch("logging.error") as mock_log_error:
        processor.process_message(msg)
        mock_log_error.assert_called_once()
        assert "Failed to create document from data" in mock_log_error.call_args[0][0]


def test_process_message_unexpected_error() -> None:
    processor = MessageProcessor()
    msg = Mock(spec=Message)
    msg.value.return_value = b"{}"
    
    with patch.object(processor, "_create_document", side_effect=RuntimeError("unexpected")):
        with patch("logging.error") as mock_log_error:
            processor.process_message(msg)
            mock_log_error.assert_called_once()
            assert "An unexpected error occurred while processing message" in mock_log_error.call_args[0][0]
