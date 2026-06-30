import sys
import json
import pytest
from unittest.mock import Mock, patch, ANY, MagicMock
from confluent_kafka import KafkaError, Message

from main import ConsoleJsonLogger, KafkaMessageConsumer, WorkerSettings, main


def test_console_json_logger(capsys) -> None:
    logger = ConsoleJsonLogger()
    logger.info("hello info", {"extra_key": 123})
    captured = capsys.readouterr()
    log_data = json.loads(captured.out.strip())
    assert log_data["level"] == "INFO"
    assert log_data["message"] == "hello info"
    assert log_data["extra"] == {"extra_key": 123}
    
    logger.error("hello error")
    captured = capsys.readouterr()
    log_data = json.loads(captured.out.strip())
    assert log_data["level"] == "ERROR"
    
    logger.warning("hello warning")
    captured = capsys.readouterr()
    log_data = json.loads(captured.out.strip())
    assert log_data["level"] == "WARNING"

    logger.debug("hello debug")
    captured = capsys.readouterr()
    log_data = json.loads(captured.out.strip())
    assert log_data["level"] == "DEBUG"


def test_consumer_poll_none() -> None:
    consumer_mock = Mock()
    settings = WorkerSettings()
    logger_mock = Mock()
    
    wrapper = KafkaMessageConsumer(consumer_mock, settings, logger_mock)
    
    def poll_side_effect(timeout):
        wrapper._running = False
        return None
        
    consumer_mock.poll.side_effect = poll_side_effect
    wrapper.start()
    
    consumer_mock.subscribe.assert_called_once_with([settings.kafka.topic])
    consumer_mock.close.assert_called_once()


def test_consumer_poll_partition_eof() -> None:
    consumer_mock = Mock()
    settings = WorkerSettings()
    logger_mock = Mock()
    
    wrapper = KafkaMessageConsumer(consumer_mock, settings, logger_mock)
    
    msg_mock = Mock(spec=Message)
    err_mock = Mock()
    err_mock.code.return_value = KafkaError._PARTITION_EOF
    msg_mock.error.return_value = err_mock
    
    def poll_side_effect(timeout):
        wrapper._running = False
        return msg_mock
        
    consumer_mock.poll.side_effect = poll_side_effect
    wrapper.start()
    
    logger_mock.error.assert_not_called()


def test_consumer_poll_kafka_error() -> None:
    consumer_mock = Mock()
    settings = WorkerSettings()
    logger_mock = Mock()
    
    wrapper = KafkaMessageConsumer(consumer_mock, settings, logger_mock)
    
    msg_mock = Mock(spec=Message)
    err_mock = MagicMock()
    err_mock.code.return_value = 999
    err_mock.__str__.return_value = "connection timeout"
    msg_mock.error.return_value = err_mock
    
    def poll_side_effect(timeout):
        wrapper._running = False
        return msg_mock
        
    consumer_mock.poll.side_effect = poll_side_effect
    wrapper.start()
    
    logger_mock.error.assert_called_once_with(
        "Kafka error: connection timeout",
        {"code": 999}
    )


def test_consumer_poll_empty_payload() -> None:
    consumer_mock = Mock()
    settings = WorkerSettings()
    logger_mock = Mock()
    
    wrapper = KafkaMessageConsumer(consumer_mock, settings, logger_mock)
    
    msg_mock = Mock(spec=Message)
    msg_mock.error.return_value = None
    msg_mock.value.return_value = None
    
    def poll_side_effect(timeout):
        wrapper._running = False
        return msg_mock
        
    consumer_mock.poll.side_effect = poll_side_effect
    wrapper.start()
    
    logger_mock.warning.assert_called_once_with("Empty payload received")


def test_consumer_poll_valid_payload() -> None:
    consumer_mock = Mock()
    settings = WorkerSettings()
    logger_mock = Mock()
    
    wrapper = KafkaMessageConsumer(consumer_mock, settings, logger_mock)
    
    msg_mock = Mock(spec=Message)
    msg_mock.error.return_value = None
    payload = {
        "id": "doc-123",
        "content": "document text",
        "metadata": {"source": "kafka"},
        "created_at": "2026-06-30T23:59:59Z"
    }
    msg_mock.value.return_value = json.dumps(payload).encode("utf-8")
    
    def poll_side_effect(timeout):
        wrapper._running = False
        return msg_mock
        
    consumer_mock.poll.side_effect = poll_side_effect
    wrapper.start()
    
    logger_mock.info.assert_any_call(
        "Document processed successfully",
        extra={
            "document": {
                "id": "doc-123",
                "content_length": 13,
                "metadata": {"source": "kafka"},
                "created_at": "2026-06-30T23:59:59+00:00"
            }
        }
    )


def test_consumer_poll_invalid_date() -> None:
    consumer_mock = Mock()
    settings = WorkerSettings()
    logger_mock = Mock()
    
    wrapper = KafkaMessageConsumer(consumer_mock, settings, logger_mock)
    
    msg_mock = Mock(spec=Message)
    msg_mock.error.return_value = None
    payload = {
        "id": "doc-123",
        "content": "document text",
        "metadata": {"source": "kafka"},
        "created_at": "invalid-date"
    }
    msg_mock.value.return_value = json.dumps(payload).encode("utf-8")
    
    def poll_side_effect(timeout):
        wrapper._running = False
        return msg_mock
        
    consumer_mock.poll.side_effect = poll_side_effect
    wrapper.start()
    
    called_args = [args[0] for args, kwargs in logger_mock.info.call_args_list]
    assert "Document processed successfully" in called_args


def test_consumer_poll_processing_exception() -> None:
    consumer_mock = Mock()
    settings = WorkerSettings()
    logger_mock = Mock()
    
    wrapper = KafkaMessageConsumer(consumer_mock, settings, logger_mock)
    
    msg_mock = Mock(spec=Message)
    msg_mock.error.return_value = None
    msg_mock.partition.return_value = 2
    msg_mock.offset.return_value = 100
    msg_mock.value.return_value = b"{"
    
    def poll_side_effect(timeout):
        wrapper._running = False
        return msg_mock
        
    consumer_mock.poll.side_effect = poll_side_effect
    wrapper.start()
    
    logger_mock.error.assert_called_once_with(
        "Error processing message",
        {
            "error": ANY,
            "partition": 2,
            "offset": 100
        }
    )


def test_main_success() -> None:
    mock_consumer_cls = Mock()
    mock_consumer_instance = Mock()
    mock_consumer_cls.return_value = mock_consumer_instance
    
    mock_wrapper_instance = Mock()
    
    with patch("main.Consumer", mock_consumer_cls), \
         patch("main.KafkaMessageConsumer", return_value=mock_wrapper_instance), \
         patch("signal.signal") as mock_signal:
         
        main()
        
        mock_consumer_cls.assert_called_once()
        mock_wrapper_instance.start.assert_called_once()
        assert mock_signal.call_count == 2


def test_main_consumer_failure() -> None:
    mock_consumer_cls = Mock()
    mock_consumer_cls.side_effect = Exception("failed to initialize")
    
    with patch("main.Consumer", mock_consumer_cls), \
         patch("sys.exit", side_effect=SystemExit(1)):
         
        with pytest.raises(SystemExit) as exc_info:
            main()
            
        assert exc_info.value.code == 1
