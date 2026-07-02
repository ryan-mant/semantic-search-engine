import sys
from unittest.mock import MagicMock

# Mock external libraries not available in the host environment
sys.modules["sentence_transformers"] = MagicMock()
sys.modules["chromadb"] = MagicMock()

import json
import pytest
import signal as py_signal
from unittest.mock import Mock, patch, ANY, MagicMock, AsyncMock
from confluent_kafka import KafkaError, Message

from main import ConsoleJsonLogger, KafkaMessageConsumer, WorkerSettings, main


@pytest.fixture
def repo_mock() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def embedding_mock() -> AsyncMock:
    mock = AsyncMock()
    mock.generate.return_value = [0.1] * 384
    return mock


@pytest.fixture
def vector_mock() -> AsyncMock:
    return AsyncMock()


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


@pytest.mark.asyncio
async def test_consumer_poll_none(repo_mock, embedding_mock, vector_mock) -> None:
    consumer_mock = Mock()
    settings = WorkerSettings()
    logger_mock = Mock()
    
    wrapper = KafkaMessageConsumer(
        consumer_mock, settings, logger_mock, repo_mock, embedding_mock, vector_mock
    )
    
    def poll_side_effect(timeout):
        wrapper._running = False
        return None
        
    consumer_mock.poll.side_effect = poll_side_effect
    await wrapper.start()
    
    consumer_mock.subscribe.assert_called_once_with([settings.kafka.topic])
    consumer_mock.close.assert_called_once()


@pytest.mark.asyncio
async def test_consumer_poll_partition_eof(repo_mock, embedding_mock, vector_mock) -> None:
    consumer_mock = Mock()
    settings = WorkerSettings()
    logger_mock = Mock()
    
    wrapper = KafkaMessageConsumer(
        consumer_mock, settings, logger_mock, repo_mock, embedding_mock, vector_mock
    )
    
    msg_mock = Mock(spec=Message)
    err_mock = Mock()
    err_mock.code.return_value = KafkaError._PARTITION_EOF
    msg_mock.error.return_value = err_mock
    
    def poll_side_effect(timeout):
        wrapper._running = False
        return msg_mock
        
    consumer_mock.poll.side_effect = poll_side_effect
    await wrapper.start()
    
    logger_mock.error.assert_not_called()


@pytest.mark.asyncio
async def test_consumer_poll_kafka_error(repo_mock, embedding_mock, vector_mock) -> None:
    consumer_mock = Mock()
    settings = WorkerSettings()
    logger_mock = Mock()
    
    wrapper = KafkaMessageConsumer(
        consumer_mock, settings, logger_mock, repo_mock, embedding_mock, vector_mock
    )
    
    msg_mock = Mock(spec=Message)
    err_mock = MagicMock()
    err_mock.code.return_value = 999
    err_mock.__str__.return_value = "connection timeout"
    msg_mock.error.return_value = err_mock
    
    def poll_side_effect(timeout):
        wrapper._running = False
        return msg_mock
        
    consumer_mock.poll.side_effect = poll_side_effect
    await wrapper.start()
    
    logger_mock.error.assert_called_once_with(
        "Kafka error: connection timeout",
        {"code": 999}
    )


@pytest.mark.asyncio
async def test_consumer_poll_empty_payload(repo_mock, embedding_mock, vector_mock) -> None:
    consumer_mock = Mock()
    settings = WorkerSettings()
    logger_mock = Mock()
    
    wrapper = KafkaMessageConsumer(
        consumer_mock, settings, logger_mock, repo_mock, embedding_mock, vector_mock
    )
    
    msg_mock = Mock(spec=Message)
    msg_mock.error.return_value = None
    msg_mock.value.return_value = None
    
    def poll_side_effect(timeout):
        wrapper._running = False
        return msg_mock
        
    consumer_mock.poll.side_effect = poll_side_effect
    await wrapper.start()
    
    logger_mock.warning.assert_called_once_with("Empty payload received")


@pytest.mark.asyncio
async def test_consumer_poll_valid_payload(repo_mock, embedding_mock, vector_mock) -> None:
    consumer_mock = Mock()
    settings = WorkerSettings()
    logger_mock = Mock()
    
    wrapper = KafkaMessageConsumer(
        consumer_mock, settings, logger_mock, repo_mock, embedding_mock, vector_mock
    )
    
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
    await wrapper.start()
    
    repo_mock.save.assert_called_once()
    embedding_mock.generate.assert_called_once_with("document text")
    vector_mock.upsert.assert_called_once_with(
        doc_id="doc-123",
        vector=ANY,
        metadata={"content": "document text", "source": "kafka"}
    )
    
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


@pytest.mark.asyncio
async def test_consumer_poll_invalid_date(repo_mock, embedding_mock, vector_mock) -> None:
    consumer_mock = Mock()
    settings = WorkerSettings()
    logger_mock = Mock()
    
    wrapper = KafkaMessageConsumer(
        consumer_mock, settings, logger_mock, repo_mock, embedding_mock, vector_mock
    )
    
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
    await wrapper.start()
    
    called_args = [args[0] for args, kwargs in logger_mock.info.call_args_list]
    assert "Document processed successfully" in called_args


@pytest.mark.asyncio
async def test_consumer_poll_processing_exception(repo_mock, embedding_mock, vector_mock) -> None:
    consumer_mock = Mock()
    settings = WorkerSettings()
    logger_mock = Mock()
    
    wrapper = KafkaMessageConsumer(
        consumer_mock, settings, logger_mock, repo_mock, embedding_mock, vector_mock
    )
    
    msg_mock = Mock(spec=Message)
    msg_mock.error.return_value = None
    msg_mock.partition.return_value = 2
    msg_mock.offset.return_value = 100
    msg_mock.value.return_value = b"{"
    
    def poll_side_effect(timeout):
        wrapper._running = False
        return msg_mock
        
    consumer_mock.poll.side_effect = poll_side_effect
    await wrapper.start()
    
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
    mock_wrapper_instance.start = AsyncMock()
    
    with patch("main.Consumer", mock_consumer_cls), \
         patch("main.KafkaMessageConsumer", return_value=mock_wrapper_instance), \
         patch("motor.motor_asyncio.AsyncIOMotorClient"), \
         patch("sentence_transformers.SentenceTransformer"), \
         patch("chromadb.HttpClient"), \
         patch("main.signal.signal") as mock_signal:
         
        import asyncio
        asyncio.run(main())
        
        mock_consumer_cls.assert_called_once()
        mock_wrapper_instance.start.assert_called_once()
        
        # Verify that SIGINT and SIGTERM handlers were registered for handle_shutdown
        shutdown_calls = [
            call for call in mock_signal.call_args_list 
            if call[0][0] in (py_signal.SIGINT, py_signal.SIGTERM)
            and "handle_shutdown" in str(call[0][1])
        ]
        assert len(shutdown_calls) == 2


def test_main_consumer_failure() -> None:
    mock_consumer_cls = Mock()
    mock_consumer_cls.side_effect = Exception("failed to initialize")
    
    with patch("main.Consumer", mock_consumer_cls), \
         patch("motor.motor_asyncio.AsyncIOMotorClient"), \
         patch("sentence_transformers.SentenceTransformer"), \
         patch("chromadb.HttpClient"), \
         patch("main.signal.signal"), \
         patch("sys.exit", side_effect=SystemExit(1)):
         
        with pytest.raises(SystemExit) as exc_info:
            import asyncio
            asyncio.run(main())
            
        assert exc_info.value.code == 1
