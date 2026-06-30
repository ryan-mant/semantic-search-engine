import pytest
from unittest.mock import Mock, patch
from datetime import datetime

from src.infrastructure.adapters.kafka.producer import KafkaEventPublisher
from src.domain.entities.document import Document
from src.domain.exceptions import EventPublishingError


def test_serialize_document_success() -> None:
    producer_mock = Mock()
    publisher = KafkaEventPublisher(producer_mock, "test-topic")
    doc = Document(
        id="doc-123",
        content="hello world",
        metadata={"author": "bob"},
        created_at=datetime(2023, 1, 1, 12, 0, 0)
    )
    result = publisher._serialize_document(doc)
    assert "doc-123" in result
    assert "hello world" in result
    assert "2023-01-01T12:00:00" in result


def test_serialize_document_failure() -> None:
    producer_mock = Mock()
    publisher = KafkaEventPublisher(producer_mock, "test-topic")
    doc = Document(
        id="doc-123",
        content="hello world",
        metadata={"unserializable": object()},
        created_at=datetime(2023, 1, 1, 12, 0, 0)
    )
    with pytest.raises(EventPublishingError) as exc_info:
        publisher._serialize_document(doc)
    assert "Failed to serialize document" in str(exc_info.value)


@pytest.mark.asyncio
async def test_publish_document_created_success() -> None:
    producer_mock = Mock()
    publisher = KafkaEventPublisher(producer_mock, "test-topic")
    doc = Document(
        id="doc-123",
        content="hello world",
        metadata={"author": "bob"},
        created_at=datetime(2023, 1, 1, 12, 0, 0)
    )

    await publisher.publish_document_created(doc)

    producer_mock.produce.assert_called_once()
    args, kwargs = producer_mock.produce.call_args
    assert kwargs["topic"] == "test-topic"
    assert kwargs["key"] == "doc-123"
    assert "hello world" in kwargs["value"]
    assert kwargs["callback"] is not None


@pytest.mark.asyncio
async def test_publish_document_created_buffer_error() -> None:
    producer_mock = Mock()
    producer_mock.produce.side_effect = BufferError("Queue full")
    publisher = KafkaEventPublisher(producer_mock, "test-topic")
    doc = Document(id="doc-123", content="hello world", metadata={})

    with pytest.raises(EventPublishingError) as exc_info:
        await publisher.publish_document_created(doc)
    assert "Kafka local producer queue is full" in str(exc_info.value)


@pytest.mark.asyncio
async def test_publish_document_created_generic_produce_error() -> None:
    producer_mock = Mock()
    producer_mock.produce.side_effect = RuntimeError("Something went wrong")
    publisher = KafkaEventPublisher(producer_mock, "test-topic")
    doc = Document(id="doc-123", content="hello world", metadata={})

    with pytest.raises(EventPublishingError) as exc_info:
        await publisher.publish_document_created(doc)
    assert "Failed to enqueue message to Kafka" in str(exc_info.value)


@pytest.mark.asyncio
async def test_publish_document_created_flush_error() -> None:
    producer_mock = Mock()
    # Mocking loop.run_in_executor to raise error
    publisher = KafkaEventPublisher(producer_mock, "test-topic")
    doc = Document(id="doc-123", content="hello world", metadata={})

    with patch("asyncio.get_running_loop") as mock_loop_factory:
        mock_loop = Mock()
        mock_loop.run_in_executor.side_effect = RuntimeError("Flush fail")
        mock_loop_factory.return_value = mock_loop

        with pytest.raises(EventPublishingError) as exc_info:
            await publisher.publish_document_created(doc)
        assert "Error during Kafka producer flush" in str(exc_info.value)


@pytest.mark.asyncio
async def test_publish_document_created_delivery_failure() -> None:
    producer_mock = Mock()
    publisher = KafkaEventPublisher(producer_mock, "test-topic")
    doc = Document(id="doc-123", content="hello world", metadata={})

    def mock_produce(topic, key, value, callback):
        # Simulate delivery error by invoking the callback
        mock_err = Mock()
        mock_err.str.return_value = "Broker: Message timed out"
        callback(mock_err, None)

    producer_mock.produce.side_effect = mock_produce

    with pytest.raises(EventPublishingError) as exc_info:
        await publisher.publish_document_created(doc)
    assert "Kafka message delivery failed: Broker: Message timed out" in str(exc_info.value)
