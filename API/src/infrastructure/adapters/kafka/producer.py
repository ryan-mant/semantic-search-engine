import json
import asyncio
from typing import Any, Dict
from confluent_kafka import Producer

from src.domain.entities.document import Document
from src.domain.ports.event_publisher import EventPublisher
from src.domain.exceptions import EventPublishingError


class KafkaEventPublisher(EventPublisher):
    """
    Kafka implementation of the EventPublisher port using confluent-kafka.
    """

    def __init__(self, producer: Producer, topic: str) -> None:
        """
        Initializes the publisher with a confluent-kafka Producer instance and a target topic.
        """
        self._producer = producer
        self._topic = topic

    def _serialize_document(self, document: Document) -> str:
        """
        Serializes a Document entity into a JSON string.
        """
        try:
            document_dict: Dict[str, Any] = {
                "id": document.id,
                "content": document.content,
                "metadata": document.metadata,
                "created_at": document.created_at.isoformat() if document.created_at else None,
            }
            return json.dumps(document_dict)
        except (TypeError, ValueError) as e:
            raise EventPublishingError(f"Failed to serialize document: {e}") from e

    async def publish_document_created(self, document: Document) -> None:
        """
        Publishes a document created event to the Kafka topic.
        Blocks asynchronously until delivery acknowledgement is received.
        """
        serialized_data = self._serialize_document(document)
        
        delivery_errors = []

        def delivery_report(err: Any, msg: Any) -> None:
            if err is not None:
                delivery_errors.append(err)

        try:
            self._producer.produce(
                topic=self._topic,
                key=str(document.id) if document.id else None,
                value=serialized_data,
                callback=delivery_report
            )
        except BufferError as e:
            raise EventPublishingError(f"Kafka local producer queue is full: {e}") from e
        except Exception as e:
            raise EventPublishingError(f"Failed to enqueue message to Kafka: {e}") from e

        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, self._producer.flush)
        except Exception as e:
            raise EventPublishingError(f"Error during Kafka producer flush: {e}") from e

        if delivery_errors:
            kafka_error = delivery_errors[0]
            error_msg = kafka_error.str() if hasattr(kafka_error, "str") else str(kafka_error)
            raise EventPublishingError(f"Kafka message delivery failed: {error_msg}")
