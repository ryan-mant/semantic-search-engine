import json
import logging
from typing import Any, Dict
from confluent_kafka import Producer

from src.domain.entities.document import Document
from src.domain.ports.event_publisher import EventPublisher
from src.domain.exceptions import EventPublishingError

logger = logging.getLogger(__name__)


class KafkaEventPublisher(EventPublisher):

    def __init__(self, producer: Producer, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    def _serialize_document(self, document: Document) -> str:
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
        serialized_data = self._serialize_document(document)
        
        def delivery_report(err: Any, msg: Any) -> None:
            if err is not None:
                logger.error(f"Kafka message delivery failed for doc {document.id}: {err}")

        try:
            self._producer.produce(
                topic=self._topic,
                key=str(document.id) if document.id else None,
                value=serialized_data,
                callback=delivery_report
            )
            self._producer.poll(0)
        except BufferError as e:
            raise EventPublishingError(f"Kafka local producer queue is full: {e}") from e
        except Exception as e:
            raise EventPublishingError(f"Failed to enqueue message to Kafka: {e}") from e
