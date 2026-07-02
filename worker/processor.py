import json
import logging
from datetime import datetime
from typing import Any

from confluent_kafka import Message

from src.domain.entities.document import Document


class MessageProcessor:
    def process_message(self, msg: Message) -> None:
        try:
            data = self._decode_message(msg)
            document = self._create_document(data)
            self._log_document(document)
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON message: {e} | Raw message: {msg.value()}")
        except (TypeError, KeyError) as e:
            logging.error(f"Failed to create document from data: {e} | Data: {data}")
        except Exception as e:
            logging.error(f"An unexpected error occurred while processing message: {e}")

    def _decode_message(self, msg: Message) -> Any:
        return json.loads(msg.value().decode('utf-8'))

    def _create_document(self, data: Any) -> Document:
        data_copy = data.copy() if isinstance(data, dict) else {}
        if "created_at" in data_copy and isinstance(data_copy["created_at"], str):
            try:
                iso_str = data_copy["created_at"].replace("Z", "+00:00")
                data_copy["created_at"] = datetime.fromisoformat(iso_str)
            except ValueError:
                pass
        return Document(**data_copy)

    def _log_document(self, document: Document) -> None:
        logging.info(
            "Successfully processed document",
            extra={
                "document_id": document.id,
                "created_at": document.created_at.isoformat(),
            },
        )
