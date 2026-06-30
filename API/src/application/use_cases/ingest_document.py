import io
import uuid
from typing import Dict, Any

from src.domain.entities.document import Document
from src.domain.ports.document_repository import DocumentRepository
from src.domain.ports.event_publisher import EventPublisher
from src.domain.ports.storage import StoragePort
from src.domain.exceptions import DocumentIngestionError, DomainError


class IngestDocumentUseCase:
    def __init__(
        self,
        document_repository: DocumentRepository,
        event_publisher: EventPublisher,
        storage_port: StoragePort,
    ) -> None:
        self._document_repository = document_repository
        self._event_publisher = event_publisher
        self._storage_port = storage_port

    async def execute(self, content: str, metadata: Dict[str, Any]) -> Document:
        try:
            document_id = str(uuid.uuid4())
            key = f"raw/{document_id}.txt"

            # Upload content to storage first to get the storage URL
            stream = io.BytesIO(content.encode("utf-8"))
            storage_url = self._storage_port.upload_stream(stream, key)

            # Create the final, immutable document entity
            final_metadata = metadata.copy()
            final_metadata["storage_url"] = storage_url
            
            document = Document(
                id=document_id,
                content=content,
                metadata=final_metadata
            )

            # Save the document once
            await self._document_repository.save(document)

            # Publish the creation event
            await self._event_publisher.publish_document_created(document)

            return document
        except DomainError as e:
            # Re-raise domain-specific errors as a clear ingestion failure
            raise DocumentIngestionError(f"Document ingestion failed: {e}") from e
        except Exception as e:
            # This block should be more specific in a real application,
            # but for now, we'll keep it to avoid swallowing all exceptions.
            # In a production system, you'd want to catch specific infrastructure errors.
            raise DocumentIngestionError(f"An unexpected error occurred during document ingestion: {e}") from e