from abc import ABC, abstractmethod

from src.domain.entities.document import Document

class EventPublisher(ABC):
    @abstractmethod
    async def publish_document_created(self, document: Document) -> None:
        pass
