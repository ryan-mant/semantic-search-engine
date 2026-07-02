from abc import ABC, abstractmethod

from src.domain.entities.document import Document


class DocumentRepository(ABC):

    @abstractmethod
    async def save(self, document: Document) -> None:
        pass
