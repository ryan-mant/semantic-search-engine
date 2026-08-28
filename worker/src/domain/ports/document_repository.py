from abc import ABC, abstractmethod
from typing import Optional

from src.domain.entities.document import Document


class DocumentRepository(ABC):

    @abstractmethod
    async def save(self, document: Document) -> None:
        pass

    @abstractmethod
    async def get_by_id(self, document_id: str) -> Optional[Document]:
        pass

    @abstractmethod
    async def update_status(
        self, document_id: str, status: str, error: Optional[str] = None
    ) -> None:
        pass

