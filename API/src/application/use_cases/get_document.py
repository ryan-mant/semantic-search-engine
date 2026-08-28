from src.domain.entities.document import Document
from src.domain.exceptions import DocumentNotFoundError
from src.domain.ports.document_repository import DocumentRepository


class GetDocumentUseCase:

    def __init__(self, document_repository: DocumentRepository) -> None:
        self._document_repository = document_repository

    async def execute(self, document_id: str) -> Document:
        if not document_id or not document_id.strip():
            raise ValueError("Document ID cannot be empty")

        doc = await self._document_repository.get_by_id(document_id.strip())
        if not doc:
            raise DocumentNotFoundError(f"Document with id '{document_id}' not found")
        return doc
