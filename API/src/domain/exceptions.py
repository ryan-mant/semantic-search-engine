class DomainError(Exception):
    """Base exception class for all domain-related errors."""
    pass


class DatabaseConnectionError(DomainError):
    """Raised when a database connection failure or timeout occurs."""
    pass


class DocumentNotFoundError(DomainError):
    """Raised when a requested document is not found."""
    pass


class EventPublishingError(DomainError):
    """Raised when an event fails to be published."""
    pass


class StorageError(DomainError):
    """Raised when a storage upload or operation fails."""
    pass


class LoggingError(DomainError):
    """Raised when writing structured logs to the logs backend fails."""
    pass


class DocumentIngestionError(DomainError):
    """Raised when document ingestion fails."""
    pass


