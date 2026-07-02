class DomainError(Exception):
    pass


class DatabaseConnectionError(DomainError):
    pass


class DocumentNotFoundError(DomainError):
    pass


class EventPublishingError(DomainError):
    pass


class StorageError(DomainError):
    pass


class LoggingError(DomainError):
    pass


class DocumentIngestionError(DomainError):
    pass
