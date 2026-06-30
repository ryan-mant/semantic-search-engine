from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class LoggerPort(ABC):
    """
    Port interface for structured logging.
    """

    @abstractmethod
    def info(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """Logs an informational message with optional structured context."""
        pass

    @abstractmethod
    def error(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """Logs an error message with optional structured context."""
        pass

    @abstractmethod
    def warning(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """Logs a warning message with optional structured context."""
        pass

    @abstractmethod
    def debug(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """Logs a debug message with optional structured context."""
        pass
