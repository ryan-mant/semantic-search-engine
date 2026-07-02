from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class LoggerPort(ABC):

    @abstractmethod
    def info(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        pass

    @abstractmethod
    def error(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        pass

    @abstractmethod
    def warning(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        pass

    @abstractmethod
    def debug(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        pass
