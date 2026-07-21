from abc import ABC, abstractmethod
from typing import BinaryIO


class StoragePort(ABC):

    @abstractmethod
    async def upload_stream(self, file_obj: BinaryIO, key: str) -> str:
        pass
