from abc import ABC, abstractmethod
from typing import BinaryIO


class StoragePort(ABC):
    """
    Port interface for binary object storage.
    """

    @abstractmethod
    def upload_stream(self, file_obj: BinaryIO, key: str) -> str:
        """
        Uploads a file-like object stream to the storage provider.
        Returns the path or URL of the stored object.
        """
        pass
