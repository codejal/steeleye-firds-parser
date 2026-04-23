"""Abstract base class defining the storage interface."""

import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class BaseStorage(ABC):
    """
    Abstract base class for storage backends.
    Eg. fsspec,
    Can be extended to more data storage like DBs
    """

    @abstractmethod
    def read(self, path: str) -> bytes:
        """
        Read the contents of an object from storage.
        (e.g. ``"s3://bucket/key.csv"`` or ``"/tmp/local.csv"``).

        :param path: Full path to the object, including protocol
        :returns: Raw bytes of the object.
        """

    @abstractmethod
    def write(self, path: str, data: bytes) -> None:
        """
        Write bytes to an object in storage.

        :param path: Destination path, including protocol.
        :param data: Bytes to upload.
        """
