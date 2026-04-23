"""Cloud-agnostic storage implementation using fsspec."""

import logging
from typing import Any, Optional

import fsspec

from .base_storage import BaseStorage

logger = logging.getLogger(__name__)


class FsspecStorage(BaseStorage):
    """
    Storage backend powered by fsspec.
    The same instance can read and write to any backend
    supports — local files, AWS S3, Azure Blob, GCS, HTTP etc
    The backend is selected automatically by the protocol prefix in
    the path:

    * ``"/tmp/file.csv"`` or ``"file:///tmp/file.csv"`` -> local disk
    * ``"s3://bucket/key.csv"`` -> AWS S3
    * ``"abfs://container/key.csv"`` -> Azure Blob Storage
    * ``"gs://bucket/key.csv"`` -> Google Cloud Storage

    Creds are picked up from the environment by default
    Pass ``storage_options`` if overwrite is required for creds
    """

    def __init__(self, storage_options: Optional[dict[str, Any]] = None) -> None:
        """
        Initialise the storage client.

        :param storage_options: Optional kwargs used to inject credentials per
        :example
            {"key": "...", "secret": "..."} for S3 or
            {"account_name": "...", "account_key": "..."} for Azure.
        """
        self._storage_options = storage_options or {}

    def read(self, path: str) -> Any:
        """
        Read bytes from any fsspec-supported path.

        :param path: Full path including protocol
        :returns: Object contents as bytes.
        """
        logger.info("Reading from %s using fsspec storage", path)
        with fsspec.open(path, "rb", **self._storage_options) as f:
            return f.read()

    def write(self, path: str, data: bytes) -> None:
        """
        Write bytes to any fsspec-supported path.

        :param path: Destination path including protocol
        :param data: Bytes to upload.
        """
        logger.info("Writing %d bytes to %s using fsspec storage", len(data), path)
        with fsspec.open(path, "wb", **self._storage_options) as f:
            f.write(data)
