"""HTTP download utilities with retry support."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Self

import httpx
from tenacity import (retry, retry_if_exception_type, stop_after_attempt,
                      wait_exponential)

from .config import (DEFAULT_DOWNLOAD_CHUNK_SIZE_KB,
                     DEFAULT_DOWNLOAD_PROGRESS_LOG_INTERVAL_MB,
                     DEFAULT_HTTP_EXPONENTIAL_BACKOFF_MAX_TIME,
                     DEFAULT_HTTP_EXPONENTIAL_BACKOFF_MIN_TIME,
                     DEFAULT_HTTP_EXPONENTIAL_BACKOFF_MULTIPLIER,
                     DEFAULT_HTTP_RETRIES, DEFAULT_HTTP_TIMEOUT)

logger = logging.getLogger(__name__)

_RETRYABLE = (httpx.TransportError, httpx.TimeoutException)


class HttpDownloader:
    """Download content over HTTP with retries on transient failures."""

    def __init__(
        self, timeout: float = DEFAULT_HTTP_TIMEOUT, max_attempts: int = DEFAULT_HTTP_RETRIES
    ) -> None:
        """Initialise the downloader with exponential back-off retry logic.

        Retries are triggered on _RETRYABLE
        The wait between attempts follows
        2^attempt * DEFAULT_HTTP_EXPONENTIAL_BACKOFF_MULTIPLIER sec, clamped to
        starting with DEFAULT_HTTP_EXPONENTIAL_BACKOFF_MIN_TIME sec,
        and max capped at DEFAULT_HTTP_EXPONENTIAL_BACKOFF_MAX_TIME sec.

        Args:
            timeout: Per-request timeout in seconds applied to every HTTP call.
            max_attempts: Total number of attempts (initial try + retries) before
                the exception is re-raised.
        """
        self._client = httpx.Client(timeout=timeout)
        self._retry = retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(
                multiplier=DEFAULT_HTTP_EXPONENTIAL_BACKOFF_MULTIPLIER,
                min=DEFAULT_HTTP_EXPONENTIAL_BACKOFF_MIN_TIME,
                max=DEFAULT_HTTP_EXPONENTIAL_BACKOFF_MAX_TIME,
            ),
            retry=retry_if_exception_type(_RETRYABLE),
            reraise=True,
        )

    def download_bytes(self, url: str) -> bytes:
        """Download a URL's content into memory and return it as bytes.

        The request is retried automatically on transient transport or timeout
        errors according to the back-off policy configured in __init__.

        Args:
            url: URL to fetch.

        Returns:
            Raw response body as bytes

        Raises:
            httpx.HTTPStatusError: If the server returns a 4xx or 5xx status
                code (not retried).
            httpx.TransportError: If all retry attempts are exhausted due to a
                transport-level failure.
            httpx.TimeoutException: If all retry attempts are exhausted due to
                request timeouts.
        """
        logger.info("Downloading %s", url)

        @self._retry
        def _get() -> bytes:
            response = self._client.get(url)
            response.raise_for_status()
            return response.content

        return _get()

    def download_to_file(self, url: str, dest: Path) -> Path:
        """Stream a URL's content to a local file and return the destination path.

        Content is written in DEFAULT_DOWNLOAD_CHUNK_SIZE_KB KiB chunks
        so that large files are never fully buffered in memory.
        Download progress is logged at INFO level every
        DEFAULT_DOWNLOAD_PROGRESS_LOG_INTERVAL_MB MiB.
        Parent directories are created automatically if they do not exist.
        The request is retried on transient failures according to the
        back-off policy configured in __init__.

        Args:
            url: Fully-qualified URL to stream.
            dest: Local filesystem path where the content will be written.
                Existing files are overwritten.

        Returns:
            The resolved dest path after the download completes.

        Raises:
            httpx.HTTPStatusError: If the server returns a 4xx or 5xx status
                code (not retried).
            httpx.TransportError: If all retry attempts are exhausted due to a
                transport-level failure.
            httpx.TimeoutException: If all retry attempts are exhausted due to
                request timeouts.
            OSError: If the destination file cannot be created or written to.
        """
        logger.info("Streaming %s -> %s", url, dest)
        dest.parent.mkdir(parents=True, exist_ok=True)

        @self._retry
        def _stream() -> None:
            with self._client.stream("GET", url) as response:
                response.raise_for_status()
                total_header = response.headers.get("Content-Length")
                total_bytes = int(total_header) if total_header and total_header.isdigit() else None
                downloaded = 0
                log_every_bytes = DEFAULT_DOWNLOAD_PROGRESS_LOG_INTERVAL_MB * 1024 * 1024
                next_log_at = log_every_bytes
                with dest.open("wb") as fh:
                    for chunk in response.iter_bytes(DEFAULT_DOWNLOAD_CHUNK_SIZE_KB * 1024):
                        fh.write(chunk)
                        downloaded += len(chunk)
                        if downloaded >= next_log_at:
                            if total_bytes:
                                pct = (downloaded / total_bytes) * 100
                                logger.info(
                                    "Downloaded %.1f%% (%d/%d bytes) -> %s",
                                    pct,
                                    downloaded,
                                    total_bytes,
                                    dest,
                                )
                            else:
                                logger.info("Downloaded %d bytes -> %s", downloaded, dest)
                            next_log_at += log_every_bytes

        _stream()
        return dest

    def close(self) -> None:
        """Close the underlying HTTP client and release its resources.

        Safe to call multiple times.  Prefer using :class:`HttpDownloader` as a
        context manager (with statement) so this is called automatically.
        """
        self._client.close()

    def __enter__(self) -> Self:
        """Return the downloader itself for use as a context manager.

        Returns:
            This :class:`HttpDownloader` instance.
        """
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        """Close the underlying HTTP client when leaving the with block.

        Args:
            exc_type: Exception class, or None if no exception was raised.
            exc: Exception instance, or None.
            tb: Traceback, or None.
        """
        self.close()
