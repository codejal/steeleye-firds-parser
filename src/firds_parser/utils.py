"""Zip extraction utility."""

from __future__ import annotations

import logging
import zipfile
from pathlib import Path

logger = logging.getLogger(__name__)


def extract_zip(zip_path: Path, dest_dir: Path) -> list[Path]:
    """
    Assumption: The file extracted will have unique name,
    or we can handle this by adding specific prefixes to dest_dir

    Extract all members from a ZIP archive and return extracted paths.

    Args:
        zip_path: Path to the source ZIP archive.
        dest_dir: Destination directory where archive members are extracted.
            The directory is created if it does not already exist.

    Returns:
        A list of paths for all archive members in ZIP order.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Extracting %s -> %s", zip_path, dest_dir)

    with zipfile.ZipFile(zip_path) as archive:
        archive.extractall(dest_dir)
        names = archive.namelist()

    return [dest_dir / name for name in names]
