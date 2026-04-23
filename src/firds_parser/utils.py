"""Zip extraction utility."""

from __future__ import annotations

import logging
import zipfile
from pathlib import Path

logger = logging.getLogger(__name__)


def extract_zip(zip_path: Path, dest_dir: Path) -> list[Path]:
    """Extract a zip archive to ``dest_dir`` and return the extracted paths."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Extracting %s -> %s", zip_path, dest_dir)

    with zipfile.ZipFile(zip_path) as archive:
        archive.extractall(dest_dir)
        names = archive.namelist()

    return [dest_dir / name for name in names]
