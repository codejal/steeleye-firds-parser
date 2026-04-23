"""End-to-end pipeline: fetch the FIRDS registry, download the DLTINS zip,
parse the XML into a CSV, and enrich it with letter-count columns."""
from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import fsspec
import pandas as pd
from .config import (
        DEFAULT_HTTP_RETRIES, 
        DEFAULT_HTTP_TIMEOUT,
        DEFAULT_OUTPUT_PATH, 
        REGISTRY_URL, 
        DEFAULT_EXTRACTED_PATH
)
from .http_downloader import HttpDownloader
from .registry_parser import RegistryParser
from .utils import extract_zip

from .firds_parser import FIRDSParser

logger = logging.getLogger(__name__)

OUTPUT_CSV = Path(__file__).parent / "temp" / "output.csv"
DLTINS_FILE_TYPE = "DLTINS"
DLTINS_LINK_INDEX = 1
FULL_NM_CLM = "FinInstrmGnlAttrbts.FullNm"


class Pipeline:
    """Orchestrates the full FIRDS data pipeline from download to enriched CSV output."""

    def __init__(self, output_path: Path = OUTPUT_CSV) -> None:
        """Set the local path where the parsed CSV will be written.

        Args:
            output_path: Destination file for the parsed CSV. Defaults to
                a ``temp/output.csv`` file next to this module.
        """
        self.output_path = output_path

    def fetch_download_link(self) -> str:
        """Download the FIRDS registry XML and return the DLTINS file's download URL.

        Returns:
            The direct download URL for the DLTINS zip file.
        """
        logger.info("Fetching registry XML from %s", REGISTRY_URL)
        with HttpDownloader(
            timeout=DEFAULT_HTTP_TIMEOUT, max_attempts=DEFAULT_HTTP_RETRIES
        ) as downloader:
            xml = downloader.download_bytes(REGISTRY_URL)
        link = RegistryParser(xml).get_nth_download_link(DLTINS_FILE_TYPE, DLTINS_LINK_INDEX)
        logger.info("Resolved download link: %s", link)
        return link

    def download_and_parse(self, download_link: str) -> int:
        """Download the DLTINS zip, extract it, parse each XML file, and write rows to CSV.

        Args:
            download_link: Direct URL to the DLTINS zip file.

        Returns:
            Total number of data rows written across all XML files.
        """
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            logger.debug("Working in temp directory: %s", tmp_dir.resolve())
            with HttpDownloader(
                timeout=DEFAULT_HTTP_TIMEOUT, max_attempts=DEFAULT_HTTP_RETRIES
            ) as downloader:
                zip_path = downloader.download_to_file(download_link, tmp_dir / "dltins.zip")
            logger.info("Downloaded zip to %s", zip_path)

            xml_files = extract_zip(zip_path, Path(DEFAULT_EXTRACTED_PATH))
            logger.debug("Extracted %d file(s) from zip", len(xml_files))
            total_rows = 0
            for xml_file in xml_files:
                logger.info("Parsing %s", xml_file)
                count = FIRDSParser(xml_file).to_csv(str(self.output_path))
                logger.info("Parsed %s -> %d rows written to %s", xml_file, count, self.output_path)
                total_rows += count

        logger.info("Total rows written across all files: %d", total_rows)
        return total_rows

    def enrich_csv(self) -> None:
        """Add ``a_count`` and ``contains_a`` columns to the parsed CSV, then write to storage.

        ``a_count`` is the number of times the letter "a" (case-insensitive) appears in the
        instrument's full name. ``contains_a`` is "YES" if that count is greater than zero,
        otherwise "NO". The enriched file is written to ``DEFAULT_OUTPUT_PATH``.
        """
        logger.info("Enriching %s with a_count and contains_a", self.output_path)
        df = pd.read_csv(self.output_path, dtype=str)
        df["a_count"] = df[FULL_NM_CLM].fillna("").str.lower().str.count("a")
        df["contains_a"] = df["a_count"].apply(lambda x: "YES" if x > 0 else "NO")

        with fsspec.open(DEFAULT_OUTPUT_PATH, "w") as f:
            df.to_csv(f, index=False)
        logger.info("Enrichment complete — %d rows written to %s", len(df), self.output_path)

    def run(self) -> None:
        """Run the full pipeline: fetch link, download and parse, then enrich the CSV."""
        logger.info("Pipeline started")
        link = self.fetch_download_link()
        self.download_and_parse(link)
        self.enrich_csv()
        logger.info("Pipeline finished")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    Pipeline().run()
