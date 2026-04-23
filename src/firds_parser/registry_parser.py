"""Parser for ESMA FIRDS registry Solr responses."""

from __future__ import annotations

import logging
from xml.etree import ElementTree as ET

logger = logging.getLogger(__name__)


class RegistryParser:
    """Parses ESMA FIRDS registry XML to extract download links.

    The ESMA registry endpoint returns a Solr response listing published
    FIRDS files.
    Each <doc> element describes one file and carries
    file_type and download_link values as <str> children.
    """

    def __init__(self, xml_bytes: bytes) -> None:
        """Initialise the parser from raw XML bytes.

        Args:
            xml_bytes: The raw XML response body.

        Raises:
            ET.ParseError: If the bytes cannot be parsed as XML.
        """
        self._root = ET.fromstring(xml_bytes)

    def get_download_links(self, file_type: str) -> list[str]:
        """Return every download link whose ``file_type`` matches.

        Args:
            file_type: The file_type to filter on (e.g. ``"DLTINS"``).

        Returns:
            A list of download links in document order. Empty if none match.
        """
        links: list[str] = []
        for doc in self._root.iter("doc"):
            fields = {str_el.get("name"): str_el.text for str_el in doc.findall("str")}
            if fields.get("file_type") == file_type:
                link = fields.get("download_link")
                if link:
                    links.append(link)

        logger.info("Found %d download links for file_type=%s", len(links), file_type)
        return links

    def get_nth_download_link(self, file_type: str, n: int) -> str:
        """Return the nth (0-indexed) download link for a given file_type.

        Args:
            file_type: The file_type to filter on (e.g. ``"DLTINS"``).
            n: Zero-based index into the filtered links. ``n=1`` returns
                the second match.

        Returns:
            The requested download link.

        Raises:
            IndexError: If fewer than ``n + 1`` links exist for file_type.
        """
        links = self.get_download_links(file_type)
        if n >= len(links):
            raise IndexError(
                f"Requested link index {n} but only {len(links)} " f"{file_type} link(s) available"
            )
        return links[n]
