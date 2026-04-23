"""Stream-parse FIRDS DLTINS XML files and write selected fields to CSV."""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Iterator

from lxml import etree

logger = logging.getLogger(__name__)

# Namespace URIs used in FIRDS XML files
_AUTH_NS = "urn:iso:std:iso:20022:tech:xsd:auth.036.001.02"

# Clark-notation tags (lxml uses the same {uri}local form)
_RECORD_TAG = f"{{{_AUTH_NS}}}TermntdRcrd"
_ATTRIBS_TAG = f"{{{_AUTH_NS}}}FinInstrmGnlAttrbts"
_ISSR_TAG = f"{{{_AUTH_NS}}}Issr"

# Fields extracted from FinInstrmGnlAttrbts, in CSV column order
_ATTRIB_FIELDS = ("Id", "FullNm", "ClssfctnTp", "CmmdtyDerivInd", "NtnlCcy")

CSV_HEADERS = [
    "FinInstrmGnlAttrbts.Id",
    "FinInstrmGnlAttrbts.FullNm",
    "FinInstrmGnlAttrbts.ClssfctnTp",
    "FinInstrmGnlAttrbts.CmmdtyDerivInd",
    "FinInstrmGnlAttrbts.NtnlCcy",
    "Issr",
]


def _extract_record(elem: etree._Element) -> dict[str, str | None]:
    """Pull the fields we care about out of a single TermntdRcrd XML element.

    Looks inside the FinInstrmGnlAttrbts child for instrument attributes
    (ISIN, name, classification, etc.) and grabs the Issr (issuer) text
    directly from the record element.  Any field not present in the XML is
    left as None so the CSV row always has every column.

    Args:
        elem: A parsed TermntdRcrd lxml element.

    Returns:
        A dict keyed by CSV_HEADERS with string values (or None if missing).
    """
    row: dict[str, str | None] = {h: None for h in CSV_HEADERS}

    attribs = elem.find(_ATTRIBS_TAG)
    if attribs is not None:
        for field in _ATTRIB_FIELDS:
            child = attribs.find(f"{{{_AUTH_NS}}}{field}")
            if child is not None:
                row[f"FinInstrmGnlAttrbts.{field}"] = child.text

    row["Issr"] = elem.findtext(_ISSR_TAG)
    return row


class FIRDSParser:  # pylint: disable=too-few-public-methods
    """Stream-parse a FIRDS DLTINS XML file and write selected fields to CSV.

    Uses lxml.etree.iterparse targeted at TermntdRcrd so libxml2 fires only
    on those elements.  Preceding siblings are deleted after each yield so
    memory stays flat regardless of file size.
    """

    def __init__(self, xml_path: str | Path) -> None:
        """Store the path to the XML file that will be parsed.

        Args:
            xml_path: Path to a FIRDS DLTINS XML file (string or Path object).
        """
        self.xml_path = Path(xml_path)

    def _iter_records(self) -> Iterator[dict[str, str | None]]:
        """Yield one dict per TermntdRcrd element found in the XML file.

        Uses lxml's iterparse in streaming mode so only one record lives in
        memory at a time — safe for files with millions of rows.  After each
        record is yielded the element is cleared and its already-processed
        siblings are removed from the tree to free memory immediately.

        Yields:
            Dicts keyed by CSV_HEADERS; missing fields are None.
        """
        logger.debug("Starting iterparse on %s", self.xml_path)
        context = etree.iterparse(  # pylint: disable=c-extension-no-member
            str(self.xml_path),
            events=("end",),
            tag=_RECORD_TAG,
            huge_tree=False,
            recover=False,
            resolve_entities=False,
        )

        for _event, elem in context:
            try:
                yield _extract_record(elem)
            finally:
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

        del context

    def to_csv(self, output_path: str | Path) -> int:
        """Parse the XML file and write every record as a row in a CSV file.

        Creates any missing parent directories automatically.  The CSV will
        have a header row followed by one data row per TermntdRcrd element.

        Args:
            output_path: Destination path for the CSV file.

        Returns:
            Number of data rows written (not counting the header).
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Writing CSV to %s", output_path)

        count = 0
        with output_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_HEADERS)
            writer.writeheader()
            for row in self._iter_records():
                writer.writerow(row)
                count += 1
                if count % 10_000 == 0:
                    logger.debug("Processed %d records so far", count)

        logger.info("Finished writing %d records to %s", count, output_path)
        return count
