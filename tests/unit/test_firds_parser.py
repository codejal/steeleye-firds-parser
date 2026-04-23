"""Unit tests for FIRDSParser.to_csv."""

# pylint: disable=missing-function-docstring,missing-class-docstring,redefined-outer-name

import csv
from pathlib import Path

import pytest

from src.firds_parser.firds_parser import FIRDSParser

_NS_URI = "urn:iso:std:iso:20022:tech:xsd:auth.036.001.02"
_PFX = "auth"
CSV_HEADERS = [
    "FinInstrmGnlAttrbts.Id",
    "FinInstrmGnlAttrbts.FullNm",
    "FinInstrmGnlAttrbts.ClssfctnTp",
    "FinInstrmGnlAttrbts.CmmdtyDerivInd",
    "FinInstrmGnlAttrbts.NtnlCcy",
    "Issr",
]


def _make_xml(records: list[dict[str, str]]) -> str:
    def record(fields: dict[str, str]) -> str:
        attribs = "".join(f"<{_PFX}:{k}>{v}</{_PFX}:{k}>" for k, v in fields.items() if k != "Issr")
        issr = f"<{_PFX}:Issr>{fields['Issr']}</{_PFX}:Issr>" if "Issr" in fields else ""
        return (
            f"<{_PFX}:TermntdRcrd>"
            f"<{_PFX}:FinInstrmGnlAttrbts>{attribs}</{_PFX}:FinInstrmGnlAttrbts>"
            f"{issr}"
            f"</{_PFX}:TermntdRcrd>"
        )

    body = "".join(record(r) for r in records)
    return (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<{_PFX}:Document xmlns:{_PFX}="{_NS_URI}">'
        f"{body}"
        f"</{_PFX}:Document>"
    )


@pytest.fixture()
def sample_xml(tmp_path: Path) -> Path:
    xml = _make_xml(
        [
            {
                "Id": "IE00B4L5Y983",
                "FullNm": "iShares Core MSCI World",
                "ClssfctnTp": "UCITS",
                "CmmdtyDerivInd": "false",
                "NtnlCcy": "USD",
                "Issr": "BLACKROCK",
            },
            {
                "Id": "US0378331005",
                "FullNm": "Apple Inc",
                "ClssfctnTp": "ESVUFR",
                "CmmdtyDerivInd": "false",
                "NtnlCcy": "USD",
                "Issr": "APPLE",
            },
        ]
    )
    p = tmp_path / "sample.xml"
    p.write_text(xml, encoding="utf-8")
    return p


class DescribeToCSV:
    def it_should_return_correct_row_count(self, sample_xml: Path, tmp_path: Path) -> None:
        count = FIRDSParser(sample_xml).to_csv(tmp_path / "out.csv")
        assert count == 2

    def it_should_write_correct_headers(self, sample_xml: Path, tmp_path: Path) -> None:
        out = tmp_path / "out.csv"
        FIRDSParser(sample_xml).to_csv(out)
        with out.open(encoding="utf-8") as fh:
            assert next(csv.reader(fh)) == CSV_HEADERS

    def it_should_write_correct_data(self, sample_xml: Path, tmp_path: Path) -> None:
        out = tmp_path / "out.csv"
        FIRDSParser(sample_xml).to_csv(out)
        with out.open(encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        assert rows[0]["FinInstrmGnlAttrbts.Id"] == "IE00B4L5Y983"
        assert rows[0]["Issr"] == "BLACKROCK"
        assert rows[1]["FinInstrmGnlAttrbts.FullNm"] == "Apple Inc"

    def it_should_create_missing_parent_directories(self, sample_xml: Path, tmp_path: Path) -> None:
        out = tmp_path / "nested" / "deep" / "out.csv"
        FIRDSParser(sample_xml).to_csv(out)
        assert out.exists()

    def it_should_write_missing_fields_as_empty(self, tmp_path: Path) -> None:
        xml_path = tmp_path / "sparse.xml"
        xml_path.write_text(_make_xml([{"Id": "IE00B4L5Y983"}]), encoding="utf-8")
        out = tmp_path / "out.csv"
        FIRDSParser(xml_path).to_csv(out)
        with out.open(encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        assert rows[0]["FinInstrmGnlAttrbts.Id"] == "IE00B4L5Y983"
        assert rows[0]["Issr"] == ""
        assert rows[0]["FinInstrmGnlAttrbts.FullNm"] == ""

    def it_should_write_only_header_for_empty_xml(self, tmp_path: Path) -> None:
        xml_path = tmp_path / "empty.xml"
        xml_path.write_text(_make_xml([]), encoding="utf-8")
        out = tmp_path / "out.csv"
        count = FIRDSParser(xml_path).to_csv(out)
        assert count == 0
        with out.open(encoding="utf-8") as fh:
            assert len(fh.readlines()) == 1
