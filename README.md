# steeleye-firds-parser

> Fetches the ESMA FIRDS instrument registry, downloads the daily DLTINS file,
> stream-parses millions of XML records into a CSV, and enriches it — all with
> automatic retries and cloud-native output support.

**What is FIRDS?**
FIRDS (Financial Instruments Reference Data System) is the ESMA database of all
financial instruments traded in the EU. Every trading day, ESMA publishes a
"DLTINS" (Delta Instruments) XML file listing new, updated, and terminated
instruments. This pipeline automates downloading and processing that file.

---

## Table of Contents

1. [Quick Start](#1-quick-start)
2. [What the Output Looks Like](#2-what-the-output-looks-like)
3. [Design Highlights](#3-design-highlights)
4. [Classes & How They Connect](#4-classes--how-they-connect)
5. [Project Structure](#5-project-structure)
6. [Assumptions & Decisions](#6-assumptions--decisions)
7. [Future Improvements](#7-future-improvements)

---

## 1. Quick Start

**Prerequisites:** Python 3.14+ and [uv](https://github.com/astral-sh/uv)

```bash
# 1. Install dependencies
uv sync --all-groups

# 2. Set your output path in src/firds_parser/config.py
DEFAULT_OUTPUT_PATH = "/tmp/firds_output.csv"

# 3. Run
uv run python -m src.firds_parser.main
```

**Run tests, linting, and type checks:**

```bash
uv run pytest
uv run mypy src/
uv run pylint src/
uv run black src/
uv run isort src/
```

---

## 2. What the Output Looks Like

The pipeline produces a CSV with one row per financial instrument record. Two
enrichment columns are added after parsing.

| Column | Source | Description |
|---|---|---|
| `FinInstrmGnlAttrbts.Id` | XML | ISIN — unique instrument identifier |
| `FinInstrmGnlAttrbts.FullNm` | XML | Full instrument name |
| `FinInstrmGnlAttrbts.ClssfctnTp` | XML | CFI classification code |
| `FinInstrmGnlAttrbts.CmmdtyDerivInd` | XML | Commodity derivative flag |
| `FinInstrmGnlAttrbts.NtnlCcy` | XML | Notional currency (e.g. EUR, USD) |
| `Issr` | XML | Issuer LEI code |
| `a_count` | Enriched | How many times the letter "a" appears in `FullNm` |
| `contains_a` | Enriched | `"YES"` if `a_count > 0`, otherwise `"NO"` |

**Example rows:**

```
FinInstrmGnlAttrbts.Id,FinInstrmGnlAttrbts.FullNm,...,a_count,contains_a
DE000A1EWWW0,Adidas AG Senior Bond,...,3,YES
XS1234567890,Zero Coupon Note,...,0,NO
```

---

## 3. Design Highlights

These are deliberate engineering choices worth noting.

**Streaming XML parse — O(1) memory regardless of file size**
`FIRDSParser` uses `lxml.etree.iterparse` targeting only `TermntdRcrd` elements.
Each element is extracted and then immediately cleared from the tree, so a 10 GB
file uses the same peak memory as a 10 KB file.

**Retry with exponential backoff — not just a sleep loop**
`HttpDownloader` wraps every request with `tenacity`. It retries on transient
network/timeout errors, waits exponentially between attempts (2s → 4s → 8s, capped
at 10s), and re-raises immediately on 4xx/5xx responses — those are caller errors,
not transient failures.

**Storage abstraction — swap backends without touching pipeline logic**
`BaseStorage` defines a simple `read`/`write` interface. `FsspecStorage` implements
it using `fsspec`, which means the same code writes to local disk, AWS S3, Azure
Blob, or GCS just by changing the path prefix. No pipeline code changes needed.

**Temp directory — no leftover files**
The downloaded zip and extracted XML files live inside a `tempfile.TemporaryDirectory`
context manager. They are deleted automatically when the pipeline finishes or crashes.

**Strict static typing throughout**
The entire codebase passes `mypy --strict` with no suppressions except for third-party
libraries (`fsspec`, `lxml`) that ship without type stubs.

---

## 4. Classes & How They Connect

### Class overview

| Class | File | Responsibility |
|---|---|---|
| `Pipeline` | `main.py` | Top-level orchestrator — runs the three stages in order |
| `HttpDownloader` | `http_downloader.py` | HTTP client with retry/backoff; downloads to memory or file |
| `RegistryParser` | `registry_parser.py` | Parses the ESMA Solr XML response to extract download links |
| `FIRDSParser` | `firds_parser.py` | Stream-parses a DLTINS XML file and writes records to CSV |
| `BaseStorage` | `base_storage.py` | Abstract interface (`read` / `write`) for storage backends |
| `FsspecStorage` | `fsspec_storage.py` | Concrete backend — local, S3, Azure Blob, GCS via `fsspec` |

### Data flow

```
  ESMA Registry URL
        │
        ▼
┌─────────────────────┐
│   HttpDownloader    │  download_bytes()  ──►  raw XML bytes
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│   RegistryParser    │  get_nth_download_link("DLTINS", 1)  ──►  zip URL
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│   HttpDownloader    │  download_to_file()  ──►  dltins.zip  (temp dir)
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│     extract_zip     │  ──►  [file1.xml, file2.xml, ...]  (temp dir)
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│    FIRDSParser      │  to_csv()  ──►  output.csv  (local, one row per record)
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│  pandas enrichment  │  adds a_count + contains_a columns
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│   fsspec.open()     │  ──►  DEFAULT_OUTPUT_PATH  (local / S3 / Azure / GCS)
└─────────────────────┘
```

---

## 5. Project Structure

```
steeleye-firds-parser/
│
├── src/
│   └── firds_parser/
│       ├── __init__.py
│       ├── main.py             # Pipeline class — top-level entry point
│       ├── config.py           # All constants: URLs, timeouts, output path
│       ├── http_downloader.py  # HTTP client with retry/backoff (httpx + tenacity)
│       ├── registry_parser.py  # Parses ESMA Solr XML to find download links
│       ├── firds_parser.py     # Streaming lxml parser → CSV writer
│       ├── base_storage.py     # Abstract storage interface (ABC)
│       ├── fsspec_storage.py   # fsspec implementation (local, S3, Azure, GCS)
│       └── utils.py            # Zip extraction helper
│
├── tests/
│   ├── unit/                   # Unit tests per class
│   └── integration/            # Integration tests
│
├── main.py                     # Thin entry point
├── pyproject.toml              # Dependencies + tool config (mypy, ruff, pylint, pytest)
├── uv.lock                     # Pinned dependency versions
└── README.md
```

---

## 6. Assumptions & Decisions

| # | Assumption | Impact |
|---|---|---|
| 1 | The ESMA API can be slow or transiently unavailable | Retries with exponential backoff on every HTTP call |
| 2 | The XML schema (`auth.036.001.02`) does not change | Parser targets fixed tag names; schema change = code change |
| 3 | We want the second DLTINS link (index 1) | Hardcoded as `DLTINS_LINK_INDEX = 1` in `config.py` |
| 4 | Files inside the zip have unique names | Duplicate names would silently overwrite; acceptable for FIRDS format |
| 5 | Missing XML fields are non-fatal | Written as empty/`None` in CSV rather than raising an error |
| 6 | Output destination may be cloud storage | `DEFAULT_OUTPUT_PATH` is a `fsspec` URI — works with S3, Azure, GCS |
| 7 | No deduplication needed across files in the zip | Each XML file's records are appended as-is; no cross-file dedup |

**On retries:** The retry policy distinguishes between retryable failures
(transport errors, timeouts) and non-retryable ones (4xx/5xx HTTP status codes).
A `404` on the download link should surface immediately, not be retried 3 times.

**On streaming:** DLTINS files regularly exceed hundreds of megabytes and contain
millions of records. Loading the full XML DOM into memory would make this
impractical. Streaming parse was a requirement, not an optimisation.

---

## 7. Future Improvements

### High value / low effort

- **CLI / env-var configuration** — expose `REGISTRY_URL`, `DLTINS_LINK_INDEX`, and
  `DEFAULT_OUTPUT_PATH` as environment variables or `argparse` flags so the pipeline
  can target different date ranges without code changes.

- **Schema validation** — warn or fail early when a required field (`Id`, `FullNm`)
  is missing from a record, rather than silently writing `None` into the CSV.

### Medium effort

- **Streaming enrichment** — `enrich_csv` currently loads the full CSV into a pandas
  DataFrame. For very large files, switching to `pd.read_csv(chunksize=N)` would make
  the enrichment step as memory-efficient as the parse step.

- **Deduplication** — if the zip ever contains overlapping records across files, a
  post-processing pass keyed on `FinInstrmGnlAttrbts.Id` could remove duplicates
  before writing the final output.

- **HTTP range-request resume** — `HttpDownloader` retries from byte 0 on failure.
  Adding `Range: bytes=N-` support would let large downloads resume mid-file instead
  of restarting.

### CI/CD & code quality automation

- **GitHub Actions CI pipeline** — add a `.github/workflows/ci.yml` that runs on every
  push and pull request. A minimal pipeline would: install dependencies with `uv sync`,
  run `mypy`, `pylint`, `ruff`, and `pytest` in sequence, and block merges if any step
  fails. This catches type errors and regressions before they reach `main`.

- **Pre-commit hooks** — the project already has `pre-commit` in dev dependencies. Wiring
  up hooks for `ruff` (lint + format), `mypy`, `isort`, and `black` would enforce code
  quality locally before a commit is even created, so CI rarely has to reject anything.

- **Automated dependency updates** — tools like Dependabot or Renovate Bot can open
  pull requests automatically when a dependency releases a new version, keeping the
  lockfile current and security patches applied without manual effort.

- **Coverage enforcement** — add `pytest-cov` to the CI step and set a minimum coverage
  threshold (e.g. 80%). PRs that drop coverage below the threshold are blocked, making
  it harder to ship untested code silently.

- **CD — publish to PyPI or a private registry** — add a `release.yml` workflow
  triggered on a Git tag push that builds the package with `uv build` and publishes it.
  This would let the parser be installed as a library (`pip install steeleye-firds-parser`)
  rather than always run from source.

- **Docker image build in CI** — build and push a container image on each release tag.
  The pipeline could then be run anywhere Docker is available (Kubernetes, ECS, Cloud Run)
  without needing Python or `uv` installed on the host.

### Larger scope

- **Structured logging** — switching from plain text logs to JSON (e.g. `python-json-logger`)
  would make log output ingestible by Datadog, CloudWatch, or Grafana Loki and enable
  alerting on row counts or error rates.

- **Automated scheduling** — the pipeline is currently triggered manually. A cron job,
  Airflow DAG, or cloud function could run it automatically on each ESMA publication date.

- **Observability / metrics** — emit row counts, download sizes, and elapsed time as
  structured metrics after each run to track data quality trends over time.
