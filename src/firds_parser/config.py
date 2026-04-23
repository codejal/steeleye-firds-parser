# src/steeleye_assessment/config.py
"""Application configuration."""

REGISTRY_URL = (
    "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select"
    "?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D"
    "&wt=xml&indent=true&start=0&rows=100"
)

DEFAULT_HTTP_TIMEOUT = 30.0
DEFAULT_HTTP_RETRIES = 3
DEFAULT_HTTP_EXPONENTIAL_BACKOFF_MULTIPLIER = 1
DEFAULT_HTTP_EXPONENTIAL_BACKOFF_MIN_TIME = 2
DEFAULT_HTTP_EXPONENTIAL_BACKOFF_MAX_TIME = 10
DEFAULT_DOWNLOAD_PROGRESS_LOG_INTERVAL_MB = 20
DEFAULT_DOWNLOAD_CHUNK_SIZE_KB = 64

DEFAULT_OUTPUT_PATH = "<your local path>"
