"""How many rows a run wrote, read from the Parquet files it left in the lake.

The count is the sum of the row counts in the footers of the ``part-*.parquet`` files of one ``ingest_date=``
partition, which DuckDB reads without scanning the data. DuckDB and its ``azure`` extension are in the dbt venv of the
Airflow image (ADR-0004), so a short subprocess of that interpreter does the counting and nothing is added to Airflow's
own environment. The Flink UI can't give this number: a job whose source and sink run in one chained task shows 0
records.

Counting never fails a run: when the count can't be made (no key, the storage can't be reached) the run is still
reported, with the reason. No Airflow import.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess

log = logging.getLogger(__name__)

# The interpreter that has duckdb and its azure extension (installed when the image is built).
DUCKDB_PYTHON = os.environ.get("DBT_PYTHON", "/opt/dbt-venv/bin/python")
# The ADLS account key, in the environment of the Airflow scheduler (where tasks run); optional.
ACCOUNT_KEY_ENV = "ADLS_ACCOUNT_KEY"
COUNT_TIMEOUT_S = 180

_ABFSS = re.compile(r"^abfss://(?P<container>[^@/]+)@(?P<account>[^./]+)\.dfs\.core\.windows\.net/(?P<path>.*)$")

# Runs in the dbt venv. The key comes through the environment, never the command line, so `ps` doesn't show it.
_SCRIPT = """
import os, sys, duckdb

con = duckdb.connect()
key = os.environ.get("LAKE_ACCOUNT_KEY")
if key:
    account = os.environ["LAKE_ACCOUNT"]
    con.execute("LOAD azure")
    con.execute(
        "CREATE SECRET (TYPE AZURE, CONNECTION_STRING "
        f"'AccountName={account};AccountKey={key};EndpointSuffix=core.windows.net')"
    )
print(con.execute("SELECT count(*) FROM read_parquet(?)", [sys.argv[1]]).fetchone()[0])
"""


class CountError(Exception):
    """The rows couldn't be counted; the message says why, and never holds the account key."""


def table_name(sink_path: str) -> str:
    """The table of a sink path ``<system>/<schema>/<table>``: its last part, for example ``customers``."""
    return sink_path.strip("/").rsplit("/", 1)[-1]


def partition_uri(sink_uri: str, ingest_date: str) -> str:
    """Where a run leaves its files: ``<table root>/ingest_date=<date>``."""
    return f"{sink_uri.rstrip('/')}/ingest_date={ingest_date}"


def _duckdb_location(uri: str) -> tuple[str, str | None]:
    """The URI as DuckDB's azure extension reads it, and the storage account (None for a local path)."""
    found = _ABFSS.match(uri)
    if not found:
        return uri, None
    account, container, path = found["account"], found["container"], found["path"]
    return f"abfss://{account}.dfs.core.windows.net/{container}/{path}", account


def count_rows(location: str, account_key: str | None = None, python: str | None = None) -> int:
    """Rows in the Parquet files directly under ``location``; CountError when they can't be counted."""
    where, account = _duckdb_location(location)
    if account and not account_key:
        raise CountError(f"{ACCOUNT_KEY_ENV} isn't set in the Airflow environment")
    env = {
        **os.environ,
        "LAKE_ACCOUNT": account or "",
        "LAKE_ACCOUNT_KEY": account_key if account and account_key else "",
    }
    try:
        done = subprocess.run(
            [python or DUCKDB_PYTHON, "-c", _SCRIPT, f"{where.rstrip('/')}/*.parquet"],
            env=env,
            capture_output=True,
            text=True,
            timeout=COUNT_TIMEOUT_S,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise CountError(f"the counting process didn't run: {type(error).__name__}") from None
    if done.returncode != 0:
        reason = (done.stderr.strip().splitlines() or ["unknown error"])[-1]
        raise CountError(reason.replace(account_key, "***") if account_key else reason)
    try:
        return int(done.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        raise CountError("the counting process printed no number") from None


def run_message(ingest_date: str, rows: int | None, table: str, path: str, reason: str = "") -> str:
    """The sentence the task ``wait`` logs when a run is done."""
    if rows is None:
        return (
            f"Ingestion run for {ingest_date} finished; row count unavailable ({reason}) for table: {table} at {path}"
        )
    return f"Ingestion run for {ingest_date} with {rows} Rows for table: {table} at {path}"


def report_run(ingest_date: str, sink_uri: str, sink_path: str, account_key: str | None = None) -> dict[str, object]:
    """Counts the rows of a finished run, logs the sentence, and returns what it says; never raises."""
    path = partition_uri(sink_uri, ingest_date)
    table = table_name(sink_path)
    rows: int | None = None
    reason = ""
    try:
        rows = count_rows(path, account_key if account_key is not None else os.environ.get(ACCOUNT_KEY_ENV))
    except CountError as error:
        reason = str(error)
        log.warning("Rows of %s not counted: %s", path, reason)
    message = run_message(ingest_date, rows, table, path, reason)
    log.info(message)
    return {"ingest_date": ingest_date, "rows": rows, "table": table, "path": path, "message": message}
