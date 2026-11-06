"""The row count of a run: read from Parquet footers, reported in one sentence, and never fatal."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import duckdb
import pytest

ABFSS = "abfss://raw@acct.dfs.core.windows.net/parquet/mysql/shop/customers"


@pytest.fixture
def lake(factory: Any) -> Any:
    """``common.lake_rows``, imported after the factory fixture has put ``dags`` on the path."""
    import importlib

    return importlib.import_module("common.lake_rows")


def _write_parquet(directory: Path, name: str, rows: int) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    duckdb.connect().execute(
        f"COPY (SELECT range AS id FROM range({rows})) TO '{(directory / name).as_posix()}' (FORMAT parquet)"
    )


def test_table_and_partition_come_from_the_sink(lake: Any) -> None:
    assert lake.table_name("mysql/shop/customers") == "customers"
    assert lake.table_name("orders") == "orders"
    assert lake.partition_uri(ABFSS + "/", "2026-11-06") == ABFSS + "/ingest_date=2026-11-06"


def test_azure_locations_are_given_to_duckdb_in_its_own_form(lake: Any) -> None:
    where, account = lake._duckdb_location(ABFSS + "/ingest_date=2026-11-06")
    assert where == "abfss://acct.dfs.core.windows.net/raw/parquet/mysql/shop/customers/ingest_date=2026-11-06"
    assert account == "acct"
    assert lake._duckdb_location("/data/x") == ("/data/x", None)


def test_rows_are_summed_over_the_files_of_the_partition(lake: Any, tmp_path: Path) -> None:
    partition = tmp_path / "ingest_date=2026-11-06"
    _write_parquet(partition, "part-a.parquet", 7000)
    _write_parquet(partition, "part-b.parquet", 3000)
    _write_parquet(tmp_path / "ingest_date=2026-11-05", "part-c.parquet", 99)  # another day: not counted

    assert lake.count_rows(partition.as_posix(), python=sys.executable) == 10000


def test_azure_without_a_key_says_so_instead_of_trying(lake: Any) -> None:
    with pytest.raises(lake.CountError, match="ADLS_ACCOUNT_KEY"):
        lake.count_rows(ABFSS, account_key=None)


def test_an_empty_partition_is_a_count_error_not_a_crash(lake: Any, tmp_path: Path) -> None:
    with pytest.raises(lake.CountError):
        lake.count_rows((tmp_path / "ingest_date=2026-11-06").as_posix(), python=sys.executable)


def test_the_report_is_the_sentence_with_date_rows_table_and_path(lake: Any, tmp_path: Path) -> None:
    partition = tmp_path / "parquet" / "mysql" / "shop" / "customers" / "ingest_date=2026-11-06"
    _write_parquet(partition, "part-a.parquet", 42)
    sink_uri = (tmp_path / "parquet" / "mysql" / "shop" / "customers").as_posix()
    lake.DUCKDB_PYTHON = sys.executable

    report = lake.report_run("2026-11-06", sink_uri, "mysql/shop/customers")

    assert report["rows"] == 42
    assert report["message"] == (
        f"Ingestion run for 2026-11-06 with 42 Rows for table: customers at {sink_uri}/ingest_date=2026-11-06"
    )


def test_a_run_whose_rows_cant_be_counted_is_still_reported(lake: Any) -> None:
    report = lake.report_run("2026-11-06", ABFSS, "mysql/shop/customers", account_key="")

    assert report["rows"] is None
    assert "row count unavailable" in report["message"]
    assert "for table: customers at " + ABFSS + "/ingest_date=2026-11-06" in report["message"]


def test_the_key_never_appears_in_an_error(lake: Any, monkeypatch: Any) -> None:
    class Done:
        returncode = 1
        stdout = ""
        stderr = "IO Error: bad key SECRET123 for account"

    monkeypatch.setattr(lake.subprocess, "run", lambda *a, **k: Done())

    with pytest.raises(lake.CountError) as error:
        lake.count_rows(ABFSS, account_key="SECRET123")

    assert "SECRET123" not in str(error.value)
