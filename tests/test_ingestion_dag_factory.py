"""One DAG per job file: the contract with data-onboarding-ingestion's files (ADR-0002)."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from conftest import FIXTURES


def test_every_fixture_job_becomes_a_dag(factory: Any) -> None:
    dags = {name for name in vars(factory) if name.startswith("ingest__")}
    assert dags == {
        "ingest__crm-customers-daily",
        "ingest__sales-orders-daily",
        "ingest__shop-api-orders-daily",
        "ingest__shop-customers-by-segment-daily",
    }
    assert factory.INVALID_FILES == {}


def test_dag_settings_come_from_the_job(factory: Any) -> None:
    dag = getattr(factory, "ingest__shop-api-orders-daily")
    assert dag.kwargs["schedule"] == "0 3 * * *"
    assert dag.kwargs["max_active_runs"] == 1
    assert dag.kwargs["catchup"] is False
    assert dag.kwargs["default_args"]["pool"] == "conn__shop-api-nonprod"
    assert {"ingestion", "connection:shop-api-nonprod", "source:rest_api"} <= set(dag.kwargs["tags"])


def test_only_api_jobs_get_a_fetch_task(factory: Any) -> None:
    assert getattr(factory, "ingest__shop-api-orders-daily").tasks == ["validate", "fetch", "submit", "wait"]
    assert getattr(factory, "ingest__sales-orders-daily").tasks == ["validate", "submit", "wait"]


def test_an_invalid_file_is_skipped_and_the_others_load(factory: Any, tmp_path: Path, monkeypatch: Any) -> None:
    jobs = tmp_path / "jobs"
    shutil.copytree(FIXTURES / "jobs", jobs)
    (jobs / "broken.yaml").write_text("kind: IngestionJob\nname: broken\n", encoding="utf-8")
    (jobs / "wrong-name.yaml").write_text(
        (FIXTURES / "jobs" / "sales-orders-daily.yaml").read_text(encoding="utf-8"), encoding="utf-8"
    )
    monkeypatch.setattr(factory, "JOBS_DIR", jobs)

    valid, invalid = factory.load_jobs()

    assert len(valid) == 4
    assert set(invalid) == {"broken.yaml", "wrong-name.yaml"}
    assert "does not match" in invalid["wrong-name.yaml"]
