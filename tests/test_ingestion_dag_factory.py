"""One DAG per job file: the contract with data-onboarding-ingestion's files (ADR-0002)."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import pytest
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


def test_an_invalid_file_is_skipped_and_the_others_load(jobs_module: Any, tmp_path: Path, monkeypatch: Any) -> None:
    jobs = tmp_path / "jobs"
    shutil.copytree(FIXTURES / "jobs", jobs)
    (jobs / "broken.yaml").write_text("kind: IngestionJob\nname: broken\n", encoding="utf-8")
    (jobs / "wrong-name.yaml").write_text(
        (FIXTURES / "jobs" / "sales-orders-daily.yaml").read_text(encoding="utf-8"), encoding="utf-8"
    )
    monkeypatch.setattr(jobs_module, "JOBS_DIR", jobs)

    valid, invalid = jobs_module.load_jobs()

    assert len(valid) == 4
    assert set(invalid) == {"broken.yaml", "wrong-name.yaml"}
    assert "does not match" in invalid["wrong-name.yaml"]


def test_the_runner_gets_the_job_file_the_run_and_the_date(flink_module: Any) -> None:
    arguments = flink_module.runner_arguments(
        "shop-orders-daily.yaml", "manual__2026-11-06T08:00:00+00:00", "2026-11-06"
    )

    assert arguments[:2] == ["-py", flink_module.RUNNER_MAIN]
    assert arguments[arguments.index("--job") + 1] == f"{flink_module.FLINK_PIPELINES_DIR}/jobs/shop-orders-daily.yaml"
    assert arguments[arguments.index("--run-id") + 1] == "manual__2026-11-06T08_00_00_00_00"
    assert arguments[arguments.index("--ingest-date") + 1] == "2026-11-06"
    assert arguments[arguments.index("--target") + 1] == flink_module.RUNNER_TARGET == "final"
    assert "--no-wait" in arguments


class FakeFlink:
    """Records the calls of ``flink_request`` and answers like the JobManager: a jar list, a run, then job states."""

    def __init__(self, states: list[str]) -> None:
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        self.states = states

    def __call__(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append((method, path, kwargs))
        if path == "/jars":
            return {"files": [{"name": "flink-python.jar", "id": "abc_flink-python.jar"}]}
        if path.endswith("/run"):
            return {"jobid": "job-1"}
        if path.startswith("/jobs/"):
            return {"state": self.states.pop(0)}
        return {}


def test_submit_runs_the_python_driver_on_the_runner(flink_module: Any, monkeypatch: Any) -> None:
    fake = FakeFlink([])
    monkeypatch.setattr(flink_module, "flink_request", fake)

    job_id = flink_module.submit_run("shop-orders-daily.yaml", "manual__x", "2026-11-06", 2)

    assert job_id == "job-1"
    method, path, kwargs = fake.calls[-1]
    assert (method, path) == ("POST", "/jars/abc_flink-python.jar/run")
    assert kwargs["json"]["entryClass"] == flink_module.PYTHON_DRIVER
    assert kwargs["json"]["parallelism"] == 2


def test_wait_returns_when_the_job_finishes_and_fails_otherwise(flink_module: Any, monkeypatch: Any) -> None:
    from datetime import timedelta

    monkeypatch.setattr(flink_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(flink_module, "flink_request", FakeFlink(["RUNNING", "FINISHED"]))
    assert flink_module.wait_for_job("job-1", timedelta(hours=1)) == "FINISHED"

    monkeypatch.setattr(flink_module, "flink_request", FakeFlink(["RUNNING", "FAILED"]))
    with pytest.raises(RuntimeError, match="ended in state FAILED"):
        flink_module.wait_for_job("job-1", timedelta(hours=1))


def test_wait_cancels_the_job_after_the_timeout(flink_module: Any, monkeypatch: Any) -> None:
    from datetime import timedelta

    fake = FakeFlink(["RUNNING"] * 5)
    monkeypatch.setattr(flink_module, "flink_request", fake)
    monkeypatch.setattr(flink_module.time, "sleep", lambda _: None)

    with pytest.raises(TimeoutError, match="cancelled"):
        flink_module.wait_for_job("job-1", timedelta(seconds=-1))

    assert ("PATCH", "/jobs/job-1?mode=cancel") in [(m, p) for m, p, _ in fake.calls]
