"""One DAG per transformation file, scheduled on the Assets of ingestion (ADR-0004)."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

from conftest import TRANSFORMATION_FIXTURES


def test_every_fixture_file_becomes_a_dag(transformation_factory: Any) -> None:
    dags = {name for name in vars(transformation_factory) if name.startswith("transform__")}
    assert dags == {"transform__sales-mart", "transform__executive-kpis", "transform__nightly-all"}
    assert transformation_factory.INVALID_FILES == {}


def test_dag_waits_for_the_assets_of_the_ingestion_jobs(transformation_factory: Any) -> None:
    dag = getattr(transformation_factory, "transform__sales-mart")
    assert dag.kwargs["schedule"] == ["asset:ingest__sales-orders-daily", "asset:ingest__crm-customers-daily"]
    assert dag.kwargs["default_args"]["pool"] == "dbt_duckdb"
    assert dag.kwargs["max_active_runs"] == 1
    assert {"transformation", "owner:data-team", "sales"} <= set(dag.kwargs["tags"])


def test_a_transformation_can_wait_for_another(transformation_factory: Any) -> None:
    dag = getattr(transformation_factory, "transform__executive-kpis")
    assert dag.kwargs["schedule"] == ["asset:transform__sales-mart"]


def test_cron_or_manual_when_there_is_no_after(transformation_factory: Any) -> None:
    assert getattr(transformation_factory, "transform__nightly-all").kwargs["schedule"] == "0 4 * * *"
    assert getattr(transformation_factory, "transform__nightly-all").kwargs["params"]["full_refresh"][0] == (True,)


def test_dbt_build_publishes_the_transformation_asset(transformation_factory: Any) -> None:
    dag = getattr(transformation_factory, "transform__sales-mart")
    assert dag.tasks == ["validate", "dbt_build"]
    assert dag.task_kwargs["dbt_build"]["outlets"] == ["asset:transform__sales-mart"]
    assert dag.task_kwargs["dbt_build"]["execution_timeout"].total_seconds() == 7200


def test_ingestion_dag_publishes_its_asset_in_the_last_task(factory: Any) -> None:
    dag = getattr(factory, "ingest__sales-orders-daily")
    assert dag.task_kwargs["wait"]["outlets"] == ["asset:ingest__sales-orders-daily"]


def test_dbt_script_runs_on_a_scratch_copy(transformation_factory: Any) -> None:
    script = transformation_factory.dbt_script(
        {"select": "tag:sales", "exclude": "tag:slow", "vars": {"a": "x y"}, "threads": 2, "full_refresh": True}
    )
    assert "cp -r " in script and "rm -rf" in script
    assert "dbt deps" in script
    assert (
        "/opt/dbt-venv/bin/dbt build --select tag:sales --threads 2 --exclude tag:slow --full-refresh "
        """--vars '{"a": "x y"}'"""
    ) in script


def test_dbt_script_quotes_the_selection(transformation_factory: Any) -> None:
    script = transformation_factory.dbt_script(
        {"select": "a; rm -rf /", "exclude": None, "vars": None, "threads": 1, "full_refresh": False}
    )
    assert "--select 'a; rm -rf /'" in script


def _write(directory: Path, name: str, body: str) -> None:
    (directory / f"{name}.yaml").write_text(
        f"kind: TransformationJob\nname: {name}\nowner: o\nselect: x\n{body}", "utf-8"
    )


def test_invalid_files_are_skipped_and_the_others_load(
    transformation_factory: Any, tmp_path: Path, monkeypatch: Any
) -> None:
    files = importlib.import_module("common.transformation_files")
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    _write(jobs, "good", "")
    _write(jobs, "unknown-ingestion", "after:\n  - ingestion: nope\n")
    _write(jobs, "both", "schedule: 0 1 * * *\nafter:\n  - ingestion: sales-orders-daily\n")
    _write(jobs, "selfish", "after:\n  - transformation: selfish\n")
    _write(jobs, "missing-upstream", "after:\n  - transformation: ghost\n")
    _write(jobs, "ping", "after:\n  - transformation: pong\n")
    _write(jobs, "pong", "after:\n  - transformation: ping\n")
    (jobs / "wrong-name.yaml").write_text(
        (TRANSFORMATION_FIXTURES / "jobs" / "sales-mart.yaml").read_text("utf-8"), "utf-8"
    )
    (jobs / "no-select.yaml").write_text("kind: TransformationJob\nname: no-select\nowner: o\n", "utf-8")
    monkeypatch.setattr(files, "JOBS_DIR", jobs)

    valid, invalid = files.load_files()

    assert [tf.name for tf in valid] == ["good"]
    assert set(invalid) == {
        "unknown-ingestion.yaml",
        "both.yaml",
        "selfish.yaml",
        "missing-upstream.yaml",
        "ping.yaml",
        "pong.yaml",
        "wrong-name.yaml",
        "no-select.yaml",
    }
    assert "not found" in invalid["unknown-ingestion.yaml"]
    assert "circular" in invalid["ping.yaml"]
    assert "does not match" in invalid["wrong-name.yaml"]
