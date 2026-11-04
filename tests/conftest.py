"""Offline setup: the fixtures stand in for the ingestion repository, and a small airflow.sdk stub records what a DAG
factory builds. CI also parses the same fixtures with real Airflow (.github/workflows/ci.yml)."""

from __future__ import annotations

import importlib
import os
import sys
import types
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "tests" / "fixtures" / "ingestion"
TRANSFORMATION_FIXTURES = ROOT / "tests" / "fixtures" / "transformation"


class StubDag:
    current: StubDag | None = None

    def __init__(self, **kwargs: Any) -> None:
        self.dag_id = kwargs["dag_id"]
        self.kwargs = kwargs
        self.tasks: list[str] = []
        self.task_kwargs: dict[str, dict[str, Any]] = {}

    def __enter__(self) -> StubDag:
        StubDag.current = self
        return self

    def __exit__(self, *_: object) -> None:
        StubDag.current = None


def _task(function: Any = None, **kwargs: Any) -> Any:
    def decorate(fn: Any) -> Any:
        def call(*__: Any) -> str:
            assert StubDag.current is not None
            StubDag.current.tasks.append(fn.__name__)
            StubDag.current.task_kwargs[fn.__name__] = kwargs
            return fn.__name__

        return call

    return decorate(function) if function else decorate


_task.bash = _task  # type: ignore[attr-defined]  # @task.bash(...) records like @task(...)


@pytest.fixture(scope="session")
def factory() -> Any:
    sdk = types.ModuleType("airflow.sdk")
    sdk.DAG = StubDag  # type: ignore[attr-defined]
    sdk.Param = lambda *args, **kwargs: (args, kwargs)  # type: ignore[attr-defined]
    sdk.get_current_context = dict  # type: ignore[attr-defined]
    sdk.task = _task  # type: ignore[attr-defined]
    sdk.Asset = lambda name: f"asset:{name}"  # type: ignore[attr-defined]
    sys.modules["airflow"] = types.ModuleType("airflow")
    sys.modules["airflow.sdk"] = sdk
    os.environ["INGESTION_PROJECT_DIR"] = str(FIXTURES)
    os.environ["TRANSFORMATION_JOBS_DIR"] = str(TRANSFORMATION_FIXTURES / "jobs")
    sys.path[:0] = [
        str(ROOT / "dags"),
        str(ROOT / "dags" / "ingestion"),
        str(ROOT / "dags" / "transformation"),
        str(ROOT / "scripts"),
    ]
    return importlib.import_module("ingestion_dag_factory")


@pytest.fixture(scope="session")
def transformation_factory(factory: Any) -> Any:
    """The transformation DAG factory, loaded after ``factory`` has installed the airflow.sdk stub."""
    return importlib.import_module("transformation_dag_factory")
