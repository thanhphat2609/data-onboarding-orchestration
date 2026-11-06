"""The Flink JobManager's REST API, and how a run is handed to it (ingestion ADR-0010).

``submit_run`` uploads the flink-python jar once and runs ``PythonDriver`` on the runner of data-onboarding-ingestion,
which is mounted on the JobManager together with the merged ``pipelines/``. ``wait_for_job`` follows the job until it
ends. ``INGESTION_SUBMIT_MODE=smoke`` runs Flink's WordCount example instead, to test the wiring only. No Airflow
import.
"""

from __future__ import annotations

import logging
import os
import re
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

import requests

log = logging.getLogger(__name__)

FLINK_REST_URL = os.environ.get("FLINK_REST_URL", "http://flink-jobmanager:8081")
FLINK_TERMINAL_STATES = {"FINISHED", "FAILED", "CANCELED"}
# Example JAR shipped with the Flink image; shared with Airflow through a volume.
SMOKE_JAR = Path(os.environ.get("FLINK_SMOKE_JAR", "/opt/flink-examples/streaming/WordCount.jar"))
# The runner: flink-python's PythonDriver runs main.py on the JobManager. The jar is copied to the shared examples
# volume by the ingestion stack; the runner and the merged pipelines/ are mounted on the JobManager.
SUBMIT_MODE = os.environ.get("INGESTION_SUBMIT_MODE", "runner")
PYTHON_JAR = Path(os.environ.get("FLINK_PYTHON_JAR", "/opt/flink-examples/flink-python.jar"))
PYTHON_DRIVER = "org.apache.flink.client.python.PythonDriver"
RUNNER_MAIN = os.environ.get("FLINK_RUNNER_MAIN", "/opt/runner/main.py")
FLINK_PIPELINES_DIR = os.environ.get("FLINK_PIPELINES_DIR", "/opt/ingestion/pipelines")
# "final": the runner writes the table path and replaces the ingest_date partition (no publish step exists yet).
# "staging": _staging/<run_id>/, for when the Phase 5 publisher moves it (ingestion ADR-0004).
RUNNER_TARGET = os.environ.get("INGESTION_RUNNER_TARGET", "final")


def flink_request(method: str, path: str, **kwargs: Any) -> dict[str, Any]:
    """One call to the Flink JobManager's REST API; the JSON answer, or an empty dict."""
    response = requests.request(method, f"{FLINK_REST_URL}{path}", timeout=30, **kwargs)
    if not response.ok:
        # Flink's answer says why (for example the Python error of a runner that failed to start); keep it.
        raise RuntimeError(f"Flink {method} {path}: {response.status_code} {response.text[:1500]}")
    return response.json() if response.content else {}


def runner_arguments(job_file: str, run_id: str, ingest_date: str) -> list[str]:
    """Command line of the runner as PythonDriver takes it; ``--no-wait`` because the task ``wait`` polls the job."""
    return [
        "-py",
        RUNNER_MAIN,
        "--job",
        f"{FLINK_PIPELINES_DIR}/jobs/{job_file}",
        "--run-id",
        re.sub(r"[^A-Za-z0-9_.-]", "_", run_id),
        "--ingest-date",
        ingest_date,
        "--target",
        RUNNER_TARGET,
        "--no-wait",
    ]


def upload_jar(jar: Path) -> str:
    """Uploads the JAR once and reuses it by file name, so repeated runs do not pile up copies."""
    for uploaded in flink_request("GET", "/jars").get("files", []):
        if uploaded["name"] == jar.name:
            return uploaded["id"]
    with jar.open("rb") as content:
        result = flink_request(
            "POST", "/jars/upload", files={"jarfile": (jar.name, content, "application/x-java-archive")}
        )
    return result["filename"].rsplit("/", 1)[-1]


def submit_run(job_file: str, run_id: str, ingest_date: str, parallelism: int) -> str:
    """Starts the Flink job of one run (the runner, or the smoke job) and returns its id."""
    if SUBMIT_MODE == "smoke":
        jar_id = upload_jar(SMOKE_JAR)
        body: dict[str, Any] = {"parallelism": parallelism, "programArgsList": []}
    else:
        jar_id = upload_jar(PYTHON_JAR)
        body = {
            "entryClass": PYTHON_DRIVER,
            "programArgsList": runner_arguments(job_file, run_id, ingest_date),
            "parallelism": parallelism,
        }
    result = flink_request("POST", f"/jars/{jar_id}/run", json=body)
    log.info("Submitted Flink job %s (ingest_date %s)", result["jobid"], ingest_date)
    return str(result["jobid"])


def wait_for_job(flink_job_id: str, timeout: timedelta) -> str:
    """Polls the JobManager until the job ends; cancels it after ``timeout`` (deferrable in Phase 5)."""
    deadline = time.monotonic() + timeout.total_seconds()
    while True:
        state = flink_request("GET", f"/jobs/{flink_job_id}")["state"]
        if state in FLINK_TERMINAL_STATES:
            break
        if time.monotonic() > deadline:
            flink_request("PATCH", f"/jobs/{flink_job_id}?mode=cancel")
            raise TimeoutError(f"Flink job {flink_job_id} exceeded runtime.timeout {timeout}; cancelled")
        time.sleep(5)
    if state != "FINISHED":
        raise RuntimeError(f"Flink job {flink_job_id} ended in state {state}")
    log.info("Flink job %s finished", flink_job_id)
    return str(state)
