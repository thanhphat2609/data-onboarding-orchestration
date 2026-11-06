"""One Airflow DAG per transformation file (ADR-0004).

Every ``pipelines/transformations/<name>.yaml`` becomes the DAG ``transform__<name>``: ``validate -> dbt_build``.
Reading and checking the files is ``common/transformation_files.py``; this module builds the DAGs. ``dbt_build`` runs
``dbt build`` from the venv in the Airflow image on a copy of the dbt project that data-onboarding-transformation
provides (``$TRANSFORMATION_PROJECT_DIR``, read-only).

A DAG is scheduled on the Assets of the ingestion jobs (or transformations) listed in ``after``, so it runs when they
have all published and neither side refers to the other's DAG. Each file is validated on its own: an invalid file is
skipped and logged, and every other DAG keeps loading.
"""

from __future__ import annotations

import json
import logging
import os
import shlex
from pathlib import Path
from typing import Any

from airflow.sdk import DAG, Param, get_current_context, task
from common.assets import transformation_asset
from common.duration import parse_duration
from common.platform import START_DATE
from common.transformation_files import TransformationFile, load_file, load_files

log = logging.getLogger(__name__)

PROJECT_DIR = Path(os.environ.get("TRANSFORMATION_PROJECT_DIR", "/opt/airflow/projects/transformation"))
DBT = os.environ.get("DBT_EXECUTABLE", "/opt/dbt-venv/bin/dbt")

DBT_POOL = "dbt_duckdb"  # one slot, created by scripts/sync_pools.py: DuckDB allows a single writer
DEFAULT_TIMEOUT = "1h"
DEFAULT_THREADS = 4


# --- dbt command --------------------------------------------------------------------------------


def dbt_script(run: dict[str, Any]) -> str:
    """The shell script of one run: dbt on a scratch copy of the read-only project, removed afterwards.

    dbt writes ``target/``, ``logs/`` and ``dbt_packages/`` next to the project, which is mounted read-only.
    """
    build = [DBT, "build", "--select", run["select"], "--threads", str(run["threads"])]
    if run.get("exclude"):
        build += ["--exclude", run["exclude"]]
    if run["full_refresh"]:
        build.append("--full-refresh")
    if run.get("vars"):
        build += ["--vars", json.dumps(run["vars"])]
    return "\n".join(
        [
            "set -euo pipefail",
            'work="$(mktemp -d)"',
            "trap 'rm -rf \"$work\"' EXIT",
            f'cp -r {shlex.quote(str(PROJECT_DIR))}/. "$work"',
            'cd "$work"',
            f"if [ -f packages.yml ]; then {shlex.quote(DBT)} deps; fi",
            shlex.join(build),
        ]
    )


# --- DAG construction --------------------------------------------------------------------------


def _schedule_text(tf: TransformationFile) -> str:
    """How the DAG starts, for its documentation page."""
    if tf.after:
        return "after " + ", ".join(f"`{kind}: {name}`" for entry in tf.after for kind, name in entry.items())
    return f"`{tf.job['schedule']}`" if tf.job.get("schedule") else "manual only"


def _doc_md(tf: TransformationFile) -> str:
    """The Markdown shown on the DAG page in Airflow."""
    job = tf.job
    return f"""
### {tf.name}

Generated from `pipelines/transformations/{tf.path.name}` of this repository by the transformation DAG factory. Edit
the YAML file, not this DAG.

| | |
|---|---|
| Owner | {job["owner"]} |
| Runs | {_schedule_text(tf)} |
| dbt | `dbt build --select {job["select"]}`{f" `--exclude {job['exclude']}`" if job.get("exclude") else ""} |
| Pool | `{DBT_POOL}` (DuckDB has one writer) |
| Publishes | Asset `transform__{tf.name}` |
"""


def build_dag(tf: TransformationFile) -> DAG:
    """The DAG ``transform__<name>`` of one file: validate, then dbt build.

    One active run at a time, no retries, the single-slot pool ``dbt_duckdb``, and a ``full_refresh`` parameter.
    """
    job = tf.job
    runtime = job.get("runtime", {})
    timeout = parse_duration(runtime.get("timeout", DEFAULT_TIMEOUT))
    tags = {"transformation", f"owner:{job['owner']}", *job.get("tags", [])}

    with DAG(
        dag_id=tf.dag_id,
        schedule=tf.assets or job.get("schedule"),
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        tags=sorted(tags),
        doc_md=_doc_md(tf),
        params={"full_refresh": Param(bool(job.get("full_refresh", False)), type="boolean")},
        default_args={"owner": job["owner"], "pool": DBT_POOL, "retries": 0},
    ) as dag:

        @task
        def validate() -> dict[str, Any]:
            """Re-reads the file: it may have changed since the DAG was parsed."""
            current = load_file(tf.path).job
            return {
                "select": current["select"],
                "exclude": current.get("exclude"),
                "vars": current.get("vars"),
                "threads": current.get("runtime", {}).get("threads", DEFAULT_THREADS),
                "full_refresh": get_current_context()["params"]["full_refresh"],
            }

        # Publishes the Asset that other transformation DAGs can wait for.
        @task.bash(execution_timeout=timeout, outlets=[transformation_asset(tf.name)])
        def dbt_build(run: dict[str, Any]) -> str:
            """Runs ``dbt build`` for the selection of this run."""
            return dbt_script(run)

        dbt_build(validate())

    return dag


_files, INVALID_FILES = load_files()
for _file in _files:
    globals()[_file.dag_id] = build_dag(_file)
log.info("Transformation DAG factory: %d DAG(s), %d invalid file(s)", len(_files), len(INVALID_FILES))
