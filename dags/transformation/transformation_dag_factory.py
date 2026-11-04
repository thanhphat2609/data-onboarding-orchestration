"""One Airflow DAG per transformation file (ADR-0004).

Every ``pipelines/transformations/<name>.yaml`` (mounted read-only at ``$TRANSFORMATION_JOBS_DIR``) becomes the DAG
``transform__<name>``: ``validate -> dbt_build``. ``dbt_build`` runs ``dbt build`` from the venv in the Airflow image
on a copy of the dbt project that data-onboarding-transformation provides (``$TRANSFORMATION_PROJECT_DIR``, read-only).

A DAG is scheduled on the Assets of the ingestion jobs (or transformations) listed in ``after``, so it runs when they
have all published and neither side refers to the other's DAG. Each file is validated on its own: an invalid file is
skipped and logged, and every other DAG keeps loading.
"""

from __future__ import annotations

import json
import logging
import os
import shlex
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import yaml
from airflow.sdk import DAG, Asset, Param, get_current_context, task
from common.assets import ingestion_asset, transformation_asset
from common.platform import START_DATE
from jsonschema import Draft7Validator

log = logging.getLogger(__name__)

JOBS_DIR = Path(os.environ.get("TRANSFORMATION_JOBS_DIR", "/opt/airflow/pipelines/transformations"))
PROJECT_DIR = Path(os.environ.get("TRANSFORMATION_PROJECT_DIR", "/opt/airflow/projects/transformation"))
INGESTION_JOBS_DIR = Path(os.environ.get("INGESTION_PROJECT_DIR", "/opt/airflow/projects/ingestion")) / "jobs"
SCHEMA_PATH = Path(__file__).parent / "schemas" / "transformation-job.schema.json"
DBT = os.environ.get("DBT_EXECUTABLE", "/opt/dbt-venv/bin/dbt")

DAG_PREFIX = "transform__"
DBT_POOL = "dbt_duckdb"  # one slot, created by scripts/sync_pools.py: DuckDB allows a single writer
DEFAULT_TIMEOUT = "1h"
DEFAULT_THREADS = 4


# --- Loading and validation -------------------------------------------------------------------


class InvalidJobFile(Exception):
    """A transformation file that cannot become a DAG; the message is shown to users."""


@dataclass(frozen=True)
class TransformationFile:
    path: Path
    job: dict[str, Any]

    @property
    def name(self) -> str:
        """The transformation's name, which is also its file name."""
        return self.job["name"]

    @property
    def dag_id(self) -> str:
        """The Airflow DAG id, ``transform__<name>``."""
        return f"{DAG_PREFIX}{self.name}"

    @property
    def after(self) -> list[dict[str, str]]:
        """The ``after`` entries, for example ``[{"ingestion": "sales-orders-daily"}]``."""
        return self.job.get("after", [])

    @property
    def upstream_transformations(self) -> list[str]:
        """Names of the transformations this one waits for."""
        return [entry["transformation"] for entry in self.after if "transformation" in entry]

    @property
    def assets(self) -> list[Asset]:
        """The Assets this DAG is scheduled on: all of them must have been published since its last run."""
        return [
            ingestion_asset(entry["ingestion"])
            if "ingestion" in entry
            else transformation_asset(entry["transformation"])
            for entry in self.after
        ]


def _schema_errors(document: dict[str, Any]) -> list[str]:
    """One message per violation of the transformation job schema."""
    validator = Draft7Validator(json.loads(SCHEMA_PATH.read_text(encoding="utf-8")))
    return [
        f"{'/'.join(map(str, error.path)) or '<root>'}: {error.message}"
        for error in sorted(validator.iter_errors(document), key=lambda e: list(map(str, e.path)))
    ]


def load_file(path: Path) -> TransformationFile:
    """Reads and checks one file on its own; raises InvalidJobFile with a one-line reason."""
    job = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(job, dict):
        raise InvalidJobFile(f"{path.name} is not a YAML mapping")
    errors = _schema_errors(job)
    if errors:
        raise InvalidJobFile("; ".join(errors))
    if job["name"] != path.stem:
        raise InvalidJobFile(f'file name does not match name "{job["name"]}"')
    if job.get("schedule") and job.get("after"):
        raise InvalidJobFile("set either schedule or after, not both")
    for entry in job.get("after", []):
        if "ingestion" in entry and not (INGESTION_JOBS_DIR / f"{entry['ingestion']}.yaml").exists():
            raise InvalidJobFile(f'after: ingestion job "{entry["ingestion"]}" not found')
        if entry.get("transformation") == job["name"]:
            raise InvalidJobFile("after: a transformation cannot wait for itself")
    return TransformationFile(path=path, job=job)


def _drop_broken_references(files: dict[str, TransformationFile], invalid: dict[str, str]) -> None:
    """Moves files that wait for a missing or invalid transformation, or sit in a cycle, to ``invalid``.

    Repeats until nothing changes, because dropping one file can orphan the files that wait for it.
    """
    changed = True
    while changed:
        changed = False
        for name, tf in list(files.items()):
            missing = [dep for dep in tf.upstream_transformations if dep not in files]
            cycle = _reaches(files, name)
            if missing or cycle:
                reason = f'after: transformation "{missing[0]}" not found' if missing else "after: circular dependency"
                invalid[tf.path.name] = reason
                del files[name]
                changed = True


def _reaches(files: dict[str, TransformationFile], start: str) -> bool:
    """True when following ``after: transformation`` links from ``start`` leads back to it."""
    seen: set[str] = set()
    stack = list(files[start].upstream_transformations)
    while stack:
        current = stack.pop()
        if current == start:
            return True
        if current in seen or current not in files:
            continue
        seen.add(current)
        stack.extend(files[current].upstream_transformations)
    return False


def load_files() -> tuple[list[TransformationFile], dict[str, str]]:
    """All valid transformation files, and an error message per invalid file name."""
    files: dict[str, TransformationFile] = {}
    invalid: dict[str, str] = {}
    for path in sorted(JOBS_DIR.glob("*.yaml")):
        try:
            tf = load_file(path)
            files[tf.name] = tf
        except yaml.YAMLError as error:
            mark = getattr(error, "problem_mark", None)
            where = f" at line {mark.line + 1}" if mark else ""
            invalid[path.name] = f"invalid YAML{where}: {getattr(error, 'problem', None) or error}"
        except (InvalidJobFile, OSError, KeyError) as error:
            invalid[path.name] = str(error)
    _drop_broken_references(files, invalid)
    for name, message in invalid.items():
        log.warning("Skipping %s: %s", name, message)
    return list(files.values()), invalid


def parse_duration(value: str) -> timedelta:
    """Turns ``30s``, ``15m`` or ``2h`` into a timedelta."""
    amount, unit = int(value[:-1]), value[-1]
    return {"s": timedelta(seconds=amount), "m": timedelta(minutes=amount), "h": timedelta(hours=amount)}[unit]


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
