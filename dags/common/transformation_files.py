"""Reading the transformation files: what the DAG factory needs before it builds a DAG.

Every ``pipelines/transformations/<name>.yaml`` (mounted read-only at ``$TRANSFORMATION_JOBS_DIR``) is checked on its
own against the transformation job schema, then against the others: a file that waits for a missing or invalid
transformation, or sits in a cycle, is dropped too. An invalid file is reported with a one-line reason and skipped;
the others keep loading.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from airflow.sdk import Asset
from jsonschema import Draft7Validator

from common.assets import ingestion_asset, transformation_asset

log = logging.getLogger(__name__)

JOBS_DIR = Path(os.environ.get("TRANSFORMATION_JOBS_DIR", "/opt/airflow/pipelines/transformations"))
INGESTION_JOBS_DIR = Path(os.environ.get("INGESTION_PROJECT_DIR", "/opt/airflow/projects/ingestion")) / "jobs"
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "transformation" / "schemas" / "transformation-job.schema.json"

DAG_PREFIX = "transform__"


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
