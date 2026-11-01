"""Prints Airflow pools for every connection file, in the format of `airflow pools import`.

Pool `conn__<name>` limits concurrent runs on one source to `limits.max_concurrent_runs` (default 2).
Usage: python sync_pools.py > pools.json && airflow pools import pools.json
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import yaml

CONNECTIONS_DIR = Path(os.environ.get("INGESTION_PROJECT_DIR", "/opt/airflow/projects/ingestion")) / "connections"
DEFAULT_MAX_CONCURRENT_RUNS = 2


def pools() -> dict[str, dict[str, object]]:
    result: dict[str, dict[str, object]] = {}
    for path in sorted(CONNECTIONS_DIR.glob("*.yaml")):
        connection = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        name = connection.get("name", path.stem)
        slots = (connection.get("limits") or {}).get("max_concurrent_runs", DEFAULT_MAX_CONCURRENT_RUNS)
        result[f"conn__{name}"] = {
            "slots": slots,
            "description": f"Concurrent runs on connection {name} (pipelines/connections/{path.name})",
            # A run waiting on Flink still occupies the source.
            "include_deferred": True,
        }
    return result


if __name__ == "__main__":
    print(json.dumps(pools(), indent=2))
