"""Pools conn__<name> from data-onboarding-ingestion's connection files."""

from __future__ import annotations

import importlib
from typing import Any


def test_one_pool_per_connection(factory: Any) -> None:
    pools = importlib.import_module("sync_pools").pools()

    connection_pools = {name: pool for name, pool in pools.items() if name.startswith("conn__")}
    assert set(connection_pools) == {
        "conn__crm-sqlserver-nonprod",
        "conn__sales-postgresql-nonprod",
        "conn__shop-api-nonprod",
        "conn__shop-mysql-nonprod",
    }
    assert all(pool["slots"] >= 1 and pool["include_deferred"] is True for pool in connection_pools.values())


def test_dbt_pool_has_one_slot(factory: Any) -> None:
    pools = importlib.import_module("sync_pools").pools()

    assert pools["dbt_duckdb"]["slots"] == 1
