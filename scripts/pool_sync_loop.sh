#!/usr/bin/env bash
# Keeps the pools conn__<name> in step with data-onboarding-ingestion's connection files (ADR-0002). Every
# POOL_SYNC_INTERVAL seconds (default 60) it renders the pools and imports them when they changed, so ingestion never
# has to call Airflow: a connection file only has to reach the checkout this repository mounts.
set -uo pipefail

interval="${POOL_SYNC_INTERVAL:-60}"
last=""
while true; do
  if pools=$(python /opt/airflow/scripts/sync_pools.py); then
    if [[ "$pools" != "$last" ]]; then
      printf '%s\n' "$pools" > /tmp/pools.json
      if airflow pools import /tmp/pools.json >/dev/null; then
        echo "Pools synced"
        last="$pools"
      fi
    fi
  else
    echo "Pool sync failed; retrying in ${interval}s" >&2
  fi
  sleep "$interval"
done
