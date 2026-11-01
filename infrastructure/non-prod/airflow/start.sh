#!/usr/bin/env bash
# Non-prod Airflow: wait for the metadata DB, migrate it, keep the pools in step with the connection files, run all
# components.
set -euo pipefail

# The shared PostgreSQL server belongs to another Compose project, so depends_on can't wait for it (ingestion ADR-0011).
for attempt in $(seq 1 60); do
  airflow db check >/dev/null 2>&1 && break
  if [[ "$attempt" == 60 ]]; then
    echo "Metadata DB not reachable after 5 minutes. Start the data-onboarding-agent stack first, or set COMPOSE_PROFILES=local-db in .env." >&2
    exit 1
  fi
  echo "Waiting for the metadata DB (${attempt}/60)..."
  sleep 5
done

airflow db migrate
bash /opt/airflow/scripts/pool_sync_loop.sh &
exec airflow standalone
