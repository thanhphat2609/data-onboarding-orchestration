#!/bin/sh
# Runs once, on the first start of an empty data volume. local-db profile only (ingestion ADR-0011).
# One instance, two databases: Airflow metadata and the platform database (ingestion architecture §3.4).
# Mirrors the shared server's init script in data-onboarding-agent (infrastructure/postgres/init/01-databases.sh),
# which also creates the agent's own database.
set -eu

psql -v ON_ERROR_STOP=1 --username postgres <<EOSQL
CREATE ROLE airflow LOGIN PASSWORD '${AIRFLOW_DB_PASSWORD}';
CREATE DATABASE airflow OWNER airflow;
CREATE ROLE platform LOGIN PASSWORD '${PLATFORM_DB_PASSWORD}';
CREATE DATABASE platform OWNER platform;
EOSQL
