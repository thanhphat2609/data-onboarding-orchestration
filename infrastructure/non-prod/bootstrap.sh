#!/usr/bin/env bash
# Prepares non-prod Airflow: .env with random secrets and the Docker networks it shares with the other stacks.
# Safe to run again; existing values are kept. Run data-onboarding-ingestion's bootstrap.sh too: it writes sources.env
# and the shared run directory.
set -euo pipefail

cd "$(dirname "$0")"

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created infrastructure/non-prod/.env"
else
  echo "Keeping existing infrastructure/non-prod/.env"
  while IFS= read -r line; do
    key=${line%%=*}
    if ! grep -qE "^${key}=" .env; then
      printf '%s\n' "$line" >> .env
      echo "Added ${key} to .env"
    fi
  done < <(grep -E '^[A-Z0-9_]+=' .env.example)
fi

while grep -qE '^[A-Z0-9_]+=__GENERATE__$' .env; do
  key=$(grep -m1 -E '^[A-Z0-9_]+=__GENERATE__$' .env | cut -d= -f1)
  if [[ "$key" == "AIRFLOW_FERNET_KEY" ]]; then
    # Fernet keys are 32 random bytes, URL-safe base64.
    value=$(openssl rand -base64 32 | tr '+/' '-_')
  else
    value=$(openssl rand -hex 16)
  fi
  sed -i "s|^${key}=__GENERATE__$|${key}=${value}|" .env
done

# Airflow reaches Flink, Redpanda, the Ingestion API and the shared PostgreSQL server over data-platform, and the
# example sources over ingestion-sources. Whichever stack prepares first creates them.
for network in data-platform ingestion-sources; do
  if ! docker network inspect "$network" >/dev/null 2>&1; then
    docker network create "$network" >/dev/null
    echo "Created Docker network ${network}"
  fi
done
