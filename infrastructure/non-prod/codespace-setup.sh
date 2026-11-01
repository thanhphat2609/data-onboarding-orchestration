#!/usr/bin/env bash
# Codespaces: clones data-onboarding-ingestion next to this repository and prepares both stacks. Safe to run again.
set -euo pipefail

cd "$(dirname "$0")/../.."
if [[ ! -d ../data-onboarding-ingestion/.git ]]; then
  git clone https://github.com/thanhphat2609/data-onboarding-ingestion ../data-onboarding-ingestion
fi
bash ../data-onboarding-ingestion/infrastructure/non-prod/bootstrap.sh
bash infrastructure/non-prod/bootstrap.sh
