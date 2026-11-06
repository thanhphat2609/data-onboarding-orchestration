#!/usr/bin/env bash
# Deploys Airflow on prod VM 1 to a commit. Run on the VM, from the repository checkout, by the Deploy workflow:
#   git -C <repo> fetch --prune origin && git -C <repo> show <sha>:infrastructure/prod/deploy.sh | bash -s -- ...
# so the script of the target commit runs, not the one currently checked out.
#
# Usage: deploy.sh <sha> [--pipelines-only] [--no-cache]
#   --pipelines-only  only move the checkout to <sha>: Airflow mounts pipelines/ from it; no rebuild, no restart
#   --no-cache   rebuild every image layer (default: reuse the cache, but always pull newer base images)
set -euo pipefail

sha="${1:?commit sha}"
shift
pipelines_only=false
no_cache=()
for arg in "$@"; do
  case "$arg" in
    --pipelines-only) pipelines_only=true ;;
    --no-cache) no_cache=(--no-cache) ;;
    *) echo "Unknown option $arg" >&2; exit 2 ;;
  esac
done

repo="${PROD_REPO_DIR:-$(pwd)}"
cd "$repo"
compose=(docker compose -f infrastructure/prod/vm1/docker-compose.yml)

echo "Deploying Airflow: $(git rev-parse --short HEAD) -> ${sha:0:7}"
git checkout --quiet --detach "$sha"

if [[ "$pipelines_only" == true ]]; then
  # The DAG processor and the pool sync pick the files up by themselves.
  echo "Pipelines at ${sha:0:7}"
  exit 0
fi

docker network inspect data-platform >/dev/null 2>&1 || docker network create data-platform
"${compose[@]}" build --pull "${no_cache[@]}"
"${compose[@]}" up -d --remove-orphans --wait --wait-timeout 600

# Keep the disk in check: dangling images from earlier builds, and build cache unused for a week.
docker image prune -f
docker builder prune -f --filter until=168h
"${compose[@]}" ps
echo "Airflow at ${sha:0:7}"
