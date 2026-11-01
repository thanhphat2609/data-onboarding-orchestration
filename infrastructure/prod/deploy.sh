#!/usr/bin/env bash
# Deploys Airflow on prod VM 1 to a commit. Run on the VM, from the repository checkout, by the Deploy workflow:
#   git -C <repo> fetch --prune origin && git -C <repo> show <sha>:infrastructure/prod/deploy.sh | bash -s -- ...
# so the script of the target commit runs, not the one currently checked out.
#
# Usage: deploy.sh <sha> [--no-cache]
#   --no-cache   rebuild every image layer (default: reuse the cache, but always pull newer base images)
set -euo pipefail

sha="${1:?commit sha}"
shift
no_cache=()
for arg in "$@"; do
  case "$arg" in
    --no-cache) no_cache=(--no-cache) ;;
    *) echo "Unknown option $arg" >&2; exit 2 ;;
  esac
done

repo="${PROD_REPO_DIR:-$(pwd)}"
cd "$repo"
compose=(docker compose -f infrastructure/prod/vm1/docker-compose.yml)

echo "Deploying Airflow: $(git rev-parse --short HEAD) -> ${sha:0:7}"
git checkout --quiet --detach "$sha"

"${compose[@]}" build --pull "${no_cache[@]}"
"${compose[@]}" up -d --remove-orphans --wait --wait-timeout 600

# Keep the disk in check: dangling images from earlier builds, and build cache unused for a week.
docker image prune -f
docker builder prune -f --filter until=168h
"${compose[@]}" ps
echo "Airflow at ${sha:0:7}"
