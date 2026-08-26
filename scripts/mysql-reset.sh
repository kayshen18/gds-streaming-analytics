#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" != "--confirm" ]]; then
  echo "usage: $0 --confirm" >&2
  echo "This deletes only the Docker volume gds-mysql-data." >&2
  exit 2
fi

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mysql_dir="$project_root/infrastructure/mysql"
env_file="$mysql_dir/.env"

if [[ ! -f "$env_file" ]]; then
  echo "missing $env_file" >&2
  exit 2
fi

set -a
source "$mysql_dir/versions.env"
set +a
docker compose --env-file "$env_file" -f "$mysql_dir/compose.yaml" down

volume_name="gds-mysql-data"
if docker volume inspect "$volume_name" >/dev/null 2>&1; then
  docker volume rm "$volume_name"
fi
echo "Deleted Docker volume $volume_name; MySQL data cannot be recovered from it"
