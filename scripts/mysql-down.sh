#!/usr/bin/env bash
set -euo pipefail

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
echo "MySQL container stopped; named data volume was preserved"
