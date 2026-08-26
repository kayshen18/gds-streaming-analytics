#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mysql_dir="$project_root/infrastructure/mysql"
env_file="$mysql_dir/.env"

if [[ ! -f "$env_file" ]]; then
  echo "missing $env_file; copy .env.example and replace both passwords" >&2
  exit 2
fi

set -a
source "$mysql_dir/versions.env"
set +a

docker compose --env-file "$env_file" -f "$mysql_dir/compose.yaml" up -d

for attempt in $(seq 1 48); do
  status="$(docker inspect gds-mysql --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' 2>/dev/null || true)"
  if [[ "$status" == "healthy" ]]; then
    published_port="$(docker port gds-mysql 3306/tcp | sed -n '1s/.*://p')"
    echo "MySQL is healthy on localhost:${published_port:-unknown}"
    exit 0
  fi
  echo "Waiting for MySQL health check ($attempt/48): ${status:-not-created}"
  sleep 5
done

docker logs --tail 100 gds-mysql >&2 || true
echo "MySQL did not become healthy within 240 seconds" >&2
exit 1
