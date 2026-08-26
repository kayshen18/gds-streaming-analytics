#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
versions_file="$project_root/infrastructure/mysql/versions.env"

if ! docker version >/dev/null 2>&1; then
  echo "Docker Engine is unavailable" >&2
  exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
  echo "Docker Compose is unavailable" >&2
  exit 1
fi
if ! command -v ss >/dev/null 2>&1; then
  echo "the ss command is required for read-only port detection" >&2
  exit 1
fi
if [[ ! -f "$versions_file" ]]; then
  echo "missing MySQL runtime contract: $versions_file" >&2
  exit 1
fi

set -a
source "$versions_file"
set +a

explicit_port="${GDS_MYSQL_HOST_PORT:-}"
if [[ -n "$explicit_port" ]]; then
  if [[ ! "$explicit_port" =~ ^[0-9]+$ ]] \
    || (( explicit_port < 1 || explicit_port > 65535 )); then
    echo "GDS_MYSQL_HOST_PORT must be an integer from 1 to 65535" >&2
    exit 2
  fi
  selected_port="$explicit_port"
  port_source="explicit"
elif ss -ltnH | awk '{print $4}' | grep -Eq '(^|:|\])3306$'; then
  selected_port=3307
  port_source="auto-3306-occupied"
else
  selected_port=3306
  port_source="auto-3306-free"
fi

echo "mysql prerequisites passed"
echo "mysql_image=$MYSQL_IMAGE"
echo "mysql_container_port=$MYSQL_CONTAINER_PORT"
echo "mysql_host_port=$selected_port"
echo "port_source=$port_source"
