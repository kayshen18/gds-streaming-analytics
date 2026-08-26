#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
set -a
source "$root/infrastructure/mysql/.env"
source "$root/infrastructure/mysql/versions.env"
set +a
export GDS_MYSQL_HOST="${GDS_MYSQL_HOST:-127.0.0.1}"
export GDS_MYSQL_HOST_PORT="${GDS_MYSQL_HOST_PORT:-3307}"

exec gds-mysql publish "$@"
