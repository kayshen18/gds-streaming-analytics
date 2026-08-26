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
source "$env_file"
set +a

required_variables=(
  MYSQL_DATABASE
  GDS_API_MYSQL_USER
  GDS_API_MYSQL_PASSWORD
)

for variable in "${required_variables[@]}"; do
  if [[ -z "${!variable:-}" ]]; then
    echo "$variable must not be blank" >&2
    exit 2
  fi
done

if [[ ! "$MYSQL_DATABASE" =~ ^[A-Za-z0-9_]{1,64}$ ]]; then
  echo "MYSQL_DATABASE contains unsupported characters" >&2
  exit 2
fi

if [[ ! "$GDS_API_MYSQL_USER" =~ ^[A-Za-z0-9_]{1,32}$ ]]; then
  echo "GDS_API_MYSQL_USER contains unsupported characters" >&2
  exit 2
fi

if [[ ! "$GDS_API_MYSQL_PASSWORD" =~ ^[-A-Za-z0-9._~!@%+=:]{16,128}$ ]]; then
  echo "GDS_API_MYSQL_PASSWORD must use safe characters and be 16-128 characters" >&2
  exit 2
fi

if ! docker inspect gds-mysql >/dev/null 2>&1; then
  echo "gds-mysql is not running; run scripts/mysql-up.sh first" >&2
  exit 2
fi

docker exec -i gds-mysql sh -lc \
  'exec mysql --protocol=socket -uroot -p"$MYSQL_ROOT_PASSWORD"' <<SQL
CREATE USER IF NOT EXISTS
  '${GDS_API_MYSQL_USER}'@'%'
  IDENTIFIED BY '${GDS_API_MYSQL_PASSWORD}';

ALTER USER
  '${GDS_API_MYSQL_USER}'@'%'
  IDENTIFIED BY '${GDS_API_MYSQL_PASSWORD}';

REVOKE ALL PRIVILEGES, GRANT OPTION
  FROM '${GDS_API_MYSQL_USER}'@'%';

GRANT SELECT
  ON \`${MYSQL_DATABASE}\`.\`hourly_airline_metrics\`
  TO '${GDS_API_MYSQL_USER}'@'%';

GRANT SELECT
  ON \`${MYSQL_DATABASE}\`.\`metric_publications\`
  TO '${GDS_API_MYSQL_USER}'@'%';

FLUSH PRIVILEGES;
SQL

echo "read-only API user is ready"
