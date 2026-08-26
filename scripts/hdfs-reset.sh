#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" != "--confirm" || "$#" -ne 1 ]]; then
  echo "Refusing destructive reset. Re-run with the literal argument: --confirm" >&2
  exit 2
fi

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$project_root/infrastructure/spark-hdfs/compose.yaml"

docker compose -f "$compose_file" down
docker volume rm gds-hdfs-namenode-data gds-hdfs-datanode-data
echo "Deleted only HDFS named volumes: gds-hdfs-namenode-data and gds-hdfs-datanode-data"

