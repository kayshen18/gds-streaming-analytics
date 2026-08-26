#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$project_root/infrastructure/spark-hdfs/compose.yaml"

docker compose -f "$compose_file" down
echo "Big-data containers stopped; named data volumes were preserved"

