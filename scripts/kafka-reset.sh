#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" != "--confirm" ]]; then
  echo "This deletes the Kafka container and gds-kafka-data volume." >&2
  echo "Run: bash scripts/kafka-reset.sh --confirm" >&2
  exit 2
fi

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$project_root/infrastructure/kafka/compose.yaml"

docker compose -f "$compose_file" down --volumes --remove-orphans
echo "Kafka container and local Kafka data volume deleted"
