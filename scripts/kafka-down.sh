#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$project_root/infrastructure/kafka/compose.yaml"

docker compose -f "$compose_file" down
echo "Kafka stopped; the gds-kafka-data volume was preserved"
