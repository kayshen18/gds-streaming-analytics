#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$project_root/infrastructure/kafka/compose.yaml"

docker network inspect gds-streaming-network >/dev/null 2>&1 || \
  docker network create gds-streaming-network >/dev/null
docker compose -f "$compose_file" up -d

for attempt in $(seq 1 36); do
  status="$(docker inspect --format='{{.State.Health.Status}}' gds-kafka 2>/dev/null || true)"
  if [[ "$status" == "healthy" ]]; then
    echo "Kafka is healthy on localhost:9092"
    exit 0
  fi
  echo "Waiting for Kafka health check ($attempt/36): ${status:-not-created}"
  sleep 5
done

docker compose -f "$compose_file" logs --tail=100 kafka
echo "Kafka did not become healthy within 180 seconds" >&2
exit 1
