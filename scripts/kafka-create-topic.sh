#!/usr/bin/env bash
set -euo pipefail

topic="gds.raw.v1"
bootstrap_server="localhost:29092"
kafka_topics="/opt/kafka/bin/kafka-topics.sh"

docker exec gds-kafka "$kafka_topics" \
  --bootstrap-server "$bootstrap_server" \
  --create \
  --if-not-exists \
  --topic "$topic" \
  --partitions 3 \
  --replication-factor 1

description="$(docker exec gds-kafka "$kafka_topics" \
  --bootstrap-server "$bootstrap_server" \
  --describe \
  --topic "$topic")"

echo "$description"
if [[ "$description" != *"PartitionCount: 3"* ]]; then
  echo "Topic $topic does not have exactly three partitions" >&2
  exit 1
fi
