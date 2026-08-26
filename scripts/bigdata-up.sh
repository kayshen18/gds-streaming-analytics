#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$project_root/infrastructure/spark-hdfs/compose.yaml"

docker compose -f "$compose_file" up -d hdfs-namenode hdfs-datanode

for attempt in $(seq 1 36); do
  report="$(docker exec gds-hdfs-namenode hdfs dfsadmin -report 2>/dev/null || true)"
  if grep -q "Live datanodes (1)" <<<"$report"; then
    docker exec gds-hdfs-namenode hdfs dfs -mkdir -p /data/gds /checkpoints/gds
    echo "HDFS is healthy: hdfs://hdfs-namenode:8020 (one live DataNode)"
    echo "NameNode UI: http://localhost:9870"
    exit 0
  fi
  echo "Waiting for HDFS ($attempt/36)"
  sleep 5
done

docker compose -f "$compose_file" logs --tail=100 hdfs-namenode hdfs-datanode
echo "HDFS did not become healthy within 180 seconds" >&2
exit 1

