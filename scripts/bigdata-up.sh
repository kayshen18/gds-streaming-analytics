#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$project_root/infrastructure/spark-hdfs/compose.yaml"

docker network inspect gds-streaming-network >/dev/null 2>&1 || \
  docker network create gds-streaming-network >/dev/null
docker compose -f "$compose_file" up -d

for attempt in $(seq 1 36); do
  report="$(docker exec gds-hdfs-namenode hdfs dfsadmin -report 2>/dev/null || true)"
  if grep -q "Live datanodes (1)" <<<"$report"; then
    docker exec gds-hdfs-namenode hdfs dfs -mkdir -p /data/gds /checkpoints/gds
    echo "HDFS is healthy: hdfs://hdfs-namenode:8020 (one live DataNode)"
    spark_master_status="$(docker inspect --format='{{.State.Health.Status}}' gds-spark-master 2>/dev/null || true)"
    spark_worker_status="$(docker inspect --format='{{.State.Health.Status}}' gds-spark-worker 2>/dev/null || true)"
    if [[ "$spark_master_status" == "healthy" && "$spark_worker_status" == "healthy" ]]; then
      echo "NameNode UI: http://localhost:9870"
      echo "Spark is healthy: spark://spark-master:7077 (one worker, 2 cores, 4 GiB)"
      echo "Spark UI: http://localhost:8080"
      exit 0
    fi
    echo "HDFS ready; waiting for Spark master=$spark_master_status worker=$spark_worker_status"
    sleep 5
    continue
  fi
  echo "Waiting for HDFS ($attempt/36)"
  sleep 5
done

docker compose -f "$compose_file" logs --tail=100 hdfs-namenode hdfs-datanode
echo "HDFS did not become healthy within 180 seconds" >&2
exit 1
