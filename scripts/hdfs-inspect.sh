#!/usr/bin/env bash
set -euo pipefail

echo "HDFS cluster report"
docker exec gds-hdfs-namenode hdfs dfsadmin -report
echo
echo "GDS paths"
docker exec gds-hdfs-namenode hdfs dfs -ls -R /data/gds /checkpoints/gds

