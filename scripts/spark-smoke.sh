#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_file="$(mktemp)"
trap 'rm -f "$output_file"' EXIT

"$project_root/scripts/spark-submit.sh" \
  /opt/gds-app/infrastructure/spark-hdfs/spark/smoke.py \
  2>&1 | tee "$output_file"

grep -q "spark_count=3" "$output_file"
grep -q "hdfs_count=3" "$output_file"
echo "Spark-to-HDFS smoke test passed"
