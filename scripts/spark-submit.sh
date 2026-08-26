#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -lt 1 ]]; then
  echo "usage: $0 <python-file> [application arguments...]" >&2
  exit 2
fi

application="$1"
shift

docker exec \
  -e HOME=/opt/spark \
  -e PYTHONPATH=/opt/gds-app/src \
  gds-spark-submit \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/opt/spark/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3 \
  "$application" "$@"
