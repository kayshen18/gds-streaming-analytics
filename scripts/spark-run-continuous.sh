#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
interval="${GDS_PROCESSING_INTERVAL:-10 seconds}"
"$project_root/scripts/spark-submit.sh" \
  /opt/gds-app/src/gds_pipeline/spark_cli.py \
  stream --trigger processing-time --processing-interval "$interval" "$@"
