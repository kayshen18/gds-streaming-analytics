#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
"$project_root/scripts/spark-submit.sh" \
  /opt/gds-app/src/gds_pipeline/spark_cli.py validate "$@"
