#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 5 ]]; then
  echo "usage: $0 <hdfs-root> <checkpoint-root> <output-version> <destination-name> <source-identity>" >&2
  exit 2
fi

hdfs_root="$1"
checkpoint_root="$2"
output_version="$3"
destination_name="$4"
source_identity="$5"

if [[ ! "$destination_name" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
  echo "destination-name must be a simple directory name" >&2
  exit 2
fi

host_destination="infrastructure/spark-hdfs/runtime/mysql-snapshots/$destination_name"
container_destination="/opt/gds-runtime/mysql-snapshots/$destination_name"
mkdir -p "$(dirname "$host_destination")"

bash scripts/spark-submit.sh \
  /opt/gds-app/src/gds_pipeline/mysql_export_runner.py \
  --hdfs-root "$hdfs_root" \
  --checkpoint-root "$checkpoint_root" \
  --output-version "$output_version" \
  --destination "$container_destination" \
  --source-identity "$source_identity"

echo "Snapshot exported to $host_destination"
