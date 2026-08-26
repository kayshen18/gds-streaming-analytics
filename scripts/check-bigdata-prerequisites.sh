#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
versions_file="$project_root/infrastructure/spark-hdfs/versions.env"

mode="full"
if [[ "${1:-}" == "--smoke" ]]; then
  mode="smoke"
elif [[ $# -ne 0 ]]; then
  echo "usage: $0 [--smoke]" >&2
  exit 2
fi

if [[ ! -r "$versions_file" ]]; then
  echo "error: version contract is not readable: $versions_file" >&2
  exit 2
fi

# shellcheck disable=SC1090
source "$versions_file"

required_keys=(
  SPARK_VERSION SCALA_BINARY_VERSION HADOOP_VERSION JAVA_MAJOR
  SPARK_IMAGE HADOOP_IMAGE SPARK_KAFKA_PACKAGE
)
for key in "${required_keys[@]}"; do
  if [[ -z "${!key:-}" ]]; then
    echo "error: missing version key: $key" >&2
    exit 2
  fi
done

docker version >/dev/null
docker compose version >/dev/null

available_kib="$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)"
disk_available_kib="$(df -Pk /mnt/d | awk 'NR == 2 {print $4}')"

if [[ "$mode" == "smoke" ]]; then
  required_memory_kib=$((6 * 1024 * 1024))
  required_disk_kib=$((8 * 1024 * 1024))
else
  required_memory_kib=$((10 * 1024 * 1024))
  required_disk_kib=$((20 * 1024 * 1024))
fi

if (( available_kib < required_memory_kib )); then
  echo "error: WSL available memory is below ${required_memory_kib} KiB" >&2
  exit 3
fi
if (( disk_available_kib < required_disk_kib )); then
  echo "error: /mnt/d free space is below ${required_disk_kib} KiB" >&2
  exit 3
fi

printf '%s\n' \
  "big-data prerequisites passed" \
  "mode=$mode" \
  "spark=$SPARK_VERSION scala=$SCALA_BINARY_VERSION java=$JAVA_MAJOR" \
  "hadoop=$HADOOP_VERSION" \
  "spark_image=$SPARK_IMAGE" \
  "hadoop_image=$HADOOP_IMAGE" \
  "kafka_package=$SPARK_KAFKA_PACKAGE" \
  "available_memory_gib=$((available_kib / 1024 / 1024))" \
  "d_drive_free_gib=$((disk_available_kib / 1024 / 1024))"
