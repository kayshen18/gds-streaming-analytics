#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

iterations=1
interval=15
events_per_cycle=30
rate=5
airlines="CZ,MU,CA,HX"
topic="gds.simulated.v1"

hdfs_root="hdfs://hdfs-namenode:8020/data/gds-live-demo"
checkpoint_root="hdfs://hdfs-namenode:8020/checkpoints/gds-live-demo"
output_version="v1"

usage() {
  cat <<'EOF'
Usage: scripts/live-demo-refresh.sh [options]

Options:
  --iterations NUMBER        Number of refresh cycles (default: 1)
  --interval SECONDS         Delay between cycles (default: 15)
  --events-per-cycle NUMBER  Events generated per cycle (default: 30)
  --rate NUMBER              Events generated per second (default: 5)
  --airlines CODES           Comma-separated airline codes
  --topic NAME               Kafka topic
  -h, --help                 Show this help
EOF
}

require_value() {
  local option="$1"
  local value="${2-}"

  if [[ -z "$value" || "$value" == --* ]]; then
    echo "error: missing value for ${option}" >&2
    exit 2
  fi
}

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --iterations)
      require_value "$1" "${2-}"
      iterations="$2"
      shift 2
      ;;
    --interval)
      require_value "$1" "${2-}"
      interval="$2"
      shift 2
      ;;
    --events-per-cycle)
      require_value "$1" "${2-}"
      events_per_cycle="$2"
      shift 2
      ;;
    --rate)
      require_value "$1" "${2-}"
      rate="$2"
      shift 2
      ;;
    --airlines)
      require_value "$1" "${2-}"
      airlines="$2"
      shift 2
      ;;
    --topic)
      require_value "$1" "${2-}"
      topic="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! "$iterations" =~ ^[1-9][0-9]*$ ]]; then
  echo "error: --iterations must be a positive integer" >&2
  exit 2
fi

if [[ ! "$interval" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "error: --interval must be a nonnegative number" >&2
  exit 2
fi

if [[ ! "$events_per_cycle" =~ ^[1-9][0-9]*$ ]]; then
  echo "error: --events-per-cycle must be a positive integer" >&2
  exit 2
fi

if [[ ! "$rate" =~ ^[1-9][0-9]*([.][0-9]+)?$ ]]; then
  echo "error: --rate must be a positive number" >&2
  exit 2
fi

if ! command -v gds-kafka >/dev/null 2>&1; then
  echo "error: gds-kafka is unavailable; activate .venv first" >&2
  exit 2
fi

for cycle in $(seq 1 "$iterations"); do
  timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
  run_id="live-cycle-${timestamp}-${cycle}"
  snapshot_name="live-${timestamp}-$$-${cycle}"
  snapshot_dir="infrastructure/spark-hdfs/runtime/mysql-snapshots/${snapshot_name}"

  echo
  echo "=== Live refresh cycle ${cycle}/${iterations} ==="
  echo "Generating ${events_per_cycle} events into ${topic}"

  gds-kafka simulate \
    --bootstrap-servers localhost:9092 \
    --topic "$topic" \
    --airlines "$airlines" \
    --rate "$rate" \
    --limit "$events_per_cycle" \
    --run-id "$run_id"

  echo "Processing new Kafka offsets and merging metrics"

  bash scripts/spark-run-available.sh \
    --topic "$topic" \
    --starting-offsets earliest \
    --hdfs-root "$hdfs_root" \
    --checkpoint-root "$checkpoint_root" \
    --output-version "$output_version"

  echo "Validating HDFS outputs"

  bash scripts/hdfs-validate-results.sh \
    --hdfs-root "$hdfs_root" \
    --checkpoint-root "$checkpoint_root" \
    --output-version "$output_version"

  echo "Exporting MySQL snapshot ${snapshot_name}"

  bash scripts/mysql-export-hdfs-snapshot.sh \
    "$hdfs_root" \
    "$checkpoint_root" \
    "$output_version" \
    "$snapshot_name" \
    "kafka:${topic}"

  echo "Publishing snapshot to MySQL"

  bash scripts/mysql-publish.sh \
    --csv "${snapshot_dir}/metrics.csv" \
    --manifest "${snapshot_dir}/manifest.json"

  echo "Validating MySQL serving snapshot"

  bash scripts/mysql-validate.sh \
    --csv "${snapshot_dir}/metrics.csv" \
    --manifest "${snapshot_dir}/manifest.json"

  echo "Cycle ${cycle} published successfully"

  if [[ "$cycle" -lt "$iterations" ]]; then
    echo "Waiting ${interval} seconds before the next cycle"
    sleep "$interval"
  fi
done

echo
echo "Live demo refresh completed: ${iterations} cycle(s)"
