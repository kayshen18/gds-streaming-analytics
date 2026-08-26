# Spark Structured Streaming and HDFS Runbook

Run every shell command below in WSL from the project worktree. The commands
preserve existing Kafka and HDFS data unless a reset command is explicitly
used.

## 1. Enter the project environment

```bash
cd /mnt/c/Users/juno-/Documents/Codex/2026-08-10/linux-shell-fpga/gds-streaming-analytics/.worktrees/offline-baseline
source ~/.bashrc
source .venv/bin/activate
```

`JAVA_HOME` should be `/usr/lib/jvm/java-17-openjdk-amd64` and `which python`
should point into this worktree's `.venv`.

## 2. Start and inspect the services

```bash
bash scripts/kafka-up.sh
bash scripts/bigdata-up.sh

docker compose -f infrastructure/kafka/compose.yaml ps
docker compose -f infrastructure/spark-hdfs/compose.yaml ps
docker exec gds-hdfs-namenode hdfs dfsadmin -report
```

Open the Spark UI at <http://localhost:8080> and the NameNode UI at
<http://localhost:9870> from Windows.

Stopping containers preserves Kafka and HDFS named volumes:

```bash
bash scripts/bigdata-down.sh
bash scripts/kafka-down.sh
```

Do not run `kafka-reset.sh` or `hdfs-reset.sh` during evidence collection;
their confirmed reset modes delete persistent data.

## 3. Test layers

The ordinary suite is safe and skips tests that require live services:

```bash
pytest -q
```

The complete 100-record route creates and cleans isolated UUID resources:

```bash
RUN_SPARK_PIPELINE_INTEGRATION=1 \
pytest tests/integration/test_spark_kafka_hdfs.py::test_one_hundred_records_reconcile_from_kafka_to_hdfs -v
```

Checkpoint recovery runs 40 records, then 60 more through the same checkpoint:

```bash
RUN_SPARK_PIPELINE_INTEGRATION=1 \
pytest tests/integration/test_spark_kafka_hdfs.py::test_checkpoint_resume_processes_only_new_kafka_offsets -v
```

## 4. Full-data preflight

The full source was already loaded into `gds.raw.v1`. Verify that its three end
offsets still sum to exactly 2,563,566 before starting Spark:

```bash
docker exec gds-kafka \
  /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:29092 \
  --topic gds.raw.v1 \
  | tee benchmarks/local/spark-full-preflight-offsets.txt

awk -F: '{sum += $3} END {print "total_offsets=" sum}' \
  benchmarks/local/spark-full-preflight-offsets.txt
```

Expected: `total_offsets=2563566`. Stop if the total differs.

Create a unique, immutable evidence location. Keep these variables in the same
terminal for all remaining commands:

```bash
run_tag="v1-full-$(date +%Y%m%d-%H%M%S)"
hdfs_root="hdfs://hdfs-namenode:8020/data/gds-full/$run_tag"
checkpoint_root="hdfs://hdfs-namenode:8020/checkpoints/gds-full/$run_tag"
evidence_dir="benchmarks/local/spark-full-run/$run_tag"
mkdir -p "$evidence_dir"

printf '%s\n' \
  "run_tag=$run_tag" \
  "hdfs_root=$hdfs_root" \
  "checkpoint_root=$checkpoint_root" \
  | tee "$evidence_dir/run.env"
```

## 5. Timed full available-now run

`available-now` consumes all offsets visible at startup, writes batch-scoped
Parquet, exits, and merges batch deltas into final hourly metrics.

```bash
set -o pipefail
/usr/bin/time -v \
  -o "$evidence_dir/spark-time.txt" \
  bash scripts/spark-run-available.sh \
    --bootstrap-servers kafka:29092 \
    --topic gds.raw.v1 \
    --starting-offsets earliest \
    --hdfs-root "$hdfs_root" \
    --checkpoint-root "$checkpoint_root" \
    --output-version v1 \
  2>&1 | tee "$evidence_dir/spark-output.log"

echo "spark_exit=$?" | tee "$evidence_dir/spark-exit.txt"
```

Do not rerun with a different checkpoint against the same output root. A retry
uses both the same `hdfs_root` and `checkpoint_root`; Spark then resumes from
checkpointed Kafka offsets and batch-id writes remain replaceable.

## 6. Validate and inspect HDFS

```bash
bash scripts/hdfs-validate-results.sh \
  --hdfs-root "$hdfs_root" \
  --checkpoint-root "$checkpoint_root" \
  --output-version v1 \
  | tee "$evidence_dir/validation.txt"

docker exec gds-hdfs-namenode hdfs dfs -count -h \
  "$hdfs_root/v1/raw_events" \
  "$hdfs_root/v1/clean_events" \
  "$hdfs_root/v1/dead_letter" \
  "$hdfs_root/v1/quality_metrics" \
  "$hdfs_root/v1/hourly_airline_metrics" \
  | tee "$evidence_dir/hdfs-count.txt"

docker exec gds-hdfs-namenode hdfs dfs -ls -R "$hdfs_root/v1" \
  | tee "$evidence_dir/hdfs-files.txt"
```

The final reconciliation must match the independent baseline: 2,563,566 input
records, 2,560,708 business-valid records, 2,858 dead letters, 3,203 final
hour-airline groups, 1,310,068 successful response records, and 2,145,511
success tokens. The CLI's lightweight `validate` command checks readability and
basic invariants; exact baseline reconciliation is recorded separately in the
full acceptance step.

The deterministic offline metrics CSV was produced twice with SHA-256
`9b0f4a3afc33e73461414ff2d60a2653e32a5fdbcfe8a810b8b2b42525fcc0be`.
The full acceptance runner compares the canonical Spark result to this digest
as well as checking all six exact totals.

The accepted 2026-08-13 full run used tag `v1-full-20260813-123126`, exited
zero after 21 minutes 17.86 seconds, occupied 526.39 MiB in HDFS, and matched
all reference totals and the deterministic metrics hash. Its ignored local
evidence is under `benchmarks/local/spark-full-run/` and the durable Parquet
and checkpoint data remain in their timestamp-specific HDFS roots.

## 7. Continuous mode

For a demonstration that stays alive and processes new Kafka messages:

```bash
bash scripts/spark-run-continuous.sh \
  --bootstrap-servers kafka:29092 \
  --topic gds.raw.v1 \
  --hdfs-root hdfs://hdfs-namenode:8020/data/gds-demo \
  --checkpoint-root hdfs://hdfs-namenode:8020/checkpoints/gds-demo \
  --output-version v1
```

Stop it with `Ctrl+C`, then run `scripts/spark-merge-metrics.sh` using the same
storage arguments to refresh the final aggregate.

The automated integration test performs a safer reproducible demonstration:

```bash
RUN_SPARK_PIPELINE_INTEGRATION=1 \
pytest tests/integration/test_spark_kafka_hdfs.py::test_processing_time_restart_processes_two_waves_once -v
```

It waits for committed progress rather than sleeping for a fixed duration,
stops each query through Spark's `query.stop()`, reuses one checkpoint, checks
100 unique Kafka partition/offset locations, and cleans its UUID resources.
The verified 2026-08-13 run completed in 287.67 seconds.

## 8. Troubleshooting

- An Ivy `Permission denied` under `/opt/spark/.ivy2` means the cache-init
  container has not prepared the named volume. Restart with
  `bash scripts/bigdata-down.sh && bash scripts/bigdata-up.sh`.
- A Kafka package download error can be diagnosed by inspecting
  `gds-spark-submit` logs and the `gds-spark-ivy-cache` volume. Successful runs
  reuse the pinned connector cache.
- Check Spark worker registration at <http://localhost:8080>. One worker with
  two cores should be visible.
- Many tiny Parquet files indicate overly frequent micro-batches. Inspect file
  counts with `hdfs dfs -count`; compact or tune triggers before presenting a
  production-scale design.
- Never delete a computed path unless its printed value is the intended
  UUID/timestamp-specific test or evidence root.
