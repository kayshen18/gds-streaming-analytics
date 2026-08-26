# Spark HDFS Streaming Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Consume versioned GDS events from Kafka with PySpark Structured Streaming, preserve raw/clean/dead-letter Parquet in HDFS, compute auditable hourly-airline metrics, and prove checkpoint recovery and full-data reconciliation.

**Architecture:** A Dockerized Spark master/worker and HDFS NameNode/DataNode join the existing Kafka network. One Structured Streaming query reads Kafka and applies shared DataFrame transformations inside `foreachBatch`; deterministic `batch_id` outputs feed a separate aggregate merge command. Pure transformations remain independently testable with a local Spark session.

**Tech Stack:** Python 3.13, PySpark, Apache Spark stable 4.x with Scala 2.13, `spark-sql-kafka-0-10_2.13`, Hadoop 3 HDFS, Kafka 4.3.1, Docker Compose, Parquet, pytest

## Global Constraints

- Never use preview releases or `latest` image tags.
- Confirm exact Spark, Kafka connector, Java, Hadoop, and Docker image compatibility before pinning.
- Keep all application business logic in Python; JVM services and connectors are runtime dependencies.
- Use one Structured Streaming query and one versioned HDFS checkpoint root.
- Support `available-now` and `processing-time`; validate `available-now` first.
- Preserve every invalid record with a stable failure stage and reason.
- Publish both `successful_response_records` and `success_token_count`.
- Treat the independent Python baseline, not the old course report, as the reconciliation reference.
- Never commit raw source data, HDFS volumes, Spark checkpoints, connector caches, or local benchmark artifacts.
- Never reset shared Kafka or HDFS state implicitly in tests.

---

### Task 1: Runtime Version and Resource Contract

**Files:**
- Create: `infrastructure/spark-hdfs/versions.env`
- Create: `infrastructure/spark-hdfs/README.md`
- Create: `scripts/check-bigdata-prerequisites.sh`
- Test: `tests/test_bigdata_prerequisites.py`

**Interfaces:**
- Produces pinned environment keys `SPARK_VERSION`, `SCALA_BINARY_VERSION`, `HADOOP_VERSION`, `JAVA_MAJOR`, `SPARK_KAFKA_PACKAGE`, and image references used by later Compose and submit scripts.
- Produces `scripts/check-bigdata-prerequisites.sh`, which exits nonzero when Docker, Compose, memory, disk, or required version keys are missing.

- [ ] **Step 1: Verify stable primary-source versions**

Record official Apache Spark release, Java support, Kafka connector coordinates, and Hadoop Docker guidance. Select one stable Spark release and matching `_2.13` connector; verify that the chosen Spark distribution includes a Hadoop 3 client. Record source links and the decision in `infrastructure/spark-hdfs/README.md`.

- [ ] **Step 2: Write failing contract tests**

Create tests that parse `versions.env` and require every key, prohibit `latest`, require the connector version to equal `SPARK_VERSION`, and require Scala binary version `2.13`.

```python
def test_connector_matches_spark_version(version_map):
    assert version_map["SPARK_KAFKA_PACKAGE"] == (
        "org.apache.spark:spark-sql-kafka-0-10_2.13:"
        + version_map["SPARK_VERSION"]
    )
```

- [ ] **Step 3: Run tests and verify RED**

Run: `pytest tests/test_bigdata_prerequisites.py -v`

Expected: FAIL because `versions.env` and the prerequisite script do not exist.

- [ ] **Step 4: Add pins and prerequisite script**

The Bash script must use `set -euo pipefail`, run `docker version`, `docker compose version`, inspect WSL available memory and `/mnt/d` free space, source `versions.env`, and print one concise summary. Require at least 10 GiB available memory and 20 GiB free disk for full-data acceptance; allow `--smoke` to require 6 GiB memory and 8 GiB disk.

- [ ] **Step 5: Verify GREEN and run the script in WSL**

Run: `pytest tests/test_bigdata_prerequisites.py -v`

User runs: `bash scripts/check-bigdata-prerequisites.sh --smoke`

Expected: tests pass and the script reports all pinned versions plus sufficient resources.

- [ ] **Step 6: Commit**

```bash
git add infrastructure/spark-hdfs scripts/check-bigdata-prerequisites.sh tests/test_bigdata_prerequisites.py
git commit -m "build: pin Spark and Hadoop runtime contract"
```

### Task 2: HDFS Compose Environment

**Files:**
- Create: `infrastructure/spark-hdfs/compose.yaml`
- Create: `infrastructure/spark-hdfs/hadoop/core-site.xml`
- Create: `infrastructure/spark-hdfs/hadoop/hdfs-site.xml`
- Create: `scripts/bigdata-up.sh`
- Create: `scripts/bigdata-down.sh`
- Create: `scripts/hdfs-reset.sh`
- Create: `scripts/hdfs-inspect.sh`
- Modify: `.gitignore`
- Test: `tests/test_bigdata_compose.py`

**Interfaces:**
- HDFS URI: `hdfs://hdfs-namenode:8020` inside Docker.
- NameNode UI: `http://localhost:9870`.
- Named volumes: explicit NameNode and DataNode volumes.
- Reset requires literal `--confirm` and deletes only named HDFS volumes and the `/data/gds`/`/checkpoints/gds` state they contain.

- [ ] **Step 1: Write failing Compose contract tests**

Test pinned images, NameNode/DataNode services, health checks, internal URI, named volumes, resource limits, no `latest`, and explicit reset confirmation.

- [ ] **Step 2: Run tests and verify RED**

Run: `pytest tests/test_bigdata_compose.py -v`

Expected: FAIL because Compose and scripts are missing.

- [ ] **Step 3: Implement the minimal HDFS topology**

Add configuration for one NameNode and one DataNode, replication factor one, WebHDFS/UI access, and Docker-network DNS names. `bigdata-up.sh` waits on HDFS health by running `hdfs dfsadmin -report`; `bigdata-down.sh` preserves volumes; `hdfs-reset.sh` requires `--confirm`.

- [ ] **Step 4: Validate syntax and start HDFS**

Run: `docker compose -f infrastructure/spark-hdfs/compose.yaml config --quiet`

User runs:

```bash
bash scripts/bigdata-up.sh
bash scripts/hdfs-inspect.sh
```

Expected: one live DataNode, replication one, and writable HDFS root.

- [ ] **Step 5: Prove HDFS read/write and persistence**

Write a marker to `/data/gds/_smoke`, stop without volumes, restart, and verify the marker remains. Then remove only the marker.

- [ ] **Step 6: Verify tests and commit**

```bash
pytest tests/test_bigdata_compose.py -v
git add .gitignore infrastructure/spark-hdfs scripts tests/test_bigdata_compose.py
git commit -m "infra: add persistent HDFS development environment"
```

### Task 3: Spark Master, Worker, and Submit Environment

**Files:**
- Modify: `infrastructure/spark-hdfs/compose.yaml`
- Create: `infrastructure/spark-hdfs/spark/spark-defaults.conf`
- Create: `scripts/spark-submit.sh`
- Create: `scripts/spark-smoke.sh`
- Modify: `tests/test_bigdata_compose.py`

**Interfaces:**
- Spark master URL: `spark://spark-master:7077`.
- Spark UI: `http://localhost:8080`.
- Worker exposes 4 GiB initially and two cores; adjust only after measured resource checks.
- `spark-submit.sh <python-file> [args...]` injects the pinned Kafka package and HDFS configuration.

- [ ] **Step 1: Extend failing tests**

Require Spark master/worker/submit services, master URL, UI port, worker memory/cores, shared application mount, matching Kafka package, and dependency health gates.

- [ ] **Step 2: Run tests and verify RED**

Run: `pytest tests/test_bigdata_compose.py -v`

Expected: FAIL for missing Spark services.

- [ ] **Step 3: Implement Spark services and submit wrapper**

Use the pinned stable image. Configure Java and Python consistently across driver and worker. Mount only repository source needed for submission; connector artifacts use an ignored cache volume or Maven cache.

- [ ] **Step 4: Run a Spark smoke job**

`spark-smoke.sh` submits a small DataFrame job that counts three rows, writes Parquet to an isolated HDFS smoke path, reads it back, requires count three, and removes the path.

- [ ] **Step 5: Verify UI, worker registration, and HDFS interoperability**

User runs `bash scripts/spark-smoke.sh`; expected output includes `spark_count=3`, `hdfs_count=3`, and successful exit.

- [ ] **Step 6: Commit**

```bash
pytest tests/test_bigdata_compose.py -v
git add infrastructure/spark-hdfs scripts tests/test_bigdata_compose.py
git commit -m "infra: run Spark with HDFS in Docker"
```

### Task 4: Spark Configuration and Dataset Paths

**Files:**
- Create: `src/gds_pipeline/spark_config.py`
- Create: `tests/test_spark_config.py`

**Interfaces:**
- `SparkPipelineSettings` validates brokers, topic, HDFS root, checkpoint root, trigger, interval, output version, and starting offsets.
- `DatasetPaths.from_settings(settings) -> DatasetPaths` produces `raw_events`, `clean_events`, `dead_letter`, `quality_metrics`, `hourly_airline_deltas`, `hourly_airline_metrics`, and checkpoint URIs.

- [ ] **Step 1: Write failing validation/path tests**

Cover blank brokers/topic, unsupported trigger, processing-time without interval, available-now with interval, non-versioned output, and exact default HDFS paths.

- [ ] **Step 2: Verify RED**

Run: `pytest tests/test_spark_config.py -v`

Expected: module missing.

- [ ] **Step 3: Implement frozen settings and paths**

Default output version is `v1`; default root is `hdfs://hdfs-namenode:8020/data/gds`; checkpoint is `hdfs://hdfs-namenode:8020/checkpoints/gds/spark-ingestion-v1`.

- [ ] **Step 4: Verify GREEN and full suite**

Run: `pytest tests/test_spark_config.py -v && pytest -q`

- [ ] **Step 5: Commit**

```bash
git add src/gds_pipeline/spark_config.py tests/test_spark_config.py
git commit -m "feat: define Spark pipeline settings and paths"
```

### Task 5: Envelope and GDS DataFrame Transformations

**Files:**
- Create: `src/gds_pipeline/spark_schema.py`
- Create: `src/gds_pipeline/spark_transform.py`
- Create: `tests/spark/conftest.py`
- Create: `tests/spark/test_spark_transform.py`
- Modify: `pyproject.toml`

**Interfaces:**
- `envelope_schema() -> StructType`.
- `parse_kafka_envelopes(df: DataFrame) -> tuple[DataFrame, DataFrame]` returns valid raw events and envelope dead letters.
- `parse_gds_records(df: DataFrame) -> tuple[DataFrame, DataFrame]` returns clean events and business dead letters.
- Functions accept static or streaming DataFrames and contain no sinks.

- [ ] **Step 1: Pin matching PySpark as a development extra**

Add a `spark` optional dependency exactly matching the pinned Spark runtime. Reinstall with `python -m pip install -e '.[dev,spark]'` in WSL.

- [ ] **Step 2: Write local Spark fixtures and failing tests**

Create a session-scoped local Spark fixture and rows covering valid JSON, invalid JSON, wrong schema, bad event ID, wrong key, comma-only record, invalid date/hour/type, Unicode, and Kafka metadata retention.

- [ ] **Step 3: Verify RED**

Run: `pytest tests/spark/test_spark_transform.py -v`

Expected: transformation modules missing.

- [ ] **Step 4: Implement schema and transformations using Spark SQL expressions**

Prefer `from_json`, `sha2`, `concat_ws`, `split`, `to_date`, `when`, `filter`, and higher-order array functions. Do not use Python row UDFs for parsing or hashing because they impede Spark optimization and serialization.

- [ ] **Step 5: Verify exact failure stages/reasons and full suite**

Run: `pytest tests/spark/test_spark_transform.py -v && pytest -q`

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml src/gds_pipeline/spark_schema.py src/gds_pipeline/spark_transform.py tests/spark
git commit -m "feat: parse Kafka and GDS records with Spark"
```

### Task 6: Airline Metric Transformations

**Files:**
- Modify: `src/gds_pipeline/spark_transform.py`
- Create: `tests/spark/test_spark_metrics.py`

**Interfaces:**
- `success_token_rows(clean_df: DataFrame) -> DataFrame` emits one row per token occurrence.
- `hourly_airline_deltas(clean_df: DataFrame, batch_id: int) -> DataFrame` emits both approved metrics.
- `quality_metrics(input_df, raw_df, clean_df, dead_df, batch_id) -> DataFrame` emits counts by reason.

- [ ] **Step 1: Write failing repeated-token tests**

Use one CA/CA/MU event and one CA event. Require CA response records two, CA tokens three, MU response records one, MU tokens one.

- [ ] **Step 2: Verify RED**

Run: `pytest tests/spark/test_spark_metrics.py -v`

- [ ] **Step 3: Implement token explode and dual aggregation**

Response counts deduplicate `(event_id, airline_code)`; token counts retain every occurrence. Join the two counts on date/hour/airline.

- [ ] **Step 4: Implement quality metrics and verify totals reconcile**

Require `input_count == envelope_invalid + business_invalid + business_valid` for each test batch.

- [ ] **Step 5: Run Spark and complete suites, then commit**

```bash
pytest tests/spark/test_spark_metrics.py -v
pytest -q
git add src/gds_pipeline/spark_transform.py tests/spark/test_spark_metrics.py
git commit -m "feat: compute auditable airline metrics in Spark"
```

### Task 7: Idempotent HDFS Batch Writer Spike

**Files:**
- Create: `src/gds_pipeline/spark_writer.py`
- Create: `tests/integration/test_hdfs_batch_writer.py`
- Modify: `pyproject.toml`

**Interfaces:**
- `write_batch_outputs(batch_df: DataFrame, batch_id: int, paths: DatasetPaths) -> BatchWriteSummary`.
- A rerun of the same `batch_id` produces one logical batch output, not duplicated files or rows.

- [ ] **Step 1: Register an opt-in `spark_hdfs_integration` marker and safety gate**

Require `RUN_SPARK_HDFS_INTEGRATION=1` before importing or connecting to HDFS. Use a UUID-isolated output root and never reset shared volumes.

- [ ] **Step 2: Write a failing real-HDFS retry test**

Write batch 7 twice, read its paths, and require counts to equal one write. Verify raw, clean, dead-letter, quality, and delta outputs independently.

- [ ] **Step 3: Run without the gate**

Run: `pytest -q`

Expected: normal tests pass and the HDFS writer module is skipped safely.

- [ ] **Step 4: Implement deterministic staging and replacement**

Write each dataset to a staging URI ending in a UUID, validate the write, remove only the exact `batch_id=N` destination if present, then rename staging to destination using Hadoop FileSystem APIs. Record completion metadata last. Cleanup is restricted to the isolated/versioned output root.

- [ ] **Step 5: Run real-HDFS integration twice**

User runs:

```bash
RUN_SPARK_HDFS_INTEGRATION=1 \
pytest tests/integration/test_hdfs_batch_writer.py -v
```

Expected: PASS on two consecutive invocations with no duplicate rows.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml src/gds_pipeline/spark_writer.py tests/integration/test_hdfs_batch_writer.py
git commit -m "feat: write idempotent Spark batches to HDFS"
```

### Task 8: Structured Streaming Query and Aggregate Merge

**Files:**
- Create: `src/gds_pipeline/spark_job.py`
- Create: `src/gds_pipeline/spark_merge.py`
- Create: `tests/spark/test_spark_job.py`
- Create: `tests/spark/test_spark_merge.py`

**Interfaces:**
- `build_kafka_stream(spark, settings) -> DataFrame`.
- `process_batch(batch_df, batch_id, settings) -> BatchWriteSummary`.
- `start_query(spark, settings) -> StreamingQuery`.
- `merge_hourly_airline_metrics(spark, paths) -> MergeSummary`.

- [ ] **Step 1: Write failing source/trigger tests with injected builders**

Verify Kafka bootstrap/topic/options, earliest vs latest starting offsets, available-now trigger, processing-time interval, and versioned checkpoint location without requiring a broker.

- [ ] **Step 2: Verify RED**

Run: `pytest tests/spark/test_spark_job.py -v`

- [ ] **Step 3: Implement one query and `foreachBatch` routing**

`process_batch` persists the input while producing multiple outputs, invokes transformations and the idempotent writer, then unpersists in `finally`.

- [ ] **Step 4: Write failing merge tests and implement final aggregation**

Read all delta batches, group by date/hour/airline, sum both metrics, write to a staging final path, and atomically replace the versioned final path.

- [ ] **Step 5: Verify Spark suites and commit**

```bash
pytest tests/spark/test_spark_job.py tests/spark/test_spark_merge.py -v
pytest -q
git add src/gds_pipeline/spark_job.py src/gds_pipeline/spark_merge.py tests/spark
git commit -m "feat: run GDS Structured Streaming pipeline"
```

### Task 9: Spark CLI and Operational Scripts

**Files:**
- Create: `src/gds_pipeline/spark_cli.py`
- Create: `tests/test_spark_cli.py`
- Modify: `pyproject.toml`
- Create: `scripts/spark-run-available.sh`
- Create: `scripts/spark-run-continuous.sh`
- Create: `scripts/spark-merge-metrics.sh`
- Create: `scripts/hdfs-validate-results.sh`

**Interfaces:**
- Entry point `gds-spark` with `stream`, `merge`, and `validate` subcommands.
- Scripts call `spark-submit.sh` and never duplicate application logic.

- [ ] **Step 1: Write failing CLI tests**

Cover brokers/topic/HDFS/checkpoint/version, both triggers, interval rules, starting offsets, merge, validation, and stable nonzero exit codes.

- [ ] **Step 2: Verify RED**

Run: `pytest tests/test_spark_cli.py -v`

- [ ] **Step 3: Implement CLI and scripts**

`available-now` waits for termination then optionally merges; continuous mode waits until signal and leaves checkpoint intact. Validation reads HDFS counts without trusting streaming logs.

- [ ] **Step 4: Verify GREEN and help output**

Run: `pytest tests/test_spark_cli.py -v && gds-spark --help`

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml src/gds_pipeline/spark_cli.py tests/test_spark_cli.py scripts
git commit -m "feat: expose Spark streaming operations"
```

### Task 10: 100-Record Kafka-to-HDFS Integration

**Files:**
- Create: `tests/integration/test_spark_kafka_hdfs.py`
- Create: `tests/fixtures/spark_expected_metrics.json`
- Modify: `pyproject.toml`

**Interfaces:**
- Requires `RUN_SPARK_PIPELINE_INTEGRATION=1`.
- Creates UUID Kafka topic plus UUID HDFS output/checkpoint roots and deletes only those resources.

- [ ] **Step 1: Write guarded integration test**

Produce 100 deterministic records containing valid, malformed, and repeated-token cases; launch available-now Spark; read every HDFS dataset; assert exact counts and metrics.

- [ ] **Step 2: Prove ordinary pytest is safe**

Run: `pytest -q`

Expected: integration module skipped and all unit/local Spark tests pass.

- [ ] **Step 3: Run real integration**

User runs:

```bash
RUN_SPARK_PIPELINE_INTEGRATION=1 \
pytest tests/integration/test_spark_kafka_hdfs.py -v
```

Require all output paths, exact 100 input records, reconciled valid/invalid counts, no unclassified loss, and expected dual metrics.

- [ ] **Step 4: Inspect HDFS Parquet and commit**

Use `hdfs dfs -count`, Spark schema printing, and representative rows. Then commit:

```bash
git add pyproject.toml tests/integration/test_spark_kafka_hdfs.py tests/fixtures/spark_expected_metrics.json
git commit -m "test: cover Kafka to HDFS streaming"
```

### Task 11: Spark Checkpoint Recovery Integration

**Files:**
- Modify: `tests/integration/test_spark_kafka_hdfs.py`

**Interfaces:**
- Reuses one UUID topic and identical HDFS output/checkpoint roots across two available-now query executions.

- [ ] **Step 1: Write recovery test**

Send 40 records, run Spark, capture Kafka end offsets and HDFS counts; send 60 more, rerun with the same checkpoint, and require the second query to process only new offsets.

- [ ] **Step 2: Verify no duplicate outputs**

Require raw plus dead-letter input totals exactly 100, clean plus business dead-letter exactly envelope-valid count, unique Kafka `(partition, offset)` locations, and correct final metrics.

- [ ] **Step 3: Run integration twice**

User runs the guarded test twice; both invocations must pass with isolated roots.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/test_spark_kafka_hdfs.py
git commit -m "test: prove Spark checkpoint recovery"
```

### Task 12: Runbook and Full-Data Acceptance

**Files:**
- Create: `docs/spark-hdfs-runbook.md`
- Modify: `README.md`
- Create ignored: `benchmarks/local/spark-full-run/`

**Interfaces:**
- Runbook covers prerequisites, service lifecycle, HDFS inspection/reset, Spark submit, both triggers, merge, validation, recovery, connector-cache troubleshooting, small-file inspection, and safe cleanup.

- [ ] **Step 1: Write and dry-run the runbook through the 100-record path**

Every command must be copied from verified scripts. Document which commands are WSL Bash and which URLs are opened on Windows.

- [ ] **Step 2: Run all tests and health checks**

Run unit/local Spark suites, all guarded real integrations, Compose config, Spark worker registration, HDFS report, and Kafka health.

- [ ] **Step 3: Prepare clean versioned full-data roots**

Use new explicit `v1-full-<timestamp>` output and checkpoint roots. Do not delete prior evidence implicitly. Prove Kafka has exactly 2,563,566 records before processing.

- [ ] **Step 4: Run timed available-now processing and merge**

Record wall time, Spark progress JSON, container resources, HDFS space, file counts, partition counts, and exit status in ignored local artifacts.

- [ ] **Step 5: Independently validate HDFS outputs**

Require the baseline reference counts from the specification. Compare hourly metrics by deterministic sorted hash as well as totals; investigate and document every difference.

- [ ] **Step 6: Run a continuous-mode demonstration**

Use a fresh UUID topic/checkpoint, start a processing-time query, produce two small waves, stop gracefully, restart, and prove only new offsets are processed.

- [ ] **Step 7: Update README with measured results and limitations**

Do not claim HBase, Hive, PostgreSQL, API, dashboard, multi-broker HA, TLS, or exactly-once unless later phases actually implement them.

- [ ] **Step 8: Final verification and commit**

```bash
pytest -q
git diff --check
git ls-files data/raw .checkpoints benchmarks/local
git status --short
git add README.md docs/spark-hdfs-runbook.md
git commit -m "docs: record Spark HDFS benchmark"
```
