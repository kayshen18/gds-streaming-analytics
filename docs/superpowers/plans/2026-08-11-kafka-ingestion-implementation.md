# Kafka Ingestion Phase Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run Kafka 4.3.1 locally and publish and independently verify every GDS source record through a versioned JSON envelope.

**Architecture:** Docker Compose runs one KRaft combined broker/controller. Pure event functions generate stable identities; a producer adapter handles asynchronous delivery and contiguous checkpoints; a separate consumer validates Kafka contents.

**Tech Stack:** Docker Desktop 4.86+, Docker Compose v5.3+, Apache Kafka 4.3.1, Python 3.11+, confluent-kafka, pytest

## Global Constraints

- Pin `apache/kafka:4.3.1`; never use `latest`.
- Use topic `gds.raw.v1`, three partitions, replication factor one.
- Never commit source data, Kafka volumes, checkpoints, or local benchmarks.
- Unit tests do not require Kafka; broker tests use pytest marker `integration`.
- Pass a clean 100-record run before attempting 2,563,566 records.
- Ordinary shutdown preserves data; only `kafka-reset.sh --confirm` deletes it.

---

### Task 1: Kafka Compose Environment

**Files:** Create `infrastructure/kafka/compose.yaml`, `scripts/kafka-up.sh`, `scripts/kafka-down.sh`, `scripts/kafka-reset.sh`, `scripts/kafka-create-topic.sh`; modify `.gitignore`.

**Interfaces:** Host clients use `localhost:9092`; container clients use `kafka:29092`; service name is `kafka`.

- [ ] Run `docker version`, `docker compose version`, and `docker info --format '{{.ServerVersion}}'`.
- [ ] Define the pinned image, KRaft combined roles, controller/internal/host listeners, host port 9092, named volume, and health check using `kafka-topics.sh --list`.
- [ ] Run `docker compose -f infrastructure/kafka/compose.yaml config --quiet`; require exit 0.
- [ ] Write scripts with `set -euo pipefail`. Up waits for health; down omits `-v`; reset requires literal `--confirm`; topic creation validates exactly three partitions.
- [ ] Ignore `.checkpoints/` and `benchmarks/local/`.
- [ ] Pull, start, create, and describe the topic. Require one broker and three partitions.
- [ ] Round-trip one message with Apache CLI producer/consumer.
- [ ] Commit with `infra: add Kafka KRaft development environment`.

### Task 2: Event Envelope Contract

**Files:** Create `src/gds_pipeline/event.py`, `tests/test_event.py`.

**Interfaces:** `event_id_for(source_sha256: str, line_number: int, raw_line: str) -> str`; `build_event(...) -> GdsEvent`; `GdsEvent.to_json_bytes() -> bytes`; `GdsEvent.kafka_key() -> bytes`.

- [ ] Write failing tests: repeated construction has the same ID; changing line/raw text changes it; Unicode round-trips; group ID is text before the first comma; comma-only data uses event ID as key.
- [ ] Run `pytest tests/test_event.py -v`; require failure because the module is missing.
- [ ] Implement the frozen event model. Canonical hash input is UTF-8 `1\n{source_sha256}\n{line_number}\n{raw_line}`. Serialize stable compact JSON with `ensure_ascii=False`; timestamp is UTC ending in `Z`.
- [ ] Run the event tests and complete suite; require PASS.
- [ ] Commit with `feat: define versioned Kafka event envelope`.

### Task 3: Producer Configuration and Delivery Accounting

**Files:** Modify `pyproject.toml`; create `src/gds_pipeline/kafka_config.py`, `src/gds_pipeline/producer.py`, `tests/test_producer.py`.

**Interfaces:** `ProducerSettings` validates brokers/topic/limit/rate/flush timeout; `DeliveryTracker.mark_success(int)` and `mark_failure(int, str)`; property `contiguous_confirmed_line`.

- [ ] Pin a verified compatible `confluent-kafka` version and reinstall editable package.
- [ ] Write failing validation tests and callback-order test `1,3,2`, expecting checkpoint progression `1,1,3`; prove a failed line blocks advancement.
- [ ] Implement settings and delivery tracker without broker access; pending successes use a set and failures use a line-to-error mapping.
- [ ] Run producer tests and full suite; require PASS.
- [ ] Commit with `feat: track Kafka delivery acknowledgements`.

### Task 4: Checkpoint and Streaming Producer

**Files:** Modify `src/gds_pipeline/producer.py`, `tests/test_producer.py`; create `tests/test_checkpoint.py`.

**Interfaces:** `Checkpoint.load(Path) -> Checkpoint | None`; `Checkpoint.save_atomic(Path) -> None`; `produce_file(ProducerSettings) -> ProductionSummary`.

- [ ] Write failing checkpoint tests: JSON round-trip, atomic replacement, source-digest mismatch, reset behavior.
- [ ] Implement checkpoint fields: schema version, source digest, last contiguous confirmed line, topic, update time.
- [ ] Write producer tests with a fake adapter: limit, checkpoint skip, queue-full polling, final flush, callback ordering, and summary counts.
- [ ] Implement streaming, event construction, async callbacks, rate pacing, progress every 10,000 submissions, bounded flush, SIGINT stop flag, and safe checkpoint writes.
- [ ] Run all tests; require PASS.
- [ ] Commit with `feat: stream source records into Kafka`.

### Task 5: Producer CLI and 100-Record Integration

**Files:** Modify `src/gds_pipeline/cli.py`, `tests/test_cli.py`, `pyproject.toml`.

**Interfaces:** Add executable `gds-kafka` with subcommand `produce`; preserve `gds-profile`.

- [ ] Write failing CLI tests for input/broker/topic, limit 100, rate, checkpoint flags, and exit codes.
- [ ] Implement CLI wiring without duplicating producer logic.
- [ ] Run all unit tests.
- [ ] Explicitly reset and recreate the topic.
- [ ] Run `gds-kafka produce --input data/raw/kafka采集数据实验.txt --bootstrap-servers localhost:9092 --topic gds.raw.v1 --limit 100 --rate 1000 --checkpoint .checkpoints/smoke.json`.
- [ ] Require 100 submitted, 100 acknowledged, zero final failures; inspect partition end offsets.
- [ ] Commit with `feat: expose Kafka producer CLI`.

### Task 6: Independent Verifier

**Files:** Create `src/gds_pipeline/verifier.py`, `tests/test_verifier.py`; modify `src/gds_pipeline/cli.py`, `tests/test_cli.py`.

**Interfaces:** `validate_message(key: bytes, value: bytes, partition: int, offset: int) -> ValidatedMessage`; `verify_topic(VerifierSettings) -> VerificationSummary`; CLI subcommand `verify`.

- [ ] Write failing tests for valid envelope, invalid UTF-8/JSON, missing field, wrong schema, bad event ID, wrong key, duplicate IDs, and partition totals.
- [ ] Implement pure validation and summary accounting.
- [ ] Add a consumer adapter using fresh group ID, earliest offsets, disabled auto-commit, and idle timeout.
- [ ] Add CLI tests and implementation; run full suite.
- [ ] Verify the clean topic with expected count 100 and idle timeout 20 seconds.
- [ ] Require 100 valid, zero invalid, zero duplicate IDs, and nonzero messages in all partitions.
- [ ] Commit with `feat: verify Kafka ingestion independently`.

### Task 7: Recovery Integration and Runbook

**Files:** Create `tests/integration/test_kafka_recovery.py`, `docs/kafka-runbook.md`; modify `pyproject.toml`.

**Interfaces:** Integration tests require environment variable `RUN_KAFKA_INTEGRATION=1` and never reset volumes implicitly.

- [ ] Register pytest marker `integration`.
- [ ] Write a 100-record round-trip integration test.
- [ ] Write a controlled interruption/resume test proving no source line is lost; report boundary duplicates instead of hiding them.
- [ ] Run integration tests against the healthy broker.
- [ ] Document start, stop, reset, topic inspection, producer, verifier, recovery, and proxy diagnostics.
- [ ] Commit with `test: cover Kafka round trip and recovery`.

### Task 8: Full-Data Acceptance

**Files:** Modify `README.md`; create ignored `.checkpoints/full.json` and `benchmarks/local/kafka-full-run.json`.

**Interfaces:** Commit only measured summaries, never raw local artifacts.

- [ ] Run all unit and integration tests.
- [ ] Explicitly reset Kafka, recreate the topic, and prove all partition end offsets are zero.
- [ ] Produce all 2,563,566 records without a rate cap; record versions, resources, elapsed time, acknowledgements, failures, and throughput.
- [ ] Verify exactly 2,563,566 valid envelopes, correct event IDs, and partition totals summing to the source count.
- [ ] Inspect broker logs and independently recompute representative event IDs.
- [ ] Add measured results and single-node limitations to README.
- [ ] Run `pytest -q`, integration tests, Compose health, `git status --short`, and `git ls-files data/raw .checkpoints benchmarks/local`.
- [ ] Commit with `docs: record Kafka ingestion benchmark`.
