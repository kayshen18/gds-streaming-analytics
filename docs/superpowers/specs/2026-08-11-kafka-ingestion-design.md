# Kafka Ingestion Phase - Design Specification

## 1. Purpose

Implement the first distributed stage of the Real-time GDS Booking Log Analytics Pipeline: reproducible ingestion of the local GDS source file into Apache Kafka and independent verification that Kafka retained the expected messages.

This phase corresponds to the Kafka Producer portion of the original laboratory project. It modernizes that work with KRaft, pinned container versions, structured message envelopes, delivery acknowledgements, stable event identities, automated tests, and measurable acceptance criteria.

## 2. Scope

### Included

- Run one Apache Kafka 4.3.1 broker/controller in KRaft combined mode through Docker Compose.
- Create a three-partition, replication-factor-one topic named `gds.raw.v1`.
- Confirm basic Kafka operation with Apache command-line tools.
- Build a Python producer that streams the source file line by line.
- Wrap each raw record in a versioned JSON event envelope.
- Use the source `group_id` as the Kafka message key when present.
- Enable producer acknowledgements, idempotence, batching, and compression.
- Provide rate limiting, progress reporting, delivery statistics, clean shutdown, and local resume checkpoints.
- Build an independent verifier consumer that checks envelope validity, message counts, partitions, and duplicate event IDs.
- Validate a 100-record smoke run before the complete 2,563,566-record run.

### Excluded

- Spark Structured Streaming.
- HDFS, Parquet, PostgreSQL, APIs, and dashboards.
- A multi-broker production cluster or high-availability claims.
- End-to-end exactly-once guarantees.
- Publishing the course-provided source data to Git or embedding it in container images.

## 3. Environment

- Windows Docker Desktop with the WSL2 backend.
- Docker Compose v5.3.1 or later.
- Apache Kafka image pinned to `apache/kafka:4.3.1`.
- KRaft mode; ZooKeeper is not used.
- Python 3.11 or later in the existing project virtual environment.
- `confluent-kafka` Python client with an exact compatible version pinned during implementation.

The Docker Desktop program and WSL data root are intended to reside primarily on drive D. Small Windows configuration and cache files may remain on drive C.

## 4. Kafka Topology

Development topology:

```text
Kafka container
├── KRaft controller role
├── broker role
├── internal listener for containers
└── host listener on localhost:9092
```

Topic contract:

| Setting | Value | Rationale |
|---|---:|---|
| Name | `gds.raw.v1` | Explicit raw-event schema version |
| Partitions | 3 | Enables later parallel Spark consumption |
| Replication factor | 1 | Required by the single-broker development topology |
| Auto-create | disabled | Topic configuration remains explicit and testable |

Kafka data persists in a named Docker volume. Scripts provide distinct stop and destructive reset operations; ordinary shutdown must not delete topic data.

## 5. Event Contract

Every Kafka value is UTF-8 JSON with these fields:

```json
{
  "schema_version": 1,
  "event_id": "sha256-hex",
  "source_file": "kafka采集数据实验.txt",
  "source_file_sha256": "sha256-hex",
  "source_line_number": 1,
  "group_id": "TB.P1780",
  "raw_line": "TB.P1780,ITARES,...",
  "produced_at": "2026-08-11T07:30:00.000000Z"
}
```

Rules:

- `schema_version` is exactly `1`.
- `source_line_number` is one-based.
- `raw_line` excludes the physical newline terminator but is otherwise unchanged.
- `group_id` is the text before the first comma, or `null` when empty.
- `source_file_sha256` is the baseline digest already measured for the input.
- `event_id` is SHA-256 of a canonical byte sequence containing schema version, source-file digest, line number, and raw line. It is stable across retries and program restarts.
- `produced_at` is an observed UTC timestamp and is not part of `event_id`.

The Kafka key is UTF-8 `group_id` where present; otherwise it is the UTF-8 `event_id`. This preserves grouping while avoiding null-key random partitioning for malformed records.

## 6. Producer Design

Command contract:

```text
gds-kafka produce
  --input PATH
  --bootstrap-servers HOSTS
  --topic TOPIC
  [--limit N]
  [--rate N]
  [--checkpoint PATH]
  [--reset-checkpoint]
```

Producer configuration:

- `acks=all`.
- `enable.idempotence=true`.
- bounded delivery timeout.
- LZ4 compression, subject to client availability verification.
- asynchronous `produce()` with delivery callbacks.
- periodic `poll()` and final `flush()`.

Behavior:

1. Validate the source exists and compute/confirm its SHA-256.
2. Load an optional local checkpoint containing the exact source digest and last contiguous confirmed line.
3. Stream lines without loading the whole file into memory.
4. Build a deterministic envelope and choose its key.
5. Apply optional rate limiting.
6. Submit asynchronously and collect delivery outcomes.
7. Advance the checkpoint only for a contiguous confirmed prefix; out-of-order callbacks must not skip unconfirmed lines.
8. On SIGINT, stop reading, poll outstanding callbacks, flush within a bounded timeout, write the safe checkpoint, print a summary, and exit nonzero if messages remain unconfirmed.

The checkpoint is a local recovery aid, not proof of exactly-once delivery. A resumed run may resend messages near the boundary; stable `event_id` enables downstream duplicate detection.

## 7. Verifier Design

Command contract:

```text
gds-kafka verify
  --bootstrap-servers HOSTS
  --topic TOPIC
  --expected-count N
  [--group-id ID]
  [--idle-timeout SECONDS]
```

The verifier uses a fresh consumer group by default and reads from the earliest available offset. For each message it validates:

- key and value decode as UTF-8;
- value is valid JSON;
- required fields exist and have correct basic types;
- `schema_version == 1`;
- `event_id` matches the canonical recomputation;
- Kafka key matches the key-selection rule;
- source line numbers are positive;
- partition is within the topic partition set.

The final report contains consumed messages, valid/invalid envelopes, duplicate event IDs, per-partition counts, first/last offsets, elapsed time, and records per second.

## 8. Infrastructure and Scripts

Planned files:

```text
infrastructure/kafka/compose.yaml
scripts/kafka-up.sh
scripts/kafka-down.sh
scripts/kafka-reset.sh
scripts/kafka-create-topic.sh
src/gds_pipeline/kafka_config.py
src/gds_pipeline/event.py
src/gds_pipeline/producer.py
src/gds_pipeline/verifier.py
tests/test_event.py
tests/test_producer.py
tests/test_verifier.py
```

Script semantics:

- `kafka-up.sh`: start Kafka and wait for a readiness check.
- `kafka-create-topic.sh`: create or validate the exact topic contract.
- `kafka-down.sh`: stop containers without deleting volumes.
- `kafka-reset.sh`: explicitly delete Kafka containers and volumes; its destructive effect must be stated before use.

## 9. Error Handling

- Docker unavailable: readiness command fails with actionable guidance.
- Image pull failure: report Docker/proxy diagnostics without changing WSL proxy settings automatically.
- Topic exists with incompatible partitions: fail rather than silently accept it.
- Source digest differs from checkpoint: refuse resume unless the user explicitly resets the checkpoint.
- Queue full: poll for delivery callbacks and retry submission without dropping the current source line.
- Delivery failure: record line number, error, and do not advance checkpoint beyond the failure.
- Serialization failure: stop before publishing the affected message.
- Verifier timeout before expected count: exit nonzero and print the deficit.
- Invalid envelope or duplicate event ID: exit nonzero after printing representative evidence.

## 10. Testing

### Unit tests

- deterministic `event_id` across repeated construction;
- different line number or raw line changes `event_id`;
- group key and fallback key behavior;
- JSON round trip preserves Unicode and raw text;
- producer limit and rate validation;
- contiguous checkpoint advancement with out-of-order successes;
- failure prevents checkpoint advancement;
- verifier rejects missing fields, wrong schema version, bad event ID, and wrong key;
- per-partition and duplicate counters.

### Integration tests

- Docker health/readiness succeeds.
- Topic has exactly three partitions.
- One message round-trips through Apache CLI tools.
- Python producer and verifier pass for a synthetic fixture.
- A 100-record source run produces and consumes exactly 100 valid envelopes with zero duplicate IDs.
- Interrupted producer resumes without losing source lines; duplicate IDs near the checkpoint boundary are measured, not hidden.

## 11. Acceptance Criteria

### Smoke acceptance

- Kafka starts through one Compose command.
- `gds.raw.v1` reports three partitions and replication factor one.
- 100 source lines receive 100 successful delivery acknowledgements.
- Verifier consumes 100 valid envelopes.
- All three partitions receive at least one message.
- Duplicate event IDs are zero in an uninterrupted clean-topic run.

### Full-data acceptance

- Start from an explicitly reset empty development topic.
- Producer reads 2,563,566 physical lines.
- Kafka acknowledges 2,563,566 messages with zero final delivery failures.
- Verifier consumes 2,563,566 valid envelopes before its timeout.
- Every event ID recomputes correctly.
- Per-partition totals sum to 2,563,566.
- Throughput, elapsed time, image version, client version, machine resources, and Kafka configuration are recorded.

The full-data result is not described as production performance because the topology has one local broker and replication factor one.

## 12. Future Integration

The subsequent Spark Structured Streaming phase consumes `gds.raw.v1`, parses `raw_line`, uses `event_id` for duplicate control, records malformed lines separately, and must reproduce the previously established offline hourly-airline baseline before adding HDFS or PostgreSQL sinks.
