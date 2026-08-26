# Real-time GDS Booking Log Analytics Pipeline

An independent reimplementation and modernization of an undergraduate team
laboratory project. The repository currently provides a deterministic offline
baseline plus a tested Kafka ingestion layer for 2.56 million real GDS booking
log records. All implementation code was rewritten from scratch.

## Implemented architecture

```text
GDS text log
    |-- offline profiler
    |     |-- strict UTF-8 parsing and data-quality classification
    |     `-- deterministic hourly-airline reference metrics
    |
    `-- Python Kafka producer
          |-- versioned JSON envelope and stable event_id
          |-- acks=all and idempotent client retries
          |-- rate limiting and asynchronous delivery accounting
          `-- atomic contiguous checkpoint
                    |
                    v
          Kafka 4.3.1 / KRaft / gds.raw.v1 / 3 partitions
                    |
                    `-- independent verifier
                          |-- earliest offsets, fresh consumer group
                          |-- schema, event_id, and Kafka-key validation
                          `-- invalid, duplicate, and partition accounting
```

The next layer will consume `gds.raw.v1` with Spark Structured Streaming,
archive clean records as HDFS Parquet, route malformed records to a dead-letter
path, and write idempotent hourly aggregates to PostgreSQL.

## Why the offline baseline comes first

A distributed job can finish successfully while producing incorrect counts.
The offline profiler is a small, auditable reference implementation whose
results the later Spark pipeline must reproduce. It streams the file instead of
loading it fully into memory and writes deterministic JSON and CSV artifacts.

## Event contract and delivery semantics

Every physical source line becomes a UTF-8 JSON event containing:

- `schema_version`
- stable SHA-256 `event_id`
- source filename and source-file SHA-256
- physical source line number
- `group_id`
- unmodified raw line
- UTC production timestamp

The canonical event identity is SHA-256 over
`1\n{source_sha256}\n{line_number}\n{raw_line}`. The timestamp is not part of
the identity, so retries of the same source record remain detectable.

The producer provides **at-least-once delivery**, not end-to-end exactly once.
Kafka acknowledgements can arrive out of order, so the checkpoint advances only
over a contiguous run of confirmed source lines. A crash after a broker
acknowledgement but before a checkpoint write can reproduce a boundary event;
the stable `event_id` allows downstream deduplication.

## Local setup

Requirements: WSL2, Python 3.11+, Docker Desktop with WSL integration, and
Docker Compose.

```bash
cd /mnt/c/Users/juno-/Documents/Codex/2026-08-10/linux-shell-fpga/gds-streaming-analytics/.worktrees/offline-baseline
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
```

Start Kafka and create the raw topic:

```bash
bash scripts/kafka-up.sh
bash scripts/kafka-create-topic.sh
```

See [`docs/kafka-runbook.md`](docs/kafka-runbook.md) for safe stop/reset,
checkpoint recovery, offset inspection, integration tests, and diagnostics.

## Commands

Run the offline profiler:

```bash
gds-profile profile \
  --input "data/raw/kafka采集数据实验.txt" \
  --output outputs/full-baseline
```

Run a 100-record Kafka smoke test:

```bash
gds-kafka produce \
  --input "data/raw/kafka采集数据实验.txt" \
  --bootstrap-servers localhost:9092 \
  --topic gds.raw.v1 \
  --limit 100 \
  --rate 1000 \
  --checkpoint .checkpoints/smoke.json \
  --reset-checkpoint

gds-kafka verify \
  --bootstrap-servers localhost:9092 \
  --topic gds.raw.v1 \
  --expected-count 100 \
  --idle-timeout 20
```

Run unit tests without touching Kafka:

```bash
pytest -q
```

Explicitly run real-broker recovery tests:

```bash
RUN_KAFKA_INTEGRATION=1 \
pytest tests/integration/test_kafka_recovery.py -v
```

## Verified source baseline

The supplied source was processed twice offline. Deterministic CSV and JSON
artifacts from both runs had identical SHA-256 hashes.

| Measurement | Result |
|---|---:|
| Source size | 211,615,939 bytes |
| Source SHA-256 | `301356b0877d4a2afb2f6c904487654ea1083f03a77e7f7c4e00cd4e62df85a7` |
| Physical records | 2,563,566 |
| Valid records | 2,560,708 |
| Invalid records | 2,858 |
| ITARES records | 1,278,977 |
| ITAREQ records | 1,281,731 |
| Hour-airline result groups | 3,203 |
| Observed airline codes | 198 |
| Successful response records | 1,310,068 |
| Success tokens | 2,145,511 |
| Duplicate groups | 1 |
| Duplicate copies after the first | 2,857 |

All invalid records are copies of the comma-only record `,,,,,,,,`, classified
as `missing_group_id`. Two offline runs took 50.8-51.8 seconds on Windows Python
3.12 against the same NTFS worktree, about 49,500-50,400 records per second.

The earlier course report stated 2,145,510 success tokens, one fewer than the
independent baseline. The discrepancy is documented rather than forcing the new
implementation to reproduce the old value.

## Verified full Kafka benchmark

Measured locally on 2026-08-12 with Python 3.13.9, confluent-kafka 2.15.0,
Kafka 4.3.1, Docker Engine 29.7.2, Docker Compose 5.3.1, and a WSL2 VM exposing
15 GiB RAM. The single broker used three partitions and replication factor one.

| Measurement | Result |
|---|---:|
| Source records submitted | 2,563,566 |
| Broker acknowledgements | 2,563,566 |
| Delivery failures | 0 |
| Messages remaining after flush | 0 |
| Final checkpoint line | 2,563,566 |
| Partition 0 end offset | 867,964 |
| Partition 1 end offset | 837,813 |
| Partition 2 end offset | 857,789 |
| Producer GNU wall time | 88.76 s |
| Producer end-to-end throughput | ~28,884 records/s |
| Producer maximum RSS | 73,648 KiB |
| Verifier records read | 2,563,566 |
| Verifier valid / invalid | 2,563,566 / 0 |
| Verifier duplicate event IDs | 0 |
| Verifier GNU wall time | 37.10 s |
| Verifier throughput | ~69,099 records/s |
| Verifier maximum RSS | 585,000 KiB |

The producer's internal timer reported 81.656 seconds, but the table uses the
more conservative GNU wall-clock measurement covering process startup and exit.
Kafka remained healthy after both runs, no swap was used, and recent broker logs
contained no matches for `error`, `exception`, `fatal`, or `outofmemory`.

## Limitations

- The local broker is a single combined controller/broker with replication
  factor one; it does not demonstrate broker failover or high availability.
- No authentication, TLS, or schema registry is configured in this local phase.
- The in-memory duplicate detector stores every event ID; the measured full run
  used about 571 MiB maximum RSS. Larger datasets should use a bounded or
  external deduplication strategy.
- Spark, HDFS Parquet, dead-letter handling, PostgreSQL aggregates, API, and
  dashboard remain future phases and are not claimed as implemented.
