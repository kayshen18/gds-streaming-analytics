# Real-time GDS Booking Log Analytics Pipeline

An independent reimplementation and modernization of an undergraduate team
laboratory project. The repository provides a deterministic offline baseline
and a tested Kafka, Spark, HDFS, and MySQL pipeline for 2.56 million real GDS
booking log records. All implementation code was rewritten from scratch.

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
                    v
          Spark 4.1.3 Structured Streaming
                    |-- versioned envelope and GDS validation
                    |-- raw, clean, dead-letter and quality Parquet
                    |-- checkpointed offset recovery
                    `-- hourly airline record/token metrics
                                      |
                                      v
                    HDFS 3.5.0 persistent named volumes
                                      |
                                      v
                    canonical complete snapshot
                                      |
                                      v
                    MySQL 8.4 serving and publication audit
```

The Kafka-to-HDFS layer is implemented and tested with isolated end-to-end and
checkpoint-recovery tests. The final HDFS aggregate is published to MySQL as a
validated, repeat-safe complete snapshot. A read-only API and ECharts dashboard
remain the next phase.

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
python -m pip install -e '.[dev,spark,mysql]'
```

Start Kafka and create the raw topic:

```bash
bash scripts/kafka-up.sh
bash scripts/kafka-create-topic.sh
```

See [`docs/kafka-runbook.md`](docs/kafka-runbook.md) for safe stop/reset,
checkpoint recovery, offset inspection, integration tests, and diagnostics.
See [`docs/spark-hdfs-runbook.md`](docs/spark-hdfs-runbook.md) for Spark/HDFS
startup, end-to-end tests, full-data processing, validation, and recovery.
See [`docs/mysql-runbook.md`](docs/mysql-runbook.md) for MySQL configuration,
snapshot export/publication, repeat safety, restart recovery, and diagnostics.

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

## Verified full Spark/HDFS benchmark

Measured locally on 2026-08-13 with Spark 4.1.3, Hadoop 3.5.0, one Spark
worker with two cores and 2 GiB executor memory, and the complete
`gds.raw.v1` topic.

| Measurement | Result |
|---|---:|
| Kafka input records | 2,563,566 |
| Business-valid records | 2,560,708 |
| Dead-letter records | 2,858 |
| Final hour-airline groups | 3,203 |
| Successful response records | 1,310,068 |
| Success tokens | 2,145,511 |
| Available-now wall time | 21 min 17.86 s |
| End-to-end throughput | ~2,006 records/s |
| HDFS space after run | 526.39 MiB |
| Missing/corrupt HDFS blocks | 0 / 0 |

Spark's canonical sorted aggregate CSV had SHA-256
`9b0f4a3afc33e73461414ff2d60a2653e32a5fdbcfe8a810b8b2b42525fcc0be`,
identical to two independent offline baseline runs. The full run exited zero,
published one batch commit marker and a matching Structured Streaming
checkpoint commit, and its log scan found no error, exception, fatal,
out-of-memory, or killed entries.

The processing-time path was also exercised with two waves against one
checkpoint: 40 records were committed, the query stopped gracefully, then the
same query restarted and consumed only 60 new records. Final reconciliation
found exactly 100 unique Kafka `(partition, offset)` locations; the automated
demonstration completed in 4 minutes 47.67 seconds.

## Verified full MySQL publication

The accepted HDFS aggregate was exported as a canonical complete snapshot and
published to persistent MySQL 8.4 on 2026-08-13.

| Measurement | Result |
|---|---:|
| Serving rows | 3,203 |
| Successful response records | 1,310,068 |
| Success tokens | 2,145,511 |
| Canonical SHA-256 | `9b0f4a3afc33e73461414ff2d60a2653e32a5fdbcfe8a810b8b2b42525fcc0be` |
| Initial publication wall time | 1.66 s |
| Repeat unchanged check | 1.25 s |
| Initial maximum RSS | 32,028 KiB |

The initial publication produced ID
`126fa842-3721-4233-991f-8fd3b9e22929`. Publishing the identical snapshot a
second time returned `unchanged` with the same ID and no metric increase.
Stopping and recreating the container preserved the snapshot in the named
volume, and post-restart validation matched all expected totals and the hash.

## Limitations

- The local broker is a single combined controller/broker with replication
  factor one; it does not demonstrate broker failover or high availability.
- No authentication, TLS, or schema registry is configured in this local phase.
- The in-memory duplicate detector stores every event ID; the measured full run
  used about 571 MiB maximum RSS. Larger datasets should use a bounded or
  external deduplication strategy.
- MySQL is a single local container without TLS, replication, automated backup,
  or high availability. Snapshot replacement is transactionally protected, but
  this does not claim a distributed exactly-once guarantee.
- The read-only API and dashboard remain future phases and are not claimed as
  implemented.
- The full Spark benchmark used one worker and one HDFS DataNode; it demonstrates
  pipeline correctness and recovery semantics, not horizontal scalability or
  infrastructure failover.
