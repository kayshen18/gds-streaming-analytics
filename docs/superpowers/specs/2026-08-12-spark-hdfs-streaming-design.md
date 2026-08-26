# Spark Structured Streaming and HDFS Design

## Objective

Extend the verified Kafka ingestion phase into a reproducible streaming
analytics pipeline that consumes `gds.raw.v1`, validates the event envelope,
parses GDS business records, preserves malformed data, computes auditable
hourly-airline metrics, and stores all durable outputs as Parquet in HDFS.

The historical course report is background material only. It is not a source of
truth because its implementation and metric definitions are incomplete. The
independent Python baseline, explicit data contracts, automated tests, and
measured Spark outputs are the acceptance authorities.

## Scope

This phase implements:

- Dockerized Spark master, one Spark worker, and a Spark submission environment.
- Dockerized single-node HDFS with one NameNode and one DataNode.
- PySpark Structured Streaming consumption from Kafka 4.3.1.
- Kafka envelope validation and GDS record parsing.
- Raw, clean, dead-letter, quality, and aggregate Parquet datasets in HDFS.
- Spark checkpoint recovery.
- `availableNow` acceptance runs and processing-time continuous demonstrations.
- Unit, local DataFrame, real-broker, recovery, and full-data validation.

This phase does not implement PostgreSQL, an API, a dashboard, Hive external
tables, or HBase. Those are later phases. HBase will not be added without a
random-access use case; Hive may later expose the Parquet datasets as external
tables.

## Runtime topology

All infrastructure runs on one Docker network:

```text
gds-kafka
    |
    v
spark-master <---- spark-worker
    ^
    |
spark-submit
    |
    v
hdfs-namenode <---- hdfs-datanode
```

Application logic remains Python. Spark runs on the JVM inside containers and
uses PySpark as the application API.

Pinned versions must be recorded in Compose and the runbook. The implementation
must use a stable Apache Spark release with matching Scala 2.13 Kafka connector
coordinates and a Hadoop 3-compatible HDFS client. Preview releases and
`latest` image tags are prohibited.

Initial resource targets are:

| Service | Memory target |
|---|---:|
| Kafka | 2 GiB |
| Spark master | 1 GiB |
| Spark worker | 4-6 GiB |
| Spark driver | 2 GiB |
| HDFS NameNode | 1 GiB |
| HDFS DataNode | 2 GiB |

The final limits must be selected after measuring the active WSL memory ceiling.
VirtualBox Hadoop machines must remain stopped during this Docker pipeline.

## Streaming execution model

One Structured Streaming query reads Kafka and delegates each micro-batch to a
`foreachBatch` function. A single query gives one Kafka offset checkpoint and
one stable `batch_id` sequence instead of five independently advancing queries.

The application supports two trigger modes:

- `available-now`: process all records currently available in Kafka and exit.
  This is the primary deterministic acceptance and benchmark mode.
- `processing-time`: process new data continuously at a configured interval.
  This is the operational demonstration mode.

The program must expose these modes explicitly in its CLI. `available-now` is
implemented and validated first.

## Kafka input contract

Spark reads Kafka key, value, topic, partition, offset, and broker timestamp. It
parses the UTF-8 JSON value according to event schema version 1:

- `schema_version`
- `event_id`
- `source_file`
- `source_file_sha256`
- `source_line_number`
- `group_id`
- `raw_line`
- `produced_at`

Envelope validation checks:

- UTF-8 and JSON validity.
- Presence and data types of all required fields.
- `schema_version == 1`.
- Positive physical source line number.
- A 64-character source SHA-256 digest.
- Stable `event_id` recomputation from
  `1\n{source_sha256}\n{line_number}\n{raw_line}`.
- Kafka key equality to non-empty `group_id`, otherwise `event_id`.

Envelope failures are retained with `failure_stage = "envelope"`.

## GDS business parsing

Records with valid envelopes are parsed from the unmodified `raw_line`. The
business fields are:

- `group_id`
- `log_type`
- `event_date`
- `event_hour`
- `event_time`
- all airline `success` tokens

Validation includes minimum field count, non-empty group ID and log type,
`yyyyMMdd` date validity, hour range 0-23, and supported log types `ITAREQ` and
`ITARES`. Business failures are retained with
`failure_stage = "gds_record"` and a stable reason such as
`missing_group_id`, `missing_log_type`, `too_few_fields`, `invalid_date`,
`invalid_hour`, or `unsupported_log_type`.

No record is silently discarded. The 2,858 comma-only source rows are expected
to become `missing_group_id`, but Spark must follow the declared parser rather
than force its result to match that expectation.

## Success metric semantics

An `ITARES` record can contain repeated success tokens. For example:

```text
CA:success;CA:success;MU:success
```

The pipeline publishes two separate metrics:

- `successful_response_records`: distinct source events containing at least one
  token for an airline. CA contributes one and MU contributes one.
- `success_token_count`: every token occurrence. CA contributes two and MU
  contributes one.

Token extraction produces one row per occurrence. Response-record counting
deduplicates by `(event_id, airline_code)` before aggregation; token counting
does not. Neither metric is labelled as passengers, tickets, or revenue.

## HDFS datasets

All paths are configurable under a default `/data/gds` root. Every query uses a
versioned checkpoint path under `/checkpoints/gds`.

### `raw_events`

Envelope-valid Kafka events with source identity, raw line, production time,
Kafka topic/partition/offset/timestamp, and `batch_id`.

### `clean_events`

Business-valid records with normalized fields, extracted token array, Kafka
location, and `batch_id`. The initial partition scheme is `event_date` only to
avoid excessive small files. Changing partition columns requires a new output
version.

### `dead_letter`

Envelope-invalid or business-invalid records containing the best available
`event_id`, source line, raw line/value, `failure_stage`, `failure_reason`, Kafka
key/location, `batch_id`, and processing timestamp.

### `quality_metrics`

Per-batch input count, envelope-valid count, business-valid count, invalid
count, counts by failure stage/reason, and processing timestamp.

### `hourly_airline_deltas`

Per-batch `event_date`, `event_hour`, `airline_code`,
`successful_response_records`, and `success_token_count`.

### `hourly_airline_metrics`

Final merged metrics across all batch-delta directories with the same business
dimensions and metric columns.

## Idempotency and checkpointing

The streaming query writes all outputs by deterministic `batch_id`. Retrying a
completed batch must replace or recognize that batch's output instead of
appending another indistinguishable copy. The implementation must verify the
filesystem behavior used for replacement on HDFS before claiming idempotency.

Hourly aggregates are first stored as auditable batch deltas. After an
`availableNow` query terminates, a Spark batch merge reads all deltas and
recomputes the final aggregate dataset. Continuous mode can run the same merge
as a separate scheduled command.

The HDFS checkpoint records Kafka source offsets and completed micro-batches.
A checkpoint cannot be reused after incompatible topic, query-plan, output
partitioning, or state configuration changes. Such changes require a new
versioned checkpoint and output root.

## Error handling and observability

- Invalid data is written to dead letter with stable reasons.
- Kafka or HDFS infrastructure errors fail the query and return a nonzero exit.
- Per-batch logs include `batch_id`, input count, output counts, elapsed time,
  and output locations.
- The CLI prints final query status and last progress information.
- Spark, HDFS, and Kafka health checks gate integration runs.
- Destructive reset commands require an explicit confirmation flag and name
  the exact volume or HDFS root affected.

## Testing strategy

### Pure unit tests

Test token extraction, failure classification, metric semantics, path
generation, and CLI validation without Spark or Docker.

### Local Spark DataFrame tests

Run static DataFrames through the same transformation functions using a local
Spark session. Cover valid envelopes, corrupted envelopes, comma-only records,
repeated airline tokens, and exact aggregate output.

### 100-record real integration

Create a UUID-named Kafka topic and isolated HDFS/output/checkpoint roots,
produce 100 source records, run `availableNow`, and validate HDFS counts and
aggregate contents. Tests never reset shared volumes implicitly.

### Recovery integration

Produce and process the first 40 records, retain the Spark checkpoint, then
produce 60 more and restart the same query. Validate that Kafka offsets prevent
reprocessing of the first 40 and the final result contains exactly 100 source
records.

### Full-data acceptance

On a clean versioned output/checkpoint root, process all 2,563,566 Kafka events
and compare Spark results with the independent Python baseline:

| Measurement | Expected reference |
|---|---:|
| Kafka events | 2,563,566 |
| Business-valid records | 2,560,708 |
| Business-invalid records | 2,858 |
| ITARES | 1,278,977 |
| ITAREQ | 1,281,731 |
| Successful response records | 1,310,068 |
| Success tokens | 2,145,511 |
| Hour-airline groups | 3,203 |

Differences trigger investigation. Tests and parsing rules are not modified
merely to reproduce old report numbers.

Acceptance also records Spark/HDFS versions, wall time, throughput, resource
limits, HDFS sizes and file counts, checkpoint recovery results, and service
health. README claims must use measured values only.

## Stage completion criteria

The phase is complete only when:

1. The Docker Spark and HDFS services start reproducibly and pass health checks.
2. Pure and local-Spark tests pass without needing Kafka or HDFS.
3. The 100-record available-now integration run passes.
4. The 40-plus-60 checkpoint recovery run passes.
5. Full Kafka input is written to HDFS Parquet with auditable dead-letter and
   quality outputs.
6. Final Spark metrics reconcile with the independent baseline or every
   difference is explained and documented.
7. Raw source data, HDFS volumes, Spark checkpoints, downloaded connector jars,
   and local benchmark artifacts are excluded from Git.
