# MySQL Snapshot Publication Design

## Objective

Publish the verified final hourly-airline metrics from HDFS Parquet into MySQL
8.4 LTS so later FastAPI and ECharts components can query them with low latency.
MySQL is a serving store, not a second analytics engine: Spark/HDFS remains the
authority that computes the complete result.

The publication model is a complete snapshot. Repeating a publication sets
MySQL to the same final HDFS result; it never adds a complete total to an old
total.

## Scope

This phase implements:

- A pinned Dockerized MySQL 8.4 LTS service with health checks and a persistent
  named volume.
- Version-controlled schema initialization for metrics and publication audit
  records.
- Python configuration, canonical snapshot validation, hashing, and repository
  code.
- A Spark-assisted export of the final HDFS aggregate into a small canonical
  snapshot for the Python publisher.
- Transactional publication from a fixed staging table into the serving table.
- Safe repeat publication, failure rollback, restart persistence, integration
  tests, full-data reconciliation, and an operations runbook.

This phase does not implement FastAPI, ECharts, direct per-micro-batch JDBC
writes, incremental counter addition, multi-primary MySQL, TLS, or cloud
deployment.

## Runtime topology

```text
Kafka -> Spark Structured Streaming -> HDFS Parquet
                                      |
                                      v
                              canonical snapshot
                                      |
                                      v
                                  MySQL 8.4
                                      |
                                      v
                             future FastAPI/ECharts
```

All application logic remains Python. MySQL joins the existing external Docker
network as `gds-mysql`, while its Compose file and lifecycle scripts remain
independent from Kafka and Spark/HDFS.

Initial local settings are:

- image pinned to an exact MySQL 8.4 patch release selected during
  implementation;
- database `gds_analytics`;
- application user `gds_app`;
- host port `3306`;
- `utf8mb4` character set and UTC server time zone;
- persistent named volume `gds-mysql-data`;
- explicit CPU and approximately 1 GiB memory limits;
- health check through `mysqladmin ping` followed by an authenticated query.

No `latest` tag is permitted.

## Secrets and configuration

The repository contains `.env.example` with placeholders only. The real
`infrastructure/mysql/.env` is ignored by Git. Python reads connection values
from environment variables and never embeds a password in source, Compose,
logs, or command history.

Required application configuration is:

- host and port;
- database;
- application username and password;
- connection and statement timeouts;
- source HDFS root, output version, and expected canonical hash when used for
  full acceptance.

Configuration objects validate missing, blank, out-of-range, or malformed
values before connecting.

## Serving schema

### `hourly_airline_metrics`

The serving table contains one row per business key:

- `stat_date DATE NOT NULL`
- `stat_hour TINYINT UNSIGNED NOT NULL`, constrained to 0-23
- `airline_code VARCHAR(8) NOT NULL`
- `successful_response_records BIGINT UNSIGNED NOT NULL`
- `success_token_count BIGINT UNSIGNED NOT NULL`
- `publication_id CHAR(36) NOT NULL`
- `updated_at TIMESTAMP(6) NOT NULL`

Primary key:

```text
(stat_date, stat_hour, airline_code)
```

Indexes support the initial API access patterns:

- one airline over a date/hour range;
- all airlines for one date/hour ordered by a success metric.

### `hourly_airline_metrics_staging`

The fixed staging table has the same business and metric columns plus
`publication_id`. It is not queried by the API. Only one publisher is permitted
at a time, enforced by a named MySQL advisory lock.

### `metric_publications`

The audit table records:

- `publication_id` UUID primary key;
- source HDFS root and output version;
- source row count and metric totals;
- canonical metrics SHA-256;
- status `preparing`, `published`, `failed`, or `unchanged`;
- start/completion timestamps;
- a bounded failure message when applicable.

A successful source identity is unique by normalized HDFS source root, output
version, and metrics SHA-256. This permits recognizing an already-published
snapshot without changing serving rows.

## Canonical snapshot contract

The publisher consumes exactly these ordered fields:

```text
stat_date,stat_hour,airline_code,
successful_response_records,successful_booking_tokens
```

Rows are sorted lexicographically by date, numeric hour, then airline code and
encoded as UTF-8 CSV with `\n` line endings. This is the same contract used by
the verified offline and Spark acceptance results.

Before MySQL publication, validation requires:

- at least one row;
- unique `(date, hour, airline)` keys;
- valid ISO dates and hours 0-23;
- non-empty bounded airline codes;
- nonnegative integer metrics;
- `success_token_count >= successful_response_records` per row;
- recomputed row count, totals, and SHA-256.

Full-data acceptance additionally requires the established reference values:

| Measurement | Expected |
|---|---:|
| Rows | 3,203 |
| Successful response records | 1,310,068 |
| Success tokens | 2,145,511 |
| SHA-256 | `9b0f4a3afc33e73461414ff2d60a2653e32a5fdbcfe8a810b8b2b42525fcc0be` |

The general publisher does not hard-code these full-dataset values; they are
explicit acceptance arguments so small fixtures and future versions remain
usable.

## HDFS-to-publisher boundary

Spark reads `hourly_airline_metrics` Parquet, selects the canonical columns,
sorts them, and produces a small local handoff artifact plus a JSON manifest.
The manifest contains source identity, row count, both totals, and SHA-256.

The handoff is an ignored local runtime artifact and is written atomically via
a temporary file and rename. The Python publisher independently reparses and
rehashes it before using MySQL. This avoids adding a MySQL driver to every
Spark worker and keeps a database outage from affecting Kafka-to-HDFS
processing.

## Transactional publication protocol

MySQL DDL statements implicitly commit, so table creation, dropping, or
renaming is not used inside the publication transaction. Schema migration is a
separate startup operation.

Publication uses this protocol:

1. Validate the canonical snapshot and manifest before opening a transaction.
2. Acquire a named advisory lock with a bounded timeout.
3. If the same successful source identity and hash is already current, record
   or return `unchanged` without rewriting serving data.
4. Create a `preparing` audit row.
5. Start a transaction and clear the fixed staging table.
6. Batch-insert every validated snapshot row into staging with the new
   publication UUID.
7. Query staging to verify key uniqueness, row count, and both totals.
8. Delete serving rows and copy the entire verified staging snapshot into the
   serving table inside the same transaction.
9. Mark the audit row `published` with completion time in that transaction.
10. Commit and release the advisory lock.

If steps 5-9 fail, rollback restores the previous serving snapshot. A separate
best-effort transaction records `failed` after rollback; inability to write
that diagnostic must not hide the original exception. Stale staging rows are
safe because every attempt clears staging only after obtaining the lock.

Readers use the serving table only. Under InnoDB, they see the old committed
snapshot before commit and the complete new snapshot after commit, never a
partially inserted result.

## Idempotency and concurrency

Complete totals are never added to existing totals. Repeating the same snapshot
is safe because either:

- its source identity and hash are recognized as current and no serving write
  occurs; or
- a forced republish replaces all serving rows with identical values.

The named advisory lock serializes publishers. A second publisher times out
with a clear nonzero result rather than interleaving staging data. The
publication UUID makes every successful serving version auditable.

This provides idempotent snapshot publication, not end-to-end exactly-once
delivery across Kafka, HDFS, and MySQL.

## CLI and operations

The Python package exposes `gds-mysql` commands for:

- `publish`: validate and publish a canonical snapshot;
- `validate`: compare serving counts, totals, current publication, and hash;
- `status`: show the current and recent publication records.

Shell scripts provide safe MySQL start, stop, health, schema initialization,
inspection, and confirmed reset. Stop preserves the named volume. Reset names
the exact volume and requires an explicit confirmation flag.

## Error handling and observability

- Configuration and snapshot errors fail before serving data changes.
- Database connection, lock, timeout, and integrity errors return nonzero.
- Logs include publication UUID, normalized source identity, lifecycle status,
  row count, totals, hash, elapsed time, and batch counts, but never secrets.
- The publisher reports `published`, `unchanged`, or `failed` explicitly.
- MySQL restart and volume persistence are verified before full acceptance.

## Testing strategy

### Pure unit tests

Test configuration validation, canonical parsing/serialization, stable hashing,
duplicate keys, invalid dimensions, negative counts, manifests, and CLI
argument handling without Docker.

### Repository tests

Use a controlled connection abstraction to verify SQL parameterization,
transaction boundaries, rollback, advisory-lock release, unchanged detection,
batch sizing, and publication status transitions. Avoid tests that merely
assert mock call counts without checking observable state transitions.

### Real MySQL integration

Guarded by an explicit environment variable, start from an isolated database
or table namespace and verify:

- schema initialization;
- first small snapshot publication;
- identical second publication remains unchanged;
- forced identical republish does not duplicate or add counts;
- validation failure leaves the previous serving snapshot intact;
- injected mid-transaction failure rolls back serving changes;
- concurrent publisher lock rejection;
- container restart preserves data.

### Full HDFS-to-MySQL acceptance

Export the accepted full HDFS aggregate, publish it, and independently query
MySQL for 3,203 rows, both established totals, and the canonical SHA-256. Run
the same publication again and prove the serving result is unchanged. Restart
MySQL, repeat validation, record duration and storage use, and scan logs for
database errors.

## Security and limitations

- Local development uses a non-root application account with only the required
  schema privileges. Root credentials are reserved for initialization.
- Port 3306 is exposed only for local development and is not a production
  network design.
- TLS, secret managers, backups, replication, failover, and migration tooling
  beyond the initial schema are outside this phase and must not be claimed.
- Snapshot replacement is appropriate for 3,203 rows. Much larger serving
  datasets may later require versioned tables, partition exchange, or another
  serving architecture.

## Stage completion criteria

The phase is complete only when:

1. Pinned MySQL starts reproducibly, becomes healthy, and preserves data across
   restart.
2. Secrets are excluded from Git and no credentials appear in logs or source.
3. Unit and repository tests pass without a live database.
4. Guarded real-MySQL tests prove publish, unchanged repeat, rollback,
   concurrency control, and persistence.
5. The full HDFS snapshot publishes with exactly 3,203 rows, both reference
   totals, and the reference SHA-256.
6. Repeating the full publication does not duplicate rows or add metrics.
7. Operations, recovery, limitations, and measured results are documented.

