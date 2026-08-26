# MySQL Snapshot Publication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish the verified final HDFS hourly-airline aggregate into a persistent MySQL serving table through a repeat-safe, transactionally replaced complete snapshot.

**Architecture:** Spark exports the small final HDFS aggregate into a canonical ignored CSV plus manifest. A Python publisher independently validates it, serializes publication with a MySQL advisory lock, verifies a fixed staging table, and transactionally replaces the serving rows. MySQL is independent of Kafka-to-HDFS processing and later FastAPI reads only its serving table.

**Tech Stack:** Python 3.13, PySpark 4.1.3, exact MySQL 8.4 LTS Docker image, mysql-connector-python, Docker Compose, InnoDB, pytest.

## Global Constraints

- Publish complete snapshots; never add complete HDFS totals to old MySQL totals.
- Do not install MySQL Server in WSL or depend on the host-installed MySQL.
- Probe host port 3306; default Docker host mapping to 3307 when occupied. Container port stays 3306.
- Pin exact image and driver versions; never use `latest`.
- Keep real secrets only in ignored `infrastructure/mysql/.env`.
- Use `utf8mb4`, UTC, InnoDB, parameterized SQL, bounded timeouts, and batched inserts.
- Run schema DDL separately because MySQL DDL implicitly commits.
- Hold a named advisory lock while using the fixed staging table.
- Roll back every failed publication so the prior serving snapshot remains.
- Do not claim exactly-once, TLS, HA, backup, FastAPI, or dashboard behavior.

---

### Task 1: Pin Runtime and Detect the Host Port

**Files:**
- Create: `infrastructure/mysql/versions.env`
- Create: `scripts/check-mysql-prerequisites.sh`
- Create: `tests/test_mysql_prerequisites.py`

**Interfaces:** Produces an exact `MYSQL_IMAGE`, internal port 3306, and a read-only prerequisite command that prints the selected host port.

- [ ] Write tests requiring an exact `mysql:8.4.<patch>` tag, Docker/Compose checks, valid explicit port overrides, and automatic 3306/3307 selection.
- [ ] Run `pytest tests/test_mysql_prerequisites.py -v` and observe missing-file failures.
- [ ] Verify the current official 8.4 LTS patch tag, record it, and implement port probing with `ss -ltn` without starting containers.
- [ ] Run the focused tests plus automatic and `GDS_MYSQL_HOST_PORT=3307` probes.
- [ ] Commit with `git commit -m "build: pin MySQL runtime contract"`.

### Task 2: Add Persistent Docker MySQL

**Files:**
- Create: `infrastructure/mysql/compose.yaml`
- Create: `infrastructure/mysql/.env.example`
- Create: `infrastructure/mysql/init/001-schema.sql`
- Create: `scripts/mysql-up.sh`
- Create: `scripts/mysql-down.sh`
- Create: `scripts/mysql-reset.sh`
- Modify: `.gitignore`
- Create: `tests/test_mysql_compose.py`

**Interfaces:** Produces healthy `gds-mysql`, database `gds_analytics`, user `gds_app`, volume `gds-mysql-data`, and tables `hourly_airline_metrics`, `hourly_airline_metrics_staging`, and `metric_publications`.

- [ ] Write static tests for pinned image interpolation, `${GDS_MYSQL_HOST_PORT:-3307}:3306`, health check, `utf8mb4`, UTC, external network, resource limits, named volume, ignored `.env`, and confirmed reset.
- [ ] Run the tests and observe expected failures.
- [ ] Implement Compose, placeholder environment file, InnoDB schema, composite key `(stat_date, stat_hour, airline_code)`, API indexes, publication audit constraints, and safe lifecycle scripts.
- [ ] Have the user copy `.env.example` to `.env` and replace passwords without printing them.
- [ ] Validate Compose, start MySQL, inspect schema, stop/start without deleting the volume, and prove schema persistence.
- [ ] Run focused tests and commit with `feat: add persistent MySQL serving store`.

### Task 3: Validate Canonical Snapshots

**Files:**
- Create: `src/gds_pipeline/mysql_snapshot.py`
- Create: `tests/test_mysql_snapshot.py`
- Modify: `pyproject.toml`

**Interfaces:** Produces immutable `MetricRow`, `SnapshotManifest`, `ValidatedSnapshot`, `canonical_snapshot_bytes(rows)`, and `load_snapshot(csv_path, manifest_path, expectations=None)`.

- [ ] Write failing tests for exact header, date/hour/airline ordering, UTF-8 LF output, and stable SHA-256.
- [ ] Implement canonical dataclasses and serialization using the standard library.
- [ ] Add failing tests for invalid date/hour/code, negative or noninteger metrics, tokens below response records, duplicate keys, manifest mismatch, and optional acceptance mismatch.
- [ ] Implement validation without hard-coding full-data totals into the general loader.
- [ ] Pin a compatible `mysql-connector-python` under a `mysql` optional extra and reinstall `.[dev,spark,mysql]`.
- [ ] Run focused tests and `pytest -q`; commit with `feat: validate canonical MySQL snapshots`.

### Task 4: Export HDFS Metrics Atomically

**Files:**
- Create: `src/gds_pipeline/mysql_export.py`
- Create: `tests/spark/test_mysql_export.py`
- Create: `scripts/mysql-export-hdfs-snapshot.sh`
- Modify: `.gitignore`

**Interfaces:** Produces `export_hdfs_snapshot(spark, paths, destination_dir, source_identity)` and ignored `metrics.csv`/`manifest.json` artifacts.

- [ ] Write a failing local-Spark test using unsorted rows; assert exact CSV bytes, manifest totals/hash/source, repeat determinism, and no partial artifact after failure.
- [ ] Implement Spark ordering, canonical conversion, temporary writes, fsync, validation, and atomic file replacement.
- [ ] Add a shell wrapper accepting explicit HDFS root, checkpoint root, output version, destination, and source identity.
- [ ] Run focused and ordinary tests; commit with `feat: export HDFS metrics snapshot`.

### Task 5: Implement Transactional Publication

**Files:**
- Create: `src/gds_pipeline/mysql_config.py`
- Create: `src/gds_pipeline/mysql_repository.py`
- Create: `tests/test_mysql_config.py`
- Create: `tests/test_mysql_repository.py`

**Interfaces:** Produces `MySQLSettings.from_environment()`, `PublicationResult`, and `MySQLRepository.publish(snapshot, force=False)`.

- [ ] Write configuration tests for required variables, hidden password representation, ports, timeouts, batch size, and lock timeout.
- [ ] Implement validated settings and run focused tests.
- [ ] Write repository tests for advisory lock, unchanged detection, preparing audit, fixed staging clear, parameterized batch insertion, SQL-side verification, serving `DELETE` plus `INSERT ... SELECT`, published audit, rollback, failed audit, and lock release.
- [ ] Implement one-connection explicit transaction flow using `GET_LOCK`/`RELEASE_LOCK`, `executemany`, commit once, rollback on error, and best-effort failure recording after rollback.
- [ ] Run focused and ordinary tests; commit with `feat: publish atomic MySQL snapshots`.

### Task 6: Expose the MySQL CLI

**Files:**
- Create: `src/gds_pipeline/mysql_cli.py`
- Create: `tests/test_mysql_cli.py`
- Modify: `pyproject.toml`
- Create: `scripts/mysql-publish.sh`
- Create: `scripts/mysql-validate.sh`

**Interfaces:** Produces `gds-mysql publish`, `validate`, and `status`, with distinct nonzero exit codes for validation, lock, database, and serving mismatches.

- [ ] Write failing parser/command tests for paths, expectations, force, sanitized summaries, error mapping, and connection closure.
- [ ] Implement the CLI and wrappers that load `.env` without echoing secrets.
- [ ] Add the console entry point, reinstall editable extras, and run CLI plus ordinary tests.
- [ ] Commit with `feat: expose MySQL publication CLI`.

### Task 7: Prove Semantics on Real MySQL

**Files:**
- Create: `tests/integration/test_mysql_publication.py`
- Create: `tests/fixtures/mysql_snapshot/metrics.csv`
- Create: `tests/fixtures/mysql_snapshot/manifest.json`
- Modify: `pyproject.toml`

**Interfaces:** Requires `RUN_MYSQL_INTEGRATION=1` and isolated fixture publication/source identities.

- [ ] Write guarded tests for first publish, unchanged repeat, forced identical republish, pre-connection validation failure, mid-transaction rollback, advisory-lock contention, audit states, and restart persistence.
- [ ] Run `pytest -q` and prove safe default skipping.
- [ ] Run `RUN_MYSQL_INTEGRATION=1 pytest tests/integration/test_mysql_publication.py -v` and fix only observed defects through TDD.
- [ ] Stop/start MySQL without volume deletion and run the persistence case again.
- [ ] Commit with `test: prove MySQL snapshot publication`.

### Task 8: Full HDFS-to-MySQL Acceptance

**Files:**
- Create ignored: `benchmarks/local/mysql-full-publication/`
- Create: `docs/mysql-runbook.md`
- Modify: `README.md`

**Interfaces:** Consumes the accepted HDFS run `v1-full-20260813-123126` unless another explicitly accepted root is recorded. Requires 3,203 rows, 1,310,068 response records, 2,145,511 tokens, and hash `9b0f4a3afc33e73461414ff2d60a2653e32a5fdbcfe8a810b8b2b42525fcc0be`.

- [ ] Write and dry-run the runbook through the small fixture, including port conflict, `.env`, lifecycle, publish/validate/status, restart, reset, lock timeout, rollback, and cleanup.
- [ ] Export the accepted HDFS aggregate into a timestamped ignored directory and record source identity, totals, hash, versions, and duration.
- [ ] Publish with explicit acceptance expectations; require `published` and save sanitized timing/resource evidence.
- [ ] Independently query MySQL, canonicalize serving rows, verify one publication ID, all totals, and the exact SHA-256.
- [ ] Publish the same snapshot again; require `unchanged` and prove no row or metric increase.
- [ ] Restart MySQL without deleting its volume and repeat validation.
- [ ] Run `pytest -q`, guarded MySQL integration, Compose validation, `git diff --check`, ignored-file checks, and clean-status review.
- [ ] Document only measured results and limitations; commit with `docs: record MySQL snapshot acceptance`.

