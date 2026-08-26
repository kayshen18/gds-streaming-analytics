# Real-time GDS Booking Log Analytics Pipeline

This repository is an independent reimplementation and planned extension of a team-based undergraduate laboratory project. All code here is rewritten from scratch. The current phase builds a deterministic offline baseline before introducing Kafka and Spark Structured Streaming.

## Why the offline baseline comes first

A distributed job can finish successfully while producing incorrect counts. This analyzer creates a small, testable reference implementation whose output the later Spark pipeline must reproduce exactly.

It streams the source file, classifies malformed records, measures duplicates, aggregates hourly airline metrics, and writes four auditable artifacts. It never loads the complete log into memory.

## Current architecture

```text
GDS text log
    -> strict UTF-8 line parser
    -> data-quality and duplicate profile
    -> hourly airline aggregation
    -> JSON and CSV reference artifacts
```

Kafka, Spark, HDFS, PostgreSQL, APIs, and dashboards are deliberately deferred until this baseline is verified.

## WSL setup

```bash
cd /mnt/c/Users/juno-/Documents/Codex/2026-08-10/linux-shell-fpga/gds-streaming-analytics/.worktrees/offline-baseline
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
```

Run the tests:

```bash
pytest -v
```

Run the publishable fixture:

```bash
gds-profile profile \
  --input tests/fixtures/mixed_records.txt \
  --output outputs/fixture-baseline \
  --overwrite
```

## Full local run

Copy the course-provided file to the ignored location described in `data/README.md`, then run:

```bash
gds-profile profile \
  --input data/raw/kafka采集数据实验.txt \
  --output outputs/full-baseline
```

Outputs:

- `profile.json`: source identity, quality counts, dimensions, duplicates, and runtime.
- `hourly_airline_metrics.csv`: both hourly success metrics by airline.
- `invalid_records.csv`: retained malformed records with stable reasons.
- `duplicate_summary.json`: repeated SHA-256 fingerprints without raw record disclosure.

See `docs/data-dictionary.md` for exact metric semantics. In particular, a success token is not claimed to represent a ticket or passenger.

## Planned phases

1. Validate this baseline against the full 2.56-million-line source.
2. Build a Python Kafka producer with rate control and delivery reporting.
3. Reproduce the baseline using Spark Structured Streaming and checkpoints.
4. Store clean Parquet data in HDFS and idempotent aggregates in PostgreSQL.
5. Add an API, ECharts dashboard, fault-recovery experiments, and benchmarks.
