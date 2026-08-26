# GDS Offline Baseline Analyzer - Design Specification

## 1. Purpose

Build the first independently implemented component of the Real-time GDS Booking Log Analytics Pipeline: a deterministic offline analyzer for the supplied GDS log file.

This component establishes the data contract, quality profile, and reference aggregation results that later Kafka and Spark Structured Streaming stages must reproduce. It intentionally excludes Kafka, Spark, HDFS, databases, APIs, dashboards, Docker, and the three-node VM cluster.

## 2. Scope

### Included

- Stream the input file line by line without loading the full file into memory.
- Compute file metadata and a SHA-256 checksum.
- Parse ITARES, ITAREQ, and unknown record types.
- Extract date, hour, event time, group identifier, airline codes, and `airline:success` tokens where present.
- Detect malformed records and assign explicit failure reasons.
- Measure duplicates without silently removing them from the primary source counts.
- Produce deterministic profile and aggregation artifacts.
- Provide unit tests for parsing and aggregation rules.
- Document metric semantics and known ambiguities in the source data.

### Excluded

- Interpreting each `success` token as a ticket, passenger, booking, or revenue event.
- Publishing the supplied 202 MB dataset to Git.
- Kafka ingestion or Spark processing.
- Distributed deployment and performance benchmarking.
- Web APIs and visualization.

## 3. Technology

- Python 3.11 or later.
- Standard library for the initial implementation where practical.
- `pytest` for automated testing.
- CSV and JSON for baseline outputs.
- UTF-8 for generated files.

The first implementation uses Python end to end. A Java producer may be evaluated later as an optional comparison, after the complete Python pipeline works.

## 4. Repository Layout

```text
gds-streaming-analytics/
├── data/
│   ├── README.md
│   ├── raw/                 # ignored; user-supplied source data
│   └── sample/              # small synthetic, publishable fixtures
├── src/
│   └── gds_pipeline/
│       ├── __init__.py
│       ├── models.py
│       ├── parser.py
│       ├── profiler.py
│       └── cli.py
├── tests/
│   ├── fixtures/
│   ├── test_parser.py
│   └── test_profiler.py
├── outputs/                 # ignored except placeholder/documentation
├── docs/
│   ├── data-dictionary.md
│   └── superpowers/specs/
├── pyproject.toml
├── .gitignore
└── README.md
```

## 5. Input Contract

The analyzer accepts one file path through the command line. The supplied source is approximately 202 MiB and was previously reported to contain 2,563,566 records; the new analyzer must independently verify both facts.

Representative records:

```text
TB.P1780,ITARES,20180830,19,19:45:36:257,,,1,CA:success;CA:success;
VA.P2241,ITAREQ,20180830,19,19:45:36:647,...,CA;CA;,,
```

The parser must not assume that every line is valid or that all records have the same number of comma-separated fields.

## 6. Record Model

Each source line produces one parse result containing:

- `line_number`: one-based physical line number.
- `raw_line`: retained only when needed for a rejected-record artifact or test diagnostics.
- `group_id`: first field when available.
- `log_type`: normalized second field when available.
- `event_date`: validated `YYYYMMDD` value when available.
- `event_hour`: integer from 0 through 23 when available.
- `event_time`: raw time field when available.
- `success_tokens`: ordered collection of matched airline codes.
- `parse_status`: `valid`, `invalid`, or `unsupported`.
- `failure_reason`: stable machine-readable reason for invalid records.

The initial success-token grammar is `[A-Z0-9]{2}:success`. Any discovered valid airline-code formats outside that grammar must be documented and added through tests before changing the parser.

## 7. Metric Semantics

The project reports two distinct success metrics:

1. `successful_response_records`: number of valid ITARES source records containing at least one success token for an airline. One source line contributes at most one count per airline to this metric.
2. `successful_booking_tokens`: total number of matched `airline:success` tokens. A line containing `CA:success;CA:success;` contributes one CA response record and two CA tokens.

Neither metric is labelled as tickets, passengers, orders, or revenue because the dataset documentation does not establish those meanings.

Duplicates are measured separately. Primary profile counts describe the source as received; deduplicated counts, if produced, must be labelled explicitly and must never overwrite raw counts.

## 8. Processing Flow

1. Open the input as bytes to compute SHA-256 and byte size.
2. Open it as UTF-8 text and iterate one physical line at a time.
3. Assign a line number and classify blank, malformed, supported, or unsupported records.
4. Validate required fields according to log type.
5. Extract success tokens only for valid ITARES records.
6. Update in-memory counters keyed by bounded dimensions such as type, date, hour, and airline.
7. Track duplicate fingerprints and duplicate counts without retaining every raw line where avoidable.
8. Write all outputs through temporary files and atomically replace final artifacts after successful completion.

If UTF-8 decoding fails, the run fails with the byte location and remediation guidance. The baseline does not silently ignore or replace undecodable bytes because doing so could invalidate the reference result.

## 9. Outputs

### `profile.json`

Contains:

- source filename, byte size, SHA-256, and processing timestamp;
- physical line count and blank-line count;
- counts by log type;
- valid, invalid, and unsupported counts;
- failure counts by reason;
- earliest and latest valid dates;
- observed hours and airline codes;
- total success tokens;
- records containing success tokens;
- duplicate-record count and duplicate-group count;
- elapsed time and records processed per second.

### `hourly_airline_metrics.csv`

Sorted deterministically by date, hour, and airline code, with columns:

```text
stat_date,stat_hour,airline_code,successful_response_records,successful_booking_tokens
```

### `invalid_records.csv`

Contains line number, stable failure reason, and raw record text. To prevent uncontrolled output growth, the CLI supports a documented maximum retained-invalid-record count while profile counters always include every invalid record.

### `duplicate_summary.json`

Contains duplicate totals and the most frequent duplicate fingerprints. Raw record values are excluded by default; representative records may be included only in local diagnostic mode.

## 10. Error Handling

- Missing or unreadable input: exit non-zero with a concise diagnostic.
- Invalid output directory: exit before scanning.
- UTF-8 decoding failure: fail rather than mutate data silently.
- Malformed record: count and quarantine it; continue processing.
- Unexpected internal exception: remove temporary output files and exit non-zero.
- Existing outputs: require an explicit overwrite option or write into a new run directory.

No source file is modified.

## 11. Testing Strategy

### Parser unit tests

- valid ITARES with one success token;
- valid ITARES with repeated success tokens;
- valid ITAREQ;
- blank input;
- too few fields;
- invalid date;
- hour below 0 or above 23;
- ITARES with no success token;
- alphanumeric airline code;
- unknown log type;
- UTF-8 and line-ending behavior.

### Profiler tests

- raw and token metrics differ correctly;
- duplicate counting does not alter primary totals;
- invalid records are counted even when artifact retention is capped;
- output ordering is deterministic;
- repeated runs over the same fixture produce identical data outputs, excluding explicitly identified runtime metadata.

### Full-data acceptance test

- completes without unbounded memory growth;
- independently reports source size, checksum, and line count;
- produces all four artifacts;
- aggregate totals reconcile with profile totals;
- a second run produces identical aggregation and quality counts.

## 12. CLI Contract

Planned invocation:

```bash
python -m gds_pipeline.cli profile \
  --input /path/to/kafka采集数据实验.txt \
  --output outputs/baseline-run
```

Optional flags are limited initially to an explicit overwrite option and the maximum number of invalid records retained. Configuration expansion is deferred until a concrete need appears.

## 13. Definition of Done

The phase is complete when:

- all automated tests pass;
- the full source file is processed successfully;
- the four specified artifacts are generated;
- all aggregate values reconcile;
- metric definitions and data ambiguities are documented;
- the raw source file is absent from Git;
- README instructions reproduce the run from a clean Python environment.

## 14. Future Integration Contract

The later Kafka/Spark pipeline must emit the same normalized fields and reproduce `hourly_airline_metrics.csv` for the same input. Differences are treated as correctness defects and investigated before adding HDFS, PostgreSQL, APIs, or dashboards.
