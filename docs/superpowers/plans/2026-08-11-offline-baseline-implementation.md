# GDS Offline Baseline Analyzer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a deterministic Python CLI that profiles the complete GDS log and produces reference results for later Spark validation.

**Architecture:** Stream the source once as bytes for SHA-256 and once as strict UTF-8 text for parsing. Keep parsing pure and typed; let a profiler own counters, duplicate detection, aggregation, and atomic artifacts.

**Tech Stack:** Python 3.11+, standard library, pytest, WSL2 Ubuntu, Git

## Global Constraints

- No Docker, Kafka, Spark, Hadoop, or database in this phase.
- Never commit the supplied 202 MB dataset.
- Never load the full log into memory.
- Decode strict UTF-8; never silently replace undecodable bytes.
- Preserve raw counts; duplicates do not change primary metrics.
- Never call success tokens tickets, passengers, orders, or revenue.
- Sort generated data deterministically and encode it as UTF-8.

---

## File Map

- `pyproject.toml`: package, Python floor, pytest, CLI entry point.
- `.gitignore`: environment, caches, raw data, outputs.
- `src/gds_pipeline/models.py`: immutable result types.
- `src/gds_pipeline/parser.py`: one-line parser.
- `src/gds_pipeline/profiler.py`: hash, counters, duplicates, metrics, artifacts.
- `src/gds_pipeline/cli.py`: arguments, exit codes, diagnostics.
- `tests/`: fixtures and package/parser/profiler/CLI tests.
- `data/README.md`, `docs/data-dictionary.md`, `README.md`: documentation.

### Task 1: Installable Test Skeleton

**Files:** Create `pyproject.toml`, `.gitignore`, `src/gds_pipeline/__init__.py`, `tests/test_package.py`.

**Interfaces:** Produce importable `gds_pipeline` with `__version__: str`.

- [ ] Enter the repository from WSL and verify Python:

```bash
wsl
cd /mnt/c/Users/juno-/Documents/Codex/2026-08-10/linux-shell-fpga/gds-streaming-analytics
python3 --version
```

Expected: Python 3.11+. Stop and report the version if older.

- [ ] Write a failing test asserting `gds_pipeline.__version__ == "0.1.0"`.
- [ ] Add `pyproject.toml` using setuptools, `requires-python = ">=3.11"`, dev dependency `pytest>=8,<9`, source root `src`, and script `gds-profile = "gds_pipeline.cli:main"`.
- [ ] Ignore `.venv/`, caches, `data/raw/*`, and `outputs/*`, retaining `.gitkeep` files.
- [ ] Create, activate, install, test:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
pytest tests/test_package.py -v
```

- [ ] Implement only `__version__ = "0.1.0"`; rerun and require PASS.
- [ ] Commit: `git commit -m "build: initialize Python analyzer package"`.

### Task 2: Typed Parser for Valid Records

**Files:** Create `src/gds_pipeline/models.py`, `src/gds_pipeline/parser.py`, `tests/test_parser.py`.

**Interfaces:** `ParseStatus(str, Enum)` has `VALID`, `INVALID`, `UNSUPPORTED`; frozen `ParsedRecord` contains line number, raw line, group ID, type, date, hour, time, token tuple, status, reason; `parse_line(line: str, line_number: int) -> ParsedRecord`.

- [ ] Write failing tests for:

```python
line = "TB.P1780,ITARES,20180830,19,19:45:36:257,,,1,CA:success;CA:success;"
result = parse_line(line, 7)
assert result.success_tokens == ("CA", "CA")
assert result.parse_status is ParseStatus.VALID
```

Also test a valid ITAREQ yields no success tokens.

- [ ] Run `pytest tests/test_parser.py -v`; require missing-module failure.
- [ ] Implement with `rstrip("\r\n").split(",")`, `datetime.strptime(value, "%Y%m%d")`, hour 0–23, and compiled token regex `(?<![A-Z0-9])([A-Z0-9]{2}):success(?![A-Za-z])`.
- [ ] Rerun tests; require PASS.
- [ ] Commit: `git commit -m "feat: parse valid GDS records"`.

### Task 3: Invalid and Unsupported Classification

**Files:** Modify `src/gds_pipeline/parser.py`, `tests/test_parser.py`.

**Interfaces:** Reasons are `blank_line`, `too_few_fields`, `missing_group_id`, `missing_log_type`, `invalid_date`, `invalid_hour`, `unsupported_log_type`. ITARES with no success token remains valid.

- [ ] Add parameterized failing cases covering every reason, including hour 24 and date 20181340.
- [ ] Run tests and require failure.
- [ ] Implement checks in the exact documented order so reasons remain stable.
- [ ] Add a passing test for valid `CA:fail` ITARES with an empty token tuple.
- [ ] Run `pytest tests/test_parser.py -v`; require PASS.
- [ ] Commit: `git commit -m "feat: classify malformed and unsupported records"`.

### Task 4: Streaming Profile and Aggregation

**Files:** Create `src/gds_pipeline/profiler.py`, `tests/fixtures/mixed_records.txt`, `tests/test_profiler.py`.

**Interfaces:** `ProfileResult` holds metadata, counters, metrics, invalid samples, duplicate summary; `profile_file(path: Path, invalid_limit: int = 1000) -> ProfileResult`; metric key is `(date: str, hour: int, airline: str)`; metric values expose `response_records` and `booking_tokens`.

- [ ] Create a six-line fixture: repeated-token ITARES, single-token ITARES, ITAREQ, malformed line, duplicate ITARES, blank line.
- [ ] Write failing assertions: six physical lines, one blank, three ITARES, one ITAREQ, two invalid, one duplicate, and CA metric `(response_records=3, booking_tokens=4)`.
- [ ] Run `pytest tests/test_profiler.py -v`; require failure.
- [ ] Implement counters. Count every token, but count response records once per distinct airline per source line. Hash normalized lines with SHA-256 and retain fingerprint counts, not all raw duplicates.
- [ ] Test `invalid_limit=2` with five invalid lines: total remains five while retained samples equal two.
- [ ] Run tests; require PASS.
- [ ] Commit: `git commit -m "feat: profile records and aggregate airline metrics"`.

### Task 5: Deterministic Atomic Artifacts

**Files:** Modify `src/gds_pipeline/profiler.py`, `tests/test_profiler.py`.

**Interfaces:** `write_artifacts(result: ProfileResult, output_dir: Path, overwrite: bool = False) -> tuple[Path, Path, Path, Path]`; outputs are `profile.json`, `hourly_airline_metrics.csv`, `invalid_records.csv`, `duplicate_summary.json`.

- [ ] Write failing tests using `tmp_path`: four files exist, CSV sorts by date/hour/airline, aggregate token sum reconciles, and a second write without overwrite raises `FileExistsError`.
- [ ] Run tests; require failure.
- [ ] Write into `.<output-name>.tmp-<pid>`, use sorted JSON and CSV with `\n`, then rename atomically. On error, remove only the verified temporary directory.
- [ ] Run `pytest tests/test_profiler.py -v`; require PASS.
- [ ] Commit: `git commit -m "feat: write deterministic baseline artifacts"`.

### Task 6: CLI and Exit Codes

**Files:** Create `src/gds_pipeline/cli.py`, `tests/test_cli.py`.

**Interfaces:** `main(argv: list[str] | None = None) -> int`; command is `gds-profile profile --input PATH --output PATH [--overwrite] [--invalid-limit N]`; codes are 0 success, 2 argument/input, 3 decoding, 4 processing/output.

- [ ] Write failing tests: missing input returns 2; fixture run returns 0 and four files; existing output returns 2; overwrite succeeds; negative invalid limit returns 2.
- [ ] Run `pytest tests/test_cli.py -v`; require failure.
- [ ] Implement argparse validation and concise summary: lines, invalid count, seconds, records/second, output directory.
- [ ] Run:

```bash
gds-profile profile --input tests/fixtures/mixed_records.txt --output outputs/smoke
pytest -v
```

- [ ] Commit: `git commit -m "feat: expose deterministic profiling CLI"`.

### Task 7: Documentation and Hygiene

**Files:** Create `data/README.md`, `data/raw/.gitkeep`, `outputs/.gitkeep`, `docs/data-dictionary.md`, `README.md`.

**Interfaces:** Document the exact implemented CLI, metrics, failures, provenance, and reproduction steps.

- [ ] State that course-provided data is not committed and local input is `data/raw/kafka采集数据实验.txt`; SHA-256 binds outputs to the source.
- [ ] Document the first five fields, token grammar, normalized fields, two metrics, duplicate semantics, and every failure reason.
- [ ] Document project motivation, course origin, independent rewrite, WSL setup, tests, fixture/full commands, outputs, ambiguity, and future streaming stages.
- [ ] Verify commands:

```bash
source .venv/bin/activate
pytest -q
gds-profile profile --input tests/fixtures/mixed_records.txt --output outputs/readme-check --overwrite
git status --short --ignored data/raw outputs
```

- [ ] Commit: `git commit -m "docs: explain data contract and reproduction workflow"`.

### Task 8: Full Dataset Acceptance

**Files:** Local ignored input `data/raw/kafka采集数据实验.txt`; ignored outputs `outputs/full-baseline-a/` and `outputs/full-baseline-b/`; modify `README.md` with measured results.

**Interfaces:** Produce the authoritative reference for later Spark work.

- [ ] From PowerShell, copy explicitly:

```powershell
Copy-Item -LiteralPath 'D:\Download\附件2-实验数据\附件2-实验数据\kafka采集数据实验.txt' -Destination 'C:\Users\juno-\Documents\Codex\2026-08-10\linux-shell-fpga\gds-streaming-analytics\data\raw\kafka采集数据实验.txt'
```

- [ ] Verify ignore rule: `git check-ignore -v data/raw/kafka采集数据实验.txt`.
- [ ] Run twice:

```bash
time gds-profile profile --input data/raw/kafka采集数据实验.txt --output outputs/full-baseline-a
time gds-profile profile --input data/raw/kafka采集数据实验.txt --output outputs/full-baseline-b
```

- [ ] Compare:

```bash
sha256sum outputs/full-baseline-a/hourly_airline_metrics.csv outputs/full-baseline-b/hourly_airline_metrics.csv
diff -u outputs/full-baseline-a/hourly_airline_metrics.csv outputs/full-baseline-b/hourly_airline_metrics.csv
diff -u outputs/full-baseline-a/invalid_records.csv outputs/full-baseline-b/invalid_records.csv
```

Expected: matching hashes and no CSV differences. Compare `profile.json` by data fields because runtime metadata differs.

- [ ] Run `pytest -q` and `git status --short`; tests pass and ignored data is absent.
- [ ] Record only emitted measurements: bytes, SHA-256, lines, types, invalids, duplicates, date range, airline count, token total, elapsed time, environment.
- [ ] Commit: `git commit -m "docs: record full-dataset baseline measurements"`.

## Final Verification

- [ ] `pytest -v` passes.
- [ ] `python -c "import gds_pipeline; print(gds_pipeline.__version__)"` prints 0.1.0.
- [ ] `gds-profile --help` exits 0.
- [ ] `git status --short` is clean.
- [ ] `git ls-files data/raw outputs` lists only `.gitkeep` files.
- [ ] Manually inspect all four full-baseline artifacts before using them as the Spark oracle.
