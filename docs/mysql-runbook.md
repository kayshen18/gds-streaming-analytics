# MySQL Snapshot Publication Runbook

Run these commands in WSL from the project worktree. MySQL stores the small,
query-oriented final aggregate; HDFS remains the durable analytical store.
Publication always replaces the complete serving snapshot instead of adding
the same totals again.

## 1. Enter the environment

```bash
cd /mnt/c/Users/juno-/Documents/Codex/2026-08-10/linux-shell-fpga/gds-streaming-analytics/.worktrees/offline-baseline
source .venv/bin/activate
```

Install all project extras when creating or repairing the virtual environment:

```bash
python -m pip install -e '.[dev,spark,mysql]'
```

## 2. Configure and start MySQL

The project uses the pinned Docker MySQL image, not a MySQL Server installed in
Windows or WSL. Create the ignored secrets file once:

```bash
cp infrastructure/mysql/.env.example infrastructure/mysql/.env
nano infrastructure/mysql/.env
```

Replace both placeholder passwords. Never commit or paste the real `.env`.
Check the selected host port, then start the persistent container:

```bash
bash scripts/check-mysql-prerequisites.sh
bash scripts/mysql-up.sh
docker ps --filter name=gds-mysql
```

The container always listens on 3306 internally. The local host defaults to
3306 when free and otherwise 3307; this worktree uses `localhost:3307`.

Stopping and starting preserves `gds-mysql-data`:

```bash
bash scripts/mysql-down.sh
bash scripts/mysql-up.sh
```

Only the explicitly confirmed reset deletes the database volume:

```bash
bash scripts/mysql-reset.sh --confirm
```

Do not use reset while preserving benchmark or acceptance data.

## 3. Export a complete HDFS snapshot

The accepted Spark result is:

```text
hdfs://hdfs-namenode:8020/data/gds-full/v1-full-20260813-123126
```

Export it into an ignored, timestamped local directory:

```bash
snapshot_dir="infrastructure/spark-hdfs/runtime/mysql-snapshots/v1-full-20260813-123126"

bash scripts/mysql-export-hdfs-snapshot.sh \
  hdfs://hdfs-namenode:8020/data/gds-full/v1-full-20260813-123126 \
  hdfs://hdfs-namenode:8020/checkpoints/gds-full/v1-full-20260813-123126 \
  v1 \
  v1-full-20260813-123126 \
  v1-full-20260813-123126
```

The exporter writes canonical `metrics.csv` and `manifest.json` files only
after validation succeeds. A failed export preserves the previous complete
snapshot and does not expose a partial artifact.

## 4. Publish with explicit acceptance values

Keep `snapshot_dir` in the same terminal, then publish:

```bash
bash scripts/mysql-publish.sh \
  --csv "$snapshot_dir/metrics.csv" \
  --manifest "$snapshot_dir/manifest.json" \
  --expected-rows 3203 \
  --expected-responses 1310068 \
  --expected-tokens 2145511 \
  --expected-sha256 \
    9b0f4a3afc33e73461414ff2d60a2653e32a5fdbcfe8a810b8b2b42525fcc0be
```

The first successful publication reports `status=published`. Publishing the
identical snapshot again reports `status=unchanged`, retains the same
publication ID, and does not increase any metric.

Use `--force` only to deliberately replace the serving table with the same
validated snapshot. Force still replaces rows; it never adds totals.

## 5. Validate and inspect

Validate MySQL independently against the snapshot:

```bash
bash scripts/mysql-validate.sh \
  --csv "$snapshot_dir/metrics.csv" \
  --manifest "$snapshot_dir/manifest.json" \
  --expected-rows 3203 \
  --expected-responses 1310068 \
  --expected-tokens 2145511 \
  --expected-sha256 \
    9b0f4a3afc33e73461414ff2d60a2653e32a5fdbcfe8a810b8b2b42525fcc0be
```

Expected: `serving snapshot matches`.

Load the ignored environment and inspect recent audit entries:

```bash
set -a
source infrastructure/mysql/.env
set +a
export GDS_MYSQL_HOST=127.0.0.1
export GDS_MYSQL_HOST_PORT=3307
gds-mysql status --limit 5
```

Directly reconcile serving totals without printing the password value:

```bash
docker exec gds-mysql sh -lc \
  'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "
    SELECT COUNT(*) AS rows_count,
           SUM(successful_response_records) AS response_total,
           SUM(success_token_count) AS token_total,
           COUNT(DISTINCT publication_id) AS publication_ids
    FROM hourly_airline_metrics;
  "'
```

Expected values are 3,203 rows, 1,310,068 response records, 2,145,511 tokens,
and one publication ID.

## 6. Restart and recovery checks

Restart without deleting the volume, then repeat validation:

```bash
bash scripts/mysql-down.sh
bash scripts/mysql-up.sh
bash scripts/mysql-validate.sh \
  --csv "$snapshot_dir/metrics.csv" \
  --manifest "$snapshot_dir/manifest.json" \
  --expected-rows 3203 \
  --expected-responses 1310068 \
  --expected-tokens 2145511 \
  --expected-sha256 \
    9b0f4a3afc33e73461414ff2d60a2653e32a5fdbcfe8a810b8b2b42525fcc0be
```

The publisher uses a named advisory lock. A second publisher that cannot obtain
the lock exits without changing the database. Snapshot validation happens
before publication, and an insertion or verification failure rolls back the
transaction so the previous serving snapshot remains visible.

## 7. Tests and troubleshooting

The ordinary suite never touches live MySQL:

```bash
pytest -q
```

Real integration tests require an empty fixture serving table and explicit
enablement. Do not run them over the accepted 3,203-row serving snapshot; their
safety check intentionally refuses a nonempty table.

- `missing infrastructure/mysql/.env`: copy `.env.example` and replace the
  placeholders.
- `MYSQL_IMAGE variable is not set` from a direct Compose command: use
  `mysql-up.sh`, or source `infrastructure/mysql/versions.env` first.
- Port 3306 occupied: use host port 3307; the container port remains 3306.
- Publication lock timeout: wait for the active publisher to finish; do not
  clear the staging table manually.
- Publication failure: inspect `metric_publications` and the sanitized CLI
  error. The old serving snapshot should still validate.
- Never print `.env`, place passwords on a shared command line, or commit local
  snapshot/evidence directories.

## 8. Measured full acceptance

On 2026-08-13 the accepted canonical snapshot published in 1.66 seconds with
about 32 MiB maximum RSS. Repeating it returned `unchanged` in 1.25 seconds.
The serving table contained exactly 3,203 rows and the accepted totals and
SHA-256 above. After stopping and recreating the MySQL container while
preserving its named volume, validation still succeeded. The publication ID is
`126fa842-3721-4233-991f-8fd3b9e22929`.
