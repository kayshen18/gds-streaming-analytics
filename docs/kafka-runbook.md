# Kafka Ingestion Runbook

This runbook operates the local single-broker Kafka development environment and
the GDS ingestion tools. Run Bash commands from WSL at the repository root.
PowerShell commands are labelled explicitly.

## Scope and guarantees

- Broker: Apache Kafka 4.3.1 in single-node KRaft combined mode.
- Host bootstrap address: `localhost:9092`.
- Container bootstrap address: `kafka:29092`.
- Raw topic: `gds.raw.v1`, three partitions, replication factor one.
- Delivery mode: at least once. Checkpoints prevent lost source lines, but a
  crash between a Kafka acknowledgement and a checkpoint write can resend a
  boundary record. Stable `event_id` values make those duplicates detectable.
- This local topology is for development and measurement, not high availability.

## Prepare Python

```bash
source .venv/bin/activate
python -m pip install -e ".[dev]"
which python
```

`which python` should resolve inside `.venv`. Conda `(base)` may also appear in
the prompt; the project virtual environment takes precedence.

## Start and inspect Kafka

```bash
bash scripts/kafka-up.sh
docker compose -f infrastructure/kafka/compose.yaml ps -a
bash scripts/kafka-create-topic.sh
```

Expected states:

- `gds-kafka-init-permissions`: `Exited (0)` after fixing volume ownership.
- `gds-kafka`: `Up ... (healthy)`.
- `gds.raw.v1`: three partitions and replication factor one.

Inspect the topic and end offsets:

```bash
docker exec gds-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 \
  --describe --topic gds.raw.v1

docker exec gds-kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:29092 \
  --topic gds.raw.v1
```

Partition end offsets sum to the number of stored records on a clean topic.
Uneven partition counts are normal because `group_id` is the message key.

## Stop or reset Kafka

Preserve the named data volume during an ordinary stop:

```bash
bash scripts/kafka-down.sh
```

Delete the broker data volume only when a clean environment is explicitly
required:

```bash
bash scripts/kafka-reset.sh --confirm
```

Reset deletes all local Kafka topics and messages in this Compose project. It
does not delete source files, code, or the Python virtual environment.

## Produce source records

Run a controlled 100-record smoke test first:

```bash
gds-kafka produce \
  --input "data/raw/kafka采集数据实验.txt" \
  --bootstrap-servers localhost:9092 \
  --topic gds.raw.v1 \
  --limit 100 \
  --rate 1000 \
  --checkpoint .checkpoints/smoke.json \
  --reset-checkpoint
```

Successful acceptance requires:

- `submitted=100`
- `acknowledged=100`
- `failed=0`
- `remaining_after_flush=0`
- `checkpoint_line=100`

`submitted` means accepted by the local client queue. `acknowledged` means the
broker confirmed the record with `acks=all`. Only contiguous acknowledgements
advance the checkpoint.

## Verify Kafka independently

```bash
gds-kafka verify \
  --bootstrap-servers localhost:9092 \
  --topic gds.raw.v1 \
  --expected-count 100 \
  --idle-timeout 20
```

The verifier uses a fresh consumer group, starts at earliest offsets, disables
auto-commit, decodes every JSON envelope, recomputes `event_id`, checks the
Kafka key, and counts invalid and duplicate records.

Expected smoke result:

```text
total=100, valid=100, invalid=0, duplicate_event_ids=0
```

## Resume from a checkpoint

On an ordinary rerun, omit `--reset-checkpoint` and reuse the same input, topic,
and checkpoint path:

```bash
gds-kafka produce \
  --input "data/raw/kafka采集数据实验.txt" \
  --bootstrap-servers localhost:9092 \
  --topic gds.raw.v1 \
  --checkpoint .checkpoints/full.json
```

The producer validates the source SHA-256 and topic before skipping confirmed
lines. A mismatch is rejected to prevent applying progress to another file or
destination. Use `--reset-checkpoint` only when intentionally starting a new
run; it ignores old progress but does not remove existing Kafka records.

## Unit and integration tests

Normal tests never connect to Kafka:

```bash
pytest -q
```

The real-broker suite is opt-in:

```bash
RUN_KAFKA_INTEGRATION=1 \
pytest tests/integration/test_kafka_recovery.py -v
```

The integration suite creates UUID-named temporary topics, tests a 100-record
round trip and a 40-plus-60 checkpoint resume, then deletes only those temporary
topics. It never resets the Kafka volume or modifies `gds.raw.v1`.

Override the host broker only when necessary:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
RUN_KAFKA_INTEGRATION=1 \
pytest tests/integration/test_kafka_recovery.py -v
```

## Diagnostics

### Kafka does not become healthy

```bash
docker compose -f infrastructure/kafka/compose.yaml ps -a
docker compose -f infrastructure/kafka/compose.yaml logs --tail=200 kafka
docker inspect gds-kafka \
  --format 'status={{.State.Status}} restart={{.RestartCount}} user={{.Config.User}}'
```

### Kafka data volume is not writable

The official image runs Kafka as UID 1000. Check ownership:

```bash
docker run --rm --user 0 \
  -v gds-kafka-data:/data \
  apache/kafka:4.3.1 \
  stat -c 'owner=%u:%g permissions=%a path=%n' /data
```

Expected owner is `1000:1000`. The Compose initialization service corrects a
new or root-owned volume before starting Kafka.

### WSL reports a localhost proxy warning

The warning does not affect this project when Docker commands and image pulls
work. If pulls fail, inspect WSL and Docker Desktop proxy configuration rather
than changing Kafka listeners first:

```bash
env | grep -i proxy
docker pull apache/kafka:4.3.1
```

### Kafka reports coordinator loading during startup

An idempotent producer may briefly log `Coordinator load in progress` just
after the broker starts. Treat it as an error only if retries do not recover or
the final summary contains failures or unflushed messages.

### WSL Git cannot recognize the worktree

This worktree was created with Windows Git, so its `.git` pointer can contain a
Windows path. Use WSL for Python and Docker commands, and Windows Git for commits
in this workspace. This does not affect project runtime behavior.
