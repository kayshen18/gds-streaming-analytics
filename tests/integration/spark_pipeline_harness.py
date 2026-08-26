"""Host-side orchestration for isolated Kafka-to-HDFS integration cases."""

import json
from pathlib import Path
import subprocess
from uuid import uuid4

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from gds_pipeline.event import build_event


def run_pipeline_case(root: Path) -> dict[str, object]:
    run_id = uuid4().hex
    topic = f"gds.integration.{run_id}"
    hdfs_root = f"hdfs://hdfs-namenode:8020/data/gds-pipeline-integration/{run_id}"
    checkpoint_root = (
        f"hdfs://hdfs-namenode:8020/checkpoints/gds-pipeline-integration/{run_id}"
    )
    admin = AdminClient({"bootstrap.servers": "localhost:9092"})
    _create_topic(admin, topic)
    try:
        _produce_case(topic)
        _run_pipeline(root, topic, hdfs_root, checkpoint_root)
        return _inspect_pipeline(root, hdfs_root, checkpoint_root)
    finally:
        _delete_topic(admin, topic)
        _cleanup_hdfs(root, hdfs_root, checkpoint_root)


def run_recovery_case(root: Path) -> dict[str, object]:
    run_id = uuid4().hex
    topic = f"gds.recovery.{run_id}"
    hdfs_root = f"hdfs://hdfs-namenode:8020/data/gds-pipeline-recovery/{run_id}"
    checkpoint_root = (
        f"hdfs://hdfs-namenode:8020/checkpoints/gds-pipeline-recovery/{run_id}"
    )
    admin = AdminClient({"bootstrap.servers": "localhost:9092"})
    _create_topic(admin, topic)
    try:
        records = _case_records()
        _produce_records(topic, records[:40])
        _run_pipeline(root, topic, hdfs_root, checkpoint_root)
        first = _inspect_pipeline(root, hdfs_root, checkpoint_root)

        _produce_records(topic, records[40:])
        _run_pipeline(root, topic, hdfs_root, checkpoint_root)
        second = _inspect_pipeline(root, hdfs_root, checkpoint_root)
        return {
            "first_run_input_count": first["input_count"],
            "second_run_input_count": second["input_count"],
            "raw_plus_envelope_dead_count": second[
                "raw_plus_envelope_dead_count"
            ],
            "clean_plus_business_dead_count": second[
                "clean_plus_business_dead_count"
            ],
            "unique_kafka_locations": second["unique_kafka_locations"],
            "airline_metrics": second["airline_metrics"],
        }
    finally:
        _delete_topic(admin, topic)
        _cleanup_hdfs(root, hdfs_root, checkpoint_root)


def run_continuous_case(root: Path) -> dict[str, object]:
    run_id = uuid4().hex
    topic = f"gds.continuous.{run_id}"
    hdfs_root = f"hdfs://hdfs-namenode:8020/data/gds-continuous/{run_id}"
    checkpoint_root = (
        f"hdfs://hdfs-namenode:8020/checkpoints/gds-continuous/{run_id}"
    )
    admin = AdminClient({"bootstrap.servers": "localhost:9092"})
    _create_topic(admin, topic)
    try:
        records = _case_records()
        _produce_records(topic, records[:40])
        _run_continuous_wave(
            root, topic, hdfs_root, checkpoint_root, expected_rows=40
        )
        _merge_pipeline(root, hdfs_root, checkpoint_root)
        first = _inspect_pipeline(root, hdfs_root, checkpoint_root)

        _produce_records(topic, records[40:])
        _run_continuous_wave(
            root, topic, hdfs_root, checkpoint_root, expected_rows=60
        )
        _merge_pipeline(root, hdfs_root, checkpoint_root)
        final = _inspect_pipeline(root, hdfs_root, checkpoint_root)
        return {
            "first_wave_input_count": first["input_count"],
            "final_input_count": final["input_count"],
            "unique_kafka_locations": final["unique_kafka_locations"],
            "raw_plus_envelope_dead_count": final[
                "raw_plus_envelope_dead_count"
            ],
            "airline_metrics": final["airline_metrics"],
        }
    finally:
        _delete_topic(admin, topic)
        _cleanup_hdfs(root, hdfs_root, checkpoint_root)


def _create_topic(admin: AdminClient, topic: str) -> None:
    future = admin.create_topics([NewTopic(topic, 3, 1)])[topic]
    future.result(timeout=20)


def _delete_topic(admin: AdminClient, topic: str) -> None:
    try:
        admin.delete_topics([topic])[topic].result(timeout=20)
    except Exception:
        pass


def _produce_case(topic: str) -> None:
    _produce_records(topic, _case_records())


def _case_records() -> list[str | bytes]:
    records: list[str | bytes] = []
    records.extend(
        f"CA{index},ITARES,20180830,19,19:00:{index:02d}:000,CA:success;"
        for index in range(50)
    )
    records.extend(
        f"BOTH{index},ITARES,20180830,19,19:01:{index:02d}:000,"
        "CA:success;CA:success;MU:success;"
        for index in range(10)
    )
    records.extend(
        f"REQ{index},ITAREQ,20180830,19,19:02:{index:02d}:000,payload"
        for index in range(20)
    )
    records.extend(
        f"BAD{index},ITARES,20180830,24,19:03:{index:02d}:000"
        for index in range(10)
    )
    records.extend(b"not-json" for _ in range(10))
    return records


def _produce_records(topic: str, records: list[str | bytes]) -> None:
    producer = Producer(
        {
            "bootstrap.servers": "localhost:9092",
            "acks": "all",
            "enable.idempotence": True,
        }
    )
    for record in records:
        if isinstance(record, bytes):
            producer.produce(topic, key=uuid4().hex.encode(), value=record)
        else:
            event = build_event(
                source_file="integration-100.txt",
                source_sha256="b" * 64,
                line_number=_record_line_number(record),
                raw_line=record,
            )
            producer.produce(
                topic, key=event.kafka_key(), value=event.to_json_bytes()
            )
        producer.poll(0)
    remaining = producer.flush(30)
    if remaining:
        raise RuntimeError(f"failed to deliver {remaining} integration messages")


def _record_line_number(record: str) -> int:
    group_id = record.split(",", 1)[0]
    if group_id.startswith("CA"):
        return int(group_id[2:]) + 1
    if group_id.startswith("BOTH"):
        return int(group_id[4:]) + 51
    if group_id.startswith("REQ"):
        return int(group_id[3:]) + 61
    return int(group_id[3:]) + 81


def _run_pipeline(
    root: Path, topic: str, hdfs_root: str, checkpoint_root: str
) -> None:
    _run_checked(
        root,
        [
            "docker", "exec", "-e", "PYTHONPATH=/opt/gds-app/src",
            "gds-spark-submit", "/opt/spark/bin/spark-submit",
            "--master", "spark://spark-master:7077",
            "/opt/gds-app/src/gds_pipeline/spark_cli.py", "stream",
            "--bootstrap-servers", "kafka:29092", "--topic", topic,
            "--starting-offsets", "earliest", "--hdfs-root", hdfs_root,
            "--checkpoint-root", checkpoint_root, "--output-version", "v1",
            "--trigger", "available-now", "--merge-after",
        ],
        timeout=360,
    )


def _run_continuous_wave(
    root: Path,
    topic: str,
    hdfs_root: str,
    checkpoint_root: str,
    *,
    expected_rows: int,
) -> None:
    _run_checked(
        root,
        [
            "docker", "exec", "-e", "PYTHONPATH=/opt/gds-app/src",
            "gds-spark-submit", "/opt/spark/bin/spark-submit",
            "--master", "spark://spark-master:7077",
            "/opt/gds-app/tests/integration/spark_continuous_runner.py",
            topic, hdfs_root, checkpoint_root, str(expected_rows),
        ],
        timeout=240,
    )


def _merge_pipeline(root: Path, hdfs_root: str, checkpoint_root: str) -> None:
    _run_checked(
        root,
        [
            "docker", "exec", "-e", "PYTHONPATH=/opt/gds-app/src",
            "gds-spark-submit", "/opt/spark/bin/spark-submit",
            "--master", "spark://spark-master:7077",
            "/opt/gds-app/src/gds_pipeline/spark_cli.py", "merge",
            "--hdfs-root", hdfs_root,
            "--checkpoint-root", checkpoint_root,
            "--output-version", "v1",
        ],
        timeout=240,
    )


def _inspect_pipeline(
    root: Path, hdfs_root: str, checkpoint_root: str
) -> dict[str, object]:
    inspection = _run_checked(
        root,
        [
            "docker", "exec", "-e", "PYTHONPATH=/opt/gds-app/src",
            "gds-spark-submit", "/opt/spark/bin/spark-submit",
            "--master", "spark://spark-master:7077",
            "/opt/gds-app/tests/integration/spark_pipeline_inspect_runner.py",
            hdfs_root, checkpoint_root,
        ],
        timeout=240,
    )
    marker = "PIPELINE_SUMMARY="
    summary_line = next(
        line for line in inspection.stdout.splitlines() if line.startswith(marker)
    )
    return json.loads(summary_line.removeprefix(marker))


def _run_checked(root: Path, command: list[str], *, timeout: int):
    result = subprocess.run(
        command,
        cwd=root,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(command)}\n"
            f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
        )
    return result


def _cleanup_hdfs(root: Path, hdfs_root: str, checkpoint_root: str) -> None:
    subprocess.run(
        [
            "docker",
            "exec",
            "gds-hdfs-namenode",
            "hdfs",
            "dfs",
            "-rm",
            "-r",
            "-f",
            hdfs_root,
            checkpoint_root,
        ],
        cwd=root,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
