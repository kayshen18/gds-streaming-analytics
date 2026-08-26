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
        _run_checked(
            root,
            [
                "docker",
                "exec",
                "-e",
                "PYTHONPATH=/opt/gds-app/src",
                "gds-spark-submit",
                "/opt/spark/bin/spark-submit",
                "--master",
                "spark://spark-master:7077",
                "/opt/gds-app/src/gds_pipeline/spark_cli.py",
                "stream",
                "--bootstrap-servers",
                "kafka:29092",
                "--topic",
                topic,
                "--starting-offsets",
                "earliest",
                "--hdfs-root",
                hdfs_root,
                "--checkpoint-root",
                checkpoint_root,
                "--output-version",
                "v1",
                "--trigger",
                "available-now",
                "--merge-after",
            ],
            timeout=360,
        )
        inspection = _run_checked(
            root,
            [
                "docker",
                "exec",
                "-e",
                "PYTHONPATH=/opt/gds-app/src",
                "gds-spark-submit",
                "/opt/spark/bin/spark-submit",
                "--master",
                "spark://spark-master:7077",
                "/opt/gds-app/tests/integration/spark_pipeline_inspect_runner.py",
                hdfs_root,
                checkpoint_root,
            ],
            timeout=240,
        )
        marker = "PIPELINE_SUMMARY="
        summary_line = next(
            line for line in inspection.stdout.splitlines() if line.startswith(marker)
        )
        return json.loads(summary_line.removeprefix(marker))
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
    producer = Producer(
        {
            "bootstrap.servers": "localhost:9092",
            "acks": "all",
            "enable.idempotence": True,
        }
    )
    line_number = 1

    def publish(raw_line: str) -> None:
        nonlocal line_number
        event = build_event(
            source_file="integration-100.txt",
            source_sha256="b" * 64,
            line_number=line_number,
            raw_line=raw_line,
        )
        producer.produce(topic, key=event.kafka_key(), value=event.to_json_bytes())
        producer.poll(0)
        line_number += 1

    for index in range(50):
        publish(
            f"CA{index},ITARES,20180830,19,19:00:{index:02d}:000,CA:success;"
        )
    for index in range(10):
        publish(
            f"BOTH{index},ITARES,20180830,19,19:01:{index:02d}:000,"
            "CA:success;CA:success;MU:success;"
        )
    for index in range(20):
        publish(f"REQ{index},ITAREQ,20180830,19,19:02:{index:02d}:000,payload")
    for index in range(10):
        publish(f"BAD{index},ITARES,20180830,24,19:03:{index:02d}:000")
    for index in range(10):
        producer.produce(topic, key=f"BROKEN{index}".encode(), value=b"not-json")
        producer.poll(0)
    remaining = producer.flush(30)
    if remaining:
        raise RuntimeError(f"failed to deliver {remaining} integration messages")


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
