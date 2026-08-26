from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "infrastructure" / "spark-hdfs" / "compose.yaml"
CORE_SITE = ROOT / "infrastructure" / "spark-hdfs" / "hadoop" / "core-site.xml"
HDFS_SITE = ROOT / "infrastructure" / "spark-hdfs" / "hadoop" / "hdfs-site.xml"


def read(path: Path) -> str:
    assert path.is_file(), f"missing required file: {path.relative_to(ROOT)}"
    return path.read_text(encoding="utf-8")


def test_compose_defines_pinned_hdfs_services_and_healthchecks() -> None:
    compose = read(COMPOSE)
    assert "hdfs-namenode-init:" in compose
    assert "hdfs-namenode:" in compose
    assert "hdfs-datanode:" in compose
    assert "apache/hadoop:3.5.0" in compose
    assert ":latest" not in compose
    assert compose.count("healthcheck:") >= 2
    assert "9870:9870" in compose


def test_namenode_healthcheck_does_not_start_a_slow_hadoop_cli() -> None:
    compose = read(COMPOSE)
    namenode = compose.split("  hdfs-namenode:", 1)[1].split(
        "  hdfs-datanode:", 1
    )[0]

    assert "/dev/tcp/hdfs-namenode/8020" in namenode
    assert "hdfs dfsadmin" not in namenode


def test_datanode_healthcheck_does_not_start_a_slow_hadoop_cli() -> None:
    compose = read(COMPOSE)
    datanode = compose.split("  hdfs-datanode:", 1)[1].split(
        "  spark-master:", 1
    )[0]

    assert "/dev/tcp/hdfs-datanode/9866" in datanode
    assert "hdfs dfsadmin" not in datanode


def test_namenode_is_formatted_once_without_overwriting_existing_metadata() -> None:
    compose = read(COMPOSE)
    assert "/data/dfs/name/current/VERSION" in compose
    assert "hdfs namenode -format" in compose
    assert "service_completed_successfully" in compose


def test_hdfs_uses_explicit_persistent_volumes_and_resource_limits() -> None:
    compose = read(COMPOSE)
    assert "gds-hdfs-namenode-data" in compose
    assert "gds-hdfs-datanode-data" in compose
    assert "/data/dfs/name" in compose
    assert "/data/dfs/data" in compose
    assert compose.count("mem_limit:") >= 2
    assert compose.count("cpus:") >= 2


def test_hadoop_configuration_uses_internal_uri_and_single_replica() -> None:
    core_site = read(CORE_SITE)
    hdfs_site = read(HDFS_SITE)
    assert "fs.defaultFS" in core_site
    assert "hdfs://hdfs-namenode:8020" in core_site
    assert "dfs.replication" in hdfs_site
    assert "<value>1</value>" in hdfs_site
    assert "dfs.namenode.name.dir" in hdfs_site
    assert "dfs.datanode.data.dir" in hdfs_site


def test_operational_scripts_are_safe_and_preserve_data_by_default() -> None:
    up = read(ROOT / "scripts" / "bigdata-up.sh")
    down = read(ROOT / "scripts" / "bigdata-down.sh")
    inspect = read(ROOT / "scripts" / "hdfs-inspect.sh")
    reset = read(ROOT / "scripts" / "hdfs-reset.sh")

    for script in (up, down, inspect, reset):
        assert "set -euo pipefail" in script
    assert "dfsadmin -report" in up
    assert "-v" not in down
    assert "dfsadmin -report" in inspect
    assert "--confirm" in reset
    assert "gds-hdfs-namenode-data" in reset
    assert "gds-hdfs-datanode-data" in reset


def test_generated_state_is_ignored() -> None:
    ignore = read(ROOT / ".gitignore")
    assert ".ivy2/" in ignore
    assert "spark-events/" in ignore
    assert "infrastructure/spark-hdfs/runtime/" in ignore


def test_compose_defines_pinned_spark_master_worker_and_submit_services() -> None:
    compose = read(COMPOSE)
    assert "spark-master:" in compose
    assert "spark-worker:" in compose
    assert "spark-submit:" in compose
    assert compose.count(
        "apache/spark:4.1.3-scala2.13-java17-python3-ubuntu"
    ) >= 3
    assert "spark://spark-master:7077" in compose
    assert '"8080:8080"' in compose
    assert "/dev/tcp/spark-master/7077" in compose
    assert "/dev/tcp/localhost/7077" not in compose


def test_spark_worker_has_explicit_resources_and_hdfs_configuration() -> None:
    compose = read(COMPOSE)
    assert "SPARK_WORKER_CORES: 2" in compose
    assert "SPARK_WORKER_MEMORY: 4g" in compose
    assert "HADOOP_CONF_DIR: /opt/hadoop-conf" in compose
    assert compose.count("./hadoop:/opt/hadoop-conf:ro") >= 3
    assert "service_healthy" in compose


def test_spark_services_mount_application_and_dependency_cache() -> None:
    compose = read(COMPOSE)
    assert "spark-ivy-init-permissions:" in compose
    assert 'user: "0:0"' in compose
    assert "chown -R 185:185 /opt/spark/.ivy2" in compose
    assert "../../:/opt/gds-app" in compose
    assert "spark-ivy-cache:/opt/spark/.ivy2" in compose
    assert "gds-spark-ivy-cache" in compose
    kafka_compose = read(ROOT / "infrastructure" / "kafka" / "compose.yaml")
    for content in (compose, kafka_compose):
        assert "gds-streaming-network" in content
        assert "external: true" in content


def test_spark_defaults_and_submit_scripts_use_pinned_contract() -> None:
    defaults = read(
        ROOT / "infrastructure" / "spark-hdfs" / "spark" / "spark-defaults.conf"
    )
    submit = read(ROOT / "scripts" / "spark-submit.sh")
    smoke = read(ROOT / "scripts" / "spark-smoke.sh")

    assert "spark.master spark://spark-master:7077" in defaults
    assert "spark-sql-kafka-0-10_2.13:4.1.3" in defaults
    assert "set -euo pipefail" in submit
    assert "spark-submit" in submit
    assert "PYTHONPATH=/opt/gds-app/src" in submit
    assert "set -euo pipefail" in smoke
    assert "tee" in smoke
    assert "mktemp" in smoke
    assert "spark_count=3" in smoke
    assert "hdfs_count=3" in smoke
