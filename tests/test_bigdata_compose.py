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
