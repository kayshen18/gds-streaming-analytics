from pathlib import Path


ROOT = Path(__file__).parents[1]
VERSIONS_PATH = ROOT / "infrastructure" / "spark-hdfs" / "versions.env"
CHECK_SCRIPT = ROOT / "scripts" / "check-bigdata-prerequisites.sh"


def load_versions() -> dict[str, str]:
    assert VERSIONS_PATH.is_file(), f"missing version contract: {VERSIONS_PATH}"
    values: dict[str, str] = {}
    for raw_line in VERSIONS_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        values[key] = value
    return values


def test_version_contract_has_all_required_keys() -> None:
    versions = load_versions()

    assert set(versions) == {
        "SPARK_VERSION",
        "SCALA_BINARY_VERSION",
        "HADOOP_VERSION",
        "JAVA_MAJOR",
        "SPARK_IMAGE",
        "HADOOP_IMAGE",
        "SPARK_KAFKA_PACKAGE",
    }


def test_runtime_images_are_immutable_version_tags() -> None:
    versions = load_versions()

    assert versions["SPARK_IMAGE"] == (
        "apache/spark:4.1.3-scala2.13-java17-python3-ubuntu"
    )
    assert versions["HADOOP_IMAGE"] == "apache/hadoop:3.5.0"
    assert all(":latest" not in value for value in versions.values())


def test_connector_matches_spark_and_scala_versions() -> None:
    versions = load_versions()

    assert versions["SCALA_BINARY_VERSION"] == "2.13"
    assert versions["SPARK_KAFKA_PACKAGE"] == (
        "org.apache.spark:spark-sql-kafka-0-10_2.13:"
        + versions["SPARK_VERSION"]
    )


def test_java_and_hadoop_versions_are_pinned() -> None:
    versions = load_versions()

    assert versions["JAVA_MAJOR"] == "17"
    assert versions["HADOOP_VERSION"] == "3.5.0"


def test_prerequisite_script_has_resource_modes_and_safety() -> None:
    script = CHECK_SCRIPT.read_text(encoding="utf-8")

    assert "set -euo pipefail" in script
    assert '"--smoke"' in script
    assert "docker version" in script
    assert "docker compose version" in script
    assert "/proc/meminfo" in script
    assert "/mnt/d" in script
    assert "10 * 1024 * 1024" in script
    assert "20 * 1024 * 1024" in script
    assert "6 * 1024 * 1024" in script
    assert "8 * 1024 * 1024" in script
