"""Validated settings and versioned HDFS paths for the Spark pipeline."""

from dataclasses import dataclass
import re


DEFAULT_HDFS_ROOT = "hdfs://hdfs-namenode:8020/data/gds"
DEFAULT_CHECKPOINT_ROOT = "hdfs://hdfs-namenode:8020/checkpoints/gds"
SUPPORTED_TRIGGERS = frozenset({"available-now", "processing-time"})
SUPPORTED_STARTING_OFFSETS = frozenset({"earliest", "latest"})
VERSION_PATTERN = re.compile(r"v[1-9][0-9]*\Z")


@dataclass(frozen=True, slots=True)
class SparkPipelineSettings:
    bootstrap_servers: str = "kafka:29092"
    topic: str = "gds.raw.v1"
    hdfs_root: str = DEFAULT_HDFS_ROOT
    checkpoint_root: str = DEFAULT_CHECKPOINT_ROOT
    trigger: str = "available-now"
    processing_interval: str | None = None
    output_version: str = "v1"
    starting_offsets: str = "earliest"

    def __post_init__(self) -> None:
        for field_name in (
            "bootstrap_servers",
            "topic",
            "hdfs_root",
            "checkpoint_root",
        ):
            value = getattr(self, field_name)
            if not value.strip():
                raise ValueError(f"{field_name} must not be blank")

        if self.trigger not in SUPPORTED_TRIGGERS:
            raise ValueError(
                f"trigger must be one of {sorted(SUPPORTED_TRIGGERS)}"
            )
        if self.trigger == "processing-time" and not self.processing_interval:
            raise ValueError(
                "processing_interval is required for processing-time trigger"
            )
        if self.trigger == "available-now" and self.processing_interval is not None:
            raise ValueError(
                "processing_interval must be omitted for available-now trigger"
            )
        if self.starting_offsets not in SUPPORTED_STARTING_OFFSETS:
            raise ValueError(
                "starting_offsets must be either 'earliest' or 'latest'"
            )
        if VERSION_PATTERN.fullmatch(self.output_version) is None:
            raise ValueError("output_version must use the form v1, v2, ...")


@dataclass(frozen=True, slots=True)
class DatasetPaths:
    raw_events: str
    clean_events: str
    dead_letter: str
    quality_metrics: str
    hourly_airline_deltas: str
    hourly_airline_metrics: str
    checkpoint: str

    @classmethod
    def from_settings(cls, settings: SparkPipelineSettings) -> "DatasetPaths":
        version_root = _join_uri(settings.hdfs_root, settings.output_version)
        return cls(
            raw_events=_join_uri(version_root, "raw_events"),
            clean_events=_join_uri(version_root, "clean_events"),
            dead_letter=_join_uri(version_root, "dead_letter"),
            quality_metrics=_join_uri(version_root, "quality_metrics"),
            hourly_airline_deltas=_join_uri(
                version_root, "hourly_airline_deltas"
            ),
            hourly_airline_metrics=_join_uri(
                version_root, "hourly_airline_metrics"
            ),
            checkpoint=_join_uri(
                settings.checkpoint_root,
                f"spark-ingestion-{settings.output_version}",
            ),
        )


def _join_uri(root: str, child: str) -> str:
    return f"{root.rstrip('/')}/{child.strip('/')}"
