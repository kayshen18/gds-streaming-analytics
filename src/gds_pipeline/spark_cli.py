"""Command-line operations for the Spark/HDFS pipeline."""

import argparse
from collections.abc import Callable, Sequence

from pyspark.sql import SparkSession

from gds_pipeline.spark_config import DatasetPaths, SparkPipelineSettings
from gds_pipeline.spark_job import start_query
from gds_pipeline.spark_merge import merge_hourly_airline_metrics


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gds-spark")
    subparsers = parser.add_subparsers(dest="command", required=True)

    stream = subparsers.add_parser("stream", help="consume Kafka into HDFS")
    _add_settings_arguments(stream)
    stream.add_argument(
        "--trigger",
        choices=("available-now", "processing-time"),
        default="available-now",
    )
    stream.add_argument("--processing-interval")
    stream.add_argument("--merge-after", action="store_true")

    merge = subparsers.add_parser("merge", help="merge hourly batch deltas")
    _add_storage_arguments(merge)

    validate = subparsers.add_parser("validate", help="validate HDFS outputs")
    _add_storage_arguments(validate)
    return parser


def _add_settings_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--bootstrap-servers", default="kafka:29092")
    parser.add_argument("--topic", default="gds.raw.v1")
    parser.add_argument(
        "--starting-offsets", choices=("earliest", "latest"), default="earliest"
    )
    _add_storage_arguments(parser)


def _add_storage_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--hdfs-root", default="hdfs://hdfs-namenode:8020/data/gds"
    )
    parser.add_argument(
        "--checkpoint-root",
        default="hdfs://hdfs-namenode:8020/checkpoints/gds",
    )
    parser.add_argument("--output-version", default="v1")


def _settings(args: argparse.Namespace) -> SparkPipelineSettings:
    return SparkPipelineSettings(
        bootstrap_servers=getattr(args, "bootstrap_servers", "kafka:29092"),
        topic=getattr(args, "topic", "gds.raw.v1"),
        hdfs_root=args.hdfs_root,
        checkpoint_root=args.checkpoint_root,
        trigger=getattr(args, "trigger", "available-now"),
        processing_interval=getattr(args, "processing_interval", None),
        output_version=args.output_version,
        starting_offsets=getattr(args, "starting_offsets", "earliest"),
    )


def _default_spark_factory() -> SparkSession:
    return SparkSession.builder.appName("gds-streaming-analytics").getOrCreate()


def validate_outputs(spark: SparkSession, paths: DatasetPaths) -> bool:
    """Read persisted Parquet and verify the principal count invariants."""

    try:
        raw_count = spark.read.parquet(paths.raw_events).count()
        clean_count = spark.read.parquet(paths.clean_events).count()
        dead_count = spark.read.parquet(paths.dead_letter).count()
        metric_rows = spark.read.parquet(paths.quality_metrics).count()
        final_rows = spark.read.parquet(paths.hourly_airline_metrics).count()
    except Exception as error:
        print(f"validation failed: {error}")
        return False
    valid = raw_count >= clean_count and metric_rows > 0 and final_rows >= 0
    print(
        "validation complete: "
        f"raw={raw_count}, clean={clean_count}, dead={dead_count}, "
        f"quality_rows={metric_rows}, final_metric_rows={final_rows}, "
        f"valid={str(valid).lower()}"
    )
    return valid


def main(
    argv: Sequence[str] | None = None,
    *,
    spark_factory: Callable[[], SparkSession] = _default_spark_factory,
    query_starter=start_query,
    merger=merge_hourly_airline_metrics,
    validator=validate_outputs,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if (
        args.command == "stream"
        and args.trigger == "processing-time"
        and not args.processing_interval
    ):
        parser.error("--processing-interval is required for processing-time")
    if (
        args.command == "stream"
        and args.trigger == "available-now"
        and args.processing_interval
    ):
        parser.error("--processing-interval is only valid for processing-time")

    settings = _settings(args)
    paths = DatasetPaths.from_settings(settings)
    spark = spark_factory()
    try:
        if args.command == "stream":
            query = query_starter(spark, settings)
            query.awaitTermination()
            if args.merge_after:
                summary = merger(spark, paths)
                print(f"merge complete: output_rows={summary.output_rows}")
            return 0
        if args.command == "merge":
            summary = merger(spark, paths)
            print(f"merge complete: output_rows={summary.output_rows}")
            return 0
        return 0 if validator(spark, paths) else 3
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
