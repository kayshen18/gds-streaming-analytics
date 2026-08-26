"""Command-line interface for Kafka ingestion."""

import argparse
from pathlib import Path
import sys

from .kafka_config import ProducerSettings
from .producer import produce_file


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gds-kafka")
    commands = parser.add_subparsers(dest="command", required=True)
    produce = commands.add_parser("produce", help="publish a GDS log to Kafka")
    produce.add_argument("--input", required=True, type=Path)
    produce.add_argument("--bootstrap-servers", required=True)
    produce.add_argument("--topic", required=True)
    produce.add_argument("--limit", type=int)
    produce.add_argument("--rate", type=float)
    produce.add_argument("--checkpoint", type=Path)
    produce.add_argument("--reset-checkpoint", action="store_true")
    produce.add_argument("--flush-timeout", type=float, default=30.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run Kafka ingestion and return an automation-friendly exit code."""

    try:
        args = _parser().parse_args(argv)
    except SystemExit as error:
        return int(error.code)

    source = args.input.resolve()
    if not source.is_file():
        print(f"error: input is not a readable file: {source}", file=sys.stderr)
        return 2
    checkpoint = args.checkpoint.resolve() if args.checkpoint else None
    try:
        settings = ProducerSettings(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            input_path=source,
            checkpoint_path=checkpoint,
            reset_checkpoint=args.reset_checkpoint,
            limit=args.limit,
            rate=args.rate,
            flush_timeout=args.flush_timeout,
        )
    except ValueError as error:
        print(f"error: invalid producer settings: {error}", file=sys.stderr)
        return 2

    try:
        summary = produce_file(settings)
    except UnicodeDecodeError as error:
        print(
            f"error: input is not valid UTF-8 near byte {error.start}: {error}",
            file=sys.stderr,
        )
        return 3
    except (OSError, RuntimeError, ValueError) as error:
        print(f"error: production failed: {error}", file=sys.stderr)
        return 4

    print(
        "production complete: "
        f"submitted={summary.submitted}, "
        f"acknowledged={summary.acknowledged}, "
        f"failed={summary.failed}, "
        f"remaining_after_flush={summary.remaining_after_flush}, "
        f"checkpoint_line={summary.last_contiguous_confirmed_line}, "
        f"seconds={summary.elapsed_seconds:.3f}"
    )
    if summary.interrupted:
        return 130
    if summary.failed:
        return 5
    if summary.remaining_after_flush:
        return 6
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
