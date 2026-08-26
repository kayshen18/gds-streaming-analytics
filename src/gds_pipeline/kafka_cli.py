"""Command-line interface for Kafka ingestion."""

import argparse
from pathlib import Path
import sys

from .kafka_config import ProducerSettings
from .producer import produce_file
from .verifier import VerifierSettings, verify_topic

from uuid import uuid4

from .simulator import (
    SimulatedRecordGenerator,
    simulate_to_kafka,
)

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
    simulate = commands.add_parser(
        "simulate",
        help="continuously publish synthetic GDS records",
    )
    simulate.add_argument(
        "--bootstrap-servers",
        required=True,
    )
    simulate.add_argument("--topic", required=True)
    simulate.add_argument(
        "--airlines",
        default="CZ,MU,CA,HX,CX,KA,HU,KL,MF,AF",
    )
    simulate.add_argument(
        "--rate",
        type=float,
        default=100.0,
    )
    simulate.add_argument("--limit", type=int)
    simulate.add_argument("--seed", type=int, default=0)
    simulate.add_argument("--run-id")
    simulate.add_argument(
        "--response-probability",
        type=float,
        default=0.5,
    )
    simulate.add_argument(
        "--success-probability",
        type=float,
        default=0.9,
    )
    simulate.add_argument(
        "--flush-timeout",
        type=float,
        default=30.0,
    )
    verify = commands.add_parser("verify", help="independently verify a Kafka topic")
    verify.add_argument("--bootstrap-servers", required=True)
    verify.add_argument("--topic", required=True)
    verify.add_argument("--expected-count", type=int)
    verify.add_argument("--idle-timeout", type=float, default=20.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run Kafka ingestion and return an automation-friendly exit code."""

    try:
        args = _parser().parse_args(argv)
    except SystemExit as error:
        return int(error.code)

    if args.command == "verify":
        return _run_verify(args)

    if args.command == "simulate":
        return _run_simulate(args)

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


def _run_simulate(args: argparse.Namespace) -> int:
    airline_codes = tuple(
        code.strip()
        for code in args.airlines.split(",")
        if code.strip()
    )
    run_id = args.run_id or uuid4().hex

    try:
        settings = ProducerSettings(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            limit=args.limit,
            rate=args.rate,
            flush_timeout=args.flush_timeout,
        )
        generator = SimulatedRecordGenerator(
            airline_codes=airline_codes,
            seed=args.seed,
            response_probability=(
                args.response_probability
            ),
            success_probability=args.success_probability,
        )
    except ValueError as error:
        print(
            f"error: invalid simulation settings: {error}",
            file=sys.stderr,
        )
        return 2

    try:
        summary = simulate_to_kafka(
            settings,
            generator=generator,
            run_id=run_id,
        )
    except (OSError, RuntimeError, ValueError) as error:
        print(
            f"error: simulation failed: {error}",
            file=sys.stderr,
        )
        return 4

    print(
        "simulation complete: "
        f"run_id={run_id}, "
        f"submitted={summary.submitted}, "
        f"acknowledged={summary.acknowledged}, "
        f"failed={summary.failed}, "
        "remaining_after_flush="
        f"{summary.remaining_after_flush}, "
        f"seconds={summary.elapsed_seconds:.3f}"
    )

    if summary.interrupted:
        return 130
    if summary.failed:
        return 5
    if summary.remaining_after_flush:
        return 6
    return 0

def _run_verify(args: argparse.Namespace) -> int:
    try:
        settings = VerifierSettings(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            expected_count=args.expected_count,
            idle_timeout=args.idle_timeout,
        )
        summary = verify_topic(settings)
    except ValueError as error:
        print(f"error: invalid verifier settings: {error}", file=sys.stderr)
        return 2
    except (OSError, RuntimeError) as error:
        print(f"error: verification failed: {error}", file=sys.stderr)
        return 4

    partitions = ",".join(
        f"{partition}:{count}"
        for partition, count in summary.partition_counts.items()
    )
    print(
        "verification complete: "
        f"total={summary.total}, "
        f"valid={summary.valid}, "
        f"invalid={summary.invalid}, "
        f"duplicate_event_ids={summary.duplicate_event_ids}, "
        f"partition_counts={partitions}"
    )
    if (
        settings.expected_count is not None
        and summary.total != settings.expected_count
    ):
        return 7
    if summary.invalid or summary.duplicate_event_ids:
        return 8
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
