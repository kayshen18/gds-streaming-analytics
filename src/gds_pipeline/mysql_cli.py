"""Command-line interface for MySQL snapshot publication."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from .mysql_config import MySQLSettings
from .mysql_repository import MySQLRepository, PublicationLockError
from .mysql_snapshot import (
    SnapshotExpectations,
    SnapshotValidationError,
    load_snapshot,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gds-mysql")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("publish", "validate"):
        command = commands.add_parser(name)
        command.add_argument("--csv", required=True, type=Path)
        command.add_argument("--manifest", required=True, type=Path)
        command.add_argument("--expected-rows", type=int)
        command.add_argument("--expected-responses", type=int)
        command.add_argument("--expected-tokens", type=int)
        command.add_argument("--expected-sha256")
        if name == "publish":
            command.add_argument("--force", action="store_true")
    status = commands.add_parser("status")
    status.add_argument("--limit", type=int, default=10)
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
        if args.command == "status":
            repository = _repository()
            for item in repository.recent_publications(args.limit):
                print(
                    "publication "
                    f"id={item['publication_id']} status={item['status']} "
                    f"rows={item['row_count']} sha256={item['metrics_sha256']} "
                    f"completed_at={item['completed_at']}"
                )
            return 0

        for path in (args.csv, args.manifest):
            if not path.is_file():
                print(f"error: file does not exist: {path}", file=sys.stderr)
                return 2
        expectations = SnapshotExpectations(
            row_count=args.expected_rows,
            successful_response_records=args.expected_responses,
            success_token_count=args.expected_tokens,
            metrics_sha256=args.expected_sha256,
        )
        snapshot = load_snapshot(
            args.csv.resolve(), args.manifest.resolve(), expectations=expectations
        )
        repository = _repository()
        if args.command == "validate":
            valid, message = repository.validate_serving(snapshot)
            stream = sys.stdout if valid else sys.stderr
            print(message, file=stream)
            return 0 if valid else 6
        result = repository.publish(snapshot, force=args.force)
        print(
            "publication complete: "
            f"status={result.status} publication_id={result.publication_id} "
            f"rows={result.row_count} "
            f"responses={result.successful_response_records} "
            f"tokens={result.success_token_count} sha256={result.metrics_sha256}"
        )
        return 0
    except SnapshotValidationError as error:
        print(f"error: snapshot validation failed: {error}", file=sys.stderr)
        return 3
    except PublicationLockError as error:
        print(f"error: publication lock failed: {error}", file=sys.stderr)
        return 4
    except (OSError, RuntimeError, ValueError) as error:
        print(f"error: MySQL operation failed: {error}", file=sys.stderr)
        return 5


def _repository() -> MySQLRepository:
    return MySQLRepository(MySQLSettings.from_environment())


if __name__ == "__main__":
    raise SystemExit(main())
