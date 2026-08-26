"""Command-line interface for the offline baseline analyzer."""

import argparse
from pathlib import Path
import sys

from .profiler import profile_file, write_artifacts


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gds-profile")
    commands = parser.add_subparsers(dest="command", required=True)
    profile = commands.add_parser("profile", help="profile a GDS log file")
    profile.add_argument("--input", required=True, type=Path)
    profile.add_argument("--output", required=True, type=Path)
    profile.add_argument("--overwrite", action="store_true")
    profile.add_argument("--invalid-limit", type=int, default=1000)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the CLI and return a stable process exit code."""

    try:
        args = _parser().parse_args(argv)
    except SystemExit as error:
        return int(error.code)

    source = args.input.resolve()
    output = args.output.resolve()
    if not source.is_file():
        print(f"error: input is not a readable file: {source}", file=sys.stderr)
        return 2
    if source == output:
        print("error: input and output paths must differ", file=sys.stderr)
        return 2
    if args.invalid_limit < 0:
        print("error: --invalid-limit must be nonnegative", file=sys.stderr)
        return 2

    try:
        result = profile_file(source, invalid_limit=args.invalid_limit)
        write_artifacts(result, output, overwrite=args.overwrite)
    except FileExistsError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    except UnicodeDecodeError as error:
        print(
            f"error: input is not valid UTF-8 near byte {error.start}: {error}",
            file=sys.stderr,
        )
        return 3
    except (OSError, ValueError) as error:
        print(f"error: processing failed: {error}", file=sys.stderr)
        return 4

    print(
        "profile complete: "
        f"lines={result.physical_line_count}, "
        f"invalid={result.invalid_count}, "
        f"seconds={result.elapsed_seconds:.3f}, "
        f"records_per_second={result.records_per_second:.1f}, "
        f"output={output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
