import json
from pathlib import Path

import pytest

from gds_pipeline.checkpoint import Checkpoint, load_checkpoint


def make_checkpoint(**overrides: object) -> Checkpoint:
    values: dict[str, object] = {
        "schema_version": 1,
        "source_sha256": "a" * 64,
        "last_contiguous_confirmed_line": 42,
        "topic": "gds.raw.v1",
        "updated_at": "2026-08-11T10:00:00.000Z",
    }
    values.update(overrides)
    return Checkpoint(**values)


def test_checkpoint_json_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "producer.json"
    checkpoint = make_checkpoint()

    checkpoint.save_atomic(path)

    assert Checkpoint.load(path) == checkpoint
    assert json.loads(path.read_text(encoding="utf-8"))["schema_version"] == 1


def test_loading_missing_checkpoint_returns_none(tmp_path: Path) -> None:
    assert Checkpoint.load(tmp_path / "missing.json") is None


def test_atomic_save_replaces_previous_checkpoint(tmp_path: Path) -> None:
    path = tmp_path / "producer.json"
    make_checkpoint(last_contiguous_confirmed_line=10).save_atomic(path)

    replacement = make_checkpoint(last_contiguous_confirmed_line=25)
    replacement.save_atomic(path)

    assert Checkpoint.load(path) == replacement
    assert list(tmp_path.glob("*.tmp")) == []


def test_source_digest_mismatch_is_rejected() -> None:
    checkpoint = make_checkpoint()

    with pytest.raises(ValueError, match="source SHA-256"):
        checkpoint.assert_compatible("b" * 64, "gds.raw.v1")


def test_topic_mismatch_is_rejected() -> None:
    checkpoint = make_checkpoint()

    with pytest.raises(ValueError, match="topic"):
        checkpoint.assert_compatible("a" * 64, "other.topic")


def test_explicit_reset_ignores_existing_checkpoint(tmp_path: Path) -> None:
    path = tmp_path / "producer.json"
    make_checkpoint().save_atomic(path)

    loaded = load_checkpoint(
        path,
        source_sha256="b" * 64,
        topic="other.topic",
        reset=True,
    )

    assert loaded is None
