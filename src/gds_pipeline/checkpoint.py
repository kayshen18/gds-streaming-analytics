"""Durable producer checkpoint persistence."""

from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import tempfile


CHECKPOINT_SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class Checkpoint:
    """Last contiguous source line acknowledged by Kafka."""

    schema_version: int
    source_sha256: str
    last_contiguous_confirmed_line: int
    topic: str
    updated_at: str

    @classmethod
    def load(cls, path: Path) -> "Checkpoint | None":
        """Load a checkpoint, returning ``None`` when it does not exist."""

        if not path.exists():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        checkpoint = cls(**payload)
        if checkpoint.schema_version != CHECKPOINT_SCHEMA_VERSION:
            raise ValueError(
                "unsupported checkpoint schema_version: "
                f"{checkpoint.schema_version}"
            )
        return checkpoint

    def save_atomic(self, path: Path) -> None:
        """Durably replace ``path`` without exposing partial JSON."""

        path.parent.mkdir(parents=True, exist_ok=True)
        serialized = json.dumps(
            asdict(self),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=path.parent,
                prefix=f"{path.name}.",
                suffix=".tmp",
                delete=False,
            ) as temporary:
                temporary_path = Path(temporary.name)
                temporary.write(serialized)
                temporary.write("\n")
                temporary.flush()
                os.fsync(temporary.fileno())
            os.replace(temporary_path, path)
            temporary_path = None
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    def assert_compatible(self, source_sha256: str, topic: str) -> None:
        """Refuse to apply progress to a different input or destination."""

        if self.source_sha256 != source_sha256:
            raise ValueError("checkpoint source SHA-256 does not match input")
        if self.topic != topic:
            raise ValueError("checkpoint topic does not match producer topic")


def load_checkpoint(
    path: Path,
    *,
    source_sha256: str,
    topic: str,
    reset: bool = False,
) -> Checkpoint | None:
    """Load compatible progress unless an explicit reset was requested."""

    if reset:
        return None
    checkpoint = Checkpoint.load(path)
    if checkpoint is not None:
        checkpoint.assert_compatible(source_sha256, topic)
    return checkpoint
