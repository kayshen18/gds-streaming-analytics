"""Validated settings for Kafka ingestion."""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ProducerSettings:
    """Configuration shared by the producer API and future CLI."""

    bootstrap_servers: str
    topic: str
    input_path: Path | None = None
    checkpoint_path: Path | None = None
    reset_checkpoint: bool = False
    limit: int | None = None
    rate: float | None = None
    flush_timeout: float = 30.0

    def __post_init__(self) -> None:
        if not self.bootstrap_servers.strip():
            raise ValueError("bootstrap_servers must not be blank")
        if not self.topic.strip():
            raise ValueError("topic must not be blank")
        if self.limit is not None and self.limit <= 0:
            raise ValueError("limit must be greater than zero")
        if self.rate is not None and self.rate <= 0:
            raise ValueError("rate must be greater than zero")
        if self.flush_timeout <= 0:
            raise ValueError("flush_timeout must be greater than zero")

    def to_client_config(self) -> dict[str, object]:
        """Return reliability-critical confluent-kafka settings."""

        return {
            "bootstrap.servers": self.bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
        }
