"""Kafka delivery accounting independent of the client adapter."""


class DeliveryTracker:
    """Advance only through a contiguous sequence of acknowledged lines."""

    def __init__(self, contiguous_confirmed_line: int = 0) -> None:
        if contiguous_confirmed_line < 0:
            raise ValueError("contiguous_confirmed_line must not be negative")
        self._contiguous_confirmed_line = contiguous_confirmed_line
        self._pending_successes: set[int] = set()
        self._failures: dict[int, str] = {}

    @property
    def contiguous_confirmed_line(self) -> int:
        return self._contiguous_confirmed_line

    @property
    def failures(self) -> dict[int, str]:
        return dict(self._failures)

    def mark_success(self, line_number: int) -> None:
        """Record an acknowledgement and advance over any closed gap."""

        if line_number <= self._contiguous_confirmed_line:
            return
        self._pending_successes.add(line_number)
        next_line = self._contiguous_confirmed_line + 1
        while next_line in self._pending_successes:
            self._pending_successes.remove(next_line)
            self._contiguous_confirmed_line = next_line
            next_line += 1

    def mark_failure(self, line_number: int, error: str) -> None:
        """Record a failed delivery without advancing the checkpoint."""

        self._failures[line_number] = error
