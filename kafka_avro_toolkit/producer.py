from __future__ import annotations
from typing import Any, Callable
from .serde import Serde

class Producer:
    """Thin wrapper: serialise, attach headers, surface delivery failures instead of dropping them."""
    def __init__(self, client, serde: Serde, on_error: Callable[[str, Exception], None] | None = None):
        self.client, self.serde = client, serde
        self.failures: list[tuple[str, str]] = []
        self.on_error = on_error

    def _report(self, err, msg):
        if err is not None:
            self.failures.append((msg.topic(), str(err)))
            if self.on_error:
                self.on_error(msg.topic(), Exception(str(err)))

    def send(self, topic: str, value: Any, key: str | None = None, headers: dict[str, str] | None = None) -> None:
        self.client.produce(topic, value=self.serde.encode(value), key=key.encode() if key else None,
                            headers=list((headers or {}).items()), on_delivery=self._report)
        self.client.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        remaining = self.client.flush(timeout)
        if remaining:
            raise TimeoutError(f"{remaining} messages not delivered within {timeout}s")
        return remaining
