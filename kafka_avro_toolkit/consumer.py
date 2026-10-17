from __future__ import annotations
from typing import Any, Callable

class Consumer:
    """Poll loop with at-least-once semantics: offsets are committed only after the handler succeeds.

    Messages that fail to decode or whose handler raises go to `dead_letter` instead of
    blocking the partition.
    """
    def __init__(self, client, serde, handler: Callable[[Any], None], dead_letter: Callable[[bytes, Exception], None]):
        self.client, self.serde, self.handler, self.dead_letter = client, serde, handler, dead_letter

    def poll_once(self, timeout: float = 1.0) -> bool:
        msg = self.client.poll(timeout)
        if msg is None:
            return False
        if msg.error():
            raise RuntimeError(str(msg.error()))
        try:
            self.handler(self.serde.decode(msg.value()))
        except Exception as exc:  # noqa: BLE001 - routed, not swallowed
            self.dead_letter(msg.value(), exc)
        self.client.commit(message=msg, asynchronous=False)
        return True
