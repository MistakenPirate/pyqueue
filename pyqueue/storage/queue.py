"""High-level persistent queue combining log storage and offset tracking."""

import threading
from pathlib import Path
from typing import Optional, List

from .log import AppendLog
from .offsets import OffsetStore


class PersistentQueue:
    """A persistent message queue with independent consumer offsets.

    Messages are stored in an append-only log. Each consumer maintains
    their own offset, allowing multiple consumers to read independently.

    Thread-safe: all operations are protected by a lock.
    """

    def __init__(self, data_dir: str) -> None:
        self._data_dir = Path(data_dir)
        self._data_dir.mkdir(parents=True, exist_ok=True)

        self._log = AppendLog(str(self._data_dir / "queue.log"))
        self._offsets = OffsetStore(str(self._data_dir / "offsets.json"))
        self._lock = threading.Lock()

        # In-memory index: list of (offset, length) for each message
        self._index: List[int] = []

        # Recover state from disk
        self._recover()

    def _recover(self) -> None:
        """Replay log to rebuild in-memory index."""
        for offset, _ in self._log.replay():
            self._index.append(offset)

    def push(self, message: str) -> int:
        """Push a message to the queue. Returns the message index."""
        with self._lock:
            data = message.encode("utf-8")
            offset = self._log.append(data)
            index = len(self._index)
            self._index.append(offset)
            return index

    def pull(self, consumer_id: str) -> Optional[str]:
        """Pull the next message for a consumer.

        Returns None if no new messages are available.
        Advances the consumer's offset on success.
        """
        with self._lock:
            consumer_offset = self._offsets.get(consumer_id)

            if consumer_offset >= len(self._index):
                return None

            log_offset = self._index[consumer_offset]
            data = self._log.read_at(log_offset)
            message = data.decode("utf-8")

            self._offsets.set(consumer_id, consumer_offset + 1)
            return message

    def message_count(self) -> int:
        """Return the total number of messages in the queue."""
        with self._lock:
            return len(self._index)

    def consumer_offset(self, consumer_id: str) -> int:
        """Return the current offset for a consumer."""
        with self._lock:
            return self._offsets.get(consumer_id)

    def close(self) -> None:
        """Close the queue and underlying storage."""
        with self._lock:
            self._log.close()
