"""Consumer offset tracking with JSON persistence."""

import json
from pathlib import Path
from typing import Dict


class OffsetStore:
    """Persists consumer offsets to a JSON file.

    Each consumer has an independent offset tracking their position in the queue.
    """

    def __init__(self, path: str) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._offsets: Dict[str, int] = {}
        self.load()

    def get(self, consumer_id: str) -> int:
        """Get the current offset for a consumer. Returns 0 if not found."""
        return self._offsets.get(consumer_id, 0)

    def set(self, consumer_id: str, offset: int) -> None:
        """Set the offset for a consumer and persist to disk."""
        self._offsets[consumer_id] = offset
        self.save()

    def save(self) -> None:
        """Persist offsets to disk."""
        with open(self._path, "w") as f:
            json.dump(self._offsets, f)

    def load(self) -> None:
        """Load offsets from disk if the file exists."""
        if self._path.exists():
            try:
                with open(self._path, "r") as f:
                    self._offsets = json.load(f)
            except (json.JSONDecodeError, IOError):
                self._offsets = {}

    def all_offsets(self) -> Dict[str, int]:
        """Return a copy of all consumer offsets."""
        return self._offsets.copy()
