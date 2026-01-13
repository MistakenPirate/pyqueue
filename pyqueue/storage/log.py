"""Append-only log storage for messages."""

import struct
from pathlib import Path
from typing import Iterator


class AppendLog:
    """Manages an append-only log file with length-prefixed records.

    Storage format: [4-byte length][message bytes][4-byte length][message bytes]...
    Uses big-endian unsigned int for length prefix (supports up to 4GB messages).
    """

    HEADER_SIZE = 4  # bytes for length prefix

    def __init__(self, path: str) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self._path, "ab+")
        self._file.seek(0, 2)  # Seek to end
        self._write_offset = self._file.tell()

    def append(self, data: bytes) -> int:
        """Append data to the log and return the offset where it was written."""
        offset = self._write_offset
        length = len(data)
        header = struct.pack(">I", length)
        self._file.write(header + data)
        self._file.flush()
        self._write_offset += self.HEADER_SIZE + length
        return offset

    def read_at(self, offset: int) -> bytes:
        """Read a record at the given offset."""
        with open(self._path, "rb") as f:
            f.seek(offset)
            header = f.read(self.HEADER_SIZE)
            if len(header) < self.HEADER_SIZE:
                raise ValueError(f"Invalid offset {offset}: incomplete header")
            length = struct.unpack(">I", header)[0]
            data = f.read(length)
            if len(data) < length:
                raise ValueError(f"Invalid offset {offset}: incomplete data")
            return data

    def replay(self) -> Iterator[tuple[int, bytes]]:
        """Replay all records from the log for recovery.

        Yields (offset, data) tuples for each record.
        """
        with open(self._path, "rb") as f:
            offset = 0
            while True:
                header = f.read(self.HEADER_SIZE)
                if len(header) == 0:
                    break  # EOF
                if len(header) < self.HEADER_SIZE:
                    break  # Truncated header, stop recovery
                length = struct.unpack(">I", header)[0]
                data = f.read(length)
                if len(data) < length:
                    break  # Truncated data, stop recovery
                yield (offset, data)
                offset += self.HEADER_SIZE + length

    def flush(self) -> None:
        """Flush pending writes to disk."""
        self._file.flush()

    def close(self) -> None:
        """Close the log file."""
        self._file.close()

    @property
    def size(self) -> int:
        """Return the current size of the log."""
        return self._write_offset
