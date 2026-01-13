"""Python client SDK for PyQueue."""

import socket
from typing import Optional


class PyQueueError(Exception):
    """Raised when the broker returns an error."""

    pass


class PyQueueClient:
    """Synchronous client for connecting to a PyQueue broker.

    Usage:
        client = PyQueueClient()
        client.connect()
        client.push("hello world")
        msg = client.pull("my-consumer")
        client.close()
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 5555,
        timeout: float = 30.0,
    ) -> None:
        self._host = host
        self._port = port
        self._timeout = timeout
        self._socket: Optional[socket.socket] = None

    def connect(self) -> None:
        """Connect to the broker."""
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(self._timeout)
        self._socket.connect((self._host, self._port))

    def close(self) -> None:
        """Close the connection to the broker."""
        if self._socket:
            self._socket.close()
            self._socket = None

    def push(self, message: str) -> bool:
        """Push a message to the queue.

        Args:
            message: The message to push. Must not contain newlines.

        Returns:
            True if the message was pushed successfully.

        Raises:
            PyQueueError: If the broker returns an error.
            ValueError: If the message contains newlines.
        """
        if "\n" in message:
            raise ValueError("Message cannot contain newlines")

        self._send(f"PUSH {message}\n")
        response = self._receive()

        if response.startswith("OK"):
            return True
        elif response.startswith("ERR"):
            raise PyQueueError(response[4:].strip())
        else:
            raise PyQueueError(f"Unexpected response: {response}")

    def pull(self, consumer_id: str) -> Optional[str]:
        """Pull the next message for a consumer.

        Args:
            consumer_id: Unique identifier for this consumer.

        Returns:
            The message, or None if no messages are available.

        Raises:
            PyQueueError: If the broker returns an error.
        """
        self._send(f"PULL {consumer_id}\n")
        response = self._receive()

        if response.startswith("MSG "):
            return response[4:].strip()
        elif response.startswith("EMPTY"):
            return None
        elif response.startswith("ERR"):
            raise PyQueueError(response[4:].strip())
        else:
            raise PyQueueError(f"Unexpected response: {response}")

    def _send(self, data: str) -> None:
        """Send data to the broker."""
        if not self._socket:
            raise PyQueueError("Not connected")
        self._socket.sendall(data.encode("utf-8"))

    def _receive(self) -> str:
        """Receive a line from the broker."""
        if not self._socket:
            raise PyQueueError("Not connected")

        data = b""
        while True:
            chunk = self._socket.recv(1)
            if not chunk:
                raise PyQueueError("Connection closed by broker")
            data += chunk
            if chunk == b"\n":
                break

        return data.decode("utf-8")

    def __enter__(self) -> "PyQueueClient":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
