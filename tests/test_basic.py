"""Integration tests for PyQueue."""

import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyqueue.client import PyQueueClient, PyQueueError
from pyqueue.broker.config import BrokerConfig


def wait_for_port(host: str, port: int, timeout: float = 10.0) -> bool:
    """Wait for a port to become available."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except (socket.error, ConnectionRefusedError):
            time.sleep(0.1)
    return False


def wait_for_port_closed(host: str, port: int, timeout: float = 10.0) -> bool:
    """Wait for a port to be closed."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                time.sleep(0.1)
        except (socket.error, ConnectionRefusedError):
            return True
    return False


class BrokerProcess:
    """Manages a broker subprocess for testing."""

    def __init__(self, data_dir: str, port: int = 5556):
        self.data_dir = data_dir
        self.port = port
        self.host = "127.0.0.1"
        self.process = None

    def start(self) -> None:
        """Start the broker process."""
        env = os.environ.copy()
        env["PYTHONPATH"] = str(Path(__file__).parent.parent)

        self.process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                f"""
import sys
sys.path.insert(0, '{Path(__file__).parent.parent}')
from pyqueue.broker.server import run_server
from pyqueue.broker.config import BrokerConfig
config = BrokerConfig(host='{self.host}', port={self.port}, data_dir='{self.data_dir}')
run_server(config)
""",
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        if not wait_for_port(self.host, self.port):
            self.stop()
            raise RuntimeError("Broker failed to start")

    def stop(self) -> None:
        """Stop the broker process."""
        if self.process:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            self.process = None

        wait_for_port_closed(self.host, self.port)

    def restart(self) -> None:
        """Restart the broker (simulates crash recovery)."""
        self.stop()
        self.start()


@pytest.fixture
def temp_data_dir():
    """Create a temporary data directory for tests."""
    data_dir = tempfile.mkdtemp(prefix="pyqueue_test_")
    yield data_dir
    shutil.rmtree(data_dir, ignore_errors=True)


@pytest.fixture
def broker(temp_data_dir):
    """Start a broker for testing."""
    broker = BrokerProcess(temp_data_dir)
    broker.start()
    yield broker
    broker.stop()


@pytest.fixture
def client(broker):
    """Create a connected client."""
    client = PyQueueClient(host=broker.host, port=broker.port)
    client.connect()
    yield client
    client.close()


class TestBrokerStartup:
    """Tests for broker startup."""

    def test_broker_starts(self, broker):
        """Test that the broker starts and accepts connections."""
        assert wait_for_port(broker.host, broker.port)

    def test_client_connects(self, broker):
        """Test that a client can connect to the broker."""
        client = PyQueueClient(host=broker.host, port=broker.port)
        client.connect()
        client.close()


class TestPushPull:
    """Tests for push and pull operations."""

    def test_push_returns_true(self, client):
        """Test that push returns True on success."""
        result = client.push("test message")
        assert result is True

    def test_pull_returns_message(self, client):
        """Test that pull returns the pushed message."""
        client.push("hello world")
        msg = client.pull("test-consumer")
        assert msg == "hello world"

    def test_pull_empty_returns_none(self, client):
        """Test that pull returns None when queue is empty."""
        msg = client.pull("new-consumer")
        assert msg is None

    def test_push_message_with_spaces(self, client):
        """Test that messages with spaces work correctly."""
        client.push("hello world with spaces")
        msg = client.pull("consumer")
        assert msg == "hello world with spaces"


class TestMessageOrdering:
    """Tests for message ordering guarantees."""

    def test_fifo_ordering(self, client):
        """Test that messages are delivered in FIFO order."""
        messages = ["first", "second", "third", "fourth", "fifth"]
        for msg in messages:
            client.push(msg)

        received = []
        for _ in range(len(messages)):
            msg = client.pull("order-consumer")
            received.append(msg)

        assert received == messages

    def test_ordering_with_multiple_pushes(self, client):
        """Test ordering with many messages."""
        count = 100
        for i in range(count):
            client.push(f"message-{i}")

        for i in range(count):
            msg = client.pull("bulk-consumer")
            assert msg == f"message-{i}"


class TestMultipleConsumers:
    """Tests for multiple consumer support."""

    def test_independent_offsets(self, client):
        """Test that consumers have independent offsets."""
        client.push("msg1")
        client.push("msg2")
        client.push("msg3")

        # Consumer A reads all messages
        assert client.pull("consumer-a") == "msg1"
        assert client.pull("consumer-a") == "msg2"
        assert client.pull("consumer-a") == "msg3"
        assert client.pull("consumer-a") is None

        # Consumer B should still see all messages
        assert client.pull("consumer-b") == "msg1"
        assert client.pull("consumer-b") == "msg2"
        assert client.pull("consumer-b") == "msg3"
        assert client.pull("consumer-b") is None

    def test_consumer_resumes_from_offset(self, client):
        """Test that a consumer resumes from their last offset."""
        client.push("msg1")
        client.push("msg2")
        client.push("msg3")

        # Consumer reads first two messages
        assert client.pull("resuming-consumer") == "msg1"
        assert client.pull("resuming-consumer") == "msg2"

        # Add more messages
        client.push("msg4")
        client.push("msg5")

        # Consumer should resume from msg3
        assert client.pull("resuming-consumer") == "msg3"
        assert client.pull("resuming-consumer") == "msg4"
        assert client.pull("resuming-consumer") == "msg5"


class TestPersistence:
    """Tests for persistence across broker restarts."""

    def test_messages_persist_after_restart(self, broker, temp_data_dir):
        """Test that messages persist after broker restart."""
        # Push messages
        client = PyQueueClient(host=broker.host, port=broker.port)
        client.connect()
        client.push("persistent-1")
        client.push("persistent-2")
        client.push("persistent-3")
        client.close()

        # Restart broker
        broker.restart()

        # Verify messages are still there
        client = PyQueueClient(host=broker.host, port=broker.port)
        client.connect()
        assert client.pull("recovery-consumer") == "persistent-1"
        assert client.pull("recovery-consumer") == "persistent-2"
        assert client.pull("recovery-consumer") == "persistent-3"
        assert client.pull("recovery-consumer") is None
        client.close()

    def test_consumer_offsets_persist_after_restart(self, broker, temp_data_dir):
        """Test that consumer offsets persist after broker restart."""
        # Push messages and consume some
        client = PyQueueClient(host=broker.host, port=broker.port)
        client.connect()
        client.push("offset-1")
        client.push("offset-2")
        client.push("offset-3")
        assert client.pull("offset-consumer") == "offset-1"
        assert client.pull("offset-consumer") == "offset-2"
        client.close()

        # Restart broker
        broker.restart()

        # Consumer should resume from offset 2 (after msg2)
        client = PyQueueClient(host=broker.host, port=broker.port)
        client.connect()
        assert client.pull("offset-consumer") == "offset-3"
        assert client.pull("offset-consumer") is None
        client.close()


class TestErrorHandling:
    """Tests for error handling."""

    def test_push_without_message_fails(self, broker):
        """Test that PUSH without a message returns an error."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((broker.host, broker.port))
        sock.sendall(b"PUSH\n")
        response = sock.recv(1024).decode()
        sock.close()
        assert response.startswith("ERR")

    def test_unknown_command_fails(self, broker):
        """Test that unknown commands return an error."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((broker.host, broker.port))
        sock.sendall(b"INVALID command\n")
        response = sock.recv(1024).decode()
        sock.close()
        assert response.startswith("ERR")

    def test_message_with_newline_rejected(self, client):
        """Test that messages with newlines are rejected."""
        with pytest.raises(ValueError):
            client.push("message\nwith\nnewlines")


class TestConcurrency:
    """Tests for concurrent client handling."""

    def test_multiple_clients(self, broker):
        """Test that multiple clients can connect simultaneously."""
        clients = []
        for i in range(5):
            client = PyQueueClient(host=broker.host, port=broker.port)
            client.connect()
            clients.append(client)

        # Each client pushes a message
        for i, client in enumerate(clients):
            client.push(f"client-{i}-message")

        # First client reads all messages
        messages = []
        while True:
            msg = clients[0].pull("multi-client-consumer")
            if msg is None:
                break
            messages.append(msg)

        assert len(messages) == 5

        for client in clients:
            client.close()
