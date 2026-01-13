# PyQueue

A lightweight, persistent message broker written in Python. Inspired by Kafka's log-based architecture, PyQueue provides durable message storage with independent consumer offsets.

## Architecture

```
┌─────────────┐     TCP      ┌─────────────────────────────────────┐
│  Producer   │─────────────▶│           PyQueue Broker            │
└─────────────┘              │                                     │
                             │  ┌─────────────┐  ┌──────────────┐  │
┌─────────────┐     TCP      │  │   Command   │  │   Persistent │  │
│  Consumer A │◀────────────▶│  │   Handler   │──│    Queue     │  │
└─────────────┘              │  └─────────────┘  └──────────────┘  │
                             │                          │          │
┌─────────────┐     TCP      │                   ┌──────┴───────┐  │
│  Consumer B │◀────────────▶│                   │              │  │
└─────────────┘              │               ┌───┴───┐  ┌───────┴┐ │
                             │               │ Log   │  │Offsets │ │
                             │               │ File  │  │ Store  │ │
                             │               └───────┘  └────────┘ │
                             └─────────────────────────────────────┘
```

## Features

- **Persistent Storage**: Messages are durably stored in an append-only log
- **Crash Recovery**: Broker recovers state from disk on restart
- **Independent Consumers**: Each consumer tracks their own offset
- **Simple Protocol**: Text-based TCP protocol for easy debugging
- **Async Broker**: Non-blocking I/O with asyncio
- **Thread-Safe Storage**: Safe for concurrent access
- **Zero Dependencies**: Uses only Python standard library

## Quick Start

### Start the Broker

```bash
python -m pyqueue.broker.server
```

The broker listens on `127.0.0.1:5555` by default.

### Push Messages (Producer)

```python
from pyqueue.client import PyQueueClient

with PyQueueClient() as client:
    client.push("Hello, PyQueue!")
    client.push("Another message")
```

### Pull Messages (Consumer)

```python
from pyqueue.client import PyQueueClient

with PyQueueClient() as client:
    while True:
        msg = client.pull("my-consumer-id")
        if msg is None:
            break
        print(f"Received: {msg}")
```

### Run Examples

```bash
# Terminal 1: Start the broker
python -m pyqueue.broker.server

# Terminal 2: Run the producer
python examples/producer.py

# Terminal 3: Run the consumer
python examples/consumer.py
```

## Protocol

PyQueue uses a simple text-based protocol over TCP:

### Commands

| Command | Description | Example |
|---------|-------------|---------|
| `PUSH <message>` | Push a message to the queue | `PUSH Hello World` |
| `PULL <consumer_id>` | Pull next message for consumer | `PULL my-consumer` |

### Responses

| Response | Description |
|----------|-------------|
| `OK` | Command succeeded |
| `MSG <message>` | Message payload |
| `EMPTY` | No messages available |
| `ERR <reason>` | Error occurred |

## Client API

```python
from pyqueue.client import PyQueueClient, PyQueueError

# Create client
client = PyQueueClient(
    host="127.0.0.1",  # Broker host
    port=5555,         # Broker port
    timeout=30.0       # Socket timeout in seconds
)

# Connect
client.connect()

# Push a message (returns True on success)
client.push("my message")

# Pull next message (returns None if empty)
message = client.pull("consumer-id")

# Close connection
client.close()

# Or use as context manager
with PyQueueClient() as client:
    client.push("hello")
```

## Broker Configuration

```python
from pyqueue.broker import PyQueueServer, BrokerConfig

config = BrokerConfig(
    host="0.0.0.0",      # Listen address
    port=5555,           # Listen port
    data_dir="./data"    # Data directory for persistence
)

server = PyQueueServer(config)
await server.start()
```

## Storage Format

### Message Log (`data/queue.log`)

Messages are stored as length-prefixed records:

```
[4-byte length][message bytes][4-byte length][message bytes]...
```

- Length: Big-endian unsigned 32-bit integer
- Maximum message size: ~4GB

### Consumer Offsets (`data/offsets.json`)

```json
{
  "consumer-a": 5,
  "consumer-b": 3
}
```

## Guarantees

- **Ordering**: Messages are delivered in FIFO order within the queue
- **Durability**: Messages are flushed to disk before acknowledgment
- **At-least-once**: Messages may be redelivered after consumer failure
- **Persistence**: Data survives broker restarts

## Limitations

- Single queue (no topics/partitions)
- No replication
- No TLS encryption
- No authentication
- No exactly-once semantics
- Messages cannot contain newlines

## Running Tests

```bash
pip install pytest
pytest tests/ -v
```

## Project Structure

```
PyQueue/
├── pyqueue/
│   ├── broker/
│   │   ├── config.py      # Broker configuration
│   │   ├── handlers.py    # Command handlers
│   │   ├── protocol.py    # Protocol parser
│   │   └── server.py      # Async TCP server
│   ├── storage/
│   │   ├── log.py         # Append-only log
│   │   ├── offsets.py     # Consumer offset tracking
│   │   └── queue.py       # High-level queue abstraction
│   └── client/
│       └── client.py      # Python client SDK
├── examples/
│   ├── producer.py
│   └── consumer.py
├── tests/
│   └── test_basic.py
└── data/                   # Runtime data (created by broker)
```

## License

MIT
