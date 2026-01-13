"""TCP protocol parser and formatter for PyQueue."""

from dataclasses import dataclass
from typing import List


class Command:
    """Command type constants."""

    PUSH = "PUSH"
    PULL = "PULL"


@dataclass
class ParsedCommand:
    """Represents a parsed command from a client."""

    cmd: str
    args: List[str]


class ProtocolError(Exception):
    """Raised when a protocol parsing error occurs."""

    pass


def parse_command(line: str) -> ParsedCommand:
    """Parse a command line from the client.

    Format:
        PUSH <message>
        PULL <consumer_id>

    Everything after the command is treated as a single argument.
    """
    line = line.strip()
    if not line:
        raise ProtocolError("Empty command")

    parts = line.split(" ", 1)
    cmd = parts[0].upper()

    if cmd not in (Command.PUSH, Command.PULL):
        raise ProtocolError(f"Unknown command: {parts[0]}")

    if len(parts) < 2 or not parts[1]:
        raise ProtocolError(f"{cmd} requires an argument")

    return ParsedCommand(cmd=cmd, args=[parts[1]])


def format_ok() -> str:
    """Format an OK response."""
    return "OK\n"


def format_msg(message: str) -> str:
    """Format a message response."""
    return f"MSG {message}\n"


def format_empty() -> str:
    """Format an EMPTY response (no messages available)."""
    return "EMPTY\n"


def format_error(reason: str) -> str:
    """Format an error response."""
    return f"ERR {reason}\n"
