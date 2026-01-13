"""Command handlers for the broker."""

import logging
from typing import Optional

from .protocol import ParsedCommand, Command, format_ok, format_msg, format_empty, format_error
from ..storage.queue import PersistentQueue


logger = logging.getLogger(__name__)


class CommandHandler:
    """Handles incoming commands from clients."""

    def __init__(self, queue: PersistentQueue) -> None:
        self._queue = queue

    def handle(self, cmd: ParsedCommand) -> str:
        """Handle a parsed command and return the response string."""
        try:
            if cmd.cmd == Command.PUSH:
                return self._handle_push(cmd.args[0])
            elif cmd.cmd == Command.PULL:
                return self._handle_pull(cmd.args[0])
            else:
                return format_error(f"Unknown command: {cmd.cmd}")
        except Exception as e:
            logger.exception("Error handling command")
            return format_error(str(e))

    def _handle_push(self, message: str) -> str:
        """Handle a PUSH command."""
        self._queue.push(message)
        logger.debug(f"Pushed message: {message[:50]}...")
        return format_ok()

    def _handle_pull(self, consumer_id: str) -> str:
        """Handle a PULL command."""
        message: Optional[str] = self._queue.pull(consumer_id)
        if message is None:
            return format_empty()
        logger.debug(f"Pulled message for {consumer_id}: {message[:50]}...")
        return format_msg(message)
