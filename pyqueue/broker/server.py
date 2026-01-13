"""Async TCP server for the PyQueue broker."""

import asyncio
import logging
import signal
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from .config import BrokerConfig
from .protocol import parse_command, format_error, ProtocolError
from .handlers import CommandHandler
from ..storage.queue import PersistentQueue


logger = logging.getLogger(__name__)


class PyQueueServer:
    """Async TCP server for PyQueue.

    Uses asyncio for handling multiple concurrent clients.
    Storage operations are run in a thread pool to avoid blocking.
    """

    def __init__(self, config: Optional[BrokerConfig] = None) -> None:
        self._config = config or BrokerConfig()
        self._queue = PersistentQueue(self._config.data_dir)
        self._handler = CommandHandler(self._queue)
        self._server: Optional[asyncio.AbstractServer] = None
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._running = False

    async def start(self) -> None:
        """Start the broker server."""
        self._running = True
        self._server = await asyncio.start_server(
            self._handle_client,
            self._config.host,
            self._config.port,
        )

        addr = self._server.sockets[0].getsockname()
        logger.info(f"PyQueue broker listening on {addr[0]}:{addr[1]}")

        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        """Stop the broker server."""
        self._running = False
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        self._queue.close()
        self._executor.shutdown(wait=True)
        logger.info("PyQueue broker stopped")

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle a connected client."""
        addr = writer.get_extra_info("peername")
        logger.info(f"Client connected: {addr}")

        try:
            while self._running:
                line = await reader.readline()
                if not line:
                    break  # Client disconnected

                try:
                    line_str = line.decode("utf-8").strip()
                    if not line_str:
                        continue

                    logger.debug(f"Received from {addr}: {line_str}")
                    cmd = parse_command(line_str)

                    # Run handler in thread pool to avoid blocking
                    loop = asyncio.get_event_loop()
                    response = await loop.run_in_executor(
                        self._executor, self._handler.handle, cmd
                    )

                except ProtocolError as e:
                    response = format_error(str(e))
                except UnicodeDecodeError:
                    response = format_error("Invalid UTF-8 encoding")

                writer.write(response.encode("utf-8"))
                await writer.drain()

        except ConnectionResetError:
            logger.debug(f"Client {addr} connection reset")
        except Exception as e:
            logger.exception(f"Error handling client {addr}")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            logger.info(f"Client disconnected: {addr}")


def run_server(config: Optional[BrokerConfig] = None) -> None:
    """Run the broker server (blocking)."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    server = PyQueueServer(config)

    async def main():
        loop = asyncio.get_event_loop()

        def signal_handler():
            asyncio.create_task(server.stop())

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, signal_handler)

        await server.start()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    run_server()
