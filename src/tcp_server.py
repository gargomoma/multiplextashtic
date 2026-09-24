import asyncio
import logging
import time

from src import config as cfg
from src.client_handler import ClientHandler
from src.message_queue import MessageQueue

logger = logging.getLogger("multiplextashtic.tcp_server")

CLIENT_TIMEOUT_SECONDS = 300
CLEANUP_INTERVAL_SECONDS = 60


class TCPServer:
    def __init__(
        self,
        config: cfg.ServerConfig,
        phys_mgr,
        app_config: cfg.AppConfig,
        cache=None,
        config_capture=None,
        mqtt_bridge=None,
    ):
        self._config = config
        self._phys_mgr = phys_mgr
        self._app_config = app_config
        self._cache = cache
        self._config_capture = config_capture
        self._mqtt_bridge = mqtt_bridge
        self._msg_queue = MessageQueue(phys_mgr)
        self._server: asyncio.Server | None = None
        self._clients: dict[str, ClientHandler] = {}
        self._next_client_id = 1
        self._cleanup_task: asyncio.Task | None = None
        self._serve_task: asyncio.Task | None = None

    def set_mqtt_bridge(self, mqtt_bridge) -> None:
        self._mqtt_bridge = mqtt_bridge
        for handler in self._clients.values():
            handler.set_mqtt_bridge(mqtt_bridge)

    async def start(self):
        self._server = await asyncio.start_server(
            self._handle_client,
            self._config.host,
            self._config.port,
        )
        addr = self._server.sockets[0].getsockname()
        logger.info(f"TCP server listening on {addr[0]}:{addr[1]}")
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        self._serve_task = asyncio.create_task(self._serve_forever())

    async def _serve_forever(self):
        async with self._server:
            await self._server.serve_forever()

    async def stop(self):
        if self._cleanup_task:
            self._cleanup_task.cancel()
            self._cleanup_task = None
        if self._serve_task:
            self._serve_task.cancel()
            try:
                await self._serve_task
            except (asyncio.CancelledError, Exception):
                pass
            self._serve_task = None
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        for cid in list(self._clients.keys()):
            await self._close_client(cid)
        logger.info("TCP server stopped")

    async def refresh_all_clients(self) -> None:
        logger.info(f"Refreshing config for {len(self._clients)} connected client(s)")
        for cid, handler in list(self._clients.items()):
            try:
                await handler.refresh_config()
                logger.info(f"Client {cid}: config refreshed after device reconnect")
            except Exception as e:
                logger.warning(f"Client {cid}: refresh failed: {e}")

    async def broadcast_from_radio(self, from_radio_bytes: bytes) -> None:
        disconnected = []
        for cid, handler in self._clients.items():
            try:
                await handler.queue.put(from_radio_bytes)
            except Exception as e:
                logger.warning(f"Broadcast to {cid} failed: {e}")
                disconnected.append(cid)
        for cid in disconnected:
            await self._close_client(cid)

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info("peername", ("unknown", 0))
        if len(self._clients) >= self._config.max_clients:
            logger.warning(f"Rejecting connection from {peer[0]}:{peer[1]} (max_clients={self._config.max_clients})")
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            return
        cid = f"client-{self._next_client_id}"
        self._next_client_id += 1

        logger.info(f"TCP connection accepted: {cid} from {peer[0]}:{peer[1]}")

        handler = ClientHandler(cid, reader, writer, self._config, self._phys_mgr, self._app_config, self._cache, self._config_capture, self._msg_queue, self.broadcast_from_radio, self._mqtt_bridge)
        self._clients[cid] = handler

        try:
            await handler.handle()
        except Exception as e:
            logger.warning(f"Client {cid}: handler error: {e}")
        finally:
            logger.info(f"Client {cid}: disconnected from {peer[0]}:{peer[1]}")
            self._remove_client(cid)

    def _remove_client(self, cid: str) -> None:
        self._clients.pop(cid, None)

    async def _close_client(self, cid: str) -> None:
        handler = self._clients.pop(cid, None)
        if handler is None:
            return
        try:
            writer = getattr(handler, "_writer", None)
            if writer is not None:
                writer.close()
                await writer.wait_closed()
        except Exception:
            pass

    async def _cleanup_loop(self):
        while True:
            await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)
            now = time.monotonic()
            expired = []
            for cid, handler in self._clients.items():
                last = getattr(handler, "last_activity_monotonic", None)
                if last is None:
                    # Fall back to wall-clock last_activity for older handlers.
                    last_wall = getattr(handler, "last_activity", None)
                    if last_wall is None:
                        continue
                    if time.time() - last_wall > CLIENT_TIMEOUT_SECONDS:
                        expired.append(cid)
                elif now - last > CLIENT_TIMEOUT_SECONDS:
                    logger.info(f"Client {cid}: inactive for >{CLIENT_TIMEOUT_SECONDS}s, disconnecting")
                    expired.append(cid)
            for cid in expired:
                await self._close_client(cid)
