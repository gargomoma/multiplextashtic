import asyncio
import logging
import socket
import time
from typing import Callable, Awaitable

from meshtastic.protobuf import mesh_pb2

from src import config as cfg
from src.protocol import encode_frame, find_frame

logger = logging.getLogger("multiplextashtic.physical_node")

HEARTBEAT_INTERVAL_SECONDS = 60


class PhysicalNodeManager:
    my_node_num = 0
    my_info = None

    def __init__(self, config: cfg.PhysicalNodeConfig):
        self._config = config
        self._callbacks: list[Callable[[bytes], Awaitable[None]]] = []
        self._reader = None
        self._writer = None
        self._connected = False
        self._running = False
        self._reconnect_task: asyncio.Task | None = None
        self._resync_task: asyncio.Task | None = None
        self._read_loop_task: asyncio.Task | None = None
        self._read_queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._my_info_captured = asyncio.Event()
        self._metadata_captured = asyncio.Event()
        self._last_data_time: float = 0.0
        self._last_heartbeat_time: float = 0.0
        self._on_reconnect: Callable[[], Awaitable[None]] | None = None
        self._did_initial_connect = False

        self.my_node_num = 0
        self.my_info = None
        self.metadata = None
        # Raw FromRadio bytes from the node's own config dump, keyed by sub-type
        # (e.g. "lora", "security", "mqtt"), in the order the firmware sent them.
        self.config_frames: dict[str, bytes] = {}
        self.module_config_frames: dict[str, bytes] = {}

    def _start_reconnect_loop(self) -> None:
        if self._reconnect_task is not None and not self._reconnect_task.done():
            return
        self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def connect(self):
        self._running = True
        try:
            await self._do_connect()
        except Exception:
            if self._config.reconnect.enabled:
                self._start_reconnect_loop()

    async def _do_connect(self):
        # Firmware serves a single API client: close any stale connection first
        # so we don't leave half-open sockets behind on every reconnect.
        await self._close_transport()
        if self._config.connection_type == "serial":
            import serial_asyncio
            self._reader, self._writer = await serial_asyncio.open_serial_connection(
                url=self._config.serial_port, baudrate=self._config.baud_rate,
            )
            await self._serial_handshake()
        else:
            self._reader, self._writer = await asyncio.open_connection(
                self._config.tcp_host, self._config.tcp_port,
            )
            self._enable_tcp_keepalive()
            await self._send_want_config()
            logger.info("Sent want_config_id to tcp node")
        self._connected = True
        self._last_data_time = time.time()
        self._last_heartbeat_time = time.time()
        self._my_info_captured.clear()
        self._metadata_captured.clear()
        logger.info(f"Connected to {self._config.connection_type} node")
        self._read_loop_task = asyncio.create_task(self._read_loop())
        if self._did_initial_connect and self._on_reconnect:
            asyncio.create_task(self._on_reconnect())
        self._did_initial_connect = True

    def _enable_tcp_keepalive(self) -> None:
        """Let the kernel detect a dead node (power loss, WiFi drop) instead of
        relying on mesh traffic gaps, which are normal on a quiet mesh."""
        sock = self._writer.get_extra_info("socket") if self._writer else None
        if sock is None:
            return
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if hasattr(socket, "TCP_KEEPIDLE"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
        except OSError as e:
            logger.debug(f"Could not enable TCP keepalive: {e}")

    async def _close_transport(self) -> None:
        if self._writer:
            try:
                self._writer.close()
                if hasattr(self._writer, "wait_closed"):
                    await asyncio.wait_for(self._writer.wait_closed(), timeout=2.0)
            except Exception:
                pass
        self._reader = None
        self._writer = None

    async def _send_heartbeat(self) -> None:
        """Keep the firmware's phone-API session alive (like the official clients do)."""
        self._last_heartbeat_time = time.time()
        to_radio = mesh_pb2.ToRadio()
        to_radio.heartbeat.CopyFrom(mesh_pb2.Heartbeat())
        try:
            self._writer.write(encode_frame(to_radio.SerializeToString()))
            await self._writer.drain()
        except Exception as e:
            logger.debug(f"Heartbeat send failed: {e}")

    async def _send_want_config(self):
        import random
        from meshtastic.protobuf import mesh_pb2
        from src.protocol import encode_frame

        to_radio = mesh_pb2.ToRadio()
        to_radio.want_config_id = random.randint(0, 0xFFFFFFFF)
        frame = encode_frame(to_radio.SerializeToString())
        self._writer.write(frame)
        await self._writer.drain()

    async def _serial_handshake(self):
        from src.protocol import START2

        wake = bytes([START2] * 32)
        self._writer.write(wake)
        await self._writer.drain()
        await asyncio.sleep(0.1)

        await self._send_want_config()
        logger.info("Sent serial handshake (wake + want_config_id)")

    async def _read_loop(self):
        buffer = b""
        is_serial = self._config.connection_type == "serial"
        # Idle watchdog. TCP has always had one (tcp_idle_timeout); serial did
        # NOT, which meant a USB CDC re-enumeration (the /dev/ttyACM* device
        # silently going away and coming back under a new fd) left this loop
        # reading a dead handle forever -- no EOF, no error -- so reconnect never
        # fired. For serial we now:
        #   1) actively poke the link (want_config) after `poke_timeout` of
        #      silence to distinguish a quiet mesh from a dead fd, then
        #   2) declare the link dead after `idle_timeout` and trigger reconnect,
        #      which re-opens the port fresh on the current device node.
        # The local node emits telemetry ~every 60s, so idle_timeout is set well
        # above that (default 90s) to avoid false positives during quiet periods.
        if is_serial:
            idle_timeout = self._config.serial_idle_timeout
            poke_timeout = self._config.serial_poke_timeout
        else:
            idle_timeout = self._config.tcp_idle_timeout
            poke_timeout = 0
        poked = False
        while self._running and self._connected:
            try:
                chunk = await asyncio.wait_for(self._reader.read(4096), timeout=1.0)
                if not chunk:
                    break
                self._last_data_time = time.time()
                poked = False
            except asyncio.TimeoutError:
                if time.time() - self._last_heartbeat_time > HEARTBEAT_INTERVAL_SECONDS:
                    await self._send_heartbeat()
                silent = time.time() - self._last_data_time
                # Serial: poke the link once before giving up on it.
                if is_serial and poke_timeout > 0 and not poked and silent > poke_timeout:
                    logger.info(
                        f"No serial data for {int(silent)}s; poking link with want_config"
                    )
                    try:
                        await self._send_want_config()
                    except Exception as e:
                        logger.warning(f"Serial poke failed ({e!r}); treating link as down")
                        self._connected = False
                        if self._running and self._config.reconnect.enabled:
                            self._start_reconnect_loop()
                        return
                    poked = True
                if idle_timeout > 0 and silent > idle_timeout:
                    logger.warning(
                        f"No data from physical node for {int(silent)}s, assuming "
                        f"disconnect (likely USB re-enumeration); reconnecting"
                    )
                    self._connected = False
                    if self._running and self._config.reconnect.enabled:
                        self._start_reconnect_loop()
                    return
                continue
            except Exception as e:
                logger.error(f"Read error: {e}")
                break

            buffer += chunk
            while True:
                payload, buffer = find_frame(buffer)
                if payload is None:
                    break
                self._maybe_capture_my_info(payload)
                self._read_queue.put_nowait(payload)

        self._connected = False
        logger.warning("Read loop ended")
        if self._running and self._config.reconnect.enabled:
            self._start_reconnect_loop()

    def _maybe_capture_my_info(self, from_radio_bytes: bytes) -> None:
        try:
            fr = mesh_pb2.FromRadio()
            fr.ParseFromString(from_radio_bytes)
            if fr.HasField("my_info"):
                self.my_node_num = fr.my_info.my_node_num
                self.my_info = fr.my_info
                logger.info(f"Captured my_info: node_num=0x{self.my_node_num:08x}")
                self._my_info_captured.set()
            elif fr.HasField("metadata"):
                self.metadata = fr.metadata
                fw = fr.metadata.firmware_version or "unknown"
                logger.info(f"Captured metadata: firmware_version={fw}")
                self._metadata_captured.set()
            elif fr.HasField("config"):
                sub = fr.config.WhichOneof("payload_variant")
                if sub:
                    self.config_frames[sub] = from_radio_bytes
            elif fr.HasField("moduleConfig"):
                sub = fr.moduleConfig.WhichOneof("payload_variant")
                if sub:
                    self.module_config_frames[sub] = from_radio_bytes
            elif fr.config_complete_id:
                logger.info(f"Node config dump complete: {len(self.config_frames)} configs, "
                            f"{len(self.module_config_frames)} module configs")
        except Exception:
            pass

    def schedule_resync(self, delay: float = 3.0) -> None:
        """Re-read the node's config dump shortly after a client changed settings,
        so the replay cache (configs, own position, channels) doesn't go stale.
        Debounced: a burst of admin writes triggers a single re-read."""
        if self._resync_task and not self._resync_task.done():
            self._resync_task.cancel()
        self._resync_task = asyncio.create_task(self._resync_after(delay))

    async def _resync_after(self, delay: float) -> None:
        await asyncio.sleep(delay)
        if self._connected and self._writer:
            logger.info("Re-reading node config after client settings change")
            await self._send_want_config()

    def set_on_reconnect(self, callback: Callable[[], Awaitable[None]]):
        self._on_reconnect = callback

    async def send_raw_to_radio(self, raw_to_radio: bytes) -> None:
        if not self._connected or not self._writer:
            logger.warning("Cannot send: not connected")
            return
        try:
            frame = encode_frame(raw_to_radio)
            self._writer.write(frame)
            await self._writer.drain()
        except Exception as e:
            logger.error(f"Error sending to radio: {e}")

    async def wait_for_my_info(self, timeout: float = 10.0) -> bool:
        try:
            await asyncio.wait_for(self._my_info_captured.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            logger.error("Timed out waiting for my_info from device")
            return False

    async def wait_for_metadata(self, timeout: float = 10.0) -> bool:
        try:
            await asyncio.wait_for(self._metadata_captured.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            logger.warning("Timed out waiting for metadata from device")
            return False

    def subscribe(self, callback: Callable[[bytes], Awaitable[None]]):
        self._callbacks.append(callback)

    async def process_incoming(self):
        while self._running:
            try:
                packet = await asyncio.wait_for(self._read_queue.get(), timeout=1.0)
                for cb in self._callbacks:
                    await cb(packet)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error processing packet: {e}")

    async def disconnect(self):
        self._running = False
        if self._read_loop_task:
            self._read_loop_task.cancel()
            try:
                await self._read_loop_task
            except (asyncio.CancelledError, Exception):
                pass
            self._read_loop_task = None
        if self._reconnect_task:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except (asyncio.CancelledError, Exception):
                pass
            self._reconnect_task = None
        if self._writer:
            try:
                self._writer.close()
                if hasattr(self._writer, 'wait_closed'):
                    await self._writer.wait_closed()
            except Exception:
                pass
        self._reader = None
        self._writer = None
        self._connected = False
        logger.info("Physical node disconnected")

    async def _reconnect_loop(self):
        delay = self._config.reconnect.initial_delay
        while self._running and not self._connected:
            logger.info(f"Reconnect attempt in {delay}s...")
            await asyncio.sleep(delay)
            try:
                await self._do_connect()
                if self._connected:
                    logger.info("Reconnected successfully")
                    return
            except Exception as e:
                logger.warning(f"Reconnect failed: {e}")
            delay = min(
                delay * self._config.reconnect.multiplier,
                self._config.reconnect.max_delay,
            )
