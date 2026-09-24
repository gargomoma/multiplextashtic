import asyncio
import logging
import socket
import time
from typing import Callable

from zeroconf import IPVersion, ServiceInfo
from zeroconf.asyncio import AsyncZeroconf

from src import __version__
from src import config as cfg

logger = logging.getLogger("multiplextashtic.mdns")

_MDNS_TIMEOUT = 5.0

# Node-id resolution retry policy: if the radio has not yet reported a real
# node id but a node IS connected, the id may still arrive, so wait and retry
# before falling back to a timestamp-based name.
_NODEID_RETRY_DELAY = 60.0   # seconds between attempts
_NODEID_MAX_ATTEMPTS = 3     # 1 initial + 2 retries, then give up

# Node ids that are not real, per main.py: !00000000 (unset placeholder) and
# !12345678 (the synthetic fallback used when wait_for_my_info times out).
_SYNTHETIC_NODE_IDS = {"00000000", "12345678"}


def _get_local_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


def _node_slug(node_id: str) -> str | None:
    """Return a clean slug from a REAL node id, or None if it is missing/synthetic."""
    slug = "".join(c for c in (node_id or "").lstrip("!") if c.isalnum())
    if not slug or slug in _SYNTHETIC_NODE_IDS or set(slug) <= {"0"}:
        return None
    return slug


class MDNSService:
    def __init__(
        self,
        app_config: cfg.AppConfig,
        short_name: str = "VMUX",
        node_id: str = "!00000000",
        node_id_provider: Callable[[], str] | None = None,
        is_connected: Callable[[], bool] | None = None,
    ):
        self._config = app_config.discovery
        self._server_config = app_config.server
        self._short_name = short_name
        self._node_id = node_id
        # Optional live hooks so start() can re-read a fresh node id on retry
        # and check whether a node is currently connected.
        self._node_id_provider = node_id_provider
        self._is_connected = is_connected
        self._azc: AsyncZeroconf | None = None
        self._info: ServiceInfo | None = None

    def set_short_name(self, name: str) -> None:
        self._short_name = name

    def _current_node_id(self) -> str:
        if self._node_id_provider is not None:
            try:
                return self._node_id_provider() or self._node_id
            except Exception:
                return self._node_id
        return self._node_id

    def _connected(self) -> bool:
        if self._is_connected is not None:
            try:
                return bool(self._is_connected())
            except Exception:
                return False
        return False

    async def _resolve_node_slug(self) -> str:
        """Prefer a real node id. If none yet but a node is connected, retry
        every _NODEID_RETRY_DELAY up to _NODEID_MAX_ATTEMPTS, then fall back to
        the last 4 digits of the unix timestamp."""
        slug = _node_slug(self._current_node_id())
        if slug:
            return slug

        for attempt in range(2, _NODEID_MAX_ATTEMPTS + 1):
            if not self._connected():
                # No node to wait for -- go straight to the timestamp fallback.
                break
            logger.info(
                f"mDNS: node connected but node id not ready "
                f"(attempt {attempt - 1}/{_NODEID_MAX_ATTEMPTS - 1}); "
                f"retrying in {int(_NODEID_RETRY_DELAY)}s"
            )
            await asyncio.sleep(_NODEID_RETRY_DELAY)
            slug = _node_slug(self._current_node_id())
            if slug:
                return slug

        ts_slug = str(int(time.time()))[-4:]
        logger.warning(
            f"mDNS: no real node id after {_NODEID_MAX_ATTEMPTS} attempts; "
            f"using timestamp-based name suffix {ts_slug}"
        )
        return ts_slug

    async def start(self) -> None:
        if not self._config.enabled:
            logger.info("mDNS discovery disabled")
            return

        try:
            local_ip = _get_local_ip()
            service_type = "_meshtastic._tcp.local."

            # Instance name MUST be unique per node on the LAN. mDNS keys a
            # service on its instance name, not its IP, so a hardcoded constant
            # collides the moment a second node of this app appears (two records
            # with the same name -> NonUniqueNameException). Prefer the real node
            # id; retry while a node is connected; else use a timestamp suffix.
            node_slug = await self._resolve_node_slug()
            service_name = f"Multiplextashtic-{node_slug}"

            # Refresh node id for the TXT record too (it may have arrived on retry).
            current_node_id = self._current_node_id()

            properties = {
                b"shortname": self._short_name.encode("utf-8"),
                b"id": current_node_id.encode("utf-8"),
                b"version": __version__.encode("utf-8"),
                b"pio_env": b"python-mux",
            }

            self._info = ServiceInfo(
                type_=service_type,
                name=f"{service_name}.{service_type}",
                addresses=[socket.inet_aton(local_ip)],
                port=self._server_config.port,
                properties=properties,
                server=f"{service_name}.local.",
            )

            # Use zeroconf's async API on the running event loop.
            # NOTE: constructing Zeroconf() and calling register_service() from
            # within asyncio.to_thread() (i.e. a worker thread while the app's
            # loop is running) makes zeroconf >=0.39 raise NonUniqueNameException
            # with an empty message -- the source of the blank
            # "Failed to register mDNS service:" warnings. AsyncZeroconf runs the
            # registration coroutine directly on this loop and avoids that.
            # allow_name_change stays as a last-resort safety net in case two
            # nodes ever share the same node id.
            self._azc = AsyncZeroconf(ip_version=IPVersion.V4Only)
            await asyncio.wait_for(
                self._azc.async_register_service(self._info, allow_name_change=True),
                timeout=_MDNS_TIMEOUT,
            )

            logger.info(
                f"mDNS service registered: {service_name} "
                f"(_meshtastic._tcp.local.) addr={local_ip} "
                f"port={self._server_config.port} "
                f"shortname={self._short_name} id={current_node_id}"
            )
        except asyncio.TimeoutError:
            logger.warning("mDNS registration timed out (port 5353 may be in use)")
            await self._safe_close()
        except Exception as e:
            # Repr, so an exception with an empty str() (e.g. NonUniqueNameException)
            # still logs its type instead of a blank message.
            logger.warning(f"Failed to register mDNS service: {e!r}")
            await self._safe_close()

    async def _safe_close(self) -> None:
        if self._azc is not None:
            try:
                await self._azc.async_close()
            except Exception:
                pass
            self._azc = None
        self._info = None

    async def stop(self) -> None:
        if self._azc:
            try:
                if self._info:
                    await self._azc.async_unregister_service(self._info)
            except Exception:
                pass
            try:
                await self._azc.async_close()
            except Exception:
                pass
            self._azc = None
            self._info = None
            logger.info("mDNS service unregistered")
