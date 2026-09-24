import asyncio
import logging
import random
import time

from meshtastic.protobuf import mesh_pb2, portnums_pb2

logger = logging.getLogger("multiplextashtic.mock")


class MockPhysicalNodeManager:
    def __init__(self):
        self._callbacks = []
        self._running = False
        self._connected = False
        self._inject_task: asyncio.Task | None = None

        self.my_node_num = 0x12345678
        self.my_info = {
            "my_node_num": self.my_node_num,
            "reboot_count": 42,
        }
        self.metadata = None

    async def connect(self):
        logger.info("MockPhysicalNode: Connecting (simulated)...")
        await asyncio.sleep(0.5)
        self._connected = True
        self._running = True
        logger.info(f"MockPhysicalNode: Connected. my_node_num=0x{self.my_node_num:08x}")
        self._inject_task = asyncio.create_task(self._inject_messages())

    async def disconnect(self):
        self._running = False
        if self._inject_task:
            self._inject_task.cancel()
            self._inject_task = None
        self._connected = False
        logger.info("MockPhysicalNode: Disconnected")

    async def send_to_radio(self, mesh_packet):
        logger.debug(f"MockPhysicalNode: Received MeshPacket (simulated)")

    async def send_raw_to_radio(self, raw_to_radio: bytes) -> None:
        logger.debug(f"MockPhysicalNode: Received {len(raw_to_radio)} raw bytes (simulated)")

    async def wait_for_my_info(self, timeout: float = 10.0) -> bool:
        return True

    async def wait_for_metadata(self, timeout: float = 10.0) -> bool:
        return False

    def set_on_reconnect(self, callback) -> None:
        pass

    def subscribe(self, callback):
        self._callbacks.append(callback)
        logger.info("MockPhysicalNode: Subscribed callback")

    async def process_incoming(self):
        logger.info("MockPhysicalNode: process_incoming started")
        while self._running:
            await asyncio.sleep(0.5)

    async def _inject_messages(self):
        await asyncio.sleep(2)
        while self._running:
            packet = self._make_fake_from_radio()
            for cb in self._callbacks:
                await cb(packet)
            await asyncio.sleep(random.uniform(10, 30))

    def _make_fake_from_radio(self) -> bytes:
        fake_from = random.choice([0xABCD0001, 0xABCD0002, 0xABCD0003, 0xABCD0004])
        greetings = [b"Hello from mock!", b"Testing mesh net", b"How's the weather?", b"73 de MockNode"]

        mp = mesh_pb2.MeshPacket()
        mp.id = random.randint(1000, 9999)
        setattr(mp, "from", fake_from)
        mp.to = 0xFFFFFFFF
        mp.rx_time = int(time.time())
        mp.rx_snr = random.uniform(5.0, 12.0)
        mp.hop_limit = 3
        mp.hop_start = 3
        decoded = mesh_pb2.Data()
        decoded.portnum = portnums_pb2.TEXT_MESSAGE_APP
        decoded.payload = random.choice(greetings)
        mp.decoded.CopyFrom(decoded)

        fr = mesh_pb2.FromRadio()
        fr.packet.CopyFrom(mp)
        return fr.SerializeToString()
