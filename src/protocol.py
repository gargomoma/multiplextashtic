"""
Meshtastic serial framing protocol used over TCP.
Frame format: [0x94, 0xC3, len_hi, len_lo, payload...]
"""

START1 = 0x94
START2 = 0xC3
MAX_PACKET_SIZE = 512  # Matches firmware MAX_TO_FROM_RADIO_SIZE (PhoneAPI.h)


def encode_frame(payload: bytes) -> bytes:
    length = len(payload)
    if length > MAX_PACKET_SIZE:
        raise ValueError(f"Payload size {length} exceeds MAX_PACKET_SIZE {MAX_PACKET_SIZE}")
    header = bytes([
        START1,
        START2,
        (length >> 8) & 0xFF,
        length & 0xFF,
    ])
    return header + payload


def find_frame(data: bytes) -> tuple:
    """Find and extract a complete frame from a buffer.
    Returns (payload, remaining) or (None, data) if incomplete/invalid."""
    if len(data) < 4:
        return None, data

    start = -1
    for i in range(len(data) - 1):
        if data[i] == START1 and data[i + 1] == START2:
            start = i
            break

    if start == -1:
        # No sync found: keep a trailing partial sync byte (0x94) in case
        # it is the first half of a split START1+START2 pair.
        if data[-1:] == bytes([START1]):
            return None, data[-1:]
        return None, b""

    if start > 0:
        data = data[start:]

    if len(data) < 4:
        return None, data

    payload_length = (data[2] << 8) | data[3]

    if payload_length > MAX_PACKET_SIZE:
        # Oversize: skip the full frame when buffered, else drop only the
        # 4-byte header and resync from the remainder.
        frame_length = 4 + payload_length
        if len(data) >= frame_length:
            return None, data[frame_length:]
        return None, data[4:]

    frame_length = 4 + payload_length
    if len(data) < frame_length:
        return None, data

    payload = data[4:frame_length]
    remaining = data[frame_length:]
    return payload, remaining
