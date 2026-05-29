import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import Optional


class MessageType(IntEnum):
    HANDSHAKE        = 1
    BLOCK_REQUEST    = 2
    BLOCK_RESPONSE   = 3
    BLOCK_NOT_FOUND  = 4
    METADATA_REQUEST = 5
    METADATA_RESPONSE = 6


# Wire format: [4 bytes type (big-endian)] [4 bytes payload length] [N bytes payload]
_HEADER_FMT = "!II"
_HEADER_SIZE = struct.calcsize(_HEADER_FMT)


@dataclass
class Message:
    type: MessageType
    payload: bytes = b""

    def serialize(self) -> bytes:
        header = struct.pack(_HEADER_FMT, int(self.type), len(self.payload))
        return header + self.payload

    @staticmethod
    def deserialize(data: bytes) -> "Message":
        if len(data) < _HEADER_SIZE:
            raise ValueError("Data too short to contain a message header")
        msg_type, payload_len = struct.unpack(_HEADER_FMT, data[:_HEADER_SIZE])
        payload = data[_HEADER_SIZE: _HEADER_SIZE + payload_len]
        if len(payload) != payload_len:
            raise ValueError(
                f"Payload length mismatch: expected {payload_len}, got {len(payload)}"
            )
        return Message(type=MessageType(msg_type), payload=payload)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Message):
            return NotImplemented
        return self.type == other.type and self.payload == other.payload


def encode_index(index: int) -> bytes:
    return struct.pack("!I", index)


def decode_index(data: bytes) -> int:
    return struct.unpack("!I", data)[0]


def send_message(sock, message: Message) -> None:
    data = message.serialize()
    sock.sendall(data)


def recv_message(sock) -> Message:
    header = _recv_exact(sock, _HEADER_SIZE)
    msg_type, payload_len = struct.unpack(_HEADER_FMT, header)
    payload = _recv_exact(sock, payload_len) if payload_len > 0 else b""
    return Message(type=MessageType(msg_type), payload=payload)


def _recv_exact(sock, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Connection closed before receiving expected bytes")
        buf.extend(chunk)
    return bytes(buf)
