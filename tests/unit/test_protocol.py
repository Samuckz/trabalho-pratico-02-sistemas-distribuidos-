import os
import struct

import pytest

from p2p.models import FileMetadata
from p2p.protocol import (
    Message,
    MessageType,
    decode_index,
    encode_index,
)


# ---------------------------------------------------------------------------
# Serialization round-trip tests
# ---------------------------------------------------------------------------

def test_handshake_round_trip():
    msg = Message(type=MessageType.HANDSHAKE, payload=b"peer-A:9001")
    restored = Message.deserialize(msg.serialize())
    assert restored == msg


def test_block_request_round_trip():
    msg = Message(type=MessageType.BLOCK_REQUEST, payload=encode_index(42))
    restored = Message.deserialize(msg.serialize())
    assert restored.type == MessageType.BLOCK_REQUEST
    assert decode_index(restored.payload) == 42


def test_block_response_round_trip():
    data = os.urandom(1024)
    msg = Message(type=MessageType.BLOCK_RESPONSE, payload=data)
    restored = Message.deserialize(msg.serialize())
    assert restored.type == MessageType.BLOCK_RESPONSE
    assert restored.payload == data


def test_block_not_found_round_trip():
    msg = Message(type=MessageType.BLOCK_NOT_FOUND, payload=encode_index(7))
    restored = Message.deserialize(msg.serialize())
    assert restored.type == MessageType.BLOCK_NOT_FOUND
    assert decode_index(restored.payload) == 7


def test_metadata_request_round_trip():
    msg = Message(type=MessageType.METADATA_REQUEST, payload=b"")
    restored = Message.deserialize(msg.serialize())
    assert restored == msg


def test_metadata_response_round_trip():
    meta = FileMetadata(
        name="file.bin",
        total_size=10240,
        block_size=1024,
        total_blocks=10,
        block_hashes=["abc123"] * 10,
    )
    msg = Message(type=MessageType.METADATA_RESPONSE, payload=meta.to_bytes())
    restored = Message.deserialize(msg.serialize())
    assert restored.type == MessageType.METADATA_RESPONSE
    restored_meta = FileMetadata.from_bytes(restored.payload)
    assert restored_meta.name == meta.name
    assert restored_meta.total_blocks == meta.total_blocks
    assert restored_meta.block_hashes == meta.block_hashes


# ---------------------------------------------------------------------------
# Wire format validation
# ---------------------------------------------------------------------------

def test_serialize_has_correct_header_size():
    msg = Message(type=MessageType.BLOCK_REQUEST, payload=encode_index(0))
    raw = msg.serialize()
    # header = 4 bytes type + 4 bytes length = 8 bytes
    assert len(raw) == 8 + len(msg.payload)


def test_message_type_encoded_as_big_endian():
    msg = Message(type=MessageType.HANDSHAKE, payload=b"")
    raw = msg.serialize()
    msg_type = struct.unpack("!I", raw[:4])[0]
    assert msg_type == int(MessageType.HANDSHAKE)


def test_payload_length_in_header_matches_actual():
    payload = os.urandom(512)
    msg = Message(type=MessageType.BLOCK_RESPONSE, payload=payload)
    raw = msg.serialize()
    length_in_header = struct.unpack("!I", raw[4:8])[0]
    assert length_in_header == 512


def test_deserialize_empty_payload():
    msg = Message(type=MessageType.METADATA_REQUEST, payload=b"")
    restored = Message.deserialize(msg.serialize())
    assert restored.payload == b""


def test_deserialize_raises_on_truncated_data():
    msg = Message(type=MessageType.BLOCK_RESPONSE, payload=os.urandom(256))
    raw = msg.serialize()
    with pytest.raises((ValueError, struct.error)):
        Message.deserialize(raw[:4])  # only type, no length


def test_deserialize_raises_on_payload_length_mismatch():
    msg = Message(type=MessageType.BLOCK_RESPONSE, payload=b"x" * 10)
    raw = msg.serialize()
    # truncate payload by 5 bytes but keep header claiming 10
    with pytest.raises(ValueError):
        Message.deserialize(raw[:-5])


# ---------------------------------------------------------------------------
# encode/decode index helpers
# ---------------------------------------------------------------------------

def test_encode_decode_index_zero():
    assert decode_index(encode_index(0)) == 0


def test_encode_decode_index_large():
    assert decode_index(encode_index(65535)) == 65535


def test_encode_index_is_4_bytes():
    assert len(encode_index(0)) == 4


# ---------------------------------------------------------------------------
# Message equality
# ---------------------------------------------------------------------------

def test_message_equality():
    a = Message(type=MessageType.HANDSHAKE, payload=b"hello")
    b = Message(type=MessageType.HANDSHAKE, payload=b"hello")
    assert a == b


def test_message_inequality_different_type():
    a = Message(type=MessageType.HANDSHAKE, payload=b"x")
    b = Message(type=MessageType.BLOCK_REQUEST, payload=b"x")
    assert a != b


def test_message_inequality_different_payload():
    a = Message(type=MessageType.HANDSHAKE, payload=b"hello")
    b = Message(type=MessageType.HANDSHAKE, payload=b"world")
    assert a != b
