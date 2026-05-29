"""
Testes de integração para PeerServer + ClientHandler.

Sobe um servidor real em localhost em porta efêmera e conecta um socket de teste
para verificar o comportamento dos handlers sem subir o peer completo.
"""

import os
import socket
import time

import pytest

from p2p.block_registry import BlockRegistry
from p2p.models import Block, FileMetadata
from p2p.protocol import (
    Message,
    MessageType,
    decode_index,
    encode_index,
    recv_message,
    send_message,
)
from p2p.server import BlockStore, PeerServer


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_metadata(name="file.bin", total_size=3072, block_size=1024, n=3):
    hashes = ["aabbcc"] * n
    return FileMetadata(
        name=name,
        total_size=total_size,
        block_size=block_size,
        total_blocks=n,
        block_hashes=hashes,
    )


@pytest.fixture
def store(tmp_path):
    return BlockStore(str(tmp_path / "blocks"))


@pytest.fixture
def registry():
    return BlockRegistry(3)


@pytest.fixture
def server(tmp_path, store, registry):
    meta = _make_metadata()
    port = _free_port()
    srv = PeerServer(
        host="127.0.0.1",
        port=port,
        registry=registry,
        block_store=store,
        metadata=meta,
    )
    srv.start()
    time.sleep(0.05)
    yield srv
    srv.stop()


def _connect(port: int) -> socket.socket:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", port))
    s.settimeout(5.0)
    return s


# ---------------------------------------------------------------------------
# Testes de inicialização
# ---------------------------------------------------------------------------

def test_server_starts_and_accepts_connections(server):
    s = _connect(server._port)
    s.close()


def test_server_accepts_multiple_simultaneous_connections(server):
    conns = [_connect(server._port) for _ in range(5)]
    time.sleep(0.05)
    for c in conns:
        c.close()


# ---------------------------------------------------------------------------
# METADATA_REQUEST
# ---------------------------------------------------------------------------

def test_server_responds_to_metadata_request(server):
    s = _connect(server._port)
    send_message(s, Message(type=MessageType.METADATA_REQUEST))
    response = recv_message(s)
    s.close()

    assert response.type == MessageType.METADATA_RESPONSE
    meta = FileMetadata.from_bytes(response.payload)
    assert meta.name == "file.bin"
    assert meta.total_blocks == 3


def test_metadata_response_contains_correct_block_count(server):
    s = _connect(server._port)
    send_message(s, Message(type=MessageType.METADATA_REQUEST))
    response = recv_message(s)
    s.close()

    meta = FileMetadata.from_bytes(response.payload)
    assert meta.total_blocks == 3
    assert meta.block_size == 1024


# ---------------------------------------------------------------------------
# BLOCK_REQUEST — bloco presente
# ---------------------------------------------------------------------------

def test_server_responds_with_block_when_owned(server, store, registry):
    data = os.urandom(1024)
    store.write(1, data)
    registry.mark_owned(1)

    s = _connect(server._port)
    send_message(s, Message(type=MessageType.BLOCK_REQUEST, payload=encode_index(1)))
    response = recv_message(s)
    s.close()

    assert response.type == MessageType.BLOCK_RESPONSE
    assert response.payload == data


def test_server_responds_with_correct_data_for_multiple_blocks(server, store, registry):
    payloads = {i: os.urandom(1024) for i in range(3)}
    for i, data in payloads.items():
        store.write(i, data)
        registry.mark_owned(i)

    for i in range(3):
        s = _connect(server._port)
        send_message(s, Message(type=MessageType.BLOCK_REQUEST, payload=encode_index(i)))
        response = recv_message(s)
        s.close()
        assert response.type == MessageType.BLOCK_RESPONSE
        assert response.payload == payloads[i]


# ---------------------------------------------------------------------------
# BLOCK_REQUEST — bloco ausente
# ---------------------------------------------------------------------------

def test_server_responds_block_not_found_when_not_owned(server):
    s = _connect(server._port)
    send_message(s, Message(type=MessageType.BLOCK_REQUEST, payload=encode_index(0)))
    response = recv_message(s)
    s.close()

    assert response.type == MessageType.BLOCK_NOT_FOUND
    assert decode_index(response.payload) == 0


def test_server_block_not_found_contains_requested_index(server):
    s = _connect(server._port)
    send_message(s, Message(type=MessageType.BLOCK_REQUEST, payload=encode_index(2)))
    response = recv_message(s)
    s.close()

    assert decode_index(response.payload) == 2


# ---------------------------------------------------------------------------
# Múltiplas requisições na mesma conexão
# ---------------------------------------------------------------------------

def test_server_handles_multiple_requests_on_same_connection(server, store, registry):
    data = os.urandom(1024)
    store.write(0, data)
    registry.mark_owned(0)

    s = _connect(server._port)

    send_message(s, Message(type=MessageType.METADATA_REQUEST))
    r1 = recv_message(s)
    assert r1.type == MessageType.METADATA_RESPONSE

    send_message(s, Message(type=MessageType.BLOCK_REQUEST, payload=encode_index(0)))
    r2 = recv_message(s)
    assert r2.type == MessageType.BLOCK_RESPONSE
    assert r2.payload == data

    send_message(s, Message(type=MessageType.BLOCK_REQUEST, payload=encode_index(1)))
    r3 = recv_message(s)
    assert r3.type == MessageType.BLOCK_NOT_FOUND

    s.close()


# ---------------------------------------------------------------------------
# BlockStore
# ---------------------------------------------------------------------------

def test_block_store_write_and_read(tmp_path):
    store = BlockStore(str(tmp_path / "blocks"))
    data = os.urandom(512)
    store.write(5, data)
    assert store.read(5) == data


def test_block_store_exists(tmp_path):
    store = BlockStore(str(tmp_path / "blocks"))
    assert store.exists(0) is False
    store.write(0, b"hello")
    assert store.exists(0) is True


def test_block_store_creates_directory_if_missing(tmp_path):
    store = BlockStore(str(tmp_path / "new" / "nested" / "blocks"))
    store.write(0, b"data")
    assert store.exists(0) is True


# ---------------------------------------------------------------------------
# Handshake (não deve derrubar o servidor)
# ---------------------------------------------------------------------------

def test_server_ignores_handshake_gracefully(server):
    s = _connect(server._port)
    send_message(s, Message(type=MessageType.HANDSHAKE, payload=b"peer-X:9999"))
    # servidor não responde ao handshake, mas deve continuar vivo
    time.sleep(0.05)
    s.close()

    # servidor ainda deve responder a nova conexão
    s2 = _connect(server._port)
    send_message(s2, Message(type=MessageType.METADATA_REQUEST))
    r = recv_message(s2)
    s2.close()
    assert r.type == MessageType.METADATA_RESPONSE
