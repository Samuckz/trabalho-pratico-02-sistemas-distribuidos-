"""
Testes de integração para PeerClient e DownloadManager.

Cada teste sobe um PeerServer real em porta efêmera para ser o "seeder",
depois exercita PeerClient / DownloadManager como "leecher".
"""

import hashlib
import os
import time

import pytest

from p2p.block_registry import BlockRegistry
from p2p.client import BlockIntegrityError, DownloadManager, PeerClient
from p2p.models import FileMetadata
from p2p.server import BlockStore, PeerServer
from p2p.transfer import FileFragmenter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _free_port() -> int:
    import socket
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _seed_server(tmp_path, file_size: int, block_size: int):
    """Cria um servidor já populado com todos os blocos de um arquivo aleatório."""
    data = os.urandom(file_size)
    src = str(tmp_path / "source.bin")
    with open(src, "wb") as f:
        f.write(data)

    store_dir = str(tmp_path / "seeder_blocks")
    store = BlockStore(store_dir)
    metadata, blocks = FileFragmenter.fragment(src, block_size=block_size)
    registry = BlockRegistry(metadata.total_blocks)

    for block in blocks:
        store.write(block.index, block.data)
        registry.mark_owned(block.index)

    port = _free_port()
    srv = PeerServer(
        host="127.0.0.1",
        port=port,
        registry=registry,
        block_store=store,
        metadata=metadata,
    )
    srv.start()
    time.sleep(0.05)
    return srv, metadata, src


# ---------------------------------------------------------------------------
# PeerClient — fetch_metadata
# ---------------------------------------------------------------------------

def test_client_fetches_metadata_from_server(tmp_path):
    srv, metadata, _ = _seed_server(tmp_path, 3 * 1024, 1024)
    try:
        leecher_registry = BlockRegistry(metadata.total_blocks)
        leecher_store = BlockStore(str(tmp_path / "leecher_blocks"))
        client = PeerClient(
            neighbor=("127.0.0.1", srv._port),
            registry=leecher_registry,
            block_store=leecher_store,
            metadata=None,
            peer_id="leecher:0",
        )
        fetched = client.fetch_metadata()
        assert fetched is not None
        assert fetched.total_blocks == metadata.total_blocks
        assert fetched.block_size == metadata.block_size
        assert fetched.block_hashes == metadata.block_hashes
    finally:
        srv.stop()


def test_client_returns_none_when_server_unreachable(tmp_path):
    leecher_registry = BlockRegistry(3)
    leecher_store = BlockStore(str(tmp_path / "leecher_blocks"))
    client = PeerClient(
        neighbor=("127.0.0.1", 19999),  # porta fechada
        registry=leecher_registry,
        block_store=leecher_store,
        metadata=None,
        peer_id="leecher:0",
        timeout=1.0,
    )
    assert client.fetch_metadata() is None


# ---------------------------------------------------------------------------
# PeerClient — download_missing_blocks
# ---------------------------------------------------------------------------

def test_client_downloads_all_blocks(tmp_path):
    srv, metadata, src = _seed_server(tmp_path, 5 * 1024, 1024)
    try:
        leecher_registry = BlockRegistry(metadata.total_blocks)
        leecher_store = BlockStore(str(tmp_path / "leecher_blocks"))
        client = PeerClient(
            neighbor=("127.0.0.1", srv._port),
            registry=leecher_registry,
            block_store=leecher_store,
            metadata=metadata,
            peer_id="leecher:0",
        )
        received = client.download_missing_blocks()
        assert received == metadata.total_blocks
        assert leecher_registry.is_complete()
    finally:
        srv.stop()


def test_client_skips_already_owned_blocks(tmp_path):
    srv, metadata, src = _seed_server(tmp_path, 3 * 1024, 1024)
    try:
        leecher_registry = BlockRegistry(metadata.total_blocks)
        leecher_store = BlockStore(str(tmp_path / "leecher_blocks"))

        # pré-popula bloco 0 no leecher
        with open(str(tmp_path / "source.bin"), "rb") as f:
            block0_data = f.read(1024)
        leecher_store.write(0, block0_data)
        leecher_registry.mark_owned(0)

        client = PeerClient(
            neighbor=("127.0.0.1", srv._port),
            registry=leecher_registry,
            block_store=leecher_store,
            metadata=metadata,
            peer_id="leecher:0",
        )
        received = client.download_missing_blocks()
        assert received == 2  # apenas blocos 1 e 2
        assert leecher_registry.is_complete()
    finally:
        srv.stop()


def test_client_validates_block_hash(tmp_path):
    """Bloco com hash errado deve levantar BlockIntegrityError."""
    srv, metadata, _ = _seed_server(tmp_path, 2 * 1024, 1024)
    try:
        # Corromper o hash esperado no metadata para forçar divergência
        bad_hashes = list(metadata.block_hashes)
        bad_hashes[0] = "0" * 64  # hash incorreto
        bad_metadata = FileMetadata(
            name=metadata.name,
            total_size=metadata.total_size,
            block_size=metadata.block_size,
            total_blocks=metadata.total_blocks,
            block_hashes=bad_hashes,
        )

        leecher_registry = BlockRegistry(bad_metadata.total_blocks)
        leecher_store = BlockStore(str(tmp_path / "leecher_blocks"))
        client = PeerClient(
            neighbor=("127.0.0.1", srv._port),
            registry=leecher_registry,
            block_store=leecher_store,
            metadata=bad_metadata,
            peer_id="leecher:0",
        )
        with pytest.raises(BlockIntegrityError):
            client.download_missing_blocks()
    finally:
        srv.stop()


def test_client_on_block_received_callback(tmp_path):
    srv, metadata, _ = _seed_server(tmp_path, 3 * 1024, 1024)
    try:
        leecher_registry = BlockRegistry(metadata.total_blocks)
        leecher_store = BlockStore(str(tmp_path / "leecher_blocks"))
        received_indices = []

        client = PeerClient(
            neighbor=("127.0.0.1", srv._port),
            registry=leecher_registry,
            block_store=leecher_store,
            metadata=metadata,
            peer_id="leecher:0",
            on_block_received=received_indices.append,
        )
        client.download_missing_blocks()
        assert sorted(received_indices) == [0, 1, 2]
    finally:
        srv.stop()


def test_client_returns_zero_when_unreachable(tmp_path):
    metadata = FileMetadata("f.bin", 1024, 1024, 1, ["abc"])
    registry = BlockRegistry(1)
    store = BlockStore(str(tmp_path / "blocks"))
    client = PeerClient(
        neighbor=("127.0.0.1", 19998),
        registry=registry,
        block_store=store,
        metadata=metadata,
        peer_id="leecher:0",
        timeout=1.0,
    )
    assert client.download_missing_blocks() == 0
    assert not registry.is_complete()


# ---------------------------------------------------------------------------
# DownloadManager — integração com servidor real
# ---------------------------------------------------------------------------

def test_download_manager_completes_download(tmp_path):
    srv, metadata, src = _seed_server(tmp_path, 10 * 1024, 1024)
    try:
        leecher_registry = BlockRegistry(metadata.total_blocks)
        leecher_store = BlockStore(str(tmp_path / "leecher_blocks"))

        manager = DownloadManager(
            neighbors=[("127.0.0.1", srv._port)],
            registry=leecher_registry,
            block_store=leecher_store,
            peer_id="leecher:0",
            retry_interval=0.1,
        )
        manager.set_metadata(metadata)
        success = manager.run()

        assert success is True
        assert leecher_registry.is_complete()
    finally:
        srv.stop()


def test_download_manager_fetches_metadata_automatically(tmp_path):
    srv, metadata, src = _seed_server(tmp_path, 3 * 1024, 1024)
    try:
        leecher_registry = BlockRegistry(metadata.total_blocks)
        leecher_store = BlockStore(str(tmp_path / "leecher_blocks"))

        manager = DownloadManager(
            neighbors=[("127.0.0.1", srv._port)],
            registry=leecher_registry,
            block_store=leecher_store,
            peer_id="leecher:0",
            retry_interval=0.1,
        )
        # sem chamar set_metadata — manager deve buscar do vizinho
        success = manager.run()

        assert success is True
        assert manager.get_metadata() is not None
        assert leecher_registry.is_complete()
    finally:
        srv.stop()


def test_download_manager_on_complete_callback(tmp_path):
    srv, metadata, _ = _seed_server(tmp_path, 2 * 1024, 1024)
    try:
        leecher_registry = BlockRegistry(metadata.total_blocks)
        leecher_store = BlockStore(str(tmp_path / "leecher_blocks"))
        completed = []

        manager = DownloadManager(
            neighbors=[("127.0.0.1", srv._port)],
            registry=leecher_registry,
            block_store=leecher_store,
            peer_id="leecher:0",
            on_complete=lambda: completed.append(True),
            retry_interval=0.1,
        )
        manager.set_metadata(metadata)
        manager.run()

        assert completed == [True]
    finally:
        srv.stop()


def test_download_manager_returns_false_when_no_peers(tmp_path):
    registry = BlockRegistry(3)
    store = BlockStore(str(tmp_path / "blocks"))
    manager = DownloadManager(
        neighbors=[("127.0.0.1", 29999)],
        registry=registry,
        block_store=store,
        peer_id="leecher:0",
        retry_interval=0.1,
        max_retries=2,
    )
    success = manager.run()
    assert success is False


def test_download_manager_multiple_neighbors(tmp_path):
    """Dois seeders com blocos distintos — manager deve completar usando ambos."""
    # Seeder A tem blocos 0..4, Seeder B tem blocos 5..9
    file_size = 10 * 1024
    block_size = 1024
    data = os.urandom(file_size)
    src = str(tmp_path / "source.bin")
    with open(src, "wb") as f:
        f.write(data)

    from p2p.transfer import FileFragmenter
    metadata, all_blocks = FileFragmenter.fragment(src, block_size=block_size)

    def make_partial_server(indices, subdir):
        store = BlockStore(str(tmp_path / subdir))
        registry = BlockRegistry(metadata.total_blocks)
        for i in indices:
            store.write(all_blocks[i].index, all_blocks[i].data)
            registry.mark_owned(i)
        port = _free_port()
        srv = PeerServer("127.0.0.1", port, registry, store, metadata)
        srv.start()
        time.sleep(0.05)
        return srv

    srv_a = make_partial_server(range(0, 5), "seeder_a")
    srv_b = make_partial_server(range(5, 10), "seeder_b")

    try:
        leecher_registry = BlockRegistry(metadata.total_blocks)
        leecher_store = BlockStore(str(tmp_path / "leecher_blocks"))

        manager = DownloadManager(
            neighbors=[("127.0.0.1", srv_a._port), ("127.0.0.1", srv_b._port)],
            registry=leecher_registry,
            block_store=leecher_store,
            peer_id="leecher:0",
            retry_interval=0.1,
        )
        manager.set_metadata(metadata)
        success = manager.run()

        assert success is True
        assert leecher_registry.is_complete()
    finally:
        srv_a.stop()
        srv_b.stop()


def test_downloaded_blocks_pass_integrity_check(tmp_path):
    """Blocos baixados devem ter o mesmo SHA-256 registrado no metadata."""
    srv, metadata, _ = _seed_server(tmp_path, 4 * 1024, 1024)
    try:
        leecher_registry = BlockRegistry(metadata.total_blocks)
        leecher_store = BlockStore(str(tmp_path / "leecher_blocks"))

        manager = DownloadManager(
            neighbors=[("127.0.0.1", srv._port)],
            registry=leecher_registry,
            block_store=leecher_store,
            peer_id="leecher:0",
            retry_interval=0.1,
        )
        manager.set_metadata(metadata)
        manager.run()

        for i in range(metadata.total_blocks):
            block_data = leecher_store.read(i)
            actual_hash = hashlib.sha256(block_data).hexdigest()
            assert actual_hash == metadata.block_hashes[i], f"Block {i} hash mismatch"
    finally:
        srv.stop()


def test_download_manager_stop_aborts_run(tmp_path):
    """stop() deve interromper o run() prematuramente."""
    registry = BlockRegistry(100)
    store = BlockStore(str(tmp_path / "blocks"))
    manager = DownloadManager(
        neighbors=[("127.0.0.1", 29998)],  # porta inexistente
        registry=registry,
        block_store=store,
        peer_id="leecher:0",
        retry_interval=5.0,  # longo para garantir que stop() interrompa
        max_retries=10,
    )

    import threading
    result = []

    def run_manager():
        result.append(manager.run())

    t = threading.Thread(target=run_manager)
    t.start()
    time.sleep(0.2)
    manager.stop()
    t.join(timeout=3.0)

    assert not t.is_alive(), "run() não foi interrompido pelo stop()"
    assert result == [False]
