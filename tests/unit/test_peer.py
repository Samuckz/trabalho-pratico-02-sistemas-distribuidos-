"""
Testes de integração para a classe Peer (H6).

Todos os peers sobem dentro do mesmo processo Python — sem subprocessos.
Isso permite testar a integração seeder↔leecher de forma rápida e determinística.
"""

import os
import time

import pytest

from p2p.config import PeerConfig
from p2p.peer import Peer
from p2p.transfer import ChecksumUtil


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

KB = 1024
MB = 1024 * 1024


def _free_port() -> int:
    import socket
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_file(path, size: int) -> str:
    data = os.urandom(size)
    p = str(path)
    with open(p, "wb") as f:
        f.write(data)
    return p


def _seeder_config(port, neighbor_ports, file_path, meta_path, block_size=KB):
    return PeerConfig(
        host="127.0.0.1",
        port=port,
        neighbors=[("127.0.0.1", p) for p in neighbor_ports],
        block_size=block_size,
        role="seeder",
        file_path=file_path,
        metadata_path=meta_path,
        output_dir="",
    )


def _leecher_config(port, neighbor_ports, meta_path, output_dir, block_size=KB):
    return PeerConfig(
        host="127.0.0.1",
        port=port,
        neighbors=[("127.0.0.1", p) for p in neighbor_ports],
        block_size=block_size,
        role="leecher",
        file_path="",
        metadata_path=meta_path,
        output_dir=str(output_dir),
    )


# ---------------------------------------------------------------------------
# Seeder
# ---------------------------------------------------------------------------

def test_seeder_starts_and_fragments_file(tmp_path):
    src = _make_file(tmp_path / "file.bin", 10 * KB)
    cfg = _seeder_config(_free_port(), [], src, str(tmp_path / "meta.json"))
    peer = Peer(cfg)
    try:
        peer.start()
        assert peer.metadata is not None
        assert peer.metadata.total_blocks == 10
        assert peer.registry.is_complete()
    finally:
        peer.stop()


def test_seeder_saves_metadata_to_disk(tmp_path):
    src = _make_file(tmp_path / "file.bin", 5 * KB)
    meta_path = str(tmp_path / "meta.json")
    cfg = _seeder_config(_free_port(), [], src, meta_path)
    peer = Peer(cfg)
    try:
        peer.start()
        assert os.path.exists(meta_path)
        from p2p.models import FileMetadata
        loaded = FileMetadata.load(meta_path)
        assert loaded.total_blocks == 5
    finally:
        peer.stop()


def test_seeder_raises_when_file_missing(tmp_path):
    cfg = _seeder_config(_free_port(), [], "/nonexistent/file.bin", "")
    peer = Peer(cfg)
    with pytest.raises(FileNotFoundError):
        peer.start()


def test_seeder_reuses_existing_metadata(tmp_path):
    src = _make_file(tmp_path / "file.bin", 3 * KB)
    meta_path = str(tmp_path / "meta.json")

    # primeira vez: gera metadata
    cfg = _seeder_config(_free_port(), [], src, meta_path)
    peer = Peer(cfg, output_dir=str(tmp_path / "s1"))
    peer.start()
    peer.stop()

    # segunda vez: reutiliza metadata existente
    cfg2 = _seeder_config(_free_port(), [], src, meta_path)
    peer2 = Peer(cfg2, output_dir=str(tmp_path / "s2"))
    peer2.start()
    assert peer2.metadata.total_blocks == 3
    peer2.stop()


# ---------------------------------------------------------------------------
# 2 Peers — Seeder + Leecher
# ---------------------------------------------------------------------------

def test_leecher_downloads_and_assembles_file(tmp_path):
    src = _make_file(tmp_path / "original.bin", 10 * KB)
    original_hash = ChecksumUtil.sha256_file(src)

    port_s, port_l = _free_port(), _free_port()
    meta_path = str(tmp_path / "meta.json")
    out_dir = tmp_path / "leecher_out"
    out_dir.mkdir()

    seeder = Peer(_seeder_config(port_s, [port_l], src, meta_path))
    leecher = Peer(_leecher_config(port_l, [port_s], meta_path, out_dir))

    try:
        seeder.start()
        time.sleep(0.1)
        leecher.start()

        completed = leecher.wait_until_done(timeout=30)
        assert completed, "Leecher não completou o download no tempo limite"

        restored = out_dir / "original.bin"
        assert restored.exists(), "Arquivo remontado não encontrado"
        assert restored.stat().st_size == 10 * KB
        assert ChecksumUtil.sha256_file(str(restored)) == original_hash
    finally:
        seeder.stop()
        leecher.stop()


def test_leecher_creates_done_marker(tmp_path):
    src = _make_file(tmp_path / "file.bin", 2 * KB)
    port_s, port_l = _free_port(), _free_port()
    meta_path = str(tmp_path / "meta.json")
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    seeder = Peer(_seeder_config(port_s, [], src, meta_path))
    leecher = Peer(_leecher_config(port_l, [port_s], meta_path, out_dir))

    try:
        seeder.start()
        time.sleep(0.1)
        leecher.start()
        leecher.wait_until_done(timeout=20)
        assert (out_dir / ".done").exists()
    finally:
        seeder.stop()
        leecher.stop()


def test_leecher_without_local_metadata_fetches_from_seeder(tmp_path):
    """Leecher sem metadata local deve obtê-lo do seeder automaticamente."""
    src = _make_file(tmp_path / "file.bin", 3 * KB)
    port_s, port_l = _free_port(), _free_port()
    seeder_meta = str(tmp_path / "seeder_meta.json")
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    seeder = Peer(_seeder_config(port_s, [], src, seeder_meta))
    # leecher não tem metadata_path local
    leecher = Peer(_leecher_config(port_l, [port_s], meta_path="", output_dir=out_dir))

    try:
        seeder.start()
        time.sleep(0.1)
        leecher.start()
        completed = leecher.wait_until_done(timeout=20)
        assert completed
        assert (out_dir / "file.bin").exists()
    finally:
        seeder.stop()
        leecher.stop()


def test_integrity_sha256_matches_original(tmp_path):
    """RNF-04: SHA-256 do arquivo remontado deve ser idêntico ao original."""
    src = _make_file(tmp_path / "data.bin", 10 * KB)
    original_hash = ChecksumUtil.sha256_file(src)

    port_s, port_l = _free_port(), _free_port()
    meta_path = str(tmp_path / "meta.json")
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    seeder = Peer(_seeder_config(port_s, [], src, meta_path))
    leecher = Peer(_leecher_config(port_l, [port_s], meta_path, out_dir))

    try:
        seeder.start()
        time.sleep(0.1)
        leecher.start()
        leecher.wait_until_done(timeout=30)
        restored = out_dir / "data.bin"
        assert ChecksumUtil.sha256_file(str(restored)) == original_hash
        assert restored.stat().st_size == 10 * KB
    finally:
        seeder.stop()
        leecher.stop()


def test_transfer_with_4kb_blocks(tmp_path):
    src = _make_file(tmp_path / "file.bin", 1 * MB)
    original_hash = ChecksumUtil.sha256_file(src)
    port_s, port_l = _free_port(), _free_port()
    meta_path = str(tmp_path / "meta.json")
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    seeder = Peer(_seeder_config(port_s, [], src, meta_path, block_size=4 * KB))
    leecher = Peer(_leecher_config(port_l, [port_s], meta_path, out_dir, block_size=4 * KB))

    try:
        seeder.start()
        time.sleep(0.1)
        leecher.start()
        assert leecher.wait_until_done(timeout=60)
        assert ChecksumUtil.sha256_file(str(out_dir / "file.bin")) == original_hash
    finally:
        seeder.stop()
        leecher.stop()


# ---------------------------------------------------------------------------
# RF-05 — Leecher serve blocos antes de ter o arquivo completo
# ---------------------------------------------------------------------------

def test_leecher_serves_as_partial_seeder(tmp_path):
    """
    Topologia: A(seeder) → B(leecher) → C(leecher)
    C deve conseguir baixar o arquivo mesmo que B ainda não tenha todos os blocos,
    pois B deve servir os blocos que já recebeu de A.
    """
    src = _make_file(tmp_path / "file.bin", 10 * KB)
    original_hash = ChecksumUtil.sha256_file(src)

    port_a, port_b, port_c = _free_port(), _free_port(), _free_port()
    meta_path = str(tmp_path / "meta.json")
    out_b = tmp_path / "out_b"
    out_c = tmp_path / "out_c"
    out_b.mkdir(); out_c.mkdir()

    peer_a = Peer(_seeder_config(port_a, [], src, meta_path))
    peer_b = Peer(_leecher_config(port_b, [port_a], meta_path, out_b))
    peer_c = Peer(_leecher_config(port_c, [port_b], meta_path="", output_dir=out_c))

    try:
        peer_a.start()
        time.sleep(0.1)
        peer_b.start()
        time.sleep(0.1)
        peer_c.start()

        assert peer_b.wait_until_done(timeout=30)
        assert peer_c.wait_until_done(timeout=30)

        for out_dir, name in [(out_b, "out_b"), (out_c, "out_c")]:
            restored = out_dir / "file.bin"
            assert restored.exists(), f"{name}: arquivo não encontrado"
            assert ChecksumUtil.sha256_file(str(restored)) == original_hash, f"{name}: hash incorreto"
    finally:
        peer_a.stop()
        peer_b.stop()
        peer_c.stop()


# ---------------------------------------------------------------------------
# 4 Peers — topologia linear A → B → C → D
# ---------------------------------------------------------------------------

def test_4peers_linear_topology(tmp_path):
    src = _make_file(tmp_path / "file.bin", 10 * KB)
    original_hash = ChecksumUtil.sha256_file(src)

    ports = [_free_port() for _ in range(4)]
    pA, pB, pC, pD = ports
    meta_path = str(tmp_path / "meta.json")

    out_dirs = {}
    for name in ["B", "C", "D"]:
        d = tmp_path / f"out_{name}"
        d.mkdir()
        out_dirs[name] = d

    peer_a = Peer(_seeder_config(pA, [pB], src, meta_path))
    peer_b = Peer(_leecher_config(pB, [pA, pC], meta_path, out_dirs["B"]))
    peer_c = Peer(_leecher_config(pC, [pB, pD], meta_path="", output_dir=out_dirs["C"]))
    peer_d = Peer(_leecher_config(pD, [pC], meta_path="", output_dir=out_dirs["D"]))

    peers = [peer_a, peer_b, peer_c, peer_d]
    try:
        for p in peers:
            p.start()
            time.sleep(0.1)

        for name, peer in [("B", peer_b), ("C", peer_c), ("D", peer_d)]:
            assert peer.wait_until_done(timeout=60), f"Peer {name} não completou"
            restored = out_dirs[name] / "file.bin"
            assert restored.exists()
            assert ChecksumUtil.sha256_file(str(restored)) == original_hash
    finally:
        for p in peers:
            p.stop()
