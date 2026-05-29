"""
Testes E2E dos 9 cenários definidos na especificação (seção 4).

Cada teste:
  1. Gera um arquivo com conteúdo aleatório e calcula o SHA-256 original.
  2. Sobe os peers como subprocessos; aguarda cada porta estar disponível antes
     de subir o próximo (sem sleeps fixos).
  3. Aguarda o marcador .done que o peer escreve após montar e verificar o arquivo.
  4. Verifica SHA-256 e tamanho do arquivo remontado.

Topologias:
  2 Peers: Seeder(A) ↔ Leecher(B)
  4 Peers: Seeder(A) ↔ B ↔ C ↔ D  (linear)

Portas: efêmeras por teste (evita TIME_WAIT entre execuções no Windows).
"""

import logging
import math
import os
import socket
import time
from pathlib import Path

import pytest

from tests.e2e.helpers import (
    generate_random_file,
    make_peer_config,
    read_log_lines,
    sha256_file,
    wait_for_done_marker,
)

KB = 1024
MB = 1024 * 1024

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _fmt_bytes(n: int) -> str:
    if n >= MB:
        return f"{n // MB} MB"
    if n >= KB:
        return f"{n // KB} KB"
    return f"{n} B"


def _elapsed(start: float) -> str:
    return f"{time.time() - start:.2f}s"


def _free_ports(n: int):
    """Reserva N portas efêmeras livres e as retorna."""
    sockets = []
    ports = []
    for _ in range(n):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 0))
        ports.append(s.getsockname()[1])
        sockets.append(s)
    # Fecha depois de reservar todos (minimiza janela de race)
    for s in sockets:
        s.close()
    return ports


# ---------------------------------------------------------------------------
# Setup helpers
# ---------------------------------------------------------------------------

def _setup_2peers(tmp_path, block_size: int, file_size: int):
    total_blocks = math.ceil(file_size / block_size)
    logger.info(
        "ENTRADA | topologia=2peers  arquivo=%s  bloco=%s  total_blocos=%d",
        _fmt_bytes(file_size), _fmt_bytes(block_size), total_blocks,
    )

    t0 = time.time()
    pA, pB = _free_ports(2)
    logger.info("SETUP   | portas alocadas: seeder=%d  leecher=%d", pA, pB)

    original = generate_random_file(tmp_path / "original.bin", file_size)
    original_hash = sha256_file(original)
    logger.info(
        "SETUP   | arquivo gerado em %.2fs  sha256=%s...%s",
        time.time() - t0, original_hash[:8], original_hash[-8:],
    )

    output_dir = tmp_path / "peer_B"
    output_dir.mkdir()

    seeder_cfg = make_peer_config(
        tmp_path, port=pA,
        neighbors=[("127.0.0.1", pB)],
        block_size=block_size, role="seeder",
        file_path=str(original),
        metadata_path=str(tmp_path / "meta.json"),
    )
    leecher_cfg = make_peer_config(
        tmp_path, port=pB,
        neighbors=[("127.0.0.1", pA)],
        block_size=block_size, role="leecher",
        output_dir=str(output_dir),
        metadata_path=str(tmp_path / "meta.json"),
    )
    return original, original_hash, pA, pB, seeder_cfg, leecher_cfg, output_dir


def _setup_4peers(tmp_path, block_size: int, file_size: int):
    total_blocks = math.ceil(file_size / block_size)
    logger.info(
        "ENTRADA | topologia=4peers  arquivo=%s  bloco=%s  total_blocos=%d",
        _fmt_bytes(file_size), _fmt_bytes(block_size), total_blocks,
    )

    t0 = time.time()
    pA, pB, pC, pD = _free_ports(4)
    logger.info(
        "SETUP   | portas alocadas: seeder(A)=%d  B=%d  C=%d  D=%d",
        pA, pB, pC, pD,
    )

    original = generate_random_file(tmp_path / "original.bin", file_size)
    original_hash = sha256_file(original)
    logger.info(
        "SETUP   | arquivo gerado em %.2fs  sha256=%s...%s",
        time.time() - t0, original_hash[:8], original_hash[-8:],
    )

    output_dirs = {}
    leecher_cfgs = {}

    seeder_cfg = make_peer_config(
        tmp_path, port=pA,
        neighbors=[("127.0.0.1", pB)],
        block_size=block_size, role="seeder",
        file_path=str(original),
        metadata_path=str(tmp_path / "meta.json"),
    )

    topology = [
        ("B", pB, [pA, pC]),
        ("C", pC, [pB, pD]),
        ("D", pD, [pC]),
    ]
    for name, port, neighbors in topology:
        d = tmp_path / f"peer_{name}"
        d.mkdir()
        output_dirs[name] = d
        leecher_cfgs[name] = make_peer_config(
            tmp_path, port=port,
            neighbors=[("127.0.0.1", n) for n in neighbors],
            block_size=block_size, role="leecher",
            output_dir=str(d),
            metadata_path=str(tmp_path / "meta.json"),
        )
        logger.info(
            "SETUP   | peer %s (porta %d) vizinhos=%s",
            name, port, [str(n) for n in neighbors],
        )

    return original, original_hash, (pA, pB, pC, pD), seeder_cfg, leecher_cfgs, output_dirs


def _assert_done(output_dir, filename, original_hash, original_size, timeout, t_start: float = None):
    logger.info(
        "OPERAÇÃO | aguardando conclusão do download  timeout=%ds  dir=%s",
        timeout, output_dir,
    )
    t_wait = time.time()
    done = wait_for_done_marker(output_dir, timeout=timeout)
    elapsed_wait = time.time() - t_wait

    assert done, f"Leecher não completou em {timeout}s (sem .done em {output_dir})"
    logger.info("OPERAÇÃO | marcador .done recebido em %.2fs", elapsed_wait)

    restored = Path(output_dir) / filename
    assert restored.exists(), f"Arquivo remontado não encontrado: {restored}"

    actual_size = restored.stat().st_size
    actual_hash = sha256_file(restored)

    size_ok = actual_size == original_size
    hash_ok = actual_hash == original_hash

    logger.info(
        "RESULTADO | tamanho: esperado=%s  recebido=%s  ok=%s",
        _fmt_bytes(original_size), _fmt_bytes(actual_size), size_ok,
    )
    logger.info(
        "RESULTADO | sha256:  esperado=%s...%s  recebido=%s...%s  ok=%s",
        original_hash[:8], original_hash[-8:],
        actual_hash[:8], actual_hash[-8:],
        hash_ok,
    )

    if t_start is not None:
        logger.info("TEMPO    | execução total do teste: %s", _elapsed(t_start))

    assert size_ok, "Tamanho incorreto"
    assert hash_ok, "SHA-256 não confere"


# ---------------------------------------------------------------------------
# T1 — 2 Peers, 1 KB block, 10 KB file
# ---------------------------------------------------------------------------

@pytest.mark.e2e
def test_T1_2peers_1kb_block_10kb_file(tmp_path, cluster):
    logger.info("=" * 60)
    logger.info("TESTE    | T1 — 2 peers, bloco=1KB, arquivo=10KB")
    t0 = time.time()
    original, original_hash, pA, pB, seeder_cfg, leecher_cfg, out = _setup_2peers(
        tmp_path, block_size=KB, file_size=10 * KB
    )
    logger.info("OPERAÇÃO | iniciando seeder (porta %d)", pA)
    cluster.start(seeder_cfg, port=pA)
    logger.info("OPERAÇÃO | iniciando leecher (porta %d)", pB)
    cluster.start(leecher_cfg, port=pB)
    _assert_done(out, "original.bin", original_hash, 10 * KB, timeout=30, t_start=t0)


@pytest.mark.e2e
def test_T1_logs_record_block_source(tmp_path, cluster):
    """RF-10: logs do leecher devem indicar blocos recebidos."""
    logger.info("=" * 60)
    logger.info("TESTE    | T1-logs — verifica que logs registram blocos recebidos")
    t0 = time.time()
    original, _, pA, pB, seeder_cfg, leecher_cfg, out = _setup_2peers(
        tmp_path, block_size=KB, file_size=10 * KB
    )
    logger.info("OPERAÇÃO | iniciando seeder (porta %d) e leecher (porta %d)", pA, pB)
    cluster.start(seeder_cfg, port=pA)
    cluster.start(leecher_cfg, port=pB)

    logger.info("OPERAÇÃO | aguardando .done")
    assert wait_for_done_marker(out, timeout=30)
    log_lines = read_log_lines(out / "peer.log")
    received_entries = [l for l in log_lines if "received block" in l.lower()]
    logger.info(
        "RESULTADO | entradas 'received block' no log: %d  ok=%s",
        len(received_entries), bool(received_entries),
    )
    logger.info("TEMPO    | execução total do teste: %s", _elapsed(t0))
    assert received_entries, "Log não contém entradas de blocos recebidos"


# ---------------------------------------------------------------------------
# T2 — 2 Peers, 1 KB block, 1 MB file
# ---------------------------------------------------------------------------

@pytest.mark.e2e
def test_T2_2peers_1kb_block_1mb_file(tmp_path, cluster):
    logger.info("=" * 60)
    logger.info("TESTE    | T2 — 2 peers, bloco=1KB, arquivo=1MB")
    t0 = time.time()
    original, original_hash, pA, pB, seeder_cfg, leecher_cfg, out = _setup_2peers(
        tmp_path, block_size=KB, file_size=MB
    )
    logger.info("OPERAÇÃO | iniciando seeder (porta %d)", pA)
    cluster.start(seeder_cfg, port=pA)
    logger.info("OPERAÇÃO | iniciando leecher (porta %d)", pB)
    cluster.start(leecher_cfg, port=pB)
    _assert_done(out, "original.bin", original_hash, MB, timeout=90, t_start=t0)


# ---------------------------------------------------------------------------
# T3 — 2 Peers, 1 KB block, 10 MB file
# ---------------------------------------------------------------------------

@pytest.mark.e2e
def test_T3_2peers_1kb_block_10mb_file(tmp_path, cluster):
    logger.info("=" * 60)
    logger.info("TESTE    | T3 — 2 peers, bloco=1KB, arquivo=10MB")
    t0 = time.time()
    original, original_hash, pA, pB, seeder_cfg, leecher_cfg, out = _setup_2peers(
        tmp_path, block_size=KB, file_size=10 * MB
    )
    logger.info("OPERAÇÃO | iniciando seeder (porta %d)", pA)
    cluster.start(seeder_cfg, port=pA)
    logger.info("OPERAÇÃO | iniciando leecher (porta %d)", pB)
    cluster.start(leecher_cfg, port=pB)
    _assert_done(out, "original.bin", original_hash, 10 * MB, timeout=180, t_start=t0)


# ---------------------------------------------------------------------------
# T4 — 4 Peers, 1 KB block, 1 MB file
# ---------------------------------------------------------------------------

@pytest.mark.e2e
def test_T4_4peers_1kb_block_1mb_file(tmp_path, cluster):
    logger.info("=" * 60)
    logger.info("TESTE    | T4 — 4 peers linear (A→B→C→D), bloco=1KB, arquivo=1MB")
    t0 = time.time()
    original, original_hash, ports, seeder_cfg, leecher_cfgs, out_dirs = _setup_4peers(
        tmp_path, block_size=KB, file_size=MB
    )
    pA, pB, pC, pD = ports
    logger.info("OPERAÇÃO | iniciando seeder A (porta %d)", pA)
    cluster.start(seeder_cfg, port=pA)
    for name, port in [("B", pB), ("C", pC), ("D", pD)]:
        logger.info("OPERAÇÃO | iniciando leecher %s (porta %d)", name, port)
        cluster.start(leecher_cfgs[name], port=port)

    for name in ["B", "C", "D"]:
        logger.info("OPERAÇÃO | verificando peer %s", name)
        _assert_done(out_dirs[name], "original.bin", original_hash, MB, timeout=120)
    logger.info("TEMPO    | execução total do teste: %s", _elapsed(t0))


@pytest.mark.e2e
def test_T4_intermediate_peers_serve_blocks(tmp_path, cluster):
    """RF-05: D só consegue o arquivo porque C serviu blocos de B."""
    logger.info("=" * 60)
    logger.info("TESTE    | T4-intermediários — peer D não deve receber blocos direto do seeder A")
    t0 = time.time()
    original, original_hash, ports, seeder_cfg, leecher_cfgs, out_dirs = _setup_4peers(
        tmp_path, block_size=KB, file_size=MB
    )
    pA, pB, pC, pD = ports
    logger.info("ENTRADA  | seeder A na porta %d; D só enxerga C (porta %d)", pA, pD)
    for name, port in [("A (seeder)", pA), ("B", pB), ("C", pC), ("D", pD)]:
        logger.info("OPERAÇÃO | iniciando peer %s (porta %d)", name, port)
    cluster.start(seeder_cfg, port=pA)
    cluster.start(leecher_cfgs["B"], port=pB)
    cluster.start(leecher_cfgs["C"], port=pC)
    cluster.start(leecher_cfgs["D"], port=pD)

    logger.info("OPERAÇÃO | aguardando .done em peer D")
    assert wait_for_done_marker(out_dirs["D"], timeout=120)
    log_d = read_log_lines(out_dirs["D"] / "peer.log")
    direct_from_seeder = [
        l for l in log_d if str(pA) in l and "received block" in l.lower()
    ]
    has_direct_seeder = bool(direct_from_seeder)
    logger.info(
        "RESULTADO | blocos recebidos diretamente do seeder A por D: %d  esperado=0  ok=%s",
        len(direct_from_seeder), not has_direct_seeder,
    )
    logger.info("TEMPO    | execução total do teste: %s", _elapsed(t0))
    assert not has_direct_seeder, "Peer D não deveria receber blocos diretamente do seeder"


# ---------------------------------------------------------------------------
# T5 — 2 Peers, 4 KB block, 1 MB file
# ---------------------------------------------------------------------------

@pytest.mark.e2e
def test_T5_2peers_4kb_block_1mb_file(tmp_path, cluster):
    logger.info("=" * 60)
    logger.info("TESTE    | T5 — 2 peers, bloco=4KB, arquivo=1MB")
    t0 = time.time()
    original, original_hash, pA, pB, seeder_cfg, leecher_cfg, out = _setup_2peers(
        tmp_path, block_size=4 * KB, file_size=MB
    )
    logger.info("OPERAÇÃO | iniciando seeder (porta %d)", pA)
    cluster.start(seeder_cfg, port=pA)
    logger.info("OPERAÇÃO | iniciando leecher (porta %d)", pB)
    cluster.start(leecher_cfg, port=pB)
    _assert_done(out, "original.bin", original_hash, MB, timeout=60, t_start=t0)


@pytest.mark.e2e
def test_T5_block_count_is_256(tmp_path, cluster):
    """Com blocos de 4KB e arquivo de 1MB: exatamente 256 blocos."""
    logger.info("=" * 60)
    logger.info("TESTE    | T5-contagem — bloco=4KB, arquivo=1MB => esperado 256 blocos")
    logger.info("ENTRADA  | block_size=4096 B  file_size=1048576 B  blocos_esperados=256")
    t0 = time.time()
    original, _, pA, pB, seeder_cfg, leecher_cfg, out = _setup_2peers(
        tmp_path, block_size=4 * KB, file_size=MB
    )
    logger.info("OPERAÇÃO | iniciando peers (seeder=%d, leecher=%d)", pA, pB)
    cluster.start(seeder_cfg, port=pA)
    cluster.start(leecher_cfg, port=pB)

    logger.info("OPERAÇÃO | aguardando .done")
    assert wait_for_done_marker(out, timeout=60)
    log_lines = read_log_lines(out / "peer.log")
    received = [l for l in log_lines if "received block" in l.lower()]
    logger.info(
        "RESULTADO | blocos recebidos=%d  esperado=256  ok=%s",
        len(received), len(received) == 256,
    )
    logger.info("TEMPO    | execução total do teste: %s", _elapsed(t0))
    assert len(received) == 256, f"Esperado 256 blocos, got {len(received)}"


# ---------------------------------------------------------------------------
# T6 — 4 Peers, 4 KB block, 10 MB file
# ---------------------------------------------------------------------------

@pytest.mark.e2e
def test_T6_4peers_4kb_block_10mb_file(tmp_path, cluster):
    logger.info("=" * 60)
    logger.info("TESTE    | T6 — 4 peers linear, bloco=4KB, arquivo=10MB")
    t0 = time.time()
    original, original_hash, ports, seeder_cfg, leecher_cfgs, out_dirs = _setup_4peers(
        tmp_path, block_size=4 * KB, file_size=10 * MB
    )
    pA, pB, pC, pD = ports
    logger.info("OPERAÇÃO | iniciando seeder A (porta %d)", pA)
    cluster.start(seeder_cfg, port=pA)
    for name, port in [("B", pB), ("C", pC), ("D", pD)]:
        logger.info("OPERAÇÃO | iniciando leecher %s (porta %d)", name, port)
        cluster.start(leecher_cfgs[name], port=port)

    for name in ["B", "C", "D"]:
        logger.info("OPERAÇÃO | verificando peer %s", name)
        _assert_done(out_dirs[name], "original.bin", original_hash, 10 * MB, timeout=240)
    logger.info("TEMPO    | execução total do teste: %s", _elapsed(t0))


# ---------------------------------------------------------------------------
# T7 — 2 Peers, 1 KB block, 20 KB file
# ---------------------------------------------------------------------------

@pytest.mark.e2e
def test_T7_2peers_1kb_block_20kb_file(tmp_path, cluster):
    logger.info("=" * 60)
    logger.info("TESTE    | T7 — 2 peers, bloco=1KB, arquivo=20KB")
    t0 = time.time()
    original, original_hash, pA, pB, seeder_cfg, leecher_cfg, out = _setup_2peers(
        tmp_path, block_size=KB, file_size=20 * KB
    )
    logger.info("OPERAÇÃO | iniciando seeder (porta %d)", pA)
    cluster.start(seeder_cfg, port=pA)
    logger.info("OPERAÇÃO | iniciando leecher (porta %d)", pB)
    cluster.start(leecher_cfg, port=pB)
    _assert_done(out, "original.bin", original_hash, 20 * KB, timeout=30, t_start=t0)


# ---------------------------------------------------------------------------
# T8 — 4 Peers, 1 KB block, 5 MB file
# ---------------------------------------------------------------------------

@pytest.mark.e2e
def test_T8_4peers_1kb_block_5mb_file(tmp_path, cluster):
    logger.info("=" * 60)
    logger.info("TESTE    | T8 — 4 peers linear, bloco=1KB, arquivo=5MB")
    t0 = time.time()
    original, original_hash, ports, seeder_cfg, leecher_cfgs, out_dirs = _setup_4peers(
        tmp_path, block_size=KB, file_size=5 * MB
    )
    pA, pB, pC, pD = ports
    logger.info("OPERAÇÃO | iniciando seeder A (porta %d)", pA)
    cluster.start(seeder_cfg, port=pA)
    for name, port in [("B", pB), ("C", pC), ("D", pD)]:
        logger.info("OPERAÇÃO | iniciando leecher %s (porta %d)", name, port)
        cluster.start(leecher_cfgs[name], port=port)

    for name in ["B", "C", "D"]:
        logger.info("OPERAÇÃO | verificando peer %s", name)
        _assert_done(out_dirs[name], "original.bin", original_hash, 5 * MB, timeout=150)
    logger.info("TEMPO    | execução total do teste: %s", _elapsed(t0))


# ---------------------------------------------------------------------------
# T9 — 2 Peers, 4 KB block, 20 MB file
# ---------------------------------------------------------------------------

@pytest.mark.e2e
def test_T9_2peers_4kb_block_20mb_file(tmp_path, cluster):
    logger.info("=" * 60)
    logger.info("TESTE    | T9 — 2 peers, bloco=4KB, arquivo=20MB")
    t0 = time.time()
    original, original_hash, pA, pB, seeder_cfg, leecher_cfg, out = _setup_2peers(
        tmp_path, block_size=4 * KB, file_size=20 * MB
    )
    logger.info("OPERAÇÃO | iniciando seeder (porta %d)", pA)
    cluster.start(seeder_cfg, port=pA)
    logger.info("OPERAÇÃO | iniciando leecher (porta %d)", pB)
    cluster.start(leecher_cfg, port=pB)
    _assert_done(out, "original.bin", original_hash, 20 * MB, timeout=240, t_start=t0)


# ---------------------------------------------------------------------------
# Invariante: SHA-256 em múltiplas configurações (RNF-04)
# ---------------------------------------------------------------------------

@pytest.mark.e2e
@pytest.mark.parametrize("block_size,file_size,timeout", [
    (KB,     10 * KB,  30),
    (KB,      1 * MB,  90),
    (4 * KB,  1 * MB,  60),
])
def test_integrity_sha256_always_matches(tmp_path, cluster, block_size, file_size, timeout):
    """RNF-04: SHA-256 deve ser idêntico ao original em qualquer configuração."""
    logger.info("=" * 60)
    logger.info(
        "TESTE    | integridade SHA-256 — bloco=%s  arquivo=%s  timeout=%ds",
        _fmt_bytes(block_size), _fmt_bytes(file_size), timeout,
    )
    logger.info(
        "ENTRADA  | block_size=%d B  file_size=%d B  total_blocos=%d",
        block_size, file_size, math.ceil(file_size / block_size),
    )
    t0 = time.time()
    original, original_hash, pA, pB, seeder_cfg, leecher_cfg, out = _setup_2peers(
        tmp_path, block_size=block_size, file_size=file_size
    )
    logger.info("OPERAÇÃO | iniciando seeder (porta %d) e leecher (porta %d)", pA, pB)
    cluster.start(seeder_cfg, port=pA)
    cluster.start(leecher_cfg, port=pB)
    _assert_done(out, "original.bin", original_hash, file_size, timeout=timeout, t_start=t0)
