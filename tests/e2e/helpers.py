import hashlib
import json
import os
import socket
import time
from pathlib import Path
from typing import List, Tuple


def generate_random_file(path, size_bytes: int) -> Path:
    p = Path(path)
    p.write_bytes(os.urandom(size_bytes))
    return p


def sha256_file(path) -> str:
    h = hashlib.sha256()
    with open(str(path), "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def wait_for_port(host: str, port: int, timeout: float = 15.0, poll: float = 0.2) -> bool:
    """Aguarda até a porta estar aceitando conexões TCP."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except (ConnectionRefusedError, OSError):
            time.sleep(poll)
    return False


def wait_for_done_marker(output_dir, timeout: float = 60.0, poll: float = 0.3) -> bool:
    """Aguarda o arquivo .done que o peer escreve após montar o arquivo."""
    marker = Path(output_dir) / ".done"
    deadline = time.time() + timeout
    while time.time() < deadline:
        if marker.exists():
            return True
        time.sleep(poll)
    return False


def wait_for_file(path, timeout: float = 60.0, poll: float = 0.5) -> bool:
    """Aguarda até o arquivo existir e ter tamanho estável."""
    deadline = time.time() + timeout
    last_size = -1
    stable_count = 0
    while time.time() < deadline:
        p = Path(path)
        if p.exists():
            size = p.stat().st_size
            if size > 0 and size == last_size:
                stable_count += 1
                if stable_count >= 2:
                    return True
            else:
                stable_count = 0
            last_size = size
        time.sleep(poll)
    return False


def read_log_lines(log_path) -> List[str]:
    try:
        with open(str(log_path), "r", encoding="utf-8", errors="replace") as f:
            return f.readlines()
    except FileNotFoundError:
        return []


def make_peer_config(
    tmp_path,
    port: int,
    neighbors: List[Tuple[str, int]],
    block_size: int,
    role: str,
    file_path: str = "",
    metadata_path: str = "",
    output_dir: str = "",
) -> str:
    cfg = {
        "host": "127.0.0.1",
        "port": port,
        "neighbors": [{"host": h, "port": p} for h, p in neighbors],
        "block_size": block_size,
        "role": role,
        "file_path": file_path,
        "metadata_path": metadata_path,
        "output_dir": output_dir,
    }
    config_path = str(Path(tmp_path) / f"peer_{port}.json")
    with open(config_path, "w") as f:
        json.dump(cfg, f, indent=2)
    return config_path
