import subprocess
import sys
import time
from pathlib import Path
from typing import List

import pytest

from tests.e2e.helpers import wait_for_port


class PeerCluster:
    def __init__(self):
        self._processes: List[subprocess.Popen] = []

    def start(self, config_path: str, port: int, host: str = "127.0.0.1") -> subprocess.Popen:
        """Inicia o peer e aguarda a porta estar disponível antes de retornar."""
        p = subprocess.Popen(
            [sys.executable, "-m", "p2p.main", config_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self._processes.append(p)
        if not wait_for_port(host, port, timeout=60.0):
            p.kill()
            raise RuntimeError(
                f"Peer on port {port} did not start within 60s"
            )
        return p

    def teardown(self, timeout: float = 5.0) -> None:
        for p in self._processes:
            p.terminate()
            try:
                p.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                p.kill()
        self._processes.clear()


@pytest.fixture
def cluster():
    c = PeerCluster()
    yield c
    c.teardown()
