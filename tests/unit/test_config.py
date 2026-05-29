import json
import os

import pytest

from p2p.config import PeerConfig


def _write_config(path, data: dict) -> str:
    full = str(path)
    with open(full, "w") as f:
        json.dump(data, f)
    return full


def test_load_seeder_config(tmp_path):
    cfg_data = {
        "host": "127.0.0.1",
        "port": 9001,
        "neighbors": [{"host": "127.0.0.1", "port": 9002}],
        "block_size": 1024,
        "role": "seeder",
        "file_path": "/tmp/file.bin",
        "metadata_path": "/tmp/meta.json",
    }
    path = _write_config(tmp_path / "seeder.json", cfg_data)
    cfg = PeerConfig.from_json(path)
    assert cfg.host == "127.0.0.1"
    assert cfg.port == 9001
    assert cfg.neighbors == [("127.0.0.1", 9002)]
    assert cfg.block_size == 1024
    assert cfg.role == "seeder"


def test_load_leecher_config_with_multiple_neighbors(tmp_path):
    cfg_data = {
        "host": "127.0.0.1",
        "port": 9003,
        "neighbors": [
            {"host": "127.0.0.1", "port": 9001},
            {"host": "127.0.0.1", "port": 9002},
        ],
        "block_size": 4096,
        "role": "leecher",
        "file_path": "",
        "metadata_path": "",
    }
    path = _write_config(tmp_path / "leecher.json", cfg_data)
    cfg = PeerConfig.from_json(path)
    assert len(cfg.neighbors) == 2
    assert cfg.block_size == 4096
    assert cfg.role == "leecher"


def test_default_block_size_when_omitted(tmp_path):
    cfg_data = {"host": "127.0.0.1", "port": 9001, "neighbors": []}
    path = _write_config(tmp_path / "cfg.json", cfg_data)
    cfg = PeerConfig.from_json(path)
    assert cfg.block_size == 1024


def test_save_and_reload_config(tmp_path):
    cfg = PeerConfig(
        host="127.0.0.1",
        port=9005,
        neighbors=[("127.0.0.1", 9006)],
        block_size=4096,
        role="seeder",
        file_path="/tmp/f.bin",
        metadata_path="/tmp/m.json",
    )
    path = str(tmp_path / "cfg.json")
    cfg.to_json(path)
    loaded = PeerConfig.from_json(path)
    assert loaded.host == cfg.host
    assert loaded.port == cfg.port
    assert loaded.neighbors == cfg.neighbors
    assert loaded.block_size == cfg.block_size
    assert loaded.role == cfg.role
