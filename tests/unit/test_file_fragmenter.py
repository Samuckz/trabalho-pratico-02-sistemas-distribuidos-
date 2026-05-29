import hashlib
import os

import pytest

from p2p.transfer import FileFragmenter


def _make_file(path, size: int) -> str:
    data = os.urandom(size)
    full = str(path)
    with open(full, "wb") as f:
        f.write(data)
    return full


def test_fragment_10kb_into_1kb_blocks(tmp_path):
    path = _make_file(tmp_path / "file.bin", 10 * 1024)
    metadata, blocks = FileFragmenter.fragment(path, block_size=1024)
    assert len(blocks) == 10
    assert all(len(b.data) == 1024 for b in blocks)
    assert metadata.total_blocks == 10
    assert metadata.block_size == 1024
    assert metadata.total_size == 10 * 1024


def test_fragment_produces_sequential_indices(tmp_path):
    path = _make_file(tmp_path / "file.bin", 3 * 1024)
    _, blocks = FileFragmenter.fragment(path, block_size=1024)
    assert [b.index for b in blocks] == [0, 1, 2]


def test_last_block_smaller_when_file_not_multiple(tmp_path):
    size = 1024 + 1  # 1 byte overflow
    path = _make_file(tmp_path / "file.bin", size)
    metadata, blocks = FileFragmenter.fragment(path, block_size=1024)
    assert len(blocks) == 2
    assert len(blocks[0].data) == 1024
    assert len(blocks[1].data) == 1
    assert metadata.total_blocks == 2
    assert metadata.total_size == size


def test_metadata_name_matches_filename(tmp_path):
    path = _make_file(tmp_path / "myfile.bin", 512)
    metadata, _ = FileFragmenter.fragment(path, block_size=1024)
    assert metadata.name == "myfile.bin"


def test_metadata_block_hashes_correct(tmp_path):
    path = _make_file(tmp_path / "file.bin", 3 * 1024)
    metadata, blocks = FileFragmenter.fragment(path, block_size=1024)
    for block in blocks:
        expected = hashlib.sha256(block.data).hexdigest()
        assert metadata.block_hashes[block.index] == expected


def test_metadata_has_correct_number_of_hashes(tmp_path):
    path = _make_file(tmp_path / "file.bin", 5 * 1024)
    metadata, blocks = FileFragmenter.fragment(path, block_size=1024)
    assert len(metadata.block_hashes) == len(blocks)


def test_fragment_with_4kb_block_size(tmp_path):
    path = _make_file(tmp_path / "file.bin", 1024 * 1024)  # 1 MB
    metadata, blocks = FileFragmenter.fragment(path, block_size=4096)
    assert len(blocks) == 256  # 1MB / 4KB
    assert metadata.total_blocks == 256
    assert all(len(b.data) == 4096 for b in blocks)


def test_fragment_tiny_file_smaller_than_block(tmp_path):
    path = _make_file(tmp_path / "tiny.bin", 100)
    metadata, blocks = FileFragmenter.fragment(path, block_size=1024)
    assert len(blocks) == 1
    assert len(blocks[0].data) == 100
    assert metadata.total_blocks == 1


def test_metadata_serialization_round_trip(tmp_path):
    path = _make_file(tmp_path / "file.bin", 2 * 1024)
    metadata, _ = FileFragmenter.fragment(path, block_size=1024)
    json_path = str(tmp_path / "meta.json")
    metadata.save(json_path)

    from p2p.models import FileMetadata
    loaded = FileMetadata.load(json_path)
    assert loaded.name == metadata.name
    assert loaded.total_size == metadata.total_size
    assert loaded.total_blocks == metadata.total_blocks
    assert loaded.block_hashes == metadata.block_hashes
