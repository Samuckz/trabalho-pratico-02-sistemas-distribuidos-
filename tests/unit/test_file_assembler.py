import hashlib
import os

import pytest

from p2p.models import Block
from p2p.transfer import (
    ChecksumUtil,
    FileAssembler,
    FileFragmenter,
    IncompleteBlocksError,
    IntegrityError,
)


def _make_file(path, size: int) -> str:
    data = os.urandom(size)
    full = str(path)
    with open(full, "wb") as f:
        f.write(data)
    return full


def _fragment_and_reassemble(tmp_path, size: int, block_size: int):
    original = _make_file(tmp_path / "original.bin", size)
    metadata, blocks = FileFragmenter.fragment(original, block_size=block_size)
    output = str(tmp_path / "restored.bin")
    FileAssembler.assemble(blocks, metadata, output)
    return original, output


def test_assemble_10kb_file(tmp_path):
    original, restored = _fragment_and_reassemble(tmp_path, 10 * 1024, 1024)
    assert ChecksumUtil.sha256_file(restored) == ChecksumUtil.sha256_file(original)


def test_assemble_preserves_file_size(tmp_path):
    original, restored = _fragment_and_reassemble(tmp_path, 10 * 1024, 1024)
    assert os.path.getsize(restored) == os.path.getsize(original)


def test_assemble_with_4kb_blocks(tmp_path):
    original, restored = _fragment_and_reassemble(tmp_path, 1024 * 1024, 4096)
    assert ChecksumUtil.sha256_file(restored) == ChecksumUtil.sha256_file(original)


def test_assemble_accepts_out_of_order_blocks(tmp_path):
    original = _make_file(tmp_path / "original.bin", 3 * 1024)
    metadata, blocks = FileFragmenter.fragment(original, block_size=1024)
    shuffled = [blocks[2], blocks[0], blocks[1]]  # wrong order
    output = str(tmp_path / "restored.bin")
    FileAssembler.assemble(shuffled, metadata, output)
    assert ChecksumUtil.sha256_file(output) == ChecksumUtil.sha256_file(original)


def test_assemble_raises_on_missing_blocks(tmp_path):
    original = _make_file(tmp_path / "original.bin", 3 * 1024)
    metadata, blocks = FileFragmenter.fragment(original, block_size=1024)
    with pytest.raises(IncompleteBlocksError):
        FileAssembler.assemble(blocks[:2], metadata, str(tmp_path / "out.bin"))


def test_assemble_raises_on_corrupted_block(tmp_path):
    original = _make_file(tmp_path / "original.bin", 2 * 1024)
    metadata, blocks = FileFragmenter.fragment(original, block_size=1024)
    blocks[0] = Block(index=0, data=b"x" * 1024)  # bad data, wrong hash
    with pytest.raises(IntegrityError):
        FileAssembler.assemble(blocks, metadata, str(tmp_path / "out.bin"))


def test_assemble_raises_on_wrong_block_index(tmp_path):
    original = _make_file(tmp_path / "original.bin", 2 * 1024)
    metadata, blocks = FileFragmenter.fragment(original, block_size=1024)
    # swap indices — data is correct but reported under wrong index
    blocks[0] = Block(index=0, data=blocks[1].data)
    with pytest.raises(IntegrityError):
        FileAssembler.assemble(blocks, metadata, str(tmp_path / "out.bin"))


def test_assemble_file_not_multiple_of_block_size(tmp_path):
    original, restored = _fragment_and_reassemble(tmp_path, 1025, 1024)
    assert ChecksumUtil.sha256_file(restored) == ChecksumUtil.sha256_file(original)
    assert os.path.getsize(restored) == 1025


def test_checksum_util_sha256_file(tmp_path):
    data = os.urandom(4096)
    path = str(tmp_path / "file.bin")
    with open(path, "wb") as f:
        f.write(data)
    expected = hashlib.sha256(data).hexdigest()
    assert ChecksumUtil.sha256_file(path) == expected


def test_checksum_util_sha256_bytes():
    data = b"hello world"
    expected = hashlib.sha256(data).hexdigest()
    assert ChecksumUtil.sha256_bytes(data) == expected
