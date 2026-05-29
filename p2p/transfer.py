import hashlib
import math
import os
from typing import List, Tuple

from p2p.models import Block, FileMetadata


class IntegrityError(Exception):
    pass


class IncompleteBlocksError(Exception):
    pass


class FileFragmenter:
    @staticmethod
    def fragment(file_path: str, block_size: int) -> Tuple[FileMetadata, List[Block]]:
        with open(file_path, "rb") as f:
            data = f.read()

        total_size = len(data)
        total_blocks = math.ceil(total_size / block_size)
        blocks = []
        hashes = []

        for i in range(total_blocks):
            chunk = data[i * block_size: (i + 1) * block_size]
            block = Block(index=i, data=chunk)
            blocks.append(block)
            hashes.append(block.sha256)

        metadata = FileMetadata(
            name=os.path.basename(file_path),
            total_size=total_size,
            block_size=block_size,
            total_blocks=total_blocks,
            block_hashes=hashes,
        )
        return metadata, blocks


class FileAssembler:
    @staticmethod
    def assemble(blocks: List[Block], metadata: FileMetadata, output_path: str) -> None:
        if len(blocks) != metadata.total_blocks:
            raise IncompleteBlocksError(
                f"Expected {metadata.total_blocks} blocks, got {len(blocks)}"
            )

        sorted_blocks = sorted(blocks, key=lambda b: b.index)

        for block in sorted_blocks:
            expected_hash = metadata.block_hashes[block.index]
            if block.sha256 != expected_hash:
                raise IntegrityError(
                    f"Block {block.index} hash mismatch: "
                    f"expected {expected_hash}, got {block.sha256}"
                )

        with open(output_path, "wb") as f:
            for block in sorted_blocks:
                f.write(block.data)


class ChecksumUtil:
    @staticmethod
    def sha256_file(path: str) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()

    @staticmethod
    def sha256_bytes(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()
