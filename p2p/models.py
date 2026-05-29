import hashlib
import json
from dataclasses import dataclass, field
from typing import List


@dataclass
class Block:
    index: int
    data: bytes

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.data).hexdigest()


@dataclass
class FileMetadata:
    name: str
    total_size: int
    block_size: int
    total_blocks: int
    block_hashes: List[str]

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "total_size": self.total_size,
            "block_size": self.block_size,
            "total_blocks": self.total_blocks,
            "block_hashes": self.block_hashes,
        }

    def save(self, path: str) -> None:
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @staticmethod
    def load(path: str) -> "FileMetadata":
        with open(path, "r") as f:
            data = json.load(f)
        return FileMetadata(**data)

    def to_bytes(self) -> bytes:
        return json.dumps(self.to_dict()).encode("utf-8")

    @staticmethod
    def from_bytes(raw: bytes) -> "FileMetadata":
        data = json.loads(raw.decode("utf-8"))
        return FileMetadata(**data)
