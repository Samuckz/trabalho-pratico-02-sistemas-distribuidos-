import json
from dataclasses import dataclass, field
from typing import List, Tuple


@dataclass
class PeerConfig:
    host: str
    port: int
    neighbors: List[Tuple[str, int]]
    block_size: int = 1024
    role: str = "leecher"          # "seeder" | "leecher"
    file_path: str = ""            # seeder: arquivo fonte
    metadata_path: str = ""
    output_dir: str = ""           # leecher: diretório de saída

    @staticmethod
    def from_json(path: str) -> "PeerConfig":
        with open(path, "r") as f:
            data = json.load(f)
        neighbors = [(n["host"], n["port"]) for n in data.get("neighbors", [])]
        return PeerConfig(
            host=data["host"],
            port=data["port"],
            neighbors=neighbors,
            block_size=data.get("block_size", 1024),
            role=data.get("role", "leecher"),
            file_path=data.get("file_path", ""),
            metadata_path=data.get("metadata_path", ""),
            output_dir=data.get("output_dir", ""),
        )

    def to_json(self, path: str) -> None:
        data = {
            "host": self.host,
            "port": self.port,
            "neighbors": [{"host": h, "port": p} for h, p in self.neighbors],
            "block_size": self.block_size,
            "role": self.role,
            "file_path": self.file_path,
            "metadata_path": self.metadata_path,
            "output_dir": self.output_dir,
        }
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
