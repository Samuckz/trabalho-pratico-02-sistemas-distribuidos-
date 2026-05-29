import threading
from typing import List, Optional


class BlockRegistry:
    def __init__(self, total_blocks: int):
        self._total = total_blocks
        self._owned = [False] * total_blocks
        self._lock = threading.Lock()

    def has_block(self, index: int) -> bool:
        return self._owned[index]

    def mark_owned(self, index: int) -> None:
        with self._lock:
            self._owned[index] = True

    def is_complete(self) -> bool:
        with self._lock:
            return all(self._owned)

    def count_owned(self) -> int:
        with self._lock:
            return sum(self._owned)

    def missing_blocks(self) -> List[int]:
        with self._lock:
            return [i for i, owned in enumerate(self._owned) if not owned]

    def owned_blocks(self) -> List[int]:
        with self._lock:
            return [i for i, owned in enumerate(self._owned) if owned]
