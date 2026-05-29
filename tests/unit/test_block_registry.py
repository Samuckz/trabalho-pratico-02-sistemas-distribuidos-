import threading

import pytest

from p2p.block_registry import BlockRegistry


def test_registry_starts_empty():
    registry = BlockRegistry(10)
    for i in range(10):
        assert registry.has_block(i) is False
    assert registry.count_owned() == 0


def test_registry_is_not_complete_when_empty():
    registry = BlockRegistry(5)
    assert registry.is_complete() is False


def test_mark_owned_single_block():
    registry = BlockRegistry(5)
    registry.mark_owned(2)
    assert registry.has_block(2) is True
    assert registry.has_block(0) is False
    assert registry.has_block(4) is False
    assert registry.count_owned() == 1


def test_is_complete_only_when_all_blocks_present():
    registry = BlockRegistry(3)
    registry.mark_owned(0)
    registry.mark_owned(1)
    assert registry.is_complete() is False
    registry.mark_owned(2)
    assert registry.is_complete() is True


def test_missing_blocks_returns_unowned_indices():
    registry = BlockRegistry(4)
    registry.mark_owned(1)
    registry.mark_owned(3)
    assert registry.missing_blocks() == [0, 2]


def test_owned_blocks_returns_owned_indices():
    registry = BlockRegistry(4)
    registry.mark_owned(0)
    registry.mark_owned(2)
    assert registry.owned_blocks() == [0, 2]


def test_mark_owned_idempotent():
    registry = BlockRegistry(3)
    registry.mark_owned(1)
    registry.mark_owned(1)
    assert registry.count_owned() == 1


def test_missing_blocks_empty_when_complete():
    registry = BlockRegistry(3)
    for i in range(3):
        registry.mark_owned(i)
    assert registry.missing_blocks() == []


def test_single_block_registry():
    registry = BlockRegistry(1)
    assert registry.is_complete() is False
    registry.mark_owned(0)
    assert registry.is_complete() is True


def test_thread_safe_concurrent_writes():
    """100 threads marcando blocos distintos não devem causar race conditions."""
    total = 100
    registry = BlockRegistry(total)
    threads = [threading.Thread(target=registry.mark_owned, args=(i,)) for i in range(total)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert registry.is_complete() is True
    assert registry.count_owned() == total


def test_thread_safe_concurrent_reads_and_writes():
    """Leituras e escritas simultâneas não devem bloquear indefinidamente."""
    registry = BlockRegistry(50)
    results = []

    def writer(i):
        registry.mark_owned(i)

    def reader():
        results.append(registry.count_owned())

    threads = []
    for i in range(50):
        threads.append(threading.Thread(target=writer, args=(i,)))
        threads.append(threading.Thread(target=reader))

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert registry.count_owned() == 50
