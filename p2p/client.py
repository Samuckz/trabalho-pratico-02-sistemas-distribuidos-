import logging
import socket
import threading
from typing import Callable, List, Optional, Tuple

from p2p.block_registry import BlockRegistry
from p2p.models import Block, FileMetadata
from p2p.protocol import (
    Message,
    MessageType,
    decode_index,
    encode_index,
    recv_message,
    send_message,
)
from p2p.server import BlockStore
from p2p.transfer import ChecksumUtil

logger = logging.getLogger(__name__)


class BlockIntegrityError(Exception):
    pass


class PeerClient:
    """Conecta a um único vizinho e baixa blocos ausentes."""

    def __init__(
        self,
        neighbor: Tuple[str, int],
        registry: BlockRegistry,
        block_store: BlockStore,
        metadata: Optional[FileMetadata],
        peer_id: str,
        on_block_received: Optional[Callable[[int], None]] = None,
        timeout: float = 30.0,
    ):
        self._neighbor = neighbor
        self._registry = registry
        self._block_store = block_store
        self._metadata = metadata
        self._peer_id = peer_id
        self._on_block_received = on_block_received
        self._timeout = timeout
        self._neighbor_id = f"{neighbor[0]}:{neighbor[1]}"

    def fetch_metadata(self) -> Optional[FileMetadata]:
        """Solicita e retorna o FileMetadata do vizinho."""
        try:
            sock = self._connect()
            if sock is None:
                return None
            with sock:
                send_message(sock, Message(type=MessageType.METADATA_REQUEST))
                response = recv_message(sock)
                if response.type == MessageType.METADATA_RESPONSE:
                    meta = FileMetadata.from_bytes(response.payload)
                    logger.info(
                        "[%s] received metadata from %s (%d blocks)",
                        self._peer_id, self._neighbor_id, meta.total_blocks,
                    )
                    return meta
        except (ConnectionError, OSError, Exception) as e:
            logger.warning("[%s] failed to fetch metadata from %s: %s", self._peer_id, self._neighbor_id, e)
        return None

    def download_missing_blocks(self) -> int:
        """Tenta baixar todos os blocos ausentes deste vizinho. Retorna número de blocos recebidos."""
        if self._metadata is None:
            return 0

        received = 0
        try:
            sock = self._connect()
            if sock is None:
                return 0
            with sock:
                send_message(sock, Message(type=MessageType.HANDSHAKE, payload=self._peer_id.encode()))
                for index in self._registry.missing_blocks():
                    if self._registry.has_block(index):
                        continue
                    got = self._request_block(sock, index)
                    if got:
                        received += 1
        except (ConnectionError, OSError) as e:
            logger.warning("[%s] connection to %s lost: %s", self._peer_id, self._neighbor_id, e)
        return received

    def _request_block(self, sock: socket.socket, index: int) -> bool:
        send_message(sock, Message(type=MessageType.BLOCK_REQUEST, payload=encode_index(index)))
        response = recv_message(sock)

        if response.type == MessageType.BLOCK_NOT_FOUND:
            logger.debug("[%s] block %d not found at %s", self._peer_id, index, self._neighbor_id)
            return False

        if response.type != MessageType.BLOCK_RESPONSE:
            logger.warning("[%s] unexpected response type %s for block %d", self._peer_id, response.type, index)
            return False

        data = response.payload
        expected_hash = self._metadata.block_hashes[index]
        actual_hash = ChecksumUtil.sha256_bytes(data)
        if actual_hash != expected_hash:
            raise BlockIntegrityError(
                f"Block {index} from {self._neighbor_id}: "
                f"expected hash {expected_hash}, got {actual_hash}"
            )

        self._block_store.write(index, data)
        self._registry.mark_owned(index)
        logger.info("[%s] received block %d from %s", self._peer_id, index, self._neighbor_id)

        if self._on_block_received:
            self._on_block_received(index)
        return True

    def _connect(self) -> Optional[socket.socket]:
        try:
            host, port = self._neighbor
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self._timeout)
            sock.connect((host, port))
            return sock
        except (ConnectionRefusedError, OSError) as e:
            logger.warning("[%s] cannot connect to %s: %s", self._peer_id, self._neighbor_id, e)
            return None


class DownloadManager:
    """Gerencia o download paralelo de blocos a partir de múltiplos vizinhos."""

    def __init__(
        self,
        neighbors: List[Tuple[str, int]],
        registry: BlockRegistry,
        block_store: BlockStore,
        peer_id: str,
        on_complete: Optional[Callable[[], None]] = None,
        retry_interval: float = 1.0,
        max_retries: int = 30,
    ):
        self._neighbors = neighbors
        self._registry = registry
        self._block_store = block_store
        self._peer_id = peer_id
        self._on_complete = on_complete
        self._retry_interval = retry_interval
        self._max_retries = max_retries
        self._metadata: Optional[FileMetadata] = None
        self._metadata_lock = threading.Lock()
        self._stop_event = threading.Event()

    def set_metadata(self, metadata: FileMetadata) -> None:
        with self._metadata_lock:
            self._metadata = metadata

    def get_metadata(self) -> Optional[FileMetadata]:
        with self._metadata_lock:
            return self._metadata

    def _make_client(self, neighbor: Tuple[str, int]) -> PeerClient:
        return PeerClient(
            neighbor=neighbor,
            registry=self._registry,
            block_store=self._block_store,
            metadata=self._metadata,
            peer_id=self._peer_id,
        )

    def _fetch_metadata_from_neighbors(self) -> Optional[FileMetadata]:
        for neighbor in self._neighbors:
            client = PeerClient(
                neighbor=neighbor,
                registry=self._registry,
                block_store=self._block_store,
                metadata=None,
                peer_id=self._peer_id,
            )
            meta = client.fetch_metadata()
            if meta is not None:
                return meta
        return None

    def run(self) -> bool:
        """Executa o download bloqueante. Retorna True se completo, False se abortado."""
        # Fase 1: obter metadados se não temos ainda
        if self._metadata is None:
            for attempt in range(self._max_retries):
                if self._stop_event.is_set():
                    return False
                meta = self._fetch_metadata_from_neighbors()
                if meta:
                    self.set_metadata(meta)
                    break
                logger.info("[%s] metadata not yet available, retrying... (%d)", self._peer_id, attempt + 1)
                self._stop_event.wait(self._retry_interval)
            else:
                logger.error("[%s] failed to obtain metadata after %d retries", self._peer_id, self._max_retries)
                return False

        # Fase 2: baixar blocos em threads paralelas (uma por vizinho)
        for attempt in range(self._max_retries):
            if self._stop_event.is_set():
                return False
            if self._registry.is_complete():
                break

            threads = []
            for neighbor in self._neighbors:
                client = PeerClient(
                    neighbor=neighbor,
                    registry=self._registry,
                    block_store=self._block_store,
                    metadata=self._metadata,
                    peer_id=self._peer_id,
                )
                t = threading.Thread(target=client.download_missing_blocks, daemon=True)
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

            if self._registry.is_complete():
                break

            remaining = len(self._registry.missing_blocks())
            logger.info("[%s] %d blocks remaining, retrying round %d", self._peer_id, remaining, attempt + 1)
            self._stop_event.wait(self._retry_interval)

        if self._registry.is_complete():
            logger.info("[%s] download complete (%d blocks)", self._peer_id, self._metadata.total_blocks)
            if self._on_complete:
                self._on_complete()
            return True

        logger.error("[%s] download incomplete after %d rounds", self._peer_id, self._max_retries)
        return False

    def stop(self) -> None:
        self._stop_event.set()
