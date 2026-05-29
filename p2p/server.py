import logging
import os
import socket
import struct
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

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

logger = logging.getLogger(__name__)


class ClientHandler:
    def __init__(
        self,
        conn: socket.socket,
        addr,
        registry: BlockRegistry,
        metadata: Optional[FileMetadata],
        block_store: "BlockStore",
        peer_id: str,
    ):
        self._conn = conn
        self._addr = addr
        self._registry = registry
        self._metadata = metadata
        self._block_store = block_store
        self._peer_id = peer_id

    def handle(self) -> None:
        peer = f"{self._addr[0]}:{self._addr[1]}"
        logger.debug("[%s] connection accepted from %s", self._peer_id, peer)
        try:
            while True:
                try:
                    msg = recv_message(self._conn)
                except (ConnectionError, OSError):
                    break

                if msg.type == MessageType.HANDSHAKE:
                    logger.debug("[%s] handshake from %s", self._peer_id, peer)

                elif msg.type == MessageType.METADATA_REQUEST:
                    self._handle_metadata_request(peer)

                elif msg.type == MessageType.BLOCK_REQUEST:
                    index = decode_index(msg.payload)
                    self._handle_block_request(index, peer)

                else:
                    logger.warning(
                        "[%s] unexpected message type %s from %s",
                        self._peer_id, msg.type, peer,
                    )
        finally:
            self._conn.close()
            logger.debug("[%s] connection closed from %s", self._peer_id, peer)

    def _handle_metadata_request(self, peer: str) -> None:
        if self._metadata is None:
            logger.warning("[%s] metadata requested by %s but not available", self._peer_id, peer)
            return
        response = Message(
            type=MessageType.METADATA_RESPONSE,
            payload=self._metadata.to_bytes(),
        )
        send_message(self._conn, response)
        logger.debug("[%s] sent metadata to %s", self._peer_id, peer)

    def _handle_block_request(self, index: int, peer: str) -> None:
        if self._registry.has_block(index):
            data = self._block_store.read(index)
            response = Message(type=MessageType.BLOCK_RESPONSE, payload=data)
            send_message(self._conn, response)
            logger.info("[%s] sent block %d to %s", self._peer_id, index, peer)
        else:
            response = Message(type=MessageType.BLOCK_NOT_FOUND, payload=encode_index(index))
            send_message(self._conn, response)
            logger.debug("[%s] block %d not found, notified %s", self._peer_id, index, peer)


class BlockStore:
    """Persiste blocos em um único arquivo com acesso aleatório por seek.

    Slot layout: [4 bytes LE actual_length][data][zero-padding to slot_size].
    slot_size = 4 + max_block_size, determinado na primeira escrita.

    Um único arquivo evita o overhead de criar milhares de arquivos pequenos,
    que é crítico para transferências com muitos blocos (ex: 10MB / 1KB = 10240 blocos).
    """

    _META_FILE = "blockstore.meta"
    _DATA_FILE = "blockstore.dat"
    _LEN_PREFIX = 4  # bytes para armazenar o comprimento real do bloco

    def __init__(self, store_dir: str, max_block_size: int = 0):
        import json as _json
        self._dir = store_dir
        os.makedirs(store_dir, exist_ok=True)
        self._lock = threading.Lock()

        meta_path = os.path.join(store_dir, self._META_FILE)
        if os.path.exists(meta_path):
            with open(meta_path) as f:
                meta = _json.load(f)
            self._slot_size = meta["slot_size"]
        else:
            self._slot_size = (self._LEN_PREFIX + max_block_size) if max_block_size else 0

        self._data_path = os.path.join(store_dir, self._DATA_FILE)
        # File handle mantido aberto para evitar open/close por bloco
        self._fh = None

    def _open(self) -> None:
        """Abre (ou cria) o data file em modo r+b, mantendo o handle aberto."""
        if not os.path.exists(self._data_path):
            open(self._data_path, "wb").close()
        self._fh = open(self._data_path, "r+b")

    def _init_slot_size(self, data_len: int) -> None:
        import json as _json
        if self._slot_size == 0:
            self._slot_size = self._LEN_PREFIX + data_len
            meta_path = os.path.join(self._dir, self._META_FILE)
            with open(meta_path, "w") as f:
                _json.dump({"slot_size": self._slot_size}, f)

    def write(self, index: int, data: bytes) -> None:
        with self._lock:
            self._init_slot_size(len(data))
            slot = self._slot_size
            prefix = struct.pack("<I", len(data))
            payload = prefix + data + b"\x00" * (slot - self._LEN_PREFIX - len(data))
            offset = index * slot

            if self._fh is None:
                self._open()

            self._fh.seek(0, 2)
            if self._fh.tell() < offset + slot:
                self._fh.seek(offset + slot - 1)
                self._fh.write(b"\x00")
            self._fh.seek(offset)
            self._fh.write(payload)
            self._fh.flush()

    def read(self, index: int) -> bytes:
        with self._lock:
            if self._fh is None:
                self._open()
            offset = index * self._slot_size
            self._fh.seek(offset)
            raw = self._fh.read(self._slot_size)
            actual_len = struct.unpack("<I", raw[:self._LEN_PREFIX])[0]
            return raw[self._LEN_PREFIX: self._LEN_PREFIX + actual_len]

    def exists(self, index: int) -> bool:
        if self._slot_size == 0 or not os.path.exists(self._data_path):
            return False
        with self._lock:
            try:
                if self._fh is None:
                    self._open()
                self._fh.seek(0, 2)
                if self._fh.tell() < (index + 1) * self._slot_size:
                    return False
                self._fh.seek(index * self._slot_size)
                prefix = self._fh.read(self._LEN_PREFIX)
                return struct.unpack("<I", prefix)[0] > 0
            except OSError:
                return False

    def close(self) -> None:
        with self._lock:
            if self._fh:
                self._fh.close()
                self._fh = None


class PeerServer:
    def __init__(
        self,
        host: str,
        port: int,
        registry: BlockRegistry,
        block_store: BlockStore,
        metadata: Optional[FileMetadata] = None,
        max_workers: int = 16,
    ):
        self._host = host
        self._port = port
        self._registry = registry
        self._block_store = block_store
        self._metadata = metadata
        self._max_workers = max_workers
        self._peer_id = f"{host}:{port}"
        self._server_sock: Optional[socket.socket] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    def set_metadata(self, metadata: FileMetadata) -> None:
        self._metadata = metadata

    def start(self) -> None:
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self._host, self._port))
        self._server_sock.listen(64)
        self._server_sock.settimeout(1.0)
        self._running = True
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()
        logger.info("[%s] server listening on %s:%d", self._peer_id, self._host, self._port)

    def stop(self) -> None:
        self._running = False
        if self._server_sock:
            try:
                self._server_sock.close()
            except OSError:
                pass
        self._executor.shutdown(wait=False)
        if self._thread:
            self._thread.join(timeout=3.0)
        logger.info("[%s] server stopped", self._peer_id)

    def _accept_loop(self) -> None:
        while self._running:
            try:
                conn, addr = self._server_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            handler = ClientHandler(
                conn=conn,
                addr=addr,
                registry=self._registry,
                metadata=self._metadata,
                block_store=self._block_store,
                peer_id=self._peer_id,
            )
            self._executor.submit(handler.handle)
