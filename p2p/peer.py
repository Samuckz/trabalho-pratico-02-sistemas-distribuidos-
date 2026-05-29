import logging
import os
import threading
from typing import Optional

from p2p.block_registry import BlockRegistry
from p2p.client import DownloadManager
from p2p.config import PeerConfig
from p2p.models import FileMetadata
from p2p.server import BlockStore, PeerServer
from p2p.transfer import ChecksumUtil, FileAssembler, FileFragmenter

logger = logging.getLogger(__name__)

_DONE_MARKER = ".done"


class Peer:
    def __init__(self, config: PeerConfig, output_dir: str = ""):
        self._config = config
        self._output_dir = output_dir or config.output_dir or os.getcwd()
        self._peer_id = f"{config.host}:{config.port}"

        # Seeder: block store junto ao arquivo fonte para isolamento entre testes/runs.
        # Leecher: junto ao output_dir onde o arquivo final será montado.
        if config.role == "seeder" and config.file_path:
            store_dir = os.path.join(
                os.path.dirname(os.path.abspath(config.file_path)), ".blocks"
            )
        else:
            store_dir = os.path.join(self._output_dir, ".blocks")
        self._block_store = BlockStore(store_dir)
        self._registry: Optional[BlockRegistry] = None
        self._metadata: Optional[FileMetadata] = None
        self._server: Optional[PeerServer] = None
        self._download_manager: Optional[DownloadManager] = None
        self._done_event = threading.Event()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        if self._config.role == "seeder":
            self._init_as_seeder()
        else:
            self._init_as_leecher()

    def wait_until_done(self, timeout: Optional[float] = None) -> bool:
        """Bloqueia até o download terminar (leecher) ou até timeout. Seeder retorna True imediatamente."""
        if self._config.role == "seeder":
            return True
        return self._done_event.wait(timeout=timeout)

    def stop(self) -> None:
        if self._download_manager:
            self._download_manager.stop()
        if self._server:
            self._server.stop()

    @property
    def metadata(self) -> Optional[FileMetadata]:
        return self._metadata

    @property
    def registry(self) -> Optional[BlockRegistry]:
        return self._registry

    # ------------------------------------------------------------------
    # Seeder init
    # ------------------------------------------------------------------

    def _init_as_seeder(self) -> None:
        file_path = self._config.file_path
        if not file_path or not os.path.exists(file_path):
            raise FileNotFoundError(f"Seeder file not found: {file_path!r}")

        meta_path = self._config.metadata_path
        if meta_path and os.path.exists(meta_path):
            self._metadata = FileMetadata.load(meta_path)
            logger.info("[%s] loaded existing metadata from %s", self._peer_id, meta_path)
            total_blocks = self._metadata.total_blocks
            self._registry = BlockRegistry(total_blocks)
            self._load_seeder_blocks(file_path, self._metadata)
        else:
            logger.info("[%s] fragmenting %s (block_size=%d)", self._peer_id, file_path, self._config.block_size)
            self._metadata, blocks = FileFragmenter.fragment(file_path, self._config.block_size)
            self._registry = BlockRegistry(self._metadata.total_blocks)
            for block in blocks:
                self._block_store.write(block.index, block.data)
                self._registry.mark_owned(block.index)
            if meta_path:
                self._metadata.save(meta_path)
                logger.info("[%s] metadata saved to %s", self._peer_id, meta_path)

        self._start_server()
        logger.info("[%s] seeder ready — %d blocks available", self._peer_id, self._registry.count_owned())

    def _load_seeder_blocks(self, file_path: str, metadata: FileMetadata) -> None:
        """Popula o BlockStore a partir do arquivo fonte se os blocos ainda não existem."""
        needs_load = any(not self._block_store.exists(i) for i in range(metadata.total_blocks))
        if not needs_load:
            for i in range(metadata.total_blocks):
                self._registry.mark_owned(i)
            return

        with open(file_path, "rb") as f:
            for i in range(metadata.total_blocks):
                chunk = f.read(metadata.block_size)
                self._block_store.write(i, chunk)
                self._registry.mark_owned(i)

    # ------------------------------------------------------------------
    # Leecher init
    # ------------------------------------------------------------------

    def _init_as_leecher(self) -> None:
        meta_path = self._config.metadata_path

        # Se já temos metadata salvo localmente, carregamos; senão, DownloadManager buscará
        if meta_path and os.path.exists(meta_path):
            self._metadata = FileMetadata.load(meta_path)
            self._registry = BlockRegistry(self._metadata.total_blocks)
            logger.info("[%s] loaded local metadata (%d blocks)", self._peer_id, self._metadata.total_blocks)
        else:
            # Usamos um registry provisório de tamanho 0; DownloadManager substituirá após buscar metadata
            self._registry = BlockRegistry(0)

        self._start_server()

        self._download_manager = DownloadManager(
            neighbors=self._config.neighbors,
            registry=self._registry,
            block_store=self._block_store,
            peer_id=self._peer_id,
            on_complete=self._on_download_complete,
            retry_interval=1.0,
            max_retries=60,
        )
        if self._metadata:
            self._download_manager.set_metadata(self._metadata)

        t = threading.Thread(target=self._run_download, daemon=True, name=f"download-{self._peer_id}")
        t.start()

    def _run_download(self) -> None:
        try:
            # Se não temos metadata ainda, buscamos primeiro para poder recriar o registry com tamanho correto
            if self._metadata is None:
                meta = self._download_manager._fetch_metadata_from_neighbors()
                if meta is None:
                    logger.error("[%s] could not fetch metadata from any neighbor", self._peer_id)
                    self._done_event.set()
                    return
                self._metadata = meta
                self._registry = BlockRegistry(meta.total_blocks)
                self._download_manager._registry = self._registry
                self._download_manager.set_metadata(meta)
                self._server._registry = self._registry
                self._server.set_metadata(meta)

                meta_path = self._config.metadata_path
                if meta_path:
                    meta.save(meta_path)

            self._download_manager.run()
        except Exception:
            logger.exception("[%s] unexpected error in download thread", self._peer_id)
            self._done_event.set()

    def _on_download_complete(self) -> None:
        logger.info("[%s] all blocks received, assembling file...", self._peer_id)
        os.makedirs(self._output_dir, exist_ok=True)
        output_path = os.path.join(self._output_dir, self._metadata.name)

        blocks_data = []
        from p2p.models import Block
        for i in range(self._metadata.total_blocks):
            data = self._block_store.read(i)
            blocks_data.append(Block(index=i, data=data))

        FileAssembler.assemble(blocks_data, self._metadata, output_path)
        checksum = ChecksumUtil.sha256_file(output_path)
        logger.info("[%s] file assembled: %s (sha256=%s)", self._peer_id, output_path, checksum)

        # Escreve marcador de conclusão
        with open(os.path.join(self._output_dir, _DONE_MARKER), "w") as f:
            f.write(checksum)

        self._done_event.set()

    # ------------------------------------------------------------------
    # Server startup (shared by seeder and leecher)
    # ------------------------------------------------------------------

    def _start_server(self) -> None:
        self._server = PeerServer(
            host=self._config.host,
            port=self._config.port,
            registry=self._registry,
            block_store=self._block_store,
            metadata=self._metadata,
        )
        self._server.start()
