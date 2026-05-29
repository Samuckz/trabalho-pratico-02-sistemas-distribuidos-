"""
Ponto de entrada do peer.

Uso:
    python -m p2p.main <config.json>
"""

import signal
import sys
import threading

from p2p.config import PeerConfig
from p2p.logging_config import setup_logging
from p2p.peer import Peer


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python -m p2p.main <config.json>", file=sys.stderr)
        sys.exit(1)

    config = PeerConfig.from_json(sys.argv[1])
    output_dir = config.output_dir
    peer_id = f"{config.host}:{config.port}"
    setup_logging(peer_id, log_dir=output_dir)

    peer = Peer(config=config, output_dir=output_dir)

    stop_evt = threading.Event()

    def _shutdown(sig, frame):
        peer.stop()
        stop_evt.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    peer.start()

    if config.role == "seeder":
        # Seeder fica vivo indefinidamente até sinal de término
        try:
            stop_evt.wait()
        except KeyboardInterrupt:
            pass
    else:
        # Leecher aguarda download; depois continua vivo servindo blocos aos demais
        peer.wait_until_done(timeout=3600)
        try:
            stop_evt.wait()
        except KeyboardInterrupt:
            pass

    peer.stop()


if __name__ == "__main__":
    main()
