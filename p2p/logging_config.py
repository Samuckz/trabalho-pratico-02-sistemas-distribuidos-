import logging
import os
import sys


def setup_logging(peer_id: str, log_dir: str = "") -> None:
    fmt = f"%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    handlers = [logging.StreamHandler(sys.stdout)]

    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
        handlers.append(logging.FileHandler(os.path.join(log_dir, "peer.log"), encoding="utf-8"))

    logging.basicConfig(level=logging.INFO, format=fmt, handlers=handlers)
