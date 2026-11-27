"""One-shot refresh or scheduled polling. One worker processes one snapshot at a time."""
import argparse
import logging
import os
import signal
from threading import Event

from dotenv import load_dotenv

from .catalog import Settings
from .job import refresh
from .parquet import GoldStore


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("run", "watch"))
    parser.add_argument("--interval", type=int, default=int(os.getenv("GOLD_INTERVAL_SECONDS", "900")))
    args = parser.parse_args()
    if args.interval < 1:
        parser.error("interval must be positive")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    store = GoldStore(Settings.from_env())
    if args.command == "run":
        logging.info("%s", refresh(store))
        return
    stop = Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    while not stop.is_set():
        try:
            logging.info("%s", refresh(store))
        except Exception:
            logging.exception("Gold refresh failed; retry in %d seconds", args.interval)
        stop.wait(args.interval)


if __name__ == "__main__":
    main()
