import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    api_key: str = field(default_factory=lambda: os.getenv("LASTFM_API_KEY", ""), repr=False)
    users: tuple = field(default_factory=lambda: tuple(dict.fromkeys(
        u.strip() for u in os.getenv("LASTFM_USERS", "").split(",") if u.strip()
    )))
    bootstrap: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"))
    topic: str = field(default_factory=lambda: os.getenv("KAFKA_TOPIC", "events.bronze.lastfm"))
    dlq: str = field(default_factory=lambda: os.getenv("KAFKA_DLQ_TOPIC", "events.bronze.lastfm.dlq"))
    demo_topic: str = field(default_factory=lambda: os.getenv("KAFKA_DEMO_TOPIC", "events.demo.lastfm"))
    output: Path = field(default_factory=lambda: Path(os.getenv("PRODUCER_OUTPUT_DIR", "data/producer")))
    interval: float = field(default_factory=lambda: float(os.getenv("LASTFM_MIN_INTERVAL", "0.4")))
