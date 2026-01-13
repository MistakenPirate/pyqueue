"""Broker configuration."""

from dataclasses import dataclass


@dataclass
class BrokerConfig:
    """Configuration for the PyQueue broker."""

    host: str = "127.0.0.1"
    port: int = 5555
    data_dir: str = "./data"
