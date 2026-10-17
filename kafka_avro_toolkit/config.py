from __future__ import annotations
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class KafkaSettings:
    """Connection settings. Explicit values win, then env vars, then local defaults.

    The library never ships credentials or endpoints: each service supplies them at runtime.
    """
    bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    client_id: str = "kafka-avro-toolkit"

    @classmethod
    def resolve(cls, **overrides: str | None) -> "KafkaSettings":
        def pick(name: str, env: str, default: str) -> str:
            return overrides.get(name) or os.environ.get(env) or default
        return cls(
            bootstrap_servers=pick("bootstrap_servers", "KAFKA_BOOTSTRAP_SERVERS", cls.bootstrap_servers),
            schema_registry_url=pick("schema_registry_url", "SCHEMA_REGISTRY_URL", cls.schema_registry_url),
            client_id=pick("client_id", "KAFKA_CLIENT_ID", cls.client_id),
        )
