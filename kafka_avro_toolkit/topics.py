from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int = 1
    replication: int = 1
    schema: dict | None = None  # None -> topic without registered schema

def plan_topics(configs: Iterable[Mapping], schema_dir: Path, default_schema: str = "default.avsc") -> list[TopicSpec]:
    """Turn declarative topic config into specs. Handles four cases:

    1. schema_file given and found      -> register that schema
    2. schema_file explicitly None      -> no schema
    3. schema_file given but missing    -> fall back to default schema
    4. schema_file key omitted          -> no schema
    """
    specs = []
    for cfg in configs:
        schema = None
        if cfg.get("schema_file") is not None:
            path = schema_dir / cfg["schema_file"]
            if not path.exists():
                path = schema_dir / default_schema
            schema = json.loads(path.read_text())
        specs.append(TopicSpec(cfg["name"], int(cfg.get("partitions", 1)), int(cfg.get("replication", 1)), schema))
    return specs

class TopicManager:
    """Creates topics and registers schemas. Admin client and HTTP session are injectable."""
    def __init__(self, admin, http, registry_url: str):
        self.admin, self.http, self.registry_url = admin, http, registry_url.rstrip("/")

    def register_schema(self, topic: str, schema: dict) -> int:
        r = self.http.post(f"{self.registry_url}/subjects/{topic}-value/versions",
                           json={"schema": json.dumps(schema)}, timeout=10)
        r.raise_for_status()
        return r.json()["id"]

    def apply(self, specs: list[TopicSpec]) -> dict[str, int | None]:
        from confluent_kafka.admin import NewTopic  # lazy: tests need no broker
        existing = set(self.admin.list_topics(timeout=10).topics)
        new = [NewTopic(s.name, s.partitions, s.replication) for s in specs if s.name not in existing]
        for fut in (self.admin.create_topics(new).values() if new else []):
            fut.result()
        return {s.name: (self.register_schema(s.name, s.schema) if s.schema else None) for s in specs}
