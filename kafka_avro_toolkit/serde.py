from __future__ import annotations
import io, json, struct
from typing import Any, Protocol
import fastavro

MAGIC = 0  # Confluent wire format: magic byte + 4-byte schema id + payload

class Serde(Protocol):
    def encode(self, value: Any) -> bytes: ...
    def decode(self, data: bytes) -> Any: ...

class JsonSerde:
    def encode(self, value: Any) -> bytes:
        return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
    def decode(self, data: bytes) -> Any:
        return json.loads(data)

class AvroSerde:
    """Avro encode/decode using the Confluent wire format.

    `schema_id` comes from the registry (see TopicManager); decoding validates the magic
    byte and schema id so a message written with a different schema fails loudly.
    """
    def __init__(self, schema: dict, schema_id: int):
        self.schema = fastavro.parse_schema(schema)
        self.schema_id = schema_id

    def encode(self, value: dict) -> bytes:
        fastavro.validate(value, self.schema, raise_errors=True)
        buf = io.BytesIO()
        buf.write(struct.pack(">bI", MAGIC, self.schema_id))
        fastavro.schemaless_writer(buf, self.schema, value)
        return buf.getvalue()

    def decode(self, data: bytes) -> dict:
        if len(data) < 5:
            raise ValueError("payload too short for Avro wire format")
        magic, sid = struct.unpack(">bI", data[:5])
        if magic != MAGIC:
            raise ValueError(f"unexpected magic byte {magic}")
        if sid != self.schema_id:
            raise ValueError(f"schema id {sid} does not match expected {self.schema_id}")
        return fastavro.schemaless_reader(io.BytesIO(data[5:]), self.schema)
