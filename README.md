# kafka-avro-toolkit

A small Python toolkit that gives every service the same, boring Kafka plumbing: settings resolution, Avro (Confluent wire format) or JSON serialisation, declarative topic + schema setup, a producer that surfaces delivery failures, and an at-least-once consumer with dead-lettering.

Clean-room rewrite of a pattern I've used across microservices. No credentials or endpoints live in the library — each service supplies them at runtime.

## Why
When every service hand-rolls its producer, you get N slightly different retry, serialisation and error behaviours. A shared toolkit makes them identical and testable.

## Features
- `KafkaSettings.resolve()` — explicit args > env vars > local defaults
- `AvroSerde` — validates records, writes magic byte + schema id, refuses messages with a mismatched schema id
- `plan_topics()` — declarative topics; handles custom schema / no schema / missing-file fallback / omitted key
- `Producer` — delivery callbacks recorded, `flush()` raises if messages are left undelivered
- `Consumer` — commits offsets only after handling; poison messages go to a dead-letter callback instead of blocking the partition

## Quick start
```bash
docker compose up -d
pip install -r requirements.txt
pytest -q   # tests use fakes; no broker needed
```
```python
from confluent_kafka import Producer as CK
from kafka_avro_toolkit import KafkaSettings, JsonSerde, Producer
s = KafkaSettings.resolve()
p = Producer(CK({"bootstrap.servers": s.bootstrap_servers}), JsonSerde())
p.send("events", {"id": "1"}); p.flush()
```
MIT licensed.
