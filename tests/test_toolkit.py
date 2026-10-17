import json
from pathlib import Path
import pytest
from kafka_avro_toolkit import AvroSerde, JsonSerde, KafkaSettings, plan_topics, Producer, Consumer

SCHEMAS = Path(__file__).parent.parent / "schemas"
BOOKING = json.loads((SCHEMAS / "booking.avsc").read_text())

def test_avro_roundtrip_wire_format():
    s = AvroSerde(BOOKING, schema_id=7)
    data = s.encode({"id": "b1", "amount_cents": 1999, "currency": "EUR"})
    assert data[0] == 0 and int.from_bytes(data[1:5], "big") == 7
    assert s.decode(data)["amount_cents"] == 1999

def test_avro_rejects_wrong_schema_id():
    data = AvroSerde(BOOKING, 7).encode({"id": "b1", "amount_cents": 1, "currency": "EUR"})
    with pytest.raises(ValueError):
        AvroSerde(BOOKING, 8).decode(data)

def test_avro_rejects_invalid_record():
    with pytest.raises(Exception):
        AvroSerde(BOOKING, 1).encode({"id": "b1"})

def test_plan_topics_four_cases():
    specs = {s.name: s for s in plan_topics([
        {"name": "a", "schema_file": "booking.avsc"},
        {"name": "b", "schema_file": None},
        {"name": "c", "schema_file": "missing.avsc"},
        {"name": "d"},
    ], SCHEMAS)}
    assert specs["a"].schema["name"] == "Booking"
    assert specs["b"].schema is None
    assert specs["c"].schema["name"] == "Event"
    assert specs["d"].schema is None

def test_settings_precedence(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "env:9092")
    assert KafkaSettings.resolve().bootstrap_servers == "env:9092"
    assert KafkaSettings.resolve(bootstrap_servers="x:1").bootstrap_servers == "x:1"

class FakeMsg:
    def __init__(self, v, topic="t"): self._v, self._t = v, topic
    def value(self): return self._v
    def topic(self): return self._t
    def error(self): return None

class FakeClient:
    def __init__(self, msgs=()): self.msgs, self.sent, self.commits = list(msgs), [], 0
    def produce(self, topic, value, key, headers, on_delivery):
        self.sent.append((topic, value)); on_delivery("broker down" if topic == "bad" else None, FakeMsg(value, topic))
    def poll(self, t): return self.msgs.pop(0) if self.msgs else None
    def flush(self, t): return 0
    def commit(self, message, asynchronous): self.commits += 1

def test_producer_records_delivery_failures():
    p = Producer(FakeClient(), JsonSerde())
    p.send("ok", {"a": 1}); p.send("bad", {"a": 2})
    assert p.failures == [("bad", "broker down")]

def test_consumer_dead_letters_poison_message_and_commits():
    good, dlq = [], []
    c = Consumer(FakeClient([FakeMsg(b'{"a":1}'), FakeMsg(b"not json")]), JsonSerde(), good.append, lambda v, e: dlq.append(v))
    assert c.poll_once() and c.poll_once() and not c.poll_once()
    assert good == [{"a": 1}] and dlq == [b"not json"] and c.client.commits == 2
