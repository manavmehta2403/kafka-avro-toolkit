"""Small helpers for Kafka producers/consumers with optional Avro."""
from .config import KafkaSettings
from .serde import AvroSerde, JsonSerde
from .topics import TopicSpec, plan_topics
from .producer import Producer
from .consumer import Consumer
__all__ = ["KafkaSettings", "AvroSerde", "JsonSerde", "TopicSpec", "plan_topics", "Producer", "Consumer"]
