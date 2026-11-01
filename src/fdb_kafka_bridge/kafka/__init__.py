"""Kafka producer module for the FDB CDC bridge."""

from fdb_kafka_bridge.kafka.producer import MutationProducer, is_fatal_kafka_error

__all__ = ["MutationProducer", "is_fatal_kafka_error"]
