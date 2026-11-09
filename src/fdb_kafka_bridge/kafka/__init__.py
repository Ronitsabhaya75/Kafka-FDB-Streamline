"""Kafka producer module for the FDB CDC bridge."""

from fdb_kafka_bridge.kafka.producer import (
    FatalCause,
    MutationProducer,
    is_fatal_kafka_error,
)

__all__ = ["FatalCause", "MutationProducer", "is_fatal_kafka_error"]
