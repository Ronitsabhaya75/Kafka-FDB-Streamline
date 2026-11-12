"""Kafka producer module for the FDB CDC bridge."""

from fdb_kafka_bridge.kafka.producer import (
    MutationProducer,
    is_fatal_kafka_error,
    is_message_too_large_error,
)
from fdb_kafka_bridge.record_size import (
    DEFAULT_MESSAGE_MAX_BYTES,
    KAFKA_MESSAGE_OVERHEAD_BYTES,
    max_record_bytes_for_key,
)

__all__ = [
    "DEFAULT_MESSAGE_MAX_BYTES",
    "KAFKA_MESSAGE_OVERHEAD_BYTES",
    "MutationProducer",
    "is_fatal_kafka_error",
    "is_message_too_large_error",
    "max_record_bytes_for_key",
]
