"""Record size budgets shared by the CDC listener, config and Kafka producer.

Kept free of any Kafka client import so the CDC layer can size records without
loading ``confluent_kafka``.
"""

from typing import Final

from fdb_kafka_bridge.serialization import serialize_version_end

# The serializer's own input bounds; worst-case varints give the largest record.
from fdb_kafka_bridge.serialization.model import (
    _MAX_BRIDGE_TIMESTAMP_NS,
    _MAX_TOTAL_MUTATIONS,
    _MAX_VERSION,
)

# librdkafka's default message.max.bytes.
DEFAULT_MESSAGE_MAX_BYTES: Final[int] = 1_000_000
# RD_KAFKAP_MESSAGE_V2_MAX_OVERHEAD: produce() rejects a message with
# MSG_SIZE_TOO_LARGE when overhead + key + value + headers > message.max.bytes.
KAFKA_MESSAGE_OVERHEAD_BYTES: Final[int] = 36


def max_record_bytes_for_key(
    key: bytes, message_max_bytes: int = DEFAULT_MESSAGE_MAX_BYTES
) -> int:
    """Return the largest record value produce() accepts alongside key.

    Mirrors librdkafka's client-side size check for a header-less message.

    Args:
        key: Kafka message key the record will be produced with.
        message_max_bytes: Effective producer ``message.max.bytes``.

    Returns:
        ``message_max_bytes - len(key) - KAFKA_MESSAGE_OVERHEAD_BYTES``; less
        than 1 when no non-empty value fits.
    """
    return message_max_bytes - len(key) - KAFKA_MESSAGE_OVERHEAD_BYTES


def min_record_cap(stream_name: str) -> int:
    """Return the smallest ``max_record_bytes`` a stream can make progress under.

    Every reply that advances the watermark emits a ``VersionEnd`` carrying the
    stream name, and a too-large one fails identically on every replay. The
    result is the size of the largest ``VersionEnd`` the serializer can emit
    for ``stream_name``, so any cap at or above it always fits one.

    Args:
        stream_name: Name written into every record's envelope.

    Returns:
        Worst-case serialized ``VersionEnd`` size in bytes.

    Raises:
        InputValueError: If ``stream_name`` is empty or not UTF-8-encodable.
    """
    return len(
        serialize_version_end(
            fdb_version=_MAX_VERSION,
            total_mutations=_MAX_TOTAL_MUTATIONS,
            stream_name=stream_name,
            bridge_timestamp_ns=_MAX_BRIDGE_TIMESTAMP_NS,
        )
    )
