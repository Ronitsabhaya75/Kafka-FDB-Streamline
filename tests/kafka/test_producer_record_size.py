"""A maximum-size CDC record must be accepted by produce().

The listener caps each serialized record value at ``DEFAULT_MAX_RECORD_BYTES``;
the producer adds the stream name as the key.  Real librdkafka checks
``message.max.bytes`` locally at produce time, so no broker is needed.  It
rejects a message when ``len(key) + len(value)`` plus a fixed per-message
overhead exceeds ``message.max.bytes`` (default 1_000_000).
"""

from confluent_kafka import KafkaException

from fdb_kafka_bridge.cdc.listener import DEFAULT_MAX_RECORD_BYTES
from fdb_kafka_bridge.kafka.producer import MutationProducer
from fdbkafka.cdc.v1 import mutations_pb2
from tests.kafka.utils import build_set_mutation_record

UNREACHABLE = "127.0.0.1:1"
STREAM = "orders"


def _record_of_size(size: int) -> mutations_pb2.FDBMutationRecord:
    record = build_set_mutation_record(STREAM, 1, 0, b"k", b"")
    overhead = record.ByteSize()
    # Length-prefix varints grow with the payload; converge on the exact size.
    value_len = size - overhead
    while True:
        record.mutation.single_key_mutation.value = b"x" * value_len
        delta = size - record.ByteSize()
        if delta == 0:
            return record
        value_len += delta


# Pins INV-KAFKA-14.
def test_max_size_record_with_stream_key_is_accepted() -> None:
    record = _record_of_size(DEFAULT_MAX_RECORD_BYTES)
    assert len(record.SerializeToString()) == DEFAULT_MAX_RECORD_BYTES
    producer = MutationProducer(UNREACHABLE, {"log_level": 0})
    try:
        producer.produce("record-size-test", record, key=STREAM.encode())
    except KafkaException as exc:
        raise AssertionError(f"produce rejected max-size record: {exc}") from exc
    finally:
        producer.close(timeout=0.5)
