"""Pin max_record_bytes_for_key against librdkafka's client-side size check.

librdkafka rejects oversize messages inside ``produce()``, before any network
I/O, so these tests need no broker.
"""

from collections.abc import Iterator

import pytest
from confluent_kafka import KafkaError, KafkaException

from fdb_kafka_bridge.kafka import (
    MutationProducer,
    is_message_too_large_error,
    max_record_bytes_for_key,
)
from fdbkafka.cdc.v1 import mutations_pb2
from tests.kafka.utils import build_set_mutation_record

_KEYS = pytest.mark.parametrize(
    "key", [b"orders", b"k" * 10_000], ids=["short-key", "10k-key"]
)


@pytest.fixture
def producer() -> Iterator[MutationProducer]:
    p = MutationProducer("127.0.0.1:1", {"log_level": 0})
    yield p
    p.close(timeout=0.1)


def _record_of_size(size: int) -> mutations_pb2.FDBMutationRecord:
    """Pad a SET record's value until the serialized record is exactly size."""
    rec = build_set_mutation_record("orders", 1, 0, b"k", b"")
    n = size - rec.ByteSize()
    # Varint length prefixes grow with the value, so converge iteratively. A size
    # inside a prefix-length gap is unreachable and would oscillate forever.
    for _ in range(8):
        rec.mutation.single_key_mutation.value = b"x" * n
        if rec.ByteSize() == size:
            return rec
        n += size - rec.ByteSize()
    raise ValueError(f"no SET record serializes to exactly {size} bytes")


@_KEYS
def test_record_at_derived_budget_is_accepted(
    producer: MutationProducer, key: bytes
) -> None:
    producer.produce(
        "size-test", _record_of_size(max_record_bytes_for_key(key)), key=key
    )


@_KEYS
def test_one_byte_over_derived_budget_is_rejected(
    producer: MutationProducer, key: bytes
) -> None:
    record = _record_of_size(max_record_bytes_for_key(key) + 1)
    with pytest.raises(KafkaException) as info:
        producer.produce("size-test", record, key=key)
    assert info.value.args[0].code() == KafkaError.MSG_SIZE_TOO_LARGE
    assert is_message_too_large_error(info.value)


def test_record_of_size_rejects_unreachable_size() -> None:
    # 152 falls in a varint-length gap: no value length serializes to it.
    with pytest.raises(ValueError, match="152"):
        _record_of_size(152)
