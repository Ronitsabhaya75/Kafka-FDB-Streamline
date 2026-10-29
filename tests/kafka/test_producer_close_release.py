"""Regression tests: close() must resolve queued messages and free the producer.

Uses real librdkafka against an unreachable bootstrap address, so messages stay
queued and close()'s flush times out. No broker needed.
"""

import gc
import weakref

from confluent_kafka import KafkaError, Message

from src.kafka.producer import MutationProducer
from tests.kafka.utils import build_set_mutation_record

UNREACHABLE = "127.0.0.1:1"
TOPIC = "close-release-test"
CLOSE_TIMEOUT = 0.5
PURGE_CODES = {KafkaError._PURGE_QUEUE, KafkaError._PURGE_INFLIGHT}


def _make_producer() -> MutationProducer:
    return MutationProducer(UNREACHABLE, {"log_level": 0})


def _produce_one(producer: MutationProducer, reports: list[KafkaError | None]) -> None:
    def on_delivery(err: KafkaError | None, _msg: Message) -> None:
        reports.append(err)

    producer.produce(
        TOPIC,
        build_set_mutation_record("s", 1, 0, b"k", b"v"),
        on_delivery=on_delivery,
    )


def _collected(ref: weakref.ref[MutationProducer]) -> bool:
    gc.collect()
    return ref() is None


def test_close_purges_queued_message_and_releases_producer() -> None:
    producer = _make_producer()
    reports: list[KafkaError | None] = []
    _produce_one(producer, reports)

    producer.close(timeout=CLOSE_TIMEOUT)

    assert len(reports) == 1
    assert reports[0] is not None
    assert reports[0].code() in PURGE_CODES
    assert producer._producer is None
    # Queued messages hold the delivery closure, which holds the wrapper; the
    # cycle runs through librdkafka, so gc cannot break it unless close() does.
    ref = weakref.ref(producer)
    del producer
    assert _collected(ref)


def test_close_with_nothing_queued_releases_without_reports() -> None:
    producer = _make_producer()
    reports: list[KafkaError | None] = []

    producer.close(timeout=CLOSE_TIMEOUT)

    assert reports == []
    assert producer._producer is None
    ref = weakref.ref(producer)
    del producer
    assert _collected(ref)
