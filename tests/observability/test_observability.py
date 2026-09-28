"""Unit tests for observability metrics, health HTTP, and Kafka produce hooks."""

from __future__ import annotations

import urllib.error
import urllib.request
from collections import namedtuple
from unittest.mock import MagicMock

import pytest

from src.cdc.client import FDBClient
from src.cdc.listener import FDBMutationListener
from src.kafka import MutationProducer
from src.observability import (
    REGISTRY,
    configure_logging,
    heartbeat,
    is_live,
    is_ready,
    set_fdb_ready,
    set_kafka_ready,
    start_http_server,
    stop_http_server,
)
from src.observability.health import reset_for_tests
from src.serialization.errors import InputTypeError

MockCdcMutation = namedtuple("MockCdcMutation", ["type", "param1", "param2"])
MockVersionGroup = namedtuple("MockVersionGroup", ["version", "mutations"])
MockConsumeResult = namedtuple(
    "MockConsumeResult", ["mutations", "last_consumed_version"]
)


def _metric_value(name: str, labels: dict[str, str] | None = None) -> float:
    """Return the current sample value for a metric in the observability registry."""
    labels = labels or {}
    for family in REGISTRY.collect():
        if family.name != name and not name.startswith(family.name):
            # Counters expose *_total as family name already.
            pass
        for sample in family.samples:
            if sample.name != name:
                continue
            if all(sample.labels.get(k) == v for k, v in labels.items()):
                return float(sample.value)
    return 0.0


@pytest.fixture(autouse=True)
def _reset_health() -> None:
    reset_for_tests()
    stop_http_server()
    yield
    stop_http_server()
    reset_for_tests()


def test_healthz_and_readyz_and_metrics_endpoints() -> None:
    configure_logging()
    heartbeat()
    set_fdb_ready(True)
    set_kafka_ready(True)
    server = start_http_server(host="127.0.0.1", port=0)
    host, port = server.server_address[:2]
    base = f"http://{host}:{port}"

    with urllib.request.urlopen(f"{base}/healthz") as resp:
        assert resp.status == 200
    with urllib.request.urlopen(f"{base}/readyz") as resp:
        assert resp.status == 200
    with urllib.request.urlopen(f"{base}/metrics") as resp:
        body = resp.read().decode()
        assert "fdb_mutations_polled_total" in body
        assert "kafka_records_published_total" in body

    set_kafka_ready(False)
    with pytest.raises(urllib.error.HTTPError) as err:
        urllib.request.urlopen(f"{base}/readyz")
    assert err.value.code == 503


def test_poll_records_increments_fdb_and_serializer_metrics() -> None:
    before = _metric_value("fdb_mutations_polled_total", {"opcode": "SET_VALUE"})
    before_bytes = _metric_value("fdb_serializer_bytes_out_total")

    client = MagicMock(spec=FDBClient)
    consumer = MagicMock()
    client.create_cdc_consumer.return_value = consumer
    client.get_read_version.return_value = 1100
    batch = MockConsumeResult(
        [MockVersionGroup(1000, [MockCdcMutation(0, b"k", b"v")])],
        1000,
    )
    consumer.consume.return_value.wait.return_value = batch
    listener = FDBMutationListener(client, "metrics-stream")

    records = listener.poll_records(bridge_timestamp_ns=1)
    assert records
    assert (
        _metric_value("fdb_mutations_polled_total", {"opcode": "SET_VALUE"})
        == before + 1
    )
    assert _metric_value("fdb_serializer_bytes_out_total") > before_bytes
    assert _metric_value("fdb_cdc_latest_read_version") == 1000.0
    assert _metric_value("fdb_cdc_lag_versions") == 100.0
    assert is_live()


def test_serializer_error_increments_error_counter() -> None:
    before = _metric_value("fdb_serializer_errors_total")
    client = MagicMock(spec=FDBClient)
    consumer = MagicMock()
    client.create_cdc_consumer.return_value = consumer
    consumer.consume.return_value.wait.return_value = MockConsumeResult(
        [MockVersionGroup(1000, [MockCdcMutation(0, "not-bytes", b"v")])],
        1000,
    )
    listener = FDBMutationListener(client, "err-stream")

    with pytest.raises(InputTypeError):
        listener.poll_records(bridge_timestamp_ns=1)
    assert _metric_value("fdb_serializer_errors_total") == before + 1


def test_mutation_producer_records_kafka_metrics() -> None:
    before_pub = _metric_value("kafka_records_published_total")
    before_ve = _metric_value("kafka_version_end_markers_total")

    client = MagicMock(spec=FDBClient)
    consumer = MagicMock()
    client.create_cdc_consumer.return_value = consumer
    client.get_read_version.return_value = 1000
    consumer.consume.return_value.wait.return_value = MockConsumeResult(
        [MockVersionGroup(1000, [MockCdcMutation(0, b"k", b"v")])],
        1000,
    )
    listener = FDBMutationListener(client, "kafka-stream")
    records = listener.poll_records(bridge_timestamp_ns=1)

    producer = MutationProducer(topic="fdb-cdc", partition=0)
    assert is_ready() is False  # FDB not marked ready in this unit test
    set_fdb_ready(True)
    assert is_ready() is True

    published = producer.produce(records)
    assert published == len(records)
    assert _metric_value("kafka_records_published_total") == before_pub + published
    # batch + version_end → one VersionEnd marker
    assert _metric_value("kafka_version_end_markers_total") == before_ve + 1

    producer.close()
    assert is_ready() is False
