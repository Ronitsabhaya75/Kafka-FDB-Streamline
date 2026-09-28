"""Unit tests for observability metrics, health HTTP, and Kafka produce hooks.

Contract tests lock the Prometheus names from ``docs/OBSERVABILITY-SPEC.md`` so
a rename or deletion fails CI on every push/PR (see ``.github/workflows/tests.yml``).
"""

from __future__ import annotations

import json
import time
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
    audit_version_end,
    configure_health,
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

# Locked against docs/OBSERVABILITY-SPEC.md — do not rename without updating both.
REQUIRED_METRIC_NAMES = frozenset(
    {
        "fdb_mutations_polled_total",
        "fdb_cdc_latest_read_version",
        "fdb_cdc_lag_versions",
        "fdb_serializer_bytes_out_total",
        "fdb_serializer_errors_total",
        "kafka_records_published_total",
        "kafka_publish_latency_seconds",
        "kafka_version_end_markers_total",
    }
)

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


def test_required_prometheus_metric_names_registered() -> None:
    """Fail CI if a Trello/spec metric is removed or renamed."""
    from prometheus_client import generate_latest

    body = generate_latest(REGISTRY).decode()
    missing = [name for name in sorted(REQUIRED_METRIC_NAMES) if name not in body]
    assert not missing, f"missing Prometheus metrics: {missing}"


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


def test_healthz_returns_503_when_liveness_heartbeat_expires() -> None:
    configure_health(liveness_timeout_seconds=0.05)
    heartbeat()
    assert is_live() is True

    server = start_http_server(host="127.0.0.1", port=0)
    host, port = server.server_address[:2]
    base = f"http://{host}:{port}"

    with urllib.request.urlopen(f"{base}/healthz") as resp:
        assert resp.status == 200

    time.sleep(0.07)
    assert is_live() is False
    with pytest.raises(urllib.error.HTTPError) as err:
        urllib.request.urlopen(f"{base}/healthz")
    assert err.value.code == 503


@pytest.mark.parametrize(
    ("fdb_ready", "kafka_ready", "expect_ready"),
    [
        (False, False, False),
        (True, False, False),
        (False, True, False),
        (True, True, True),
    ],
)
def test_readyz_requires_both_fdb_and_kafka(
    fdb_ready: bool, kafka_ready: bool, expect_ready: bool
) -> None:
    set_fdb_ready(fdb_ready)
    set_kafka_ready(kafka_ready)
    assert is_ready() is expect_ready

    server = start_http_server(host="127.0.0.1", port=0)
    host, port = server.server_address[:2]
    url = f"http://{host}:{port}/readyz"

    if expect_ready:
        with urllib.request.urlopen(url) as resp:
            assert resp.status == 200
        return

    with pytest.raises(urllib.error.HTTPError) as err:
        urllib.request.urlopen(url)
    assert err.value.code == 503


def test_audit_version_end_emits_required_json_fields(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging()
    audit_version_end(
        fdb_version=42,
        mutation_count=3,
        duration_seconds=0.0125,
        topic="fdb-cdc",
        partition=1,
    )
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert lines, "expected a VersionEnd audit log line"
    payload = json.loads(lines[-1])
    assert payload["event"] == "version_end_audit"
    assert payload["fdb_version"] == 42
    assert payload["mutation_count"] == 3
    assert payload["duration_seconds"] == pytest.approx(0.0125)
    assert payload["topic"] == "fdb-cdc"
    assert payload["partition"] == 1
    assert "timestamp" in payload
    assert payload.get("level") == "info"


def test_poll_records_emits_version_end_audit(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging()
    client = MagicMock(spec=FDBClient)
    consumer = MagicMock()
    client.create_cdc_consumer.return_value = consumer
    client.get_read_version.return_value = 1000
    consumer.consume.return_value.wait.return_value = MockConsumeResult(
        [MockVersionGroup(1000, [MockCdcMutation(0, b"k", b"v")])],
        1000,
    )
    listener = FDBMutationListener(client, "audit-stream")
    records = listener.poll_records(bridge_timestamp_ns=1)
    assert any(r.WhichOneof("record") == "version_end" for r in records)

    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    audits = [json.loads(line) for line in lines if '"version_end_audit"' in line]
    assert len(audits) == 1
    assert audits[0]["fdb_version"] == 1000
    assert audits[0]["mutation_count"] == 1
    assert isinstance(audits[0]["duration_seconds"], (int, float))


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
