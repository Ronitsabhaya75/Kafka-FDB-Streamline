"""Unit tests for observability metrics, health HTTP, and listener hooks.

Contract tests lock the Prometheus names from ``docs/OBSERVABILITY-SPEC.md`` so
a rename or deletion fails CI on every push/PR (see ``.github/workflows/tests.yml``).
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from collections import namedtuple
from collections.abc import Iterator
from unittest.mock import MagicMock

import pytest
from prometheus_client import generate_latest

from src.cdc.client import FDBClient
from src.cdc.listener import FDBMutationListener
from src.kafka import MutationProducer
from src.observability import (
    audit_version_end,
    heartbeat,
    set_fdb_ready,
    set_kafka_ready,
)
from src.observability.health import (
    configure as configure_health,
)
from src.observability.health import (
    is_live,
    is_ready,
    reset_for_tests,
)
from src.observability.http import start_http_server, stop_http_server
from src.observability.logging import configure_logging
from src.observability.metrics import REGISTRY
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
        for sample in family.samples:
            if sample.name != name:
                continue
            if all(sample.labels.get(k) == v for k, v in labels.items()):
                return float(sample.value)
    return 0.0


@pytest.fixture(autouse=True)
def _reset_health() -> Iterator[None]:
    reset_for_tests()
    stop_http_server()
    yield
    stop_http_server()
    reset_for_tests()


@pytest.fixture
def listener_with_batch() -> tuple[FDBMutationListener, MagicMock]:
    """Build a started listener whose next consume returns one SET_VALUE group."""
    client = MagicMock(spec=FDBClient)
    consumer = MagicMock()
    client.create_cdc_consumer.return_value = consumer
    client.get_read_version.return_value = 1100
    consumer.consume.return_value.wait.return_value = MockConsumeResult(
        [MockVersionGroup(1000, [MockCdcMutation(0, b"k", b"v")])],
        1000,
    )
    consumer.acknowledge.return_value.wait.return_value = None
    listener = FDBMutationListener(client, "test-stream")
    return listener, client


def test_required_prometheus_metric_names_registered() -> None:
    """Fail CI if a Trello/spec metric is removed or renamed."""
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


def test_healthz_is_not_live_before_first_heartbeat() -> None:
    assert is_live() is False
    server = start_http_server(host="127.0.0.1", port=0)
    host, port = server.server_address[:2]
    with pytest.raises(urllib.error.HTTPError) as err:
        urllib.request.urlopen(f"http://{host}:{port}/healthz")
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


def test_fdb_readiness_is_refcounted() -> None:
    set_fdb_ready(True)
    set_fdb_ready(True)
    set_kafka_ready(True)
    assert is_ready() is True

    set_fdb_ready(False)
    assert is_ready() is True

    set_fdb_ready(False)
    assert is_ready() is False


def test_audit_version_end_emits_required_json_fields(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging()
    audit_version_end(
        fdb_version=42,
        mutation_count=3,
        duration_seconds=0.0125,
    )
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert lines, "expected a VersionEnd audit log line"
    payload = json.loads(lines[-1])
    assert payload["event"] == "version_end_audit"
    assert payload["fdb_version"] == 42
    assert payload["mutation_count"] == 3
    assert payload["duration_seconds"] == pytest.approx(0.0125)
    assert "topic" not in payload
    assert "partition" not in payload
    assert "timestamp" in payload
    assert payload.get("level") == "info"


def test_idle_watermark_audit_logs_at_debug(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(level=logging.DEBUG)
    audit_version_end(fdb_version=7, mutation_count=0, duration_seconds=0.001)
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert lines
    payload = json.loads(lines[-1])
    assert payload["mutation_count"] == 0
    assert payload.get("level") == "debug"


def test_idle_watermark_audit_suppressed_at_info(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging()
    audit_version_end(fdb_version=7, mutation_count=0, duration_seconds=0.001)
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert not any('"version_end_audit"' in line for line in lines)


def test_acknowledge_emits_version_end_audit(
    listener_with_batch: tuple[FDBMutationListener, MagicMock],
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging()
    listener, _client = listener_with_batch
    records = listener.poll_records(bridge_timestamp_ns=1)
    assert any(r.WhichOneof("record") == "version_end" for r in records)

    before_ack = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert not any('"version_end_audit"' in line for line in before_ack)

    listener.acknowledge()
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    audits = [json.loads(line) for line in lines if '"version_end_audit"' in line]
    assert len(audits) == 1
    assert audits[0]["fdb_version"] == 1000
    assert audits[0]["mutation_count"] == 1
    assert isinstance(audits[0]["duration_seconds"], (int, float))


def test_poll_records_increments_fdb_and_serializer_metrics(
    listener_with_batch: tuple[FDBMutationListener, MagicMock],
) -> None:
    before = _metric_value("fdb_mutations_polled_total", {"opcode": "SET_VALUE"})
    before_bytes = _metric_value("fdb_serializer_bytes_out_total")
    listener, _client = listener_with_batch

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


def test_lag_unchanged_when_get_read_version_fails(
    listener_with_batch: tuple[FDBMutationListener, MagicMock],
) -> None:
    listener, client = listener_with_batch
    listener.poll_records(bridge_timestamp_ns=1)
    assert _metric_value("fdb_cdc_lag_versions") == 100.0

    listener.acknowledge()
    client.get_read_version.side_effect = RuntimeError("cluster unavailable")
    client.get_read_version.return_value = 2000
    # Bust GRV cache so the next poll attempts a refresh.
    listener._grv_cache_monotonic = 0.0
    consumer = listener.consumer
    consumer.consume.return_value.wait.return_value = MockConsumeResult(
        [MockVersionGroup(1500, [MockCdcMutation(0, b"k2", b"v2")])],
        1500,
    )
    listener.poll_records(bridge_timestamp_ns=2)
    assert _metric_value("fdb_cdc_latest_read_version") == 1500.0
    assert _metric_value("fdb_cdc_lag_versions") == 100.0


def test_grv_is_cached_across_polls(
    listener_with_batch: tuple[FDBMutationListener, MagicMock],
) -> None:
    listener, client = listener_with_batch
    listener.poll_records(bridge_timestamp_ns=1)
    assert client.get_read_version.call_count == 1

    listener.acknowledge()
    consumer = listener.consumer
    consumer.consume.return_value.wait.return_value = MockConsumeResult(
        [MockVersionGroup(1001, [MockCdcMutation(0, b"k2", b"v2")])],
        1001,
    )
    listener.poll_records(bridge_timestamp_ns=2)
    assert client.get_read_version.call_count == 1


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


def test_mutation_producer_stub_does_not_touch_kafka_metrics_or_readyz(
    listener_with_batch: tuple[FDBMutationListener, MagicMock],
) -> None:
    before_pub = _metric_value("kafka_records_published_total")
    before_ve = _metric_value("kafka_version_end_markers_total")
    listener, _client = listener_with_batch
    records = listener.poll_records(bridge_timestamp_ns=1)

    set_fdb_ready(True)
    producer = MutationProducer(topic="fdb-cdc")
    assert is_ready() is False

    published = producer.produce(records)
    assert published == len(records)
    assert _metric_value("kafka_records_published_total") == before_pub
    assert _metric_value("kafka_version_end_markers_total") == before_ve

    producer.close()
    assert is_ready() is False
