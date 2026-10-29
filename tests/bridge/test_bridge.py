"""Unit tests for Bridge autonomous daemon and metrics."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, create_autospec

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from fdbkafka.cdc.v1 import mutations_pb2
from src.bridge.config import BridgeConfig, from_dict
from src.bridge.pipeline import (
    Bridge,
    BridgeMetrics,
    _load_cursor_from_file,
    _save_cursor_to_file,
)
from src.cdc.client import FDBClient
from src.cdc.listener import FDBMutationListener
from src.kafka.producer import MutationProducer


def _make_batch_record(mutation_count: int = 5) -> mutations_pb2.FDBMutationRecord:
    """Create a sample FDBMutationRecord with a batch of mutations."""
    record = mutations_pb2.FDBMutationRecord()
    record.stream_name = "test_stream"
    record.bridge_timestamp.CopyFrom(Timestamp(seconds=1700000000))
    for i in range(mutation_count):
        mutation = record.batch.mutations.add()
        mutation.single_key_mutation.mutation_type = 0
        mutation.single_key_mutation.key = f"key_{i}".encode()
        mutation.single_key_mutation.value = f"value_{i}".encode()
    return record


def _make_single_mutation_record() -> mutations_pb2.FDBMutationRecord:
    """Create a sample FDBMutationRecord with a single mutation."""
    record = mutations_pb2.FDBMutationRecord()
    record.stream_name = "test_stream"
    record.bridge_timestamp.CopyFrom(Timestamp(seconds=1700000000))
    record.mutation.single_key_mutation.mutation_type = 0
    record.mutation.single_key_mutation.key = b"single_key"
    record.mutation.single_key_mutation.value = b"single_value"
    return record


def test_bridge_metrics_defaults() -> None:
    """Verify BridgeMetrics initializes counters to zero."""
    metrics = BridgeMetrics()
    assert metrics.mutations_published == 0
    assert metrics.records_published == 0
    assert metrics.batches_acked == 0
    assert metrics.errors_retried == 0


def test_bridge_run_before_start_raises() -> None:
    """Verify Bridge.run raises RuntimeError when called before start."""
    cfg = BridgeConfig()
    bridge = Bridge(cfg)
    with pytest.raises(RuntimeError, match="Bridge must be started"):
        bridge.run()


def test_bridge_lifecycle_and_metrics() -> None:
    """Verify Bridge start, step iteration, metric tallies, and clean stop."""
    cfg = from_dict(
        {
            "cdc": {"stream_name": "test_stream"},
            "kafka": {"topic": "test_topic"},
            "pipeline": {
                "backoff_base_seconds": 0.01,
                "backoff_max_seconds": 0.05,
            },
        }
    )

    mock_client = create_autospec(FDBClient, instance=True)
    mock_listener = create_autospec(FDBMutationListener, instance=True)
    mock_listener.raw_stream_name = b"test_stream"
    mock_listener.stream_name = "test_stream"
    mock_listener.get_position.return_value = MagicMock(
        stream_id=1, last_consumed_version=100
    )

    mock_producer = create_autospec(MutationProducer, instance=True)
    mock_producer.transactional = False
    mock_producer.flush_or_raise.return_value = 0

    batch_rec = _make_batch_record(4)
    single_rec = _make_single_mutation_record()

    # Sequence: batch of 4, single mutation of 1, empty (backoff), then stop
    mock_listener.poll_records.side_effect = [
        [batch_rec],
        [single_rec],
        [],
    ]

    bridge = Bridge(
        cfg,
        client=mock_client,
        listener=mock_listener,
        producer=mock_producer,
    )
    bridge.start()

    assert bridge.mutations_published == 0
    assert bridge.batches_acked == 0

    # Stop daemon after third iteration
    call_count = 0

    def step_hook() -> int:
        nonlocal call_count
        call_count += 1
        if call_count >= 3:
            bridge.request_shutdown()
        return orig_step()

    orig_step = bridge._pipeline.step  # type: ignore[union-attr]
    bridge._pipeline.step = step_hook  # type: ignore[union-attr]

    bridge.run()

    # 4 from batch_rec + 1 from single_rec = 5 total mutations
    assert bridge.mutations_published == 5
    assert bridge.metrics.records_published == 2
    assert bridge.batches_acked == 2
    assert bridge.metrics.errors_retried == 0

    bridge.stop()
    mock_producer.close.assert_called_once()
    mock_listener.close.assert_called_once()
    mock_client.close.assert_called_once()


def test_bridge_run_error_retry() -> None:
    """Verify Bridge catches loop exceptions and tallies errors_retried."""
    cfg = from_dict(
        {
            "cdc": {"stream_name": "test_stream"},
            "pipeline": {
                "backoff_base_seconds": 0.01,
                "backoff_max_seconds": 0.02,
            },
        }
    )

    mock_client = create_autospec(FDBClient, instance=True)
    mock_listener = create_autospec(FDBMutationListener, instance=True)
    mock_listener.raw_stream_name = b"test_stream"
    mock_producer = create_autospec(MutationProducer, instance=True)
    mock_producer.transactional = False

    bridge = Bridge(
        cfg,
        client=mock_client,
        listener=mock_listener,
        producer=mock_producer,
    )
    bridge.start()

    attempts = 0

    def failing_step() -> int:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("Transient pipeline failure")
        bridge.request_shutdown()
        return 0

    bridge._pipeline.step = failing_step  # type: ignore[union-attr]

    bridge.run()

    assert bridge.metrics.errors_retried == 1
    bridge.stop()


def test_save_and_load_cursor_file(tmp_path: Path) -> None:
    """Verify _save_cursor_to_file and _load_cursor_from_file round trip."""
    cursor_file = str(tmp_path / "test.cursor")

    mock_pos = MagicMock(stream_id=42, last_consumed_version=123456789)
    _save_cursor_to_file(cursor_file, mock_pos)

    loaded = _load_cursor_from_file(cursor_file)
    assert loaded is not None
    assert loaded.stream_id == 42
    assert loaded.last_consumed_version == 123456789


def test_load_cursor_missing_or_corrupt_file(tmp_path: Path) -> None:
    """Verify loading from missing or corrupt cursor file returns None safely."""
    assert _load_cursor_from_file(str(tmp_path / "nonexistent")) is None

    corrupt_file = str(tmp_path / "corrupt.json")
    with open(corrupt_file, "w") as f:
        f.write("{invalid-json")
    assert _load_cursor_from_file(corrupt_file) is None
