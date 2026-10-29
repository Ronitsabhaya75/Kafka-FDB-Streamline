"""Unit tests for Bridge autonomous daemon, durability, and error recovery."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, create_autospec

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from fdbkafka.cdc.v1 import mutations_pb2
from src.bridge.config import BridgeConfig, from_dict
from src.bridge.pipeline import (
    Bridge,
    BridgeMetrics,
    BridgePipeline,
    _load_cursor_from_file,
    _save_cursor_to_file,
)
from src.cdc.client import FDBClient
from src.cdc.errors import CDCTerminalError
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


def test_pipeline_execution_order(tmp_path: Path) -> None:
    """Verify produce -> flush -> checkpoint -> acknowledge order via attach_mock."""
    cursor_file = str(tmp_path / "test.cursor")
    mock_listener = create_autospec(FDBMutationListener, instance=True)
    mock_listener.raw_stream_name = b"test_stream"
    mock_listener.stream_name = "test_stream"
    mock_pos = MagicMock(stream_id=1, last_consumed_version=500)
    mock_listener.get_position.return_value = mock_pos

    mock_producer = create_autospec(MutationProducer, instance=True)
    mock_producer.transactional = False
    mock_producer.flush_or_raise.return_value = 0

    batch_rec = _make_batch_record(2)
    mock_listener.poll_records.return_value = [batch_rec]

    checkpoint_callback = MagicMock(
        side_effect=lambda pos: _save_cursor_to_file(cursor_file, pos)
    )

    manager = MagicMock()
    manager.attach_mock(mock_producer.produce, "produce")
    manager.attach_mock(mock_producer.flush_or_raise, "flush")
    manager.attach_mock(checkpoint_callback, "checkpoint")
    manager.attach_mock(mock_listener.acknowledge, "acknowledge")

    pipeline = BridgePipeline(
        mock_listener,
        mock_producer,
        topic="test_topic",
        checkpoint=checkpoint_callback,
        partition_key=b"test_stream",
        flush_timeout_seconds=5.0,
    )
    published = pipeline.step()
    assert published == 1

    expected_call_names = [
        c[0]
        for c in manager.mock_calls
        if c[0] in ("produce", "flush", "checkpoint", "acknowledge")
    ]
    assert expected_call_names == [
        "produce",
        "flush",
        "checkpoint",
        "acknowledge",
    ]
    assert os.path.exists(cursor_file)


def test_checkpoint_persistence_failure_blocks_ack(tmp_path: Path) -> None:
    """Verify that a checkpoint failure raises and prevents FDB acknowledgement."""
    mock_listener = create_autospec(FDBMutationListener, instance=True)
    mock_listener.raw_stream_name = b"test_stream"
    mock_pos = MagicMock(stream_id=1, last_consumed_version=500)
    mock_listener.get_position.return_value = mock_pos

    mock_producer = create_autospec(MutationProducer, instance=True)
    mock_producer.transactional = False
    mock_producer.flush_or_raise.return_value = 0

    batch_rec = _make_batch_record(2)
    mock_listener.poll_records.return_value = [batch_rec]

    # Target an invalid path that cannot be written
    invalid_path = str(tmp_path / "nonexistent_dir" / "sub" / "cursor.json")

    pipeline = BridgePipeline(
        mock_listener,
        mock_producer,
        topic="test_topic",
        checkpoint=lambda pos: _save_cursor_to_file(invalid_path, pos),
        partition_key=b"test_stream",
    )

    with pytest.raises(OSError):
        pipeline.step()

    mock_listener.acknowledge.assert_not_called()


def test_save_cursor_missing_coordinates_raises() -> None:
    """Verify _save_cursor_to_file raises ValueError when coordinates are missing."""
    mock_pos = MagicMock(spec=[])  # lacks stream_id and last_consumed_version
    with pytest.raises(ValueError, match="missing stream_id or last_consumed_version"):
        _save_cursor_to_file("/tmp/dummy.cursor", mock_pos)


def test_save_and_load_cursor_file(tmp_path: Path) -> None:
    """Verify _save_cursor_to_file and _load_cursor_from_file round trip."""
    cursor_file = str(tmp_path / "test.cursor")

    mock_pos = MagicMock(stream_id=42, last_consumed_version=123456789)
    _save_cursor_to_file(cursor_file, mock_pos)

    loaded = _load_cursor_from_file(cursor_file)
    assert loaded is not None
    assert loaded.stream_id == 42
    assert loaded.last_consumed_version == 123456789


def test_load_cursor_missing_returns_none(tmp_path: Path) -> None:
    """Verify loading from non-existent cursor file returns None."""
    assert _load_cursor_from_file(str(tmp_path / "nonexistent")) is None


def test_load_cursor_corrupt_file_raises(tmp_path: Path) -> None:
    """Verify loading from corrupt cursor file raises ValueError loudly."""
    corrupt_file = str(tmp_path / "corrupt.json")
    with open(corrupt_file, "w", encoding="utf-8") as f:
        f.write("{invalid-json")
    with pytest.raises(ValueError, match="Failed to parse|Expecting"):
        _load_cursor_from_file(corrupt_file)

    non_object_file = str(tmp_path / "array.json")
    with open(non_object_file, "w", encoding="utf-8") as f:
        f.write("[1, 2, 3]")
    with pytest.raises(ValueError, match="expected JSON object"):
        _load_cursor_from_file(non_object_file)


def test_bridge_start_transactional_inits_transactions() -> None:
    """Verify Bridge.start calls init_transactions for transactional producer."""
    cfg = from_dict(
        {
            "cdc": {"stream_name": "test_stream"},
            "kafka": {"topic": "test_topic"},
        }
    )
    mock_client = create_autospec(FDBClient, instance=True)
    mock_listener = create_autospec(FDBMutationListener, instance=True)
    mock_listener.raw_stream_name = b"test_stream"

    mock_producer = create_autospec(MutationProducer, instance=True)
    mock_producer.transactional = True

    bridge = Bridge(
        cfg,
        client=mock_client,
        listener=mock_listener,
        producer=mock_producer,
    )
    bridge.start()
    mock_producer.init_transactions.assert_called_once()


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

    assert bridge.mutations_published == 5
    assert bridge.metrics.records_published == 2
    assert bridge.batches_acked == 2
    assert bridge.metrics.errors_retried == 0

    bridge.stop()
    mock_producer.close.assert_called_once()
    mock_listener.close.assert_called_once()
    mock_client.close.assert_called_once()


def test_bridge_terminates_on_cdc_terminal_error() -> None:
    """Verify Bridge terminates immediately and re-raises on CDCTerminalError."""
    cfg = from_dict(
        {
            "cdc": {"stream_name": "test_stream"},
            "pipeline": {"backoff_base_seconds": 0.01},
        }
    )
    mock_client = create_autospec(FDBClient, instance=True)
    mock_listener = create_autospec(FDBMutationListener, instance=True)
    mock_listener.raw_stream_name = b"test_stream"
    mock_listener.poll_records.side_effect = CDCTerminalError("Stream removed")

    mock_producer = create_autospec(MutationProducer, instance=True)
    mock_producer.transactional = False

    bridge = Bridge(
        cfg,
        client=mock_client,
        listener=mock_listener,
        producer=mock_producer,
    )
    bridge.start()

    with pytest.raises(CDCTerminalError, match="Stream removed"):
        bridge.run()

    bridge.stop()
