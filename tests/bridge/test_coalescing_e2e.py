"""End-to-end tests for CDC idle reply coalescing across the Bridge daemon."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, PropertyMock, create_autospec, patch

import pytest

from fdb_kafka_bridge.bridge.config import from_dict
from fdb_kafka_bridge.bridge.pipeline import (
    Bridge,
    _load_cursor_from_file,
)
from fdb_kafka_bridge.cdc.client import FDBClient
from fdb_kafka_bridge.cdc.listener import FDBMutationListener
from fdb_kafka_bridge.kafka.producer import MutationProducer
from fdbkafka.cdc.v1 import mutations_pb2


class _SimulatedNativeBatch:
    """Simulates a native FDB CDC consume result."""

    def __init__(
        self,
        watermark: int,
        mutations: list[tuple[bytes, bytes]] | None = None,
    ) -> None:
        self.last_consumed_version = watermark
        self.mutations: list[Any] = []
        if mutations:
            vg = MagicMock()
            vg.version = watermark
            vg_mutations = []
            for k, v in mutations:
                m = MagicMock()
                m.type = 0  # SET_VALUE
                m.param1 = k
                m.param2 = v
                vg_mutations.append(m)
            vg.mutations = vg_mutations
            self.mutations = [vg]


class _SimulatedNativeConsumer:
    """Simulates a native FDB CDC consumer with tracked acks and positions."""

    def __init__(
        self,
        batches: list[_SimulatedNativeBatch],
        initial_version: int = 0,
    ) -> None:
        self._batches = list(batches)
        self.current_batch: _SimulatedNativeBatch | None = None
        self.initial_version = initial_version
        self.ack_history: list[int] = []
        self.closed = False

    def consume(self) -> Any:
        class _Future:
            def __init__(self, batch: _SimulatedNativeBatch) -> None:
                self._batch = batch

            def wait(self) -> _SimulatedNativeBatch:
                return self._batch

        if not self._batches:
            # Continual empty heartbeat
            last_v = (
                self.current_batch.last_consumed_version if self.current_batch else 1000
            )
            batch = _SimulatedNativeBatch(last_v + 100)
        else:
            batch = self._batches.pop(0)

        self.current_batch = batch
        return _Future(batch)

    def acknowledge(self) -> Any:
        class _Future:
            def wait(self) -> None:
                pass

        version = (
            self.current_batch.last_consumed_version
            if self.current_batch is not None
            else self.initial_version
        )
        self.ack_history.append(version)
        return _Future()

    def get_position(self) -> Any:
        version = (
            self.current_batch.last_consumed_version
            if self.current_batch
            else self.initial_version
        )
        return MagicMock(stream_id=1, last_consumed_version=version)

    def close(self) -> None:
        self.closed = True


class _CapturedKafkaBroker:
    """Captures messages published to Kafka for end-to-end verification."""

    def __init__(self) -> None:
        self.published_records: list[
            tuple[str, mutations_pb2.FDBMutationRecord, bytes | None]
        ] = []
        self.flushes = 0

    def produce(
        self,
        topic: str,
        record: mutations_pb2.FDBMutationRecord,
        key: bytes | None = None,
    ) -> None:
        self.published_records.append((topic, record, key))

    def flush_or_raise(self, timeout: float | None = None) -> None:
        self.flushes += 1


def test_coalescing_e2e_full_lifecycle(tmp_path: Path) -> None:
    """E2E verification of data processing, coalescing, crash recovery, and stop."""
    cursor_file = str(tmp_path / "stream_cursor.json")
    stream_name = "orders_stream"

    cfg = from_dict(
        {
            "cdc": {
                "stream_name": stream_name,
                "cursor_file": cursor_file,
                "idle_ack_max_replies": 4,
            },
            "kafka": {
                "topic": "orders_topic",
            },
            "pipeline": {
                "shutdown_timeout_seconds": 5.0,
            },
        }
    )

    # 1. Define scripted CDC batches:
    # Batch 1: Data mutation @ version 100
    # Batch 2: Data mutation @ version 200
    # Batch 3..5: 3 idle heartbeats @ 300, 400, 500 (all should be coalesced / skipped)
    # Batch 6: 4th idle heartbeat @ 600 (threshold reached! flushes coalesced watermark)
    # Batch 7: Data mutation @ version 700
    # Batch 8..9: 2 idle heartbeats @ 800, 900 (coalesced, then crash simulated)
    batches_run_1 = [
        _SimulatedNativeBatch(100, [(b"k1", b"v1")]),
        _SimulatedNativeBatch(200, [(b"k2", b"v2")]),
        _SimulatedNativeBatch(300),
        _SimulatedNativeBatch(400),
        _SimulatedNativeBatch(500),
        _SimulatedNativeBatch(600),
        _SimulatedNativeBatch(700, [(b"k3", b"v3")]),
        _SimulatedNativeBatch(800),
        _SimulatedNativeBatch(900),
    ]

    consumer_1 = _SimulatedNativeConsumer(batches_run_1)
    mock_client_1 = create_autospec(FDBClient, instance=True)
    mock_client_1.create_cdc_consumer.return_value = consumer_1

    listener_1 = FDBMutationListener(
        mock_client_1,
        stream_name=stream_name,
        cursor=None,
        idle_ack_max_replies=4,
    )

    broker = _CapturedKafkaBroker()
    producer_1 = create_autospec(MutationProducer, instance=True)
    type(producer_1).transactional = PropertyMock(return_value=False)
    producer_1.produce.side_effect = broker.produce
    producer_1.flush_or_raise.side_effect = broker.flush_or_raise

    bridge_1 = Bridge(
        cfg,
        client=mock_client_1,
        listener=listener_1,
        producer=producer_1,
    )
    bridge_1.start()
    pipeline_1 = bridge_1._pipeline
    assert pipeline_1 is not None

    # Step 1: Data reply @ 100
    assert pipeline_1.step() > 0
    assert consumer_1.ack_history == [100]
    cursor_data = _load_cursor_from_file(cursor_file, stream_name=stream_name)
    assert cursor_data is not None and cursor_data.last_consumed_version == 100

    # Step 2: Data reply @ 200
    assert pipeline_1.step() > 0
    assert consumer_1.ack_history == [100, 200]
    cursor_data = _load_cursor_from_file(cursor_file, stream_name=stream_name)
    assert cursor_data is not None and cursor_data.last_consumed_version == 200

    # Steps 3, 4, 5: Idle replies @ 300, 400, 500 (withheld)
    assert pipeline_1.step() == 0
    assert pipeline_1.step() == 0
    assert pipeline_1.step() == 0
    # No acks emitted for skipped replies, cursor remains at 200
    assert consumer_1.ack_history == [100, 200]
    cursor_data = _load_cursor_from_file(cursor_file, stream_name=stream_name)
    assert cursor_data is not None and cursor_data.last_consumed_version == 200

    # Step 6: 4th idle reply @ 600 (threshold reached!)
    assert pipeline_1.step() == 1  # emits 1 VersionEnd record
    assert consumer_1.ack_history == [100, 200, 600]
    cursor_data = _load_cursor_from_file(cursor_file, stream_name=stream_name)
    assert cursor_data is not None and cursor_data.last_consumed_version == 600

    # Step 7: Data reply @ 700
    assert pipeline_1.step() > 0
    assert consumer_1.ack_history == [100, 200, 600, 700]
    cursor_data = _load_cursor_from_file(cursor_file, stream_name=stream_name)
    assert cursor_data is not None and cursor_data.last_consumed_version == 700

    # Steps 8, 9: Idle replies @ 800, 900 (withheld)
    assert pipeline_1.step() == 0
    assert pipeline_1.step() == 0
    assert consumer_1.ack_history == [100, 200, 600, 700]
    cursor_data = _load_cursor_from_file(cursor_file, stream_name=stream_name)
    assert cursor_data is not None and cursor_data.last_consumed_version == 700

    # --- CRASH SIMULATION ---
    # Process terminates abruptly without graceful stop
    producer_1.close()
    listener_1.close()

    # Verify state on disk after crash:
    # Cursor is preserved at 700 (unacknowledged 800 and 900 are not checkpointed)
    persisted = _load_cursor_from_file(cursor_file, stream_name=stream_name)
    assert persisted is not None
    assert persisted.last_consumed_version == 700

    # --- RESTART RECOVERY ---
    # New daemon instance restarts from the cursor on disk
    batches_run_2 = [
        # Replayed idle heartbeats starting from resumed cursor
        _SimulatedNativeBatch(800),
        _SimulatedNativeBatch(900),
        _SimulatedNativeBatch(1000),  # Will be flushed gracefully during stop
    ]
    consumer_2 = _SimulatedNativeConsumer(batches_run_2, initial_version=700)
    mock_client_2 = create_autospec(FDBClient, instance=True)
    mock_client_2.resume_cdc_consumer.return_value = consumer_2
    mock_client_2.get_read_version.return_value = 700

    listener_2 = FDBMutationListener(
        mock_client_2,
        stream_name=stream_name,
        cursor=persisted,
        idle_ack_max_replies=4,
    )

    producer_2 = create_autospec(MutationProducer, instance=True)
    type(producer_2).transactional = PropertyMock(return_value=False)
    producer_2.produce.side_effect = broker.produce
    producer_2.flush_or_raise.side_effect = broker.flush_or_raise

    bridge_2 = Bridge(
        cfg,
        client=mock_client_2,
        listener=listener_2,
        producer=producer_2,
    )
    bridge_2.start()
    pipeline_2 = bridge_2._pipeline
    assert pipeline_2 is not None

    # Step replayed 800, 900, 1000
    assert pipeline_2.step() == 0
    assert pipeline_2.step() == 0
    assert pipeline_2.step() == 0
    assert listener_2.has_pending_idle_progress

    # --- GRACEFUL SHUTDOWN TEST ---
    # Trigger bridge.stop(): it should flush the pending idle watermark (1000)
    bridge_2.stop()

    assert not listener_2.has_pending_idle_progress
    # Initial resume ack @ 700, then flushed watermark ack @ 1000
    assert consumer_2.ack_history == [700, 1000]

    # Final cursor on disk must be 1000
    final_cursor = _load_cursor_from_file(cursor_file, stream_name=stream_name)
    assert final_cursor is not None
    assert final_cursor.last_consumed_version == 1000

    # --- VERIFY KAFKA MESSAGES ---
    # Inspect all published records in Kafka
    records = [rec for _topic, rec, _key in broker.published_records]

    # Verify we had 3 data batches and 2 flushed idle watermarks:
    # 1. Version 100 (data)
    # 2. Version 200 (data)
    # 3. Version 600 (coalesced idle watermark)
    # 4. Version 700 (data)
    # 5. Version 1000 (gracefully flushed idle watermark on stop)
    version_ends = [
        (rec.version_end.fdb_version, rec.version_end.total_mutations)
        for rec in records
        if rec.HasField("version_end")
    ]
    assert version_ends == [(100, 1), (200, 1), (600, 0), (700, 1), (1000, 0)]


def test_coalescing_time_threshold_e2e(tmp_path: Path) -> None:
    """E2E verification that elapsed time threshold flushes idle progress."""
    cursor_file = str(tmp_path / "time_cursor.json")
    stream_name = "time_stream"

    cfg = from_dict(
        {
            "cdc": {
                "stream_name": stream_name,
                "cursor_file": cursor_file,
                "idle_ack_max_seconds": 5.0,
            }
        }
    )

    batches = [
        _SimulatedNativeBatch(100),
        _SimulatedNativeBatch(200),
        _SimulatedNativeBatch(300),
    ]
    consumer = _SimulatedNativeConsumer(batches)
    mock_client = create_autospec(FDBClient, instance=True)
    mock_client.create_cdc_consumer.return_value = consumer

    with patch("fdb_kafka_bridge.cdc.listener.time.monotonic") as mock_clock:
        mock_clock.return_value = 1000.0

        listener = FDBMutationListener(
            mock_client,
            stream_name=stream_name,
            idle_ack_max_seconds=5.0,
        )
        listener.start()

        broker = _CapturedKafkaBroker()
        producer = create_autospec(MutationProducer, instance=True)
        type(producer).transactional = PropertyMock(return_value=False)
        producer.produce.side_effect = broker.produce
        producer.flush_or_raise.side_effect = broker.flush_or_raise

        bridge = Bridge(
            cfg,
            client=mock_client,
            listener=listener,
            producer=producer,
        )
        bridge.start()
        pipeline = bridge._pipeline
        assert pipeline is not None

        # At t=1002.0, elapsed = 2.0 < 5.0 -> skipped
        mock_clock.return_value = 1002.0
        assert pipeline.step() == 0
        assert consumer.ack_history == []

        # At t=1004.9, elapsed = 4.9 < 5.0 -> skipped
        mock_clock.return_value = 1004.9
        assert pipeline.step() == 0
        assert consumer.ack_history == []

        # At t=1005.1, elapsed = 5.1 >= 5.0 -> flushed!
        mock_clock.return_value = 1005.1
        assert pipeline.step() == 1
        assert consumer.ack_history == [300]

        cursor = _load_cursor_from_file(cursor_file, stream_name=stream_name)
        assert cursor is not None and cursor.last_consumed_version == 300


def test_coalescing_checkpoint_failure_barrier_e2e(tmp_path: Path) -> None:
    """E2E verification that a checkpoint write failure blocks FDB ack."""
    cursor_file = str(tmp_path / "barrier_cursor.json")
    stream_name = "barrier_stream"

    cfg = from_dict(
        {
            "cdc": {
                "stream_name": stream_name,
                "cursor_file": cursor_file,
                "idle_ack_max_replies": 2,
            }
        }
    )

    batches = [_SimulatedNativeBatch(100), _SimulatedNativeBatch(200)]
    consumer = _SimulatedNativeConsumer(batches)
    mock_client = create_autospec(FDBClient, instance=True)
    mock_client.create_cdc_consumer.return_value = consumer

    listener = FDBMutationListener(
        mock_client,
        stream_name=stream_name,
        idle_ack_max_replies=2,
    )

    producer = create_autospec(MutationProducer, instance=True)
    type(producer).transactional = PropertyMock(return_value=False)

    bridge = Bridge(
        cfg,
        client=mock_client,
        listener=listener,
        producer=producer,
    )
    bridge.start()
    pipeline = bridge._pipeline
    assert pipeline is not None

    # Step 1: Skipped
    assert pipeline.step() == 0

    # Step 2: Threshold reached -> flush triggers, but checkpoint fails
    with patch(
        "fdb_kafka_bridge.bridge.pipeline._save_cursor_to_file",
        side_effect=OSError("Disk write error"),
    ):
        with pytest.raises(OSError, match="Disk write error"):
            pipeline.step()

    # Kafka was produced and flushed, but FDB must NOT be acknowledged!
    assert consumer.ack_history == []
    assert listener.awaiting_ack is True


def test_coalescing_fdb_ack_failure_e2e(tmp_path: Path) -> None:
    """E2E verification that an FDB ack failure leaves cursor checkpointed."""
    cursor_file = str(tmp_path / "ack_fail_cursor.json")
    stream_name = "ack_fail_stream"

    cfg = from_dict(
        {
            "cdc": {
                "stream_name": stream_name,
                "cursor_file": cursor_file,
                "idle_ack_max_replies": 2,
            }
        }
    )

    batches = [_SimulatedNativeBatch(100), _SimulatedNativeBatch(200)]
    consumer = _SimulatedNativeConsumer(batches)

    # Make acknowledge() fail on the second call (when flush happens)
    ack_mock = MagicMock()
    ack_mock.side_effect = RuntimeError("FDB cluster partition during ack")
    consumer.acknowledge = ack_mock

    mock_client = create_autospec(FDBClient, instance=True)
    mock_client.create_cdc_consumer.return_value = consumer

    listener = FDBMutationListener(
        mock_client,
        stream_name=stream_name,
        idle_ack_max_replies=2,
    )

    broker = _CapturedKafkaBroker()
    producer = create_autospec(MutationProducer, instance=True)
    type(producer).transactional = PropertyMock(return_value=False)
    producer.produce.side_effect = broker.produce
    producer.flush_or_raise.side_effect = broker.flush_or_raise

    bridge = Bridge(
        cfg,
        client=mock_client,
        listener=listener,
        producer=producer,
    )
    bridge.start()
    pipeline = bridge._pipeline
    assert pipeline is not None

    # Step 1: Withheld
    assert pipeline.step() == 0

    # Step 2: Flush reaches Kafka, checkpoints to disk, but FDB ack fails
    with pytest.raises(Exception, match="FDB cluster partition during ack"):
        pipeline.step()

    # Verify that cursor was saved to disk before ack failure
    cursor = _load_cursor_from_file(cursor_file, stream_name=stream_name)
    assert cursor is not None
    assert cursor.last_consumed_version == 200
    assert listener.awaiting_ack is True


def test_coalescing_shutdown_flush_failure_replay_e2e(tmp_path: Path) -> None:
    """E2E test that shutdown flush failure leaves previous cursor for replay."""
    cursor_file = str(tmp_path / "stop_fail_cursor.json")
    stream_name = "stop_fail_stream"

    cfg = from_dict(
        {
            "cdc": {
                "stream_name": stream_name,
                "cursor_file": cursor_file,
                "idle_ack_max_replies": 5,
            },
            "pipeline": {
                "shutdown_timeout_seconds": 2.0,
            },
        }
    )

    batches_run_1 = [
        _SimulatedNativeBatch(100, [(b"k1", b"v1")]),
        _SimulatedNativeBatch(200),
    ]
    consumer_1 = _SimulatedNativeConsumer(batches_run_1)
    mock_client_1 = create_autospec(FDBClient, instance=True)
    mock_client_1.create_cdc_consumer.return_value = consumer_1

    listener_1 = FDBMutationListener(
        mock_client_1,
        stream_name=stream_name,
        idle_ack_max_replies=5,
    )

    broker = _CapturedKafkaBroker()
    producer_1 = create_autospec(MutationProducer, instance=True)
    type(producer_1).transactional = PropertyMock(return_value=False)
    producer_1.produce.side_effect = broker.produce
    producer_1.flush_or_raise.side_effect = broker.flush_or_raise

    bridge_1 = Bridge(
        cfg,
        client=mock_client_1,
        listener=listener_1,
        producer=producer_1,
    )
    bridge_1.start()
    pipeline_1 = bridge_1._pipeline
    assert pipeline_1 is not None

    # Step 1: Data reply @ 100 -> committed
    assert pipeline_1.step() > 0
    assert consumer_1.ack_history == [100]

    # Step 2: Idle reply @ 200 -> withheld
    assert pipeline_1.step() == 0
    assert listener_1.has_pending_idle_progress

    # Shutdown with failing flush (e.g. producer flush fails during stop)
    producer_1.flush_or_raise.side_effect = RuntimeError("Broker offline")

    # stop() must not crash, must log warning and exit cleanly
    bridge_1.stop()

    # The persisted cursor on disk must remain at 100 (unflushed 200 not checkpointed)
    cursor = _load_cursor_from_file(cursor_file, stream_name=stream_name)
    assert cursor is not None
    assert cursor.last_consumed_version == 100

    # Resume with second daemon instance from checkpoint 100
    batches_run_2 = [
        _SimulatedNativeBatch(200),
        _SimulatedNativeBatch(300),
    ]
    consumer_2 = _SimulatedNativeConsumer(batches_run_2, initial_version=100)
    mock_client_2 = create_autospec(FDBClient, instance=True)
    mock_client_2.resume_cdc_consumer.return_value = consumer_2
    mock_client_2.get_read_version.return_value = 100

    listener_2 = FDBMutationListener(
        mock_client_2,
        stream_name=stream_name,
        cursor=cursor,
        idle_ack_max_replies=5,
    )

    producer_2 = create_autospec(MutationProducer, instance=True)
    type(producer_2).transactional = PropertyMock(return_value=False)
    producer_2.produce.side_effect = broker.produce
    producer_2.flush_or_raise.side_effect = broker.flush_or_raise

    bridge_2 = Bridge(
        cfg,
        client=mock_client_2,
        listener=listener_2,
        producer=producer_2,
    )
    bridge_2.start()
    pipeline_2 = bridge_2._pipeline
    assert pipeline_2 is not None

    # Step replayed 200 and 300 (both withheld)
    assert pipeline_2.step() == 0
    assert pipeline_2.step() == 0
    assert listener_2.has_pending_idle_progress

    # Normal stop cleanly flushes watermark 300
    bridge_2.stop()
    final_cursor = _load_cursor_from_file(cursor_file, stream_name=stream_name)
    assert final_cursor is not None
    assert final_cursor.last_consumed_version == 300
