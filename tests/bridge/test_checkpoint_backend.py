"""FDB backend selection and preservation of the existing pipeline state machine."""

from types import SimpleNamespace
from unittest.mock import MagicMock, create_autospec, patch

import pytest

from fdb_kafka_bridge.bridge.config import from_dict
from fdb_kafka_bridge.bridge.pipeline import Bridge
from fdb_kafka_bridge.cdc.checkpoint import (
    CheckpointCursor,
    CheckpointSinkMismatchError,
    CheckpointStore,
    CheckpointWriteError,
    SinkIdentity,
)
from fdb_kafka_bridge.cdc.client import FDBClient
from fdb_kafka_bridge.kafka.producer import MutationProducer
from fdbkafka.cdc.v1 import mutations_pb2


def _wiring() -> SimpleNamespace:
    config = from_dict(
        {
            "cdc": {"stream_name": "s", "checkpoint_path": ["checkpoints"]},
            "kafka": {"topic": "t"},
            "pipeline": {"shutdown_timeout_seconds": 0.01},
        }
    )
    client = create_autospec(FDBClient, instance=True)
    client.list_cdc_streams.return_value = [
        SimpleNamespace(name=b"s", stream_id=1, begin_key=b"a", end_key=b"b")
    ]
    store = create_autospec(CheckpointStore, instance=True)
    store.client = client
    store.sink = SinkIdentity("cluster", "t")
    store.load.return_value = CheckpointCursor(1, 10)
    producer = create_autospec(MutationProducer, instance=True)
    producer.transactional = False
    producer.closed = False
    producer.fatal = False
    producer.cluster_id.return_value = "cluster"
    bridge = Bridge(config, client=client, checkpoint_store=store, producer=producer)
    return SimpleNamespace(bridge=bridge, store=store, producer=producer, client=client)


def test_startup_resume_and_checkpoint_retry_without_republication() -> None:
    deps = _wiring()
    with patch("fdb_kafka_bridge.bridge.pipeline.FDBMutationListener") as factory:
        listener = factory.return_value
        listener.raw_stream_name = b"s"
        listener.poll_records.return_value = [
            mutations_pb2.FDBMutationRecord(stream_name="s")
        ]
        listener.get_position.return_value = CheckpointCursor(1, 20)
        deps.bridge.start()
        assert factory.call_args.kwargs["cursor"] == CheckpointCursor(1, 10)
        assert factory.call_args.kwargs["key_range"] is None
        deps.store.save.side_effect = [CheckpointWriteError("retry"), None]
        with pytest.raises(CheckpointWriteError):
            deps.bridge._pipeline.step()
        listener.acknowledge.assert_not_called()
        assert deps.bridge._pipeline.step() == 1
        deps.producer.produce.assert_called_once()
        listener.poll_records.assert_called_once()
        listener.acknowledge.assert_called_once()
        deps.store.save.assert_called_with(b"s", CheckpointCursor(1, 20))
        deps.bridge.stop()


def test_sink_change_during_rebuild_is_terminal() -> None:
    deps = _wiring()
    with patch("fdb_kafka_bridge.bridge.pipeline.FDBMutationListener") as factory:
        deps.bridge.start()
        fresh = create_autospec(MutationProducer, instance=True)
        fresh.cluster_id.return_value = "different-cluster"
        deps.bridge._producer_factory = lambda: fresh
        with pytest.raises(CheckpointSinkMismatchError):
            deps.bridge._rebuild_pipeline()
        assert factory.call_count == 1
        fresh.close.assert_called_once()
        assert deps.bridge._pipeline is None


def test_new_store_uses_metadata_and_configured_path() -> None:
    deps = _wiring()
    deps.bridge._checkpoint_store = None
    with (
        patch(
            "fdb_kafka_bridge.bridge.pipeline.CheckpointStore", return_value=deps.store
        ) as store_class,
        patch("fdb_kafka_bridge.bridge.pipeline.FDBMutationListener"),
    ):
        deps.bridge.start()
        assert store_class.call_args.kwargs["sink"] == SinkIdentity("cluster", "t")
        assert store_class.call_args.kwargs["path"] == ("checkpoints",)
        deps.bridge.stop()


def test_injected_listener_and_cursor_file_rejected() -> None:
    config = from_dict({"cdc": {"stream_name": "s", "checkpoint_path": ["c"]}})
    with pytest.raises(ValueError, match="managed listener"):
        Bridge(config, listener=MagicMock())
    config = from_dict({"cdc": {"stream_name": "s", "cursor_file": "file"}})
    with pytest.raises(ValueError, match="no cursor_file"):
        Bridge(config, checkpoint_store=MagicMock())


@pytest.mark.parametrize(
    "cdc",
    [
        {"checkpoint_path": []},
        {"checkpoint_path": "bad"},
        {"checkpoint_path": [""]},
        {"checkpoint_path": ["c"], "cursor_file": "file"},
        {"checkpoint_timeout_seconds": float("nan")},
        {"checkpoint_timeout_seconds": True},
        {"checkpoint_retry_limit": True},
        {"checkpoint_retry_limit": -1},
    ],
)
def test_invalid_backend_configuration(cdc: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        from_dict({"cdc": {"stream_name": "s", **cdc}})


def test_permanent_checkpoint_error_stops_run_loop() -> None:
    deps = _wiring()
    with patch("fdb_kafka_bridge.bridge.pipeline.FDBMutationListener"):
        deps.bridge.start()
        deps.bridge._pipeline = MagicMock()
        deps.bridge._pipeline.step.side_effect = CheckpointSinkMismatchError("sink")
        with pytest.raises(CheckpointSinkMismatchError):
            deps.bridge.run()
        assert deps.bridge._shutdown_requested.is_set()
        deps.bridge.stop()


def test_store_client_is_shared_and_mismatched_client_is_rejected() -> None:
    deps = _wiring()
    config = from_dict({"cdc": {"stream_name": "s"}})
    bridge = Bridge(config, checkpoint_store=deps.store)
    assert bridge._client is deps.client
    with pytest.raises(ValueError, match="share the FDB client"):
        Bridge(config, client=MagicMock(), checkpoint_store=deps.store)
