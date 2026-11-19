"""Crash-window recovery using real CDC and checkpoints with an in-memory sink."""

import os
import uuid
from collections.abc import Iterator
from typing import Any

import pytest

from fdb_kafka_bridge.bridge.pipeline import BridgePipeline
from fdb_kafka_bridge.bridge.startup import StartupError, resolve_startup
from fdb_kafka_bridge.cdc.checkpoint import CheckpointStore, SinkIdentity
from fdb_kafka_bridge.cdc.client import FDBClient
from fdb_kafka_bridge.cdc.listener import FDBMutationListener, strinc
from fdbkafka.cdc.v1 import mutations_pb2

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("FDB_CDC_INTEGRATION") != "1", reason="requires live FDB CDC"
    ),
]


class Sink:
    transactional = False

    def __init__(
        self, records: list[mutations_pb2.FDBMutationRecord] | None = None
    ) -> None:
        self.records = records if records is not None else []
        self.closed = False
        self.failure: str | None = None
        self.target: bytes | None = None
        self.saw_target = False

    def produce(
        self, topic: str, record: mutations_pb2.FDBMutationRecord, *, key: bytes
    ) -> None:
        assert key == record.stream_name.encode()
        self.records.append(record)
        if record.HasField("batch"):
            self.saw_target |= any(
                m.HasField("single_key_mutation")
                and m.single_key_mutation.key == self.target
                for m in record.batch.mutations
            )
        if self.saw_target and self.failure == "produce":
            raise RuntimeError("injected crash")

    def flush_or_raise(self, timeout: float) -> None:
        if self.saw_target and self.failure == "flush":
            raise RuntimeError("injected crash")
        return None

    def close(self) -> None:
        self.closed = True


def pipeline_for(
    listener: FDBMutationListener, sink: Sink, store: CheckpointStore, topic: str
) -> BridgePipeline:
    return BridgePipeline(
        listener,
        sink,
        topic,
        lambda cursor: store.save(listener.raw_stream_name, cursor),
    )


@pytest.fixture
def resources() -> Iterator[tuple[FDBClient, CheckpointStore, bytes, bytes]]:
    client = FDBClient()
    client.open()
    name = f"bridge-{uuid.uuid4().hex}".encode()
    prefix = name + b":"
    store = CheckpointStore(
        client,
        sink=SinkIdentity("cluster", "topic"),
        path=("bridge-tests", name.decode()),
    )
    client.register_cdc_stream(name, prefix, strinc(prefix))
    store.open(forbidden_ranges=[(prefix, strinc(prefix))])
    store.claim(name)
    try:
        yield client, store, name, prefix
    finally:
        client.remove_cdc_stream(name)
        del client.db[prefix : strinc(prefix)]
        begin, end = store.key_range()
        del client.db[begin:end]
        client.close()


def _until_target(pipeline: BridgePipeline, sink: Sink) -> None:
    for _ in range(15):
        pipeline.step()
        if sink.saw_target:
            return
    pytest.fail("CDC did not deliver the target mutation")


@pytest.mark.parametrize(
    "failure", ["poll", "produce", "flush", "save", "after_save", "ack"]
)
def test_restart_at_each_boundary(
    resources: tuple[FDBClient, CheckpointStore, bytes, bytes],
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    client, store, name, prefix = resources
    sink = Sink()
    listener = FDBMutationListener(client, name)
    listener.start()
    sink.target = prefix + b"initial"
    client.db[sink.target] = b"initial"
    pipeline = pipeline_for(listener, sink, store, "test")
    _until_target(pipeline, sink)
    before = store.load(name)
    assert before is not None
    sink.target = prefix + b"target"
    sink.saw_target = False
    sink.failure = failure
    original_poll = listener.poll_records
    original_ack = listener.acknowledge
    original_save = store.save

    def poll() -> list[mutations_pb2.FDBMutationRecord]:
        records = original_poll()
        has_target = any(
            record.HasField("batch")
            and any(
                m.HasField("single_key_mutation")
                and m.single_key_mutation.key == sink.target
                for m in record.batch.mutations
            )
            for record in records
        )
        if has_target and failure == "poll":
            raise RuntimeError("injected crash")
        return records

    def save(stream_name: bytes, cursor: Any) -> None:
        if sink.saw_target and failure == "save":
            raise RuntimeError("injected crash")
        original_save(stream_name, cursor)
        if sink.saw_target and failure == "after_save":
            raise RuntimeError("injected crash")

    def ack() -> None:
        if sink.saw_target and failure == "ack":
            raise RuntimeError("injected crash")
        original_ack()

    monkeypatch.setattr(listener, "poll_records", poll)
    monkeypatch.setattr(listener, "acknowledge", ack)
    monkeypatch.setattr(store, "save", save)
    client.db[sink.target] = b"target"
    try:
        with pytest.raises(RuntimeError, match="injected crash"):
            _until_target(pipeline, sink)
    finally:
        listener.close()
    monkeypatch.setattr(store, "save", original_save)
    after = store.load(name)
    assert after is not None
    if failure in ("after_save", "ack"):
        assert after.last_consumed_version > before.last_consumed_version
    else:
        assert after.last_consumed_version >= before.last_consumed_version
    sink = Sink(sink.records)
    sink.target = prefix + b"target"
    resumed = FDBMutationListener(client, name, cursor=after)
    try:
        resumed.start()
        pipeline = pipeline_for(resumed, sink, store, "test")
        if failure not in ("after_save", "ack"):
            _until_target(pipeline, sink)
        sink.target = prefix + b"sentinel"
        sink.saw_target = False
        client.db[sink.target] = b"sentinel"
        _until_target(pipeline, sink)
    finally:
        resumed.close()
    keys = {
        m.single_key_mutation.key
        for r in sink.records
        if r.HasField("batch")
        for m in r.batch.mutations
        if m.HasField("single_key_mutation")
    }
    assert {prefix + b"initial", prefix + b"target", prefix + b"sentinel"} <= keys


def test_stream_reregistration_is_rejected(
    resources: tuple[FDBClient, CheckpointStore, bytes, bytes],
) -> None:
    client, store, name, prefix = resources
    with FDBMutationListener(client, name) as listener:
        sink = Sink()
        sink.target = prefix + b"initial"
        client.db[sink.target] = b"v"
        _until_target(pipeline_for(listener, sink, store, "test"), sink)
    previous = store.load(name)
    client.remove_cdc_stream(name)
    new_id = client.register_cdc_stream(name, prefix, strinc(prefix))
    assert previous is not None and previous.stream_id != new_id
    with pytest.raises(StartupError, match="another stream"):
        resolve_startup(client, store, name, (prefix, strinc(prefix)))
    assert store.load(name) == previous


def test_fresh_consumer_skips_acked_data_and_foreign_ack_invalidates_old_cursor(
    resources: tuple[FDBClient, CheckpointStore, bytes, bytes],
) -> None:
    from fdb_kafka_bridge.cdc.errors import CDCInvalidCursorError

    client, store, name, prefix = resources
    sink = Sink()
    sink.target = prefix + b"first"
    with FDBMutationListener(client, name) as listener:
        client.db[sink.target] = b"first"
        _until_target(pipeline_for(listener, sink, store, "test"), sink)
    saved = store.load(name)
    assert saved is not None
    fresh_sink = Sink()
    fresh_sink.target = prefix + b"second"
    with FDBMutationListener(client, name) as fresh:
        client.db[fresh_sink.target] = b"second"
        _until_target(pipeline_for(fresh, fresh_sink, store, "test"), fresh_sink)
    assert all(
        m.single_key_mutation.key != prefix + b"first"
        for record in fresh_sink.records
        if record.HasField("batch")
        for m in record.batch.mutations
        if m.HasField("single_key_mutation")
    )
    stale = FDBMutationListener(client, name, cursor=saved)
    try:
        with pytest.raises(CDCInvalidCursorError):
            stale.start()
            stale.poll_records()
    finally:
        stale.close()


@pytest.mark.parametrize("delta", [0, 1, 2])
def test_live_retention_boundary(
    resources: tuple[FDBClient, CheckpointStore, bytes, bytes], delta: int
) -> None:
    from fdb_kafka_bridge.cdc.checkpoint import CheckpointCursor
    from fdb_kafka_bridge.cdc.errors import CDCInvalidCursorError

    client, store, name, prefix = resources
    stream = next(s for s in client.list_cdc_streams() if s.name == name)
    cursor = CheckpointCursor(stream.stream_id, stream.min_version - delta)
    if delta == 2:
        with pytest.raises(CDCInvalidCursorError, match="retained history"):
            client.validate_resume_cursor(name, cursor)
        native = client.resume_cdc_consumer(cursor)
        try:
            with pytest.raises(Exception) as error:
                native.acknowledge().wait()
                native.consume().wait()
            assert error.value.code == 1007
        finally:
            native.close()
    else:
        with FDBMutationListener(client, name, cursor=cursor) as listener:
            client.db[prefix + b"boundary"] = b"ok"
            records = listener.poll_records()
            assert records


def test_live_parked_consume_is_cancelled_on_stop(
    resources: tuple[FDBClient, CheckpointStore, bytes, bytes],
) -> None:
    import threading
    import time

    from fdb_kafka_bridge.cdc.errors import CDCConsumerClosedError

    client, store, name, prefix = resources
    listener = FDBMutationListener(client, name)
    listener.start()
    failures = []

    def consume() -> None:
        try:
            while True:
                records = listener.poll_records()
                if records:
                    listener.acknowledge()
        except CDCConsumerClosedError as exc:
            failures.append(exc)

    worker = threading.Thread(target=consume, daemon=True)
    worker.start()
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with listener._operation_lock:
                future = listener._pending_future
                parked = (
                    listener._pending_action == "consume"
                    and future is not None
                    and not future.is_ready()
                )
            if parked:
                break
            time.sleep(0.001)
        else:
            pytest.fail("Consumer did not park")
        began = time.monotonic()
        listener.cancel_pending()
        worker.join(1)
        assert not worker.is_alive()
        assert time.monotonic() - began < 1
        assert failures
    finally:
        listener.close()
        worker.join(1)


def test_ack_timeout_cancels_native_future(
    resources: tuple[FDBClient, CheckpointStore, bytes, bytes],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fdb_kafka_bridge.cdc.errors import CDCRetryableError

    client, store, name, prefix = resources
    listener = FDBMutationListener(client, name, ack_timeout_seconds=0.01)
    listener.start()
    tr = client.db.create_transaction()
    pending = tr.watch(prefix + b"never-changes")
    tr.commit().wait()
    try:
        listener._awaiting_ack = True
        monkeypatch.setattr(listener.consumer, "acknowledge", lambda: pending)
        with pytest.raises(CDCRetryableError, match="Timed out"):
            listener.acknowledge()
        with pytest.raises(Exception) as error:
            pending.wait()
        assert error.value.code == 1101
        assert listener.awaiting_ack
    finally:
        pending.cancel()
        listener.close()
