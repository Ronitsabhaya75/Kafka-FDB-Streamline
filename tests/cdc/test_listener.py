"""Unit tests for the FoundationDB CDC listener."""

import math
from collections import namedtuple
from unittest.mock import MagicMock, patch

import pytest

from fdb_kafka_bridge.cdc.client import FDBClient
from fdb_kafka_bridge.cdc.errors import (
    CDCConsumerClosedError,
    CDCError,
    CDCInvalidCursorError,
    CDCRetryableError,
    CDCTerminalError,
)
from fdb_kafka_bridge.cdc.listener import (
    FDBMutationListener,
    strinc,
    subspace_to_key_range,
)
from fdb_kafka_bridge.serialization.errors import InputTypeError
from fdbkafka.cdc.v1 import mutations_pb2


class _FakeFDBError(Exception):
    """Stand-in for ``fdb.FDBError`` carrying a native error code."""

    def __init__(self, code: int) -> None:
        super().__init__(f"fdb error {code}")
        self.code = code


MockCdcMutation = namedtuple("MockCdcMutation", ["type", "param1", "param2"])
MockVersionGroup = namedtuple("MockVersionGroup", ["version", "mutations"])
MockConsumeResult = namedtuple(
    "MockConsumeResult", ["mutations", "last_consumed_version"]
)
MockCursor = namedtuple("MockCursor", ["stream_id", "last_consumed_version"])


def _listener_with_batch(
    batch: MockConsumeResult,
    **kwargs: object,
) -> tuple[FDBMutationListener, MagicMock, MagicMock]:
    """Build a listener whose native consumer returns ``batch``."""
    client = MagicMock(spec=FDBClient)
    consumer = MagicMock()
    client.create_cdc_consumer.return_value = consumer
    consumer.consume.return_value.wait.return_value = batch
    listener = FDBMutationListener(client, "test-stream", **kwargs)
    return listener, client, consumer


def test_strinc_basic_and_trailing_ff() -> None:
    assert strinc(b"abc") == b"abd"
    assert strinc(b"prefix\x00") == b"prefix\x01"
    assert strinc(b"prefix\x00\xff") == b"prefix\x01"


@pytest.mark.parametrize("key", [b"", b"\xff\xff"])
def test_strinc_rejects_keys_without_successor(key: bytes) -> None:
    with pytest.raises(ValueError, match="at least one byte"):
        strinc(key)


def test_subspace_to_key_range_accepts_bytes_and_subspace() -> None:
    assert subspace_to_key_range(b"users:") == (b"users:", strinc(b"users:"))
    subspace = MagicMock()
    subspace.key.return_value = b"prefix"
    assert subspace_to_key_range(subspace) == (b"prefix", b"prefiy")


def test_subspace_to_key_range_rejects_other_values() -> None:
    with pytest.raises(TypeError, match="Expected FDB Subspace or bytes"):
        subspace_to_key_range(123)


def test_constructor_rejects_ambiguous_registration_range() -> None:
    with pytest.raises(ValueError, match="either key_range or subspace"):
        FDBMutationListener(
            MagicMock(spec=FDBClient),
            "stream",
            key_range=(b"a", b"z"),
            subspace=b"prefix",
        )


@pytest.mark.parametrize("name", ["stream", b"stream", "€" * 10, "s" * 10_000])
def test_default_max_record_bytes_derived_from_stream_name_key(
    name: str | bytes,
) -> None:
    raw = name if isinstance(name, bytes) else name.encode("utf-8")
    listener = FDBMutationListener(MagicMock(spec=FDBClient), name)
    assert listener.max_record_bytes == 1_000_000 - len(raw) - 36


def test_explicit_max_record_bytes_preserved() -> None:
    listener = FDBMutationListener(
        MagicMock(spec=FDBClient), "stream", max_record_bytes=500
    )
    assert listener.max_record_bytes == 500


# Worst-case VersionEnd for a 6-byte name is 56 bytes (8 of them name + framing).
def test_explicit_max_record_bytes_below_largest_version_end_rejected() -> None:
    client = MagicMock(spec=FDBClient)
    assert FDBMutationListener(client, "stream", max_record_bytes=56)
    with pytest.raises(ValueError, match="smaller than the 56-byte"):
        FDBMutationListener(client, "stream", max_record_bytes=55)


# Name of L >= 16_384 bytes: worst-case VersionEnd is L + 52, derived budget is
# 999_964 - L, so the longest workable name is 499_956 bytes.
def test_stream_name_too_long_for_any_version_end_rejected() -> None:
    client = MagicMock(spec=FDBClient)
    fits = FDBMutationListener(client, "s" * 499_956)
    assert fits.max_record_bytes == 499_956 + 52
    with pytest.raises(ValueError, match="smaller than the 500009-byte"):
        FDBMutationListener(client, "s" * 499_957)


def test_stream_name_leaving_no_record_budget_rejected() -> None:
    with pytest.raises(ValueError, match="stream_name"):
        FDBMutationListener(MagicMock(spec=FDBClient), "s" * (1_000_000 - 36))


def test_start_registers_only_when_range_is_supplied() -> None:
    client = MagicMock(spec=FDBClient)
    client.create_cdc_consumer.return_value = MagicMock()
    listener = FDBMutationListener(client, "stream", key_range=(b"a", b"z"))

    listener.start()

    client.register_cdc_stream.assert_called_once_with(b"stream", b"a", b"z")
    client.create_cdc_consumer.assert_called_once_with(b"stream")


def test_start_consumes_pre_registered_stream_without_a_range() -> None:
    client = MagicMock(spec=FDBClient)
    client.create_cdc_consumer.return_value = MagicMock()
    listener = FDBMutationListener(client, "stream")

    listener.start()

    client.register_cdc_stream.assert_not_called()
    client.create_cdc_consumer.assert_called_once_with(b"stream")


def test_start_propagates_registration_failure() -> None:
    client = MagicMock(spec=FDBClient)
    client.register_cdc_stream.side_effect = RuntimeError("range conflict")
    listener = FDBMutationListener(client, "stream", key_range=(b"a", b"z"))

    with pytest.raises(RuntimeError, match="range conflict"):
        listener.start()

    client.create_cdc_consumer.assert_not_called()


def test_resume_waits_for_read_version_and_restores_ack() -> None:
    client = MagicMock(spec=FDBClient)
    consumer = MagicMock()
    cursor = MockCursor(42, 500)
    client.resume_cdc_consumer.return_value = consumer
    client.get_read_version.side_effect = [499, 500]
    listener = FDBMutationListener(
        client,
        "stream",
        cursor=cursor,
        resume_poll_interval_seconds=0.001,
    )

    with patch("fdb_kafka_bridge.cdc.listener.time.sleep") as sleep:
        listener.start()

    client.resume_cdc_consumer.assert_called_once_with(cursor)
    assert client.get_read_version.call_count == 2
    sleep.assert_called_once()
    consumer.acknowledge.return_value.wait.assert_called_once()


def test_resume_timeout_closes_partial_consumer() -> None:
    client = MagicMock(spec=FDBClient)
    consumer = MagicMock()
    client.resume_cdc_consumer.return_value = consumer
    client.get_read_version.return_value = 499
    listener = FDBMutationListener(
        client,
        "stream",
        cursor=MockCursor(42, 500),
        resume_timeout_seconds=1,
    )

    with (
        patch("fdb_kafka_bridge.cdc.listener.time.monotonic", side_effect=[0.0, 2.0]),
        pytest.raises(CDCInvalidCursorError, match="Timed out"),
    ):
        listener.start()

    consumer.close.assert_called_once()
    consumer.acknowledge.assert_not_called()


def test_poll_records_serializes_group_and_requires_ack() -> None:
    batch = MockConsumeResult(
        [MockVersionGroup(1000, [MockCdcMutation(0, b"key", b"value")])],
        1000,
    )
    listener, _, consumer = _listener_with_batch(batch)

    records = listener.poll_records(bridge_timestamp_ns=1_000_000)

    assert [record.WhichOneof("record") for record in records] == [
        "batch",
        "version_end",
    ]
    assert records[0].batch.mutations[0].single_key_mutation.key == b"key"
    assert records[1].version_end.total_mutations == 1
    with pytest.raises(CDCError, match="acknowledged before polling"):
        listener.poll_records()
    listener.acknowledge()
    consumer.acknowledge.return_value.wait.assert_called_once()


def test_poll_records_preserves_watermark_after_final_group() -> None:
    batch = MockConsumeResult(
        [MockVersionGroup(1000, [MockCdcMutation(0, b"key", b"value")])],
        1050,
    )
    listener, _, _ = _listener_with_batch(batch)

    records = listener.poll_records(bridge_timestamp_ns=1_000_000)

    ends = [record.version_end for record in records if record.HasField("version_end")]
    assert [(end.fdb_version, end.total_mutations) for end in ends] == [
        (1000, 1),
        (1050, 0),
    ]


def test_poll_records_empty_watermark_requires_ack() -> None:
    listener, _, consumer = _listener_with_batch(MockConsumeResult([], 2500))

    records = listener.poll_records(bridge_timestamp_ns=1_000_000)

    assert len(records) == 1
    assert records[0].version_end.fdb_version == 2500
    listener.acknowledge()
    consumer.acknowledge.return_value.wait.assert_called_once()


def test_poll_records_slices_large_version_group_to_record_budget() -> None:
    mutations = [MockCdcMutation(0, f"key-{i}".encode(), b"v" * 50) for i in range(5)]
    listener, _, _ = _listener_with_batch(
        MockConsumeResult([MockVersionGroup(1000, mutations)], 1000),
        max_record_bytes=150,
    )

    records = listener.poll_records(bridge_timestamp_ns=1_000_000)

    batches = [record for record in records if record.HasField("batch")]
    assert len(batches) > 1
    assert all(record.ByteSize() <= 150 for record in records)
    assert records[-1].version_end.total_mutations == 5


def test_serialization_failure_cannot_be_acknowledged() -> None:
    malformed = MockCdcMutation(0, "not-bytes", b"value")
    listener, _, consumer = _listener_with_batch(
        MockConsumeResult([MockVersionGroup(1000, [malformed])], 1000)
    )

    with pytest.raises(InputTypeError):
        listener.poll_records(bridge_timestamp_ns=1_000_000)
    with pytest.raises(CDCError, match="not completely serialized"):
        listener.acknowledge()

    consumer.acknowledge.assert_not_called()


def test_close_propagates_native_error() -> None:
    listener, _, consumer = _listener_with_batch(MockConsumeResult([], -1))
    listener.start()
    consumer.close.side_effect = RuntimeError("close failed")

    with pytest.raises(RuntimeError, match="close failed"):
        listener.close()


def test_closed_listener_rejects_poll_and_ack() -> None:
    listener, _, _ = _listener_with_batch(MockConsumeResult([], -1))
    listener.close()

    with pytest.raises(CDCConsumerClosedError):
        listener.poll_records()
    with pytest.raises(CDCConsumerClosedError):
        listener.acknowledge()


def _listener_whose_consume_raises(exc: BaseException) -> FDBMutationListener:
    """Build a listener whose native consume raises ``exc``."""
    client = MagicMock(spec=FDBClient)
    consumer = MagicMock()
    client.create_cdc_consumer.return_value = consumer
    consumer.consume.return_value.wait.side_effect = exc
    return FDBMutationListener(client, "test-stream")


@pytest.mark.parametrize("code", [1007, 2000])
def test_poll_records_maps_terminal_fdb_codes(code: int) -> None:
    listener = _listener_whose_consume_raises(_FakeFDBError(code))

    with pytest.raises(CDCTerminalError) as excinfo:
        listener.poll_records()

    assert excinfo.value.code == code


def test_poll_records_maps_server_overloaded_to_retryable() -> None:
    listener = _listener_whose_consume_raises(_FakeFDBError(1211))

    with pytest.raises(CDCRetryableError) as excinfo:
        listener.poll_records()

    assert excinfo.value.code == 1211


def test_poll_records_passes_through_unknown_fdb_error() -> None:
    err = _FakeFDBError(1020)
    listener = _listener_whose_consume_raises(err)

    with pytest.raises(_FakeFDBError) as excinfo:
        listener.poll_records()

    assert excinfo.value is err


def test_poll_records_wraps_non_native_failure_as_cdc_error() -> None:
    listener = _listener_whose_consume_raises(ValueError("boom"))

    with pytest.raises(CDCError, match="consume CDC reply"):
        listener.poll_records()


def test_acknowledge_maps_native_error_code() -> None:
    batch = MockConsumeResult(
        [MockVersionGroup(1000, [MockCdcMutation(0, b"k", b"v")])], 1000
    )
    listener, _, consumer = _listener_with_batch(batch)
    listener.poll_records(bridge_timestamp_ns=1)
    consumer.acknowledge.return_value.wait.side_effect = _FakeFDBError(1211)

    with pytest.raises(CDCRetryableError) as excinfo:
        listener.acknowledge()

    assert excinfo.value.code == 1211


class _PendingFuture:
    def __init__(self) -> None:
        import threading

        self.ready = threading.Event()
        self.cancelled = False
        self.callback = None

    def is_ready(self) -> bool:
        return self.ready.is_set()

    def on_ready(self, callback: object) -> None:
        self.callback = callback
        if self.ready.is_set():
            callback(self)

    def cancel(self) -> None:
        self.cancelled = True
        self.ready.set()
        if self.callback is not None:
            self.callback(self)

    def wait(self) -> None:
        assert self.ready.wait(2)
        if self.cancelled:
            raise _FakeFDBError(1101)


@pytest.mark.parametrize("resumed", [False, True])
def test_ack_timeout_cancels_future_and_retains_pending_reply(resumed: bool) -> None:
    batch = MockConsumeResult([], 100)
    listener, client, consumer = _listener_with_batch(batch, ack_timeout_seconds=0.01)
    pending = _PendingFuture()
    if resumed:
        listener.initial_cursor = MockCursor(1, 100)
        client.resume_cdc_consumer.return_value = consumer
        client.get_read_version.return_value = 100
    else:
        listener.poll_records()
    consumer.acknowledge.return_value = pending
    with pytest.raises(CDCRetryableError, match="Timed out"):
        listener.start() if resumed else listener.acknowledge()
    assert pending.cancelled
    assert listener.awaiting_ack is not resumed
    if not resumed:
        consumer.acknowledge.return_value = MagicMock()
        listener.acknowledge()
        consumer.consume.assert_called_once()


@pytest.mark.parametrize("close", [False, True])
def test_cancel_parked_consume_without_ack(close: bool) -> None:
    import threading

    listener, _, consumer = _listener_with_batch(MockConsumeResult([], 100))
    pending = _PendingFuture()
    entered = threading.Event()

    def consume() -> _PendingFuture:
        entered.set()
        return pending

    consumer.consume.side_effect = consume
    errors = []

    def poll() -> None:
        try:
            listener.poll_records()
        except Exception as exc:
            errors.append(exc)

    worker = threading.Thread(target=poll)
    worker.start()
    assert entered.wait(2)
    listener.close() if close else listener.cancel_pending()
    worker.join(2)
    assert not worker.is_alive()
    assert pending.cancelled
    assert isinstance(errors[0], CDCConsumerClosedError)
    consumer.acknowledge.assert_not_called()
    with pytest.raises(CDCConsumerClosedError):
        listener.poll_records()


def test_first_consume_collision_is_retried_only_after_resume() -> None:
    listener, client, consumer = _listener_with_batch(MockConsumeResult([], 100))
    listener.initial_cursor = MockCursor(1, 90)
    listener._restart_poll = True
    client.resume_cdc_consumer.return_value = consumer
    client.get_read_version.return_value = 100
    consumer.consume.return_value.wait.side_effect = [
        _FakeFDBError(2000),
        MockConsumeResult([], 100),
        _FakeFDBError(2000),
    ]
    assert listener.poll_records()
    listener.acknowledge()
    with pytest.raises(CDCTerminalError) as error:
        listener.poll_records()
    assert error.value.code == 2000
    assert consumer.consume.call_count == 3


def test_restart_collision_window_expires() -> None:
    listener = _listener_whose_consume_raises(_FakeFDBError(2000))
    listener._restart_poll = True
    listener.proxy_consume_poll_timeout_seconds = 0.01
    with pytest.raises(CDCTerminalError) as error:
        listener.poll_records()
    assert error.value.code == 2000


@pytest.mark.parametrize(
    "name", ["ack_timeout_seconds", "proxy_consume_poll_timeout_seconds"]
)
@pytest.mark.parametrize("value", [0, -1, True, float("nan"), float("inf"), "5"])
def test_invalid_operation_timeouts(name: str, value: object) -> None:
    with pytest.raises(ValueError):
        FDBMutationListener(MagicMock(), "s", **{name: value})


def _listener_with_batches(
    batches: list[MockConsumeResult],
    **kwargs: object,
) -> tuple[FDBMutationListener, MagicMock, MagicMock]:
    """Build a listener whose native consumer returns ``batches`` in order."""
    listener, client, consumer = _listener_with_batch(batches[0], **kwargs)
    consumer.consume.return_value.wait.side_effect = batches
    return listener, client, consumer


def _version_ends(
    records: list[mutations_pb2.FDBMutationRecord],
) -> list[tuple[int, int]]:
    return [
        (record.version_end.fdb_version, record.version_end.total_mutations)
        for record in records
        if record.HasField("version_end")
    ]


def _consume_and_ack_order(consumer: MagicMock) -> list[str]:
    return [
        name
        for name, _, _ in consumer.mock_calls
        if name in ("consume().wait", "acknowledge().wait")
    ]


def test_idle_replies_without_coalescing_each_emit_watermark_and_require_ack() -> None:
    listener, _, consumer = _listener_with_batches(
        [MockConsumeResult([], 100), MockConsumeResult([], 200)]
    )

    first = listener.poll_records(bridge_timestamp_ns=1)
    with pytest.raises(CDCError, match="acknowledged before polling"):
        listener.poll_records()
    listener.acknowledge()
    second = listener.poll_records(bridge_timestamp_ns=1)
    listener.acknowledge()

    assert _version_ends(first) == [(100, 0)]
    assert _version_ends(second) == [(200, 0)]
    assert consumer.acknowledge.return_value.wait.call_count == 2


def test_idle_reply_below_threshold_is_skipped_without_ack() -> None:
    listener, _, consumer = _listener_with_batches(
        [MockConsumeResult([], 100), MockConsumeResult([], 200)],
        idle_ack_max_replies=3,
    )

    assert listener.poll_records(bridge_timestamp_ns=1) == []
    assert listener.poll_records(bridge_timestamp_ns=1) == []

    with pytest.raises(CDCError, match="awaiting acknowledgement"):
        listener.acknowledge()
    consumer.acknowledge.assert_not_called()


def test_idle_replies_flush_one_watermark_at_reply_threshold() -> None:
    listener, _, consumer = _listener_with_batches(
        [MockConsumeResult([], version) for version in (100, 200, 300, 400, 500)],
        idle_ack_max_replies=3,
    )

    assert listener.poll_records(bridge_timestamp_ns=1) == []
    assert listener.poll_records(bridge_timestamp_ns=1) == []
    flushed = listener.poll_records(bridge_timestamp_ns=1)
    with pytest.raises(CDCError, match="acknowledged before polling"):
        listener.poll_records()
    listener.acknowledge()

    assert _version_ends(flushed) == [(300, 0)]
    assert len(flushed) == 1
    assert _consume_and_ack_order(consumer) == [
        "consume().wait",
        "consume().wait",
        "consume().wait",
        "acknowledge().wait",
    ]
    assert listener.poll_records(bridge_timestamp_ns=1) == []
    assert listener.poll_records(bridge_timestamp_ns=1) == []


def test_idle_replies_flush_one_watermark_when_time_threshold_elapses() -> None:
    listener, _, consumer = _listener_with_batches(
        [MockConsumeResult([], version) for version in (100, 200, 300)],
        idle_ack_max_seconds=5.0,
    )

    with patch("fdb_kafka_bridge.cdc.listener.time.monotonic") as clock:
        # Non-zero: 0.0 is the window's pre-start value and would mask a missing reset.
        clock.return_value = 1000.0
        listener.start()
        clock.return_value = 1004.9
        assert listener.poll_records(bridge_timestamp_ns=1) == []
        clock.return_value = 1005.0
        flushed = listener.poll_records(bridge_timestamp_ns=1)
        listener.acknowledge()
        clock.return_value = 1009.9
        after_ack = listener.poll_records(bridge_timestamp_ns=1)

    assert _version_ends(flushed) == [(200, 0)]
    assert after_ack == []
    consumer.acknowledge.return_value.wait.assert_called_once()


def test_time_threshold_flushes_before_reply_threshold_when_both_set() -> None:
    listener, _, _ = _listener_with_batches(
        [MockConsumeResult([], version) for version in (100, 200)],
        idle_ack_max_replies=10,
        idle_ack_max_seconds=5.0,
    )

    with patch("fdb_kafka_bridge.cdc.listener.time.monotonic") as clock:
        clock.return_value = 1000.0
        listener.start()
        clock.return_value = 1004.9
        skipped = listener.poll_records(bridge_timestamp_ns=1)
        clock.return_value = 1005.0
        flushed = listener.poll_records(bridge_timestamp_ns=1)

    assert skipped == []
    assert _version_ends(flushed) == [(200, 0)]
    assert listener.awaiting_ack


def test_reply_without_watermark_does_not_count_toward_reply_threshold() -> None:
    listener, _, _ = _listener_with_batches(
        [MockConsumeResult([], version) for version in (-1, 100, 200)],
        idle_ack_max_replies=2,
    )

    assert listener.poll_records(bridge_timestamp_ns=1) == []
    assert listener.poll_records(bridge_timestamp_ns=1) == []
    flushed = listener.poll_records(bridge_timestamp_ns=1)

    assert _version_ends(flushed) == [(200, 0)]


def test_data_reply_after_skipped_idles_is_delivered_and_resets_window() -> None:
    data = MockConsumeResult(
        [MockVersionGroup(300, [MockCdcMutation(0, b"key", b"value")])], 350
    )
    listener, _, consumer = _listener_with_batches(
        [
            MockConsumeResult([], 100),
            MockConsumeResult([], 200),
            data,
            MockConsumeResult([], 400),
            MockConsumeResult([], 500),
        ],
        idle_ack_max_replies=3,
    )

    assert listener.poll_records(bridge_timestamp_ns=1) == []
    assert listener.poll_records(bridge_timestamp_ns=1) == []
    records = listener.poll_records(bridge_timestamp_ns=1)
    with pytest.raises(CDCError, match="acknowledged before polling"):
        listener.poll_records()
    listener.acknowledge()

    assert records[-1].HasField("version_end")
    assert _version_ends(records) == [(300, 1), (350, 0)]
    assert _consume_and_ack_order(consumer) == [
        "consume().wait",
        "consume().wait",
        "consume().wait",
        "acknowledge().wait",
    ]
    assert listener.poll_records(bridge_timestamp_ns=1) == []
    assert listener.poll_records(bridge_timestamp_ns=1) == []


def test_resume_starts_a_fresh_idle_window() -> None:
    client = MagicMock(spec=FDBClient)
    consumer = MagicMock()
    client.resume_cdc_consumer.return_value = consumer
    client.get_read_version.return_value = 500
    consumer.consume.return_value.wait.side_effect = [
        MockConsumeResult([], 600),
        MockConsumeResult([], 700),
    ]
    listener = FDBMutationListener(
        client,
        "stream",
        cursor=MockCursor(42, 500),
        idle_ack_max_seconds=5.0,
    )

    with patch("fdb_kafka_bridge.cdc.listener.time.monotonic") as clock:
        clock.return_value = 1000.0
        listener.start()
        clock.return_value = 1004.9
        skipped = listener.poll_records(bridge_timestamp_ns=1)
        clock.return_value = 1005.0
        flushed = listener.poll_records(bridge_timestamp_ns=1)

    assert skipped == []
    assert _version_ends(flushed) == [(700, 0)]


def test_close_mid_idle_window_then_resume_reacks_cursor_and_coalesces_afresh() -> None:
    data = MockConsumeResult(
        [MockVersionGroup(500, [MockCdcMutation(0, b"key", b"value")])], 500
    )
    first, client, first_consumer = _listener_with_batches(
        [data, MockConsumeResult([], 600), MockConsumeResult([], 700)],
        idle_ack_max_replies=3,
    )
    first.poll_records(bridge_timestamp_ns=1)
    first.acknowledge()
    assert first.poll_records(bridge_timestamp_ns=1) == []
    assert first.poll_records(bridge_timestamp_ns=1) == []
    first.close()

    first_consumer.acknowledge.return_value.wait.assert_called_once()
    first_consumer.close.assert_called_once()

    resumed_consumer = MagicMock()
    client.resume_cdc_consumer.return_value = resumed_consumer
    client.get_read_version.return_value = 500
    resumed_consumer.consume.return_value.wait.side_effect = [
        MockConsumeResult([], version) for version in (600, 700, 800)
    ]
    cursor = MockCursor(42, 500)
    resumed = FDBMutationListener(
        client, "test-stream", cursor=cursor, idle_ack_max_replies=3
    )
    resumed.start()

    client.resume_cdc_consumer.assert_called_once_with(cursor)
    resumed_consumer.acknowledge.return_value.wait.assert_called_once()
    assert resumed.poll_records(bridge_timestamp_ns=1) == []
    assert resumed.poll_records(bridge_timestamp_ns=1) == []
    flushed = resumed.poll_records(bridge_timestamp_ns=1)
    resumed.acknowledge()

    assert _version_ends(flushed) == [(800, 0)]
    assert _consume_and_ack_order(resumed_consumer) == [
        "acknowledge().wait",
        "consume().wait",
        "consume().wait",
        "consume().wait",
        "acknowledge().wait",
    ]


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"idle_ack_max_replies": True}, TypeError),
        ({"idle_ack_max_replies": 2.0}, TypeError),
        ({"idle_ack_max_replies": 0}, ValueError),
        ({"idle_ack_max_replies": -1}, ValueError),
        ({"idle_ack_max_seconds": True}, TypeError),
        ({"idle_ack_max_seconds": "5"}, TypeError),
        ({"idle_ack_max_seconds": 0}, ValueError),
        ({"idle_ack_max_seconds": -0.5}, ValueError),
        ({"idle_ack_max_seconds": math.nan}, ValueError),
        ({"idle_ack_max_seconds": math.inf}, ValueError),
        ({"idle_ack_max_seconds": -math.inf}, ValueError),
        ({"idle_ack_max_seconds": 10**400}, ValueError),
    ],
)
def test_constructor_validates_idle_ack_thresholds(
    kwargs: dict[str, object], error: type[Exception]
) -> None:
    with pytest.raises(error, match=next(iter(kwargs))):
        FDBMutationListener(MagicMock(spec=FDBClient), "stream", **kwargs)


def test_constructor_accepts_integer_idle_ack_seconds() -> None:
    listener = FDBMutationListener(
        MagicMock(spec=FDBClient), "stream", idle_ack_max_seconds=5
    )

    assert isinstance(listener.idle_ack_max_seconds, float)
    assert listener.idle_ack_max_seconds == 5.0


def test_coalesced_idle_watermark_survives_ack_timeout() -> None:
    listener, _, consumer = _listener_with_batches(
        [MockConsumeResult([], 100), MockConsumeResult([], 101)],
        idle_ack_max_replies=2,
        ack_timeout_seconds=0.01,
    )
    assert listener.poll_records() == []
    assert _version_ends(listener.poll_records()) == [(101, 0)]
    pending = _PendingFuture()
    consumer.acknowledge.return_value = pending
    with pytest.raises(CDCRetryableError, match="Timed out"):
        listener.acknowledge()
    assert pending.cancelled
    assert listener.awaiting_ack
    assert listener._skipped_idle_replies == 2
    consumer.acknowledge.return_value = MagicMock()
    listener.acknowledge()
    assert not listener.awaiting_ack
    assert listener._skipped_idle_replies == 0
    assert consumer.consume.call_count == 2
