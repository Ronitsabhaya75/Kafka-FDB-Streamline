"""Unit tests for the FoundationDB CDC listener."""

from collections import namedtuple
from unittest.mock import MagicMock, patch

import pytest

from fdb_kafka_bridge.cdc.client import FDBClient
from fdb_kafka_bridge.cdc.errors import (
    CDCConsumerClosedError,
    CDCError,
    CDCInvalidCursorError,
    CDCNotSupportedError,
    CDCRetryableError,
    CDCTerminalError,
)
from fdb_kafka_bridge.cdc.listener import (
    FDBMutationListener,
    strinc,
    subspace_to_key_range,
)
from fdb_kafka_bridge.serialization.errors import InputTypeError, RecordTooLargeError


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


# Pins INV-CDC-7.
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


# Pins INV-CDC-7.
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


# Pins INV-CDC-14.
def test_lazy_start_in_poll_keeps_cursor_timeout_class() -> None:
    client = MagicMock(spec=FDBClient)
    client.resume_cdc_consumer.return_value = MagicMock()
    client.get_read_version.return_value = 499
    listener = FDBMutationListener(
        client, "stream", cursor=MockCursor(42, 500), resume_timeout_seconds=1
    )

    with (
        patch("fdb_kafka_bridge.cdc.listener.time.monotonic", side_effect=[0.0, 2.0]),
        pytest.raises(CDCInvalidCursorError, match="Timed out"),
    ):
        listener.poll_records()


# Pins INV-CDC-14.
def test_lazy_start_in_poll_keeps_not_supported_class() -> None:
    client = MagicMock(spec=FDBClient)
    client.create_cdc_consumer.side_effect = CDCNotSupportedError("no native CDC")
    listener = FDBMutationListener(client, "stream")

    with pytest.raises(CDCNotSupportedError, match="no native CDC"):
        listener.poll_records()


# Pins INV-CDC-1.
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


# Pins INV-CDC-6.
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


# Pins INV-CDC-5.
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


# Pins INV-CDC-3.
def test_serialization_failure_blocks_ack_and_poll() -> None:
    malformed = MockCdcMutation(0, "not-bytes", b"value")
    listener, _, consumer = _listener_with_batch(
        MockConsumeResult([MockVersionGroup(1000, [malformed])], 1000)
    )

    with pytest.raises(InputTypeError):
        listener.poll_records(bridge_timestamp_ns=1_000_000)
    assert listener.delivery_failed
    with pytest.raises(CDCError, match="not completely serialized"):
        listener.acknowledge()
    with pytest.raises(CDCError, match="could not be serialized"):
        listener.poll_records(bridge_timestamp_ns=1_000_000)

    consumer.acknowledge.assert_not_called()
    assert consumer.consume.call_count == 1


def test_close_propagates_native_error() -> None:
    listener, _, consumer = _listener_with_batch(MockConsumeResult([], -1))
    listener.start()
    consumer.close.side_effect = RuntimeError("close failed")

    with pytest.raises(RuntimeError, match="close failed"):
        listener.close()


# Pins INV-CDC-13.
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
# Pins INV-CDC-8.
def test_poll_records_maps_terminal_fdb_codes(code: int) -> None:
    listener = _listener_whose_consume_raises(_FakeFDBError(code))

    with pytest.raises(CDCTerminalError) as excinfo:
        listener.poll_records()

    assert excinfo.value.code == code


# Pins INV-CDC-8.
def test_poll_records_maps_server_overloaded_to_retryable() -> None:
    listener = _listener_whose_consume_raises(_FakeFDBError(1211))

    with pytest.raises(CDCRetryableError) as excinfo:
        listener.poll_records()

    assert excinfo.value.code == 1211


# Pins INV-CDC-8.
def test_poll_records_passes_through_unknown_fdb_error() -> None:
    err = _FakeFDBError(1020)
    listener = _listener_whose_consume_raises(err)

    with pytest.raises(_FakeFDBError) as excinfo:
        listener.poll_records()

    assert excinfo.value is err


# Pins INV-CDC-8.
def test_poll_records_wraps_non_native_failure_as_cdc_error() -> None:
    listener = _listener_whose_consume_raises(ValueError("boom"))

    with pytest.raises(CDCError, match="consume CDC reply") as excinfo:
        listener.poll_records()

    assert type(excinfo.value) is CDCError
    assert isinstance(excinfo.value.__cause__, ValueError)


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


# Pins INV-CDC-2.
def test_zero_record_reply_needs_no_ack_and_does_not_block_poll() -> None:
    listener, _, consumer = _listener_with_batch(MockConsumeResult([], -1))

    assert listener.poll_records(bridge_timestamp_ns=1) == []
    assert not listener.awaiting_ack
    assert listener.poll_records(bridge_timestamp_ns=1) == []

    consumer.acknowledge.assert_not_called()
    assert consumer.consume.call_count == 2


# Pins INV-CDC-4.
def test_failed_native_ack_can_be_retried_in_place() -> None:
    batch = MockConsumeResult(
        [MockVersionGroup(1000, [MockCdcMutation(0, b"k", b"v")])], 1000
    )
    listener, _, consumer = _listener_with_batch(batch)
    listener.poll_records(bridge_timestamp_ns=1)
    consumer.acknowledge.return_value.wait.side_effect = [_FakeFDBError(1211), None]

    with pytest.raises(CDCRetryableError):
        listener.acknowledge()
    assert listener.awaiting_ack

    listener.acknowledge()
    assert not listener.awaiting_ack
    assert consumer.acknowledge.return_value.wait.call_count == 2


# Pins INV-CDC-5.
def test_oversized_watermark_version_end_raises_and_blocks_delivery() -> None:
    listener, _, _ = _listener_with_batch(
        MockConsumeResult([], 2500), max_record_bytes=1
    )

    with pytest.raises(RecordTooLargeError, match="watermark version end"):
        listener.poll_records(bridge_timestamp_ns=1)
    assert listener.delivery_failed


@pytest.mark.parametrize("watermark", [-1, 999, 1000])
# Pins INV-CDC-6.
def test_no_watermark_version_end_unless_beyond_last_group(watermark: int) -> None:
    batch = MockConsumeResult(
        [MockVersionGroup(1000, [MockCdcMutation(0, b"k", b"v")])], watermark
    )
    listener, _, _ = _listener_with_batch(batch)

    records = listener.poll_records(bridge_timestamp_ns=1)

    ends = [record.version_end for record in records if record.HasField("version_end")]
    assert [end.fdb_version for end in ends] == [1000]


# Pins INV-CDC-7.
def test_resume_reack_failure_is_classified_and_closes_partial_consumer() -> None:
    client = MagicMock(spec=FDBClient)
    consumer = MagicMock()
    client.resume_cdc_consumer.return_value = consumer
    client.get_read_version.return_value = 500
    consumer.acknowledge.return_value.wait.side_effect = _FakeFDBError(1007)
    listener = FDBMutationListener(client, "stream", cursor=MockCursor(42, 500))

    with pytest.raises(CDCTerminalError) as excinfo:
        listener.start()

    assert excinfo.value.code == 1007
    consumer.close.assert_called_once()
    consumer.consume.assert_not_called()


# Pins INV-CDC-9.
def test_close_never_acks_or_removes_stream() -> None:
    batch = MockConsumeResult(
        [MockVersionGroup(1000, [MockCdcMutation(0, b"k", b"v")])], 1000
    )
    listener, client, consumer = _listener_with_batch(batch, key_range=(b"a", b"z"))
    listener.poll_records(bridge_timestamp_ns=1)

    listener.close()

    consumer.close.assert_called_once()
    consumer.acknowledge.assert_not_called()
    client.remove_cdc_stream.assert_not_called()


# Pins INV-CDC-12.
def test_subspace_listener_registers_strinc_bounded_range() -> None:
    client = MagicMock(spec=FDBClient)
    client.create_cdc_consumer.return_value = MagicMock()
    listener = FDBMutationListener(client, "stream", subspace=b"p\xff")

    listener.start()

    client.register_cdc_stream.assert_called_once_with(b"stream", b"p\xff", b"q")


# Pins INV-CDC-13.
def test_close_racing_consume_raises_closed_error() -> None:
    listener, _, consumer = _listener_with_batch(MockConsumeResult([], -1))

    def close_then_fail() -> None:
        listener.close()
        raise _FakeFDBError(1101)

    consumer.consume.return_value.wait.side_effect = close_then_fail

    with pytest.raises(CDCConsumerClosedError):
        listener.poll_records()
