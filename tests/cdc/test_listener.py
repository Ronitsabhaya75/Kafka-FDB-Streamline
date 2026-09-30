"""Unit tests for the FoundationDB CDC listener."""

from collections import namedtuple
from unittest.mock import MagicMock, patch

import pytest

from src.cdc.client import FDBClient
from src.cdc.errors import (
    CDCConsumerClosedError,
    CDCError,
    CDCInvalidCursorError,
    CDCRetryableError,
    CDCTerminalError,
)
from src.cdc.listener import FDBMutationListener, strinc, subspace_to_key_range
from src.serialization.errors import InputTypeError


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

    with patch("src.cdc.listener.time.sleep") as sleep:
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
        patch("src.cdc.listener.time.monotonic", side_effect=[0.0, 2.0]),
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
