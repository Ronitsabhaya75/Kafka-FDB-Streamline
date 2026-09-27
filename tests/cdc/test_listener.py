"""Unit tests for FDBMutationListener, strinc, and subspace range computation."""

from collections import namedtuple
from unittest.mock import MagicMock

import pytest

from fdbkafka.cdc.v1 import mutations_pb2
from src.cdc.client import FDBClient
from src.cdc.errors import CDCConsumerClosedError, CDCError
from src.cdc.listener import (
    FDBMutationListener,
    strinc,
    subspace_to_key_range,
)

MockCdcMutation = namedtuple("MockCdcMutation", ["type", "param1", "param2"])
MockVersionGroup = namedtuple("MockVersionGroup", ["version", "mutations"])
MockConsumeResult = namedtuple(
    "MockConsumeResult", ["mutations", "last_consumed_version"]
)


def test_strinc_basic() -> None:
    assert strinc(b"abc") == b"abd"
    assert strinc(b"prefix\x00") == b"prefix\x01"


def test_strinc_trailing_ff() -> None:
    assert strinc(b"prefix\x00\xff") == b"prefix\x01"
    assert strinc(b"abc\xff\xff") == b"abd"


def test_strinc_invalid() -> None:
    with pytest.raises(ValueError, match="at least one byte"):
        strinc(b"")

    with pytest.raises(ValueError, match="at least one byte"):
        strinc(b"\xff\xff\xff")


def test_subspace_to_key_range_bytes() -> None:
    begin, end = subspace_to_key_range(b"users:")
    assert begin == b"users:"
    assert end == strinc(b"users:")


def test_subspace_to_key_range_object() -> None:
    mock_subspace = MagicMock()
    mock_subspace.key.return_value = b"\x15\x01prefix"
    begin, end = subspace_to_key_range(mock_subspace)
    assert begin == b"\x15\x01prefix"
    assert end == strinc(b"\x15\x01prefix")


def test_subspace_to_key_range_invalid() -> None:
    with pytest.raises(TypeError, match="Expected FDB Subspace or bytes"):
        subspace_to_key_range(12345)


def test_listener_start_and_poll() -> None:
    mock_client = MagicMock(spec=FDBClient)
    mock_consumer = MagicMock()
    mock_client.create_cdc_consumer.return_value = mock_consumer

    batch_result = MockConsumeResult(
        mutations=[
            MockVersionGroup(
                version=1000,
                mutations=[
                    MockCdcMutation(0, b"key1", b"val1"),
                    MockCdcMutation(1, b"start", b"end"),
                ],
            )
        ],
        last_consumed_version=1000,
    )
    mock_consumer.consume.return_value.wait.return_value = batch_result

    listener = FDBMutationListener(
        client=mock_client,
        stream_name="test_stream",
        key_range=(b"k1", b"k9"),
        auto_register=True,
    )

    listener.start()
    mock_client.register_cdc_stream.assert_called_once_with(
        b"test_stream", b"k1", b"k9"
    )
    mock_client.create_cdc_consumer.assert_called_once_with(b"test_stream")

    # Poll mutations
    mutations = listener.poll()
    assert len(mutations) == 2
    assert mutations[0].version_index.fdb_version == 1000
    assert mutations[0].version_index.sequence_no == 0
    assert mutations[0].single_key_mutation.key == b"key1"
    assert mutations[1].version_index.sequence_no == 1
    assert mutations[1].clear_range.begin_key == b"start"

    # Acknowledge
    listener.acknowledge()
    mock_consumer.acknowledge.return_value.wait.assert_called_once()

    listener.close()
    mock_consumer.close.assert_called_once()


def test_listener_propagates_registration_failure() -> None:
    mock_client = MagicMock(spec=FDBClient)
    mock_client.register_cdc_stream.side_effect = RuntimeError("range conflict")
    listener = FDBMutationListener(
        client=mock_client,
        stream_name="test_stream",
        key_range=(b"a", b"z"),
    )

    with pytest.raises(RuntimeError, match="range conflict"):
        listener.start()

    mock_client.create_cdc_consumer.assert_not_called()


def test_listener_resume_with_cursor() -> None:
    mock_client = MagicMock(spec=FDBClient)
    mock_consumer = MagicMock()
    mock_client.resume_cdc_consumer.return_value = mock_consumer

    cursor = (42, 500)
    listener = FDBMutationListener(
        client=mock_client,
        stream_name="test_stream",
        cursor=cursor,
    )
    listener.start()
    mock_client.resume_cdc_consumer.assert_called_once_with(cursor)
    mock_client.create_cdc_consumer.assert_not_called()


def test_listener_poll_records() -> None:
    mock_client = MagicMock(spec=FDBClient)
    mock_consumer = MagicMock()
    mock_client.create_cdc_consumer.return_value = mock_consumer

    batch_result = MockConsumeResult(
        mutations=[
            MockVersionGroup(
                version=2000,
                mutations=[MockCdcMutation(0, b"key1", b"val1")],
            )
        ],
        last_consumed_version=2000,
    )
    mock_consumer.consume.return_value.wait.return_value = batch_result

    with FDBMutationListener(client=mock_client, stream_name="stream_rec") as listener:
        records = listener.poll_records(bridge_timestamp_ns=1_000_000)
        assert len(records) == 2
        batch_record, version_end_record = records
        assert batch_record.stream_name == "stream_rec"
        assert batch_record.WhichOneof("record") == "batch"
        assert len(batch_record.batch.mutations) == 1
        assert version_end_record.WhichOneof("record") == "version_end"
        assert version_end_record.version_end.fdb_version == 2000
        assert version_end_record.version_end.total_mutations == 1


def test_listener_poll_records_emits_empty_watermark() -> None:
    mock_client = MagicMock(spec=FDBClient)
    mock_consumer = MagicMock()
    mock_client.create_cdc_consumer.return_value = mock_consumer
    mock_consumer.consume.return_value.wait.return_value = MockConsumeResult(
        mutations=[],
        last_consumed_version=2500,
    )
    listener = FDBMutationListener(client=mock_client, stream_name="stream_rec")

    records = listener.poll_records(bridge_timestamp_ns=1_000_000)

    assert len(records) == 1
    assert records[0].WhichOneof("record") == "version_end"
    assert records[0].version_end.fdb_version == 2500
    assert records[0].version_end.total_mutations == 0


def test_listener_closed_raises() -> None:
    mock_client = MagicMock(spec=FDBClient)
    listener = FDBMutationListener(client=mock_client, stream_name="s")
    listener.close()

    with pytest.raises(CDCConsumerClosedError):
        listener.poll()

    with pytest.raises(CDCConsumerClosedError):
        listener.acknowledge()


def test_listener_stream_mutations() -> None:
    mock_client = MagicMock(spec=FDBClient)
    mock_consumer = MagicMock()
    mock_client.create_cdc_consumer.return_value = mock_consumer

    batch_1 = MockConsumeResult(
        mutations=[
            MockVersionGroup(
                version=100,
                mutations=[
                    MockCdcMutation(0, b"k1", b"v1"),
                    MockCdcMutation(0, b"k2", b"v2"),
                ],
            )
        ],
        last_consumed_version=100,
    )
    mock_consumer.consume.return_value.wait.return_value = batch_1

    listener = FDBMutationListener(client=mock_client, stream_name="s")
    results: list[mutations_pb2.FDBMutation] = []
    for m in listener.stream_mutations(max_mutations=2, auto_ack=True):
        results.append(m)

    assert len(results) == 2
    assert results[0].single_key_mutation.key == b"k1"
    assert results[1].single_key_mutation.key == b"k2"
    mock_consumer.acknowledge.return_value.wait.assert_called_once()


def test_stream_without_auto_ack_returns_at_batch_boundary() -> None:
    mock_client = MagicMock(spec=FDBClient)
    mock_consumer = MagicMock()
    mock_client.create_cdc_consumer.return_value = mock_consumer
    mock_consumer.consume.return_value.wait.return_value = MockConsumeResult(
        mutations=[
            MockVersionGroup(
                version=100,
                mutations=[MockCdcMutation(0, b"k1", b"v1")],
            )
        ],
        last_consumed_version=100,
    )
    listener = FDBMutationListener(client=mock_client, stream_name="s")

    results = list(listener.stream_mutations())

    assert [m.single_key_mutation.key for m in results] == [b"k1"]
    assert mock_consumer.consume.call_count == 1
    mock_consumer.acknowledge.assert_not_called()
    listener.acknowledge()


def test_stream_limit_retains_unyielded_mutations_before_ack() -> None:
    mock_client = MagicMock(spec=FDBClient)
    mock_consumer = MagicMock()
    mock_client.create_cdc_consumer.return_value = mock_consumer
    mock_consumer.consume.return_value.wait.return_value = MockConsumeResult(
        mutations=[
            MockVersionGroup(
                version=100,
                mutations=[
                    MockCdcMutation(0, b"k1", b"v1"),
                    MockCdcMutation(0, b"k2", b"v2"),
                ],
            )
        ],
        last_consumed_version=100,
    )
    listener = FDBMutationListener(client=mock_client, stream_name="s")

    first = list(listener.stream_mutations(max_mutations=1, auto_ack=True))

    assert [m.single_key_mutation.key for m in first] == [b"k1"]
    mock_consumer.acknowledge.assert_not_called()
    with pytest.raises(CDCError, match="remain undelivered"):
        listener.acknowledge()

    second = list(listener.stream_mutations(max_mutations=1, auto_ack=True))

    assert [m.single_key_mutation.key for m in second] == [b"k2"]
    mock_consumer.acknowledge.return_value.wait.assert_called_once()
    assert mock_consumer.consume.call_count == 1


def test_record_limit_retains_version_end_before_ack() -> None:
    mock_client = MagicMock(spec=FDBClient)
    mock_consumer = MagicMock()
    mock_client.create_cdc_consumer.return_value = mock_consumer
    mock_consumer.consume.return_value.wait.return_value = MockConsumeResult(
        mutations=[
            MockVersionGroup(
                version=100,
                mutations=[MockCdcMutation(0, b"k1", b"v1")],
            )
        ],
        last_consumed_version=100,
    )
    listener = FDBMutationListener(client=mock_client, stream_name="s")

    first = list(listener.stream_records(max_records=1, auto_ack=True))

    assert [record.WhichOneof("record") for record in first] == ["batch"]
    mock_consumer.acknowledge.assert_not_called()

    second = list(listener.stream_records(max_records=1, auto_ack=True))

    assert [record.WhichOneof("record") for record in second] == ["version_end"]
    mock_consumer.acknowledge.return_value.wait.assert_called_once()
    assert mock_consumer.consume.call_count == 1


def test_listener_rejects_consume_before_acknowledgement() -> None:
    mock_client = MagicMock(spec=FDBClient)
    mock_consumer = MagicMock()
    mock_client.create_cdc_consumer.return_value = mock_consumer
    mock_consumer.consume.return_value.wait.return_value = MockConsumeResult(
        mutations=[
            MockVersionGroup(
                version=100,
                mutations=[MockCdcMutation(0, b"k1", b"v1")],
            )
        ],
        last_consumed_version=100,
    )
    listener = FDBMutationListener(client=mock_client, stream_name="s")

    listener.poll()

    with pytest.raises(CDCError, match="must be fully delivered and acknowledged"):
        listener.poll()


@pytest.mark.parametrize("limit", [0, -1])
def test_stream_limits_must_be_positive(limit: int) -> None:
    listener = FDBMutationListener(client=MagicMock(spec=FDBClient), stream_name="s")

    with pytest.raises(ValueError, match="max_mutations must be positive"):
        next(listener.stream_mutations(max_mutations=limit))
    with pytest.raises(ValueError, match="max_records must be positive"):
        next(listener.stream_records(max_records=limit))
