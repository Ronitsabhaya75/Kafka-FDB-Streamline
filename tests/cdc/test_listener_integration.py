"""Live integration tests for FDBClient and FDBMutationListener against FoundationDB."""

import os
import time
from collections.abc import Iterator
from typing import Any

import pytest

from fdb_kafka_bridge.cdc.client import FDB_TRANSACTION_TOO_OLD, FDBClient
from fdb_kafka_bridge.cdc.errors import CDCTerminalError
from fdb_kafka_bridge.cdc.listener import FDBMutationListener, strinc
from fdbkafka.cdc.v1 import mutations_pb2

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("FDB_CDC_INTEGRATION") != "1",
        reason="set FDB_CDC_INTEGRATION=1 to run live FoundationDB tests",
    ),
]


def test_live_cdc_mutation_listener() -> None:
    """Test full CDC listener loop against a live FoundationDB cluster."""
    client = FDBClient(api_version=800)
    db = client.open()

    stream_name = b"integ_listener_stream"
    prefix = b"integ_cdc_key:"
    key_range = (prefix, strinc(prefix))

    # Clean up any existing stream from a previous run
    _remove_stream(client, stream_name)

    with FDBMutationListener(
        client=client,
        stream_name=stream_name,
        key_range=key_range,
    ) as listener:
        # Commit a set mutation
        k1 = prefix + b"alpha"
        v1 = b"value_alpha"
        db[k1] = v1

        # Poll for mutations with bounded retry
        found_mutations: list[mutations_pb2.FDBMutation] = []
        for _ in range(10):
            records = listener.poll_records()
            for record in records:
                if record.HasField("batch"):
                    found_mutations.extend(record.batch.mutations)
            if records:
                listener.acknowledge()
            if found_mutations:
                break
            time.sleep(0.3)

        assert len(found_mutations) > 0
        first_m = found_mutations[0]
        assert first_m.WhichOneof("mutation") == "single_key_mutation"
        assert first_m.single_key_mutation.key == k1
        assert first_m.single_key_mutation.value == v1
        assert (
            first_m.single_key_mutation.mutation_type
            == mutations_pb2.FDBSingleKeyMutation.MUTATION_TYPE_SET_VALUE
        )
        assert first_m.version_index.fdb_version > 0

        # Commit a range clear
        k2 = prefix + b"beta"
        db[k2] = b"value_beta"
        del db[k1 : prefix + b"\xff"]

        found_clear = False
        for _ in range(10):
            records = listener.poll_records()
            for record in records:
                if record.HasField("batch"):
                    for mutation in record.batch.mutations:
                        if mutation.WhichOneof("mutation") == "clear_range":
                            found_clear = True
                            break
            if records:
                listener.acknowledge()
            if found_clear:
                break
            time.sleep(0.3)

        assert found_clear

        # Verify the acknowledged position
        pos = listener.get_position()
        assert pos is not None
        assert pos.last_consumed_version > 0

    # Clean up test keys and stream
    del db[prefix : strinc(prefix)]
    _remove_stream(client, stream_name)


# Native idle replies arrive every few seconds; this bounds each wait at roughly
# a minute without tying the test to the server's exact cadence.
MAX_POLLS = 12


def _remove_stream(client: FDBClient, stream_name: bytes) -> None:
    try:
        client.remove_cdc_stream(stream_name)
    except Exception:
        pass


def _record_versions(record: mutations_pb2.FDBMutationRecord) -> Iterator[int]:
    if record.HasField("batch"):
        for mutation in record.batch.mutations:
            yield mutation.version_index.fdb_version
    if record.HasField("version_end"):
        yield record.version_end.fdb_version


def _commit(db: Any, key: bytes, value: bytes) -> int:
    tr = db.create_transaction()
    tr[key] = value
    tr.commit().wait()
    return tr.get_committed_version()


def _poll_until_records(
    listener: FDBMutationListener,
) -> list[mutations_pb2.FDBMutationRecord]:
    for _ in range(MAX_POLLS):
        records = listener.poll_records()
        if records:
            return records
    pytest.fail(f"no records within {MAX_POLLS} polls")


def _poll_until_key(
    listener: FDBMutationListener,
    key: bytes,
    floor_version: int,
) -> mutations_pb2.FDBMutation:
    """Poll and ack until ``key`` is delivered; assert nothing is at/below floor."""
    for _ in range(MAX_POLLS):
        records = listener.poll_records()
        found = None
        for record in records:
            for version in _record_versions(record):
                assert version > floor_version
            if record.HasField("batch"):
                for mutation in record.batch.mutations:
                    if mutation.single_key_mutation.key == key:
                        found = mutation
        if records:
            listener.acknowledge()
        if found is not None:
            return found
    pytest.fail(f"{key!r} not delivered within {MAX_POLLS} polls")


def test_live_idle_coalescing_flush_ack_covers_skipped_replies() -> None:
    """The flush ack is cumulative: it releases versions of skipped idle replies."""
    client = FDBClient(api_version=800)
    db = client.open()
    stream_name = b"integ_idle_coalesce_ack_stream"
    prefix = b"integ_idle_coalesce_ack:"
    _remove_stream(client, stream_name)

    try:
        with FDBMutationListener(
            client=client,
            stream_name=stream_name,
            key_range=(prefix, strinc(prefix)),
            idle_ack_max_replies=2,
        ) as listener:
            # start() zeroes the skipped count, so with nothing written the first
            # reply is skipped and the second flushes.
            assert listener.poll_records() == []
            assert not listener.awaiting_ack
            skipped = listener.get_position()

            records = listener.poll_records()
            assert len(records) == 1
            flush = records[0].version_end
            assert flush.total_mutations == 0
            checkpoint = listener.get_position()
            assert flush.fdb_version == checkpoint.last_consumed_version
            assert checkpoint.last_consumed_version > skipped.last_consumed_version
            listener.acknowledge()

        # Only the flush was acked, yet the skipped cursor is now behind the
        # retained history: the ack covered it. The native layer may reject the
        # stale cursor at resume or at first consume.
        with pytest.raises(CDCTerminalError) as exc_info:
            with FDBMutationListener(
                client=client, stream_name=stream_name, cursor=skipped
            ) as stale:
                stale.poll_records()
        assert exc_info.value.code == FDB_TRANSACTION_TOO_OLD

        key = prefix + b"after_flush"
        committed = _commit(db, key, b"v")
        assert committed > checkpoint.last_consumed_version

        with FDBMutationListener(
            client=client,
            stream_name=stream_name,
            cursor=checkpoint,
            idle_ack_max_replies=2,
        ) as resumed:
            mutation = _poll_until_key(
                resumed, key, floor_version=checkpoint.last_consumed_version
            )
            assert mutation.version_index.fdb_version == committed
    finally:
        del db[prefix : strinc(prefix)]
        _remove_stream(client, stream_name)


def test_live_idle_coalescing_close_mid_window_reconsumes_skipped() -> None:
    """Skipped idle progress is not acked: resume re-consumes it and loses nothing."""
    client = FDBClient(api_version=800)
    db = client.open()
    stream_name = b"integ_idle_coalesce_close_stream"
    prefix = b"integ_idle_coalesce_close:"
    _remove_stream(client, stream_name)

    try:
        with FDBMutationListener(
            client=client,
            stream_name=stream_name,
            key_range=(prefix, strinc(prefix)),
            idle_ack_max_replies=2,
        ) as listener:
            _poll_until_records(listener)
            durable = listener.get_position()
            listener.acknowledge()

            assert listener.poll_records() == []
            skipped_high = listener.get_position().last_consumed_version
            assert skipped_high > durable.last_consumed_version

            # Committed after the last consumed reply and before close, so this
            # listener never delivers it.
            key = prefix + b"in_gap"
            committed = _commit(db, key, b"v")
            assert committed > skipped_high

        with FDBMutationListener(
            client=client,
            stream_name=stream_name,
            cursor=durable,
            idle_ack_max_replies=2,
        ) as resumed:
            # Resuming at `durable` without TOO_OLD shows the skipped replies were
            # never acked; the gap key then proves nothing past them was lost.
            mutation = _poll_until_key(
                resumed,
                key,
                floor_version=durable.last_consumed_version,
            )
            assert mutation.version_index.fdb_version == committed
    finally:
        del db[prefix : strinc(prefix)]
        _remove_stream(client, stream_name)
