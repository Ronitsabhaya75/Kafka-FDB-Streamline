"""Startup decisions must precede stream registration and store writes."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from fdb_kafka_bridge.bridge.startup import StartupError, resolve_startup
from fdb_kafka_bridge.cdc.checkpoint import CheckpointCursor, CheckpointOverlapError


@pytest.mark.parametrize(
    ("exists", "cursor", "configured", "error", "register"),
    [
        (False, None, None, True, False),
        (False, None, (b"a", b"b"), False, True),
        (False, CheckpointCursor(1, 10), (b"a", b"b"), True, False),
        (True, None, None, False, False),
        (True, None, (b"a", b"b"), False, False),
        (True, None, (b"c", b"d"), True, False),
        (True, CheckpointCursor(1, 10), None, False, False),
        (True, CheckpointCursor(1, 10), (b"a", b"b"), False, False),
        (True, CheckpointCursor(2, 10), None, True, False),
        (True, CheckpointCursor(1, 10), (b"c", b"d"), True, False),
    ],
)
def test_resolution(
    exists: bool,
    cursor: CheckpointCursor | None,
    configured: tuple[bytes, bytes] | None,
    error: bool,
    register: bool,
) -> None:
    client = MagicMock()
    stream = SimpleNamespace(name=b"s", stream_id=1, begin_key=b"a", end_key=b"b")
    client.list_cdc_streams.return_value = [stream] if exists else []
    store = MagicMock()
    store.load.return_value = cursor
    if error:
        with pytest.raises(StartupError):
            resolve_startup(client, store, b"s", configured)
        assert all(c.kwargs.get("create") is False for c in store.open.call_args_list)
    else:
        assert resolve_startup(client, store, b"s", configured) == (cursor, register)
        assert store.open.call_count == 2
    client.register_cdc_stream.assert_not_called()
    store.save.assert_not_called()


def test_directory_metadata_overlap_before_store_access() -> None:
    client, store = MagicMock(), MagicMock()
    client.list_cdc_streams.return_value = []
    with pytest.raises(CheckpointOverlapError):
        resolve_startup(client, store, b"s", (b"a", b"\xff"))
    store.open.assert_not_called()
