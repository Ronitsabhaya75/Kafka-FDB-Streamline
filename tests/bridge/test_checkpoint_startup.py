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
        assert store.open.call_count == 1
        operations = [call[0] for call in store.method_calls]
        assert operations.index("claim") < operations.index("load")
    client.register_cdc_stream.assert_not_called()
    store.save.assert_not_called()


def test_overlap_refusal_precedes_claim() -> None:
    client, store = MagicMock(), MagicMock()
    client.list_cdc_streams.return_value = []
    store.open.side_effect = CheckpointOverlapError("overlap")
    with pytest.raises(CheckpointOverlapError):
        resolve_startup(client, store, b"s", (b"a", b"\xff"))
    store.claim.assert_not_called()


def test_multi_range_stream_is_a_deterministic_refusal() -> None:
    class MultiRange:
        name = b"s"

        @property
        def begin_key(self) -> bytes:
            raise ValueError("Use ranges")

    client, store = MagicMock(), MagicMock()
    client.list_cdc_streams.return_value = [MultiRange()]
    with pytest.raises(StartupError, match="single-range"):
        resolve_startup(client, store, b"s", None)
    store.claim.assert_not_called()
