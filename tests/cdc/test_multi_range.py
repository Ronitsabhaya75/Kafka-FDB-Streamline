"""Unit tests for multi-range CDC registration in FDBClient and the listener."""

from __future__ import annotations

import ast
import importlib.util
from collections import namedtuple
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from fdb_kafka_bridge.cdc import client as client_module
from fdb_kafka_bridge.cdc.client import FDBClient, multi_range_cdc_unsupported_reason
from fdb_kafka_bridge.cdc.errors import (
    CDCInvalidRangeError,
    CDCNotSupportedError,
    CDCStreamMismatchError,
)
from fdb_kafka_bridge.cdc.listener import FDBMutationListener

KeyRange = namedtuple("KeyRange", ["begin_key", "end_key"])
StreamInfo = namedtuple("StreamInfo", ["name", "stream_id", "ranges", "min_version"])
LegacyStreamInfo = namedtuple(
    "LegacyStreamInfo", ["name", "stream_id", "begin_key", "end_key", "min_version"]
)
MockCursor = namedtuple("MockCursor", ["stream_id", "last_consumed_version"])

RANGES = [(b"\x15\x30", b"\x15\x31"), (b"\x15\x05", b"\x15\x06")]
SORTED = [(b"\x15\x05", b"\x15\x06"), (b"\x15\x30", b"\x15\x31")]


class _SupportedDatabase:
    def register_cdc_stream(  # signature mirrors foundationdb 8.0.0
        self,
        name: bytes,
        begin_key: bytes | None = None,
        end_key: bytes | None = None,
        *,
        ranges: Any = None,
        split_points: Any = None,
    ) -> Any:
        raise NotImplementedError


class _SingleRangeDatabase:
    def register_cdc_stream(self, name: bytes, begin_key: bytes, end_key: bytes) -> Any:
        raise NotImplementedError


class _PositionalRangesDatabase:
    def register_cdc_stream(self, name: bytes, ranges: Any) -> Any:
        raise NotImplementedError


def _binding(database: type | None = _SupportedDatabase, **extra: Any) -> Any:
    attrs: dict[str, Any] = {
        "is_api_version_selected": lambda: True,
        "CdcKeyRange": KeyRange,
    }
    if database is not None:
        attrs["Database"] = database
    attrs.update(extra)
    return SimpleNamespace(**attrs)


@pytest.fixture
def supported() -> Iterator[None]:
    with patch.object(client_module, "fdb", _binding()):
        yield


def _client(*streams: Any) -> tuple[FDBClient, MagicMock]:
    db = MagicMock()
    db.list_cdc_streams.return_value.wait.return_value = list(streams)
    db.register_cdc_stream.return_value.wait.return_value = 41
    client = FDBClient()
    client._db = db
    return client, db


# --- capability --------------------------------------------------------------


def test_capability_supported_binding() -> None:
    with patch.object(client_module, "fdb", _binding()):
        assert multi_range_cdc_unsupported_reason() is None


@pytest.mark.parametrize(
    ("binding", "match"),
    [
        (None, "not installed"),
        (_binding(is_api_version_selected=lambda: False), "no FDB API version"),
        (_binding(database=None), "no Database.register_cdc_stream"),
        (_binding(_SingleRangeDatabase), "keyword-only 'ranges'"),
        (_binding(_PositionalRangesDatabase), "keyword-only 'ranges'"),
        (
            SimpleNamespace(
                is_api_version_selected=lambda: True, Database=_SupportedDatabase
            ),
            "CdcKeyRange",
        ),
    ],
)
def test_capability_reports_exact_missing_piece(binding: Any, match: str) -> None:
    with patch.object(client_module, "fdb", binding):
        reason = multi_range_cdc_unsupported_reason()
    assert reason is not None and match in reason


def test_pinned_binding_source_satisfies_the_capability_check() -> None:
    # Reads the installed binding's source without importing fdb.impl, which
    # would need libfdb_c.
    spec = importlib.util.find_spec("fdb")
    if spec is None or spec.submodule_search_locations is None:
        pytest.skip("foundationdb binding not installed")
    source = Path(next(iter(spec.submodule_search_locations)), "impl.py").read_text()
    tree = ast.parse(source)
    database = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "Database"
    )
    register = next(
        node
        for node in database.body
        if isinstance(node, ast.FunctionDef) and node.name == "register_cdc_stream"
    )
    assert "ranges" in [arg.arg for arg in register.args.kwonlyargs]
    assert any(
        isinstance(node, ast.ClassDef) and node.name == "CdcKeyRange"
        for node in tree.body
    )


# --- registration ------------------------------------------------------------


def test_register_ranges_makes_exactly_one_native_call(supported: None) -> None:
    client, db = _client()
    assert client.register_cdc_stream("s", ranges=RANGES) == 41
    db.register_cdc_stream.assert_called_once_with(b"s", ranges=SORTED)


def test_register_single_range_path_is_unchanged(supported: None) -> None:
    client, db = _client()
    client.register_cdc_stream("s", b"a", b"b")
    db.register_cdc_stream.assert_called_once_with(b"s", b"a", b"b")


def test_register_rejects_ranges_mixed_with_single_range(supported: None) -> None:
    client, db = _client()
    with pytest.raises(TypeError, match="cannot be combined"):
        client.register_cdc_stream("s", b"a", ranges=RANGES)
    db.register_cdc_stream.assert_not_called()


def test_register_validates_whole_collection_before_native_call(
    supported: None,
) -> None:
    client, db = _client()
    with pytest.raises(CDCInvalidRangeError, match="overlap"):
        client.register_cdc_stream("s", ranges=[*RANGES, (b"\x15\x05", b"\x15\x07")])
    db.register_cdc_stream.assert_not_called()


def test_register_raises_actionable_error_when_unsupported() -> None:
    client, db = _client()
    with patch.object(client_module, "fdb", _binding(_SingleRangeDatabase)):
        with pytest.raises(CDCNotSupportedError, match="foundationdb>=8.0.0"):
            client.register_cdc_stream("s", ranges=RANGES)
    db.register_cdc_stream.assert_not_called()


def test_native_runtime_error_is_translated(supported: None) -> None:
    client, db = _client()
    db.register_cdc_stream.side_effect = RuntimeError(
        "The loaded FoundationDB C library does not support native CDC"
    )
    with pytest.raises(CDCNotSupportedError):
        client.register_cdc_stream("s", ranges=RANGES)


# --- ensure_cdc_stream -------------------------------------------------------


def test_ensure_registers_new_stream_once(supported: None) -> None:
    client, db = _client()
    assert client.ensure_cdc_stream(b"s", RANGES) == 41
    db.register_cdc_stream.assert_called_once_with(b"s", ranges=SORTED)


def test_ensure_accepts_existing_stream_with_canonical_union(supported: None) -> None:
    # Native listing merges adjacent ranges; the request is still equivalent.
    adjacent = [(b"a", b"b"), (b"b", b"c"), (b"x", b"y")]
    listed = StreamInfo(b"s", 41, (KeyRange(b"a", b"c"), KeyRange(b"x", b"y")), 5)
    client, db = _client(StreamInfo(b"other", 1, (), 0), listed)
    assert client.ensure_cdc_stream("s", adjacent) == 41
    db.register_cdc_stream.assert_called_once()
    db.remove_cdc_stream.assert_not_called()


def test_ensure_refuses_mismatched_existing_stream(supported: None) -> None:
    listed = StreamInfo(b"s", 41, (KeyRange(b"\x15\x05", b"\x15\x06"),), 5)
    client, db = _client(listed)
    with pytest.raises(CDCStreamMismatchError, match="immutable"):
        client.ensure_cdc_stream("s", RANGES)
    db.register_cdc_stream.assert_not_called()
    db.remove_cdc_stream.assert_not_called()


def test_ensure_handles_single_range_listing_shape(supported: None) -> None:
    legacy = LegacyStreamInfo(b"s", 41, b"\x15\x05", b"\x15\x06", 5)
    client, _ = _client(legacy)
    assert client.ensure_cdc_stream("s", [(b"\x15\x05", b"\x15\x06")]) == 41


def test_ensure_detects_stream_id_change_during_registration(supported: None) -> None:
    listed = StreamInfo(b"s", 7, tuple(KeyRange(*r) for r in SORTED), 5)
    client, db = _client(listed)
    db.register_cdc_stream.return_value.wait.return_value = 8
    with pytest.raises(CDCStreamMismatchError, match="changed id"):
        client.ensure_cdc_stream("s", RANGES)


def test_ensure_checks_capability_before_listing() -> None:
    client, db = _client()
    with patch.object(client_module, "fdb", _binding(_SingleRangeDatabase)):
        with pytest.raises(CDCNotSupportedError):
            client.ensure_cdc_stream("s", RANGES)
    db.list_cdc_streams.assert_not_called()


# --- listener ----------------------------------------------------------------


@pytest.mark.parametrize(
    "extra",
    [{"key_range": (b"a", b"b")}, {"subspace": b"p"}],
)
def test_listener_key_ranges_are_mutually_exclusive(extra: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="only one of"):
        FDBMutationListener(MagicMock(spec=FDBClient), "s", key_ranges=RANGES, **extra)


def test_listener_validates_key_ranges_before_any_side_effect() -> None:
    client = MagicMock(spec=FDBClient)
    with pytest.raises(CDCInvalidRangeError, match="duplicate"):
        FDBMutationListener(client, "s", key_ranges=[RANGES[0], RANGES[0]])
    assert client.method_calls == []


def test_listener_registers_all_ranges_through_ensure() -> None:
    client = MagicMock(spec=FDBClient)
    client.ensure_cdc_stream.return_value = 41
    listener = FDBMutationListener(client, "s", key_ranges=RANGES)
    assert listener.key_ranges == tuple(SORTED)
    listener.start()
    client.ensure_cdc_stream.assert_called_once_with(b"s", tuple(SORTED))
    client.register_cdc_stream.assert_not_called()
    client.create_cdc_consumer.assert_called_once_with(b"s")
    assert listener.stream_id == 41


def test_listener_refuses_cursor_for_other_stream_id_before_resume() -> None:
    client = MagicMock(spec=FDBClient)
    client.ensure_cdc_stream.return_value = 41
    listener = FDBMutationListener(
        client, "s", key_ranges=RANGES, cursor=MockCursor(40, 100)
    )
    with pytest.raises(CDCStreamMismatchError, match="re-registered"):
        listener.start()
    client.resume_cdc_consumer.assert_not_called()


def test_listener_resumes_and_reacknowledges_matching_cursor() -> None:
    client = MagicMock(spec=FDBClient)
    client.ensure_cdc_stream.return_value = 41
    client.get_read_version.return_value = 200
    consumer = client.resume_cdc_consumer.return_value
    listener = FDBMutationListener(
        client, "s", key_ranges=RANGES, cursor=MockCursor(41, 100)
    )
    listener.start()
    client.resume_cdc_consumer.assert_called_once()
    consumer.acknowledge.assert_called_once()
