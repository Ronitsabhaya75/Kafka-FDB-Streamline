"""Pure cursor integrity and key-range guards; transactions are tested live."""

import pytest

from fdb_kafka_bridge.cdc.checkpoint import (
    CheckpointCursor,
    CheckpointFormatError,
    CheckpointOverlapError,
    CheckpointRegressionError,
    CheckpointStreamMismatchError,
    SinkIdentity,
    check_overlap,
    validate_progress,
)


@pytest.mark.parametrize(
    ("stream", "version"),
    [(True, 1), (-1, 1), (2**64, 1), (1, True), (1, -2), (1, 2**63)],
)
def test_invalid_cursor(stream: int, version: int) -> None:
    with pytest.raises(CheckpointFormatError):
        CheckpointCursor(stream, version)


def test_monotonic_save_guards() -> None:
    cursor = CheckpointCursor(2**64 - 1, 100)
    assert validate_progress(None, cursor)
    assert not validate_progress(cursor, cursor)
    assert validate_progress(cursor, CheckpointCursor(cursor.stream_id, 101))
    with pytest.raises(CheckpointRegressionError):
        validate_progress(cursor, CheckpointCursor(cursor.stream_id, 99))
    with pytest.raises(CheckpointStreamMismatchError):
        validate_progress(cursor, CheckpointCursor(1, 101))


@pytest.mark.parametrize(
    "captured", [[(b"b", b"c")], [(b"a", b"z")], [(b"a", b"c")], [(b"c", b"z")]]
)
def test_overlap(captured: list[tuple[bytes, bytes]]) -> None:
    with pytest.raises(CheckpointOverlapError):
        check_overlap((b"b", b"d"), captured)


def test_adjacent_ranges_are_disjoint() -> None:
    check_overlap((b"b", b"d"), [(b"a", b"b"), (b"d", b"z")])


@pytest.mark.parametrize(
    "kwargs",
    [
        {"path": ()},
        {"path": ("",)},
        {"timeout_s": 0},
        {"timeout_s": float("nan")},
        {"timeout_s": True},
        {"retry_limit": -1},
        {"retry_limit": True},
    ],
)
def test_store_configuration(kwargs: dict[str, object]) -> None:
    from unittest.mock import MagicMock

    from fdb_kafka_bridge.cdc.checkpoint import CheckpointStore

    with pytest.raises(ValueError):
        CheckpointStore(MagicMock(), sink=SinkIdentity("cluster", "topic"), **kwargs)


@pytest.mark.parametrize(
    ("cluster", "topic"), [("", "t"), (" ", "t"), ("c", ""), (None, "t")]
)
def test_sink_identity_requires_cluster_and_topic(cluster: object, topic: str) -> None:
    with pytest.raises(ValueError):
        SinkIdentity(cluster, topic)
