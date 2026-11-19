"""Stream validation and ownership claim before registration or checkpoint advance."""

import logging
from typing import Any

from fdb_kafka_bridge.cdc.checkpoint import (
    CheckpointCursor,
    CheckpointStore,
    KeyRange,
)
from fdb_kafka_bridge.cdc.client import FDBClient

logger = logging.getLogger(__name__)


class StartupError(ValueError):
    """Operator action is required before the bridge can safely start."""


def resolve_startup(
    client: FDBClient,
    store: CheckpointStore,
    stream_name: bytes,
    key_range: KeyRange | None,
) -> tuple[CheckpointCursor | None, bool]:
    """Validate ranges, claim ownership, and validate the resume cursor.

    Args:
        client: Active native CDC client.
        store: Checkpoint store, initially unopened.
        stream_name: Stream registration name.
        key_range: Optional requested registration range.

    Returns:
        Resume cursor and whether the stream needs registration.

    Raises:
        StartupError: If stream identity, existence or configured range conflicts.
        CheckpointOverlapError: If checkpoint writes could be captured.
    """
    streams = client.list_cdc_streams()
    stream = next((s for s in streams if s.name == stream_name), None)
    captured: list[KeyRange] = []
    if stream is not None:
        captured = stream_ranges(stream)
        if key_range is not None and captured != [key_range]:
            raise StartupError("Configured range differs from registered stream")
    elif key_range is not None:
        captured = [key_range]
    store.open(create=False, forbidden_ranges=captured)
    store.claim(stream_name)
    cursor = store.load(stream_name)
    if cursor is not None:
        if stream is None:
            raise StartupError("Checkpoint exists but stream is missing")
        if cursor.stream_id != stream.stream_id:
            raise StartupError("Checkpoint belongs to another stream registration")
        client.validate_resume_cursor(stream_name, cursor)
    elif stream is None and key_range is None:
        raise StartupError("Stream is missing; provide --subspace-prefix to register")
    elif stream is not None:
        logger.warning("No checkpoint for %r; using cluster min_version", stream_name)
    return cursor, stream is None


def stream_ranges(stream: Any) -> list[KeyRange]:
    """Extract captured ranges from the pinned single-range CDC binding.

    Args:
        stream: Native CdcStreamInfo.

    Returns:
        List of half-open ranges.
    """
    try:
        return [(stream.begin_key, stream.end_key)]
    except ValueError as exc:
        raise StartupError("Bridge requires a single-range CDC stream") from exc
