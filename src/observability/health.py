"""Process liveness and dependency readiness for health endpoints."""

from __future__ import annotations

import threading
import time
from typing import Final

DEFAULT_LIVENESS_TIMEOUT_SECONDS: Final[float] = 60.0

_lock = threading.Lock()
_last_heartbeat_monotonic: float = time.monotonic()
_fdb_ready: bool = False
_kafka_ready: bool = False
_liveness_timeout_seconds: float = DEFAULT_LIVENESS_TIMEOUT_SECONDS


def configure(
    *, liveness_timeout_seconds: float = DEFAULT_LIVENESS_TIMEOUT_SECONDS
) -> None:
    """Configure health thresholds.

    Args:
        liveness_timeout_seconds: Max age of the last heartbeat for ``/healthz``.

    Raises:
        ValueError: If the timeout is not positive.
    """
    if liveness_timeout_seconds <= 0:
        raise ValueError("liveness_timeout_seconds must be positive")
    global _liveness_timeout_seconds
    with _lock:
        _liveness_timeout_seconds = float(liveness_timeout_seconds)


def heartbeat() -> None:
    """Mark the process loop as responsive (call from the poll path)."""
    global _last_heartbeat_monotonic
    with _lock:
        _last_heartbeat_monotonic = time.monotonic()


def set_fdb_ready(ready: bool) -> None:
    """Set FoundationDB client readiness for ``/readyz``.

    Args:
        ready: Whether the FDB handle is open and usable.
    """
    global _fdb_ready
    with _lock:
        _fdb_ready = bool(ready)


def set_kafka_ready(ready: bool) -> None:
    """Set Kafka producer readiness for ``/readyz``.

    Args:
        ready: Whether the producer handle is established and healthy.
    """
    global _kafka_ready
    with _lock:
        _kafka_ready = bool(ready)


def is_live() -> bool:
    """Return whether the process loop heartbeat is within the timeout."""
    with _lock:
        age = time.monotonic() - _last_heartbeat_monotonic
        timeout = _liveness_timeout_seconds
    return age <= timeout


def is_ready() -> bool:
    """Return whether FDB and Kafka dependencies are both ready."""
    with _lock:
        return _fdb_ready and _kafka_ready


def reset_for_tests() -> None:
    """Reset health state (unit tests only)."""
    global _last_heartbeat_monotonic
    global _fdb_ready
    global _kafka_ready
    global _liveness_timeout_seconds
    with _lock:
        _last_heartbeat_monotonic = time.monotonic()
        _fdb_ready = False
        _kafka_ready = False
        _liveness_timeout_seconds = DEFAULT_LIVENESS_TIMEOUT_SECONDS
