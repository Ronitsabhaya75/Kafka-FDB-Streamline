"""Process liveness and dependency readiness for health endpoints."""

from __future__ import annotations

import threading
import time
from typing import Final

# Longer than a typical CDC long-poll idle wait so /healthz stays green while
# consume().wait() blocks; heartbeat runs after consume returns and after ack.
DEFAULT_LIVENESS_TIMEOUT_SECONDS: Final[float] = 120.0


class HealthState:
    """Mutable liveness / readiness state for one process.

    FDB readiness is refcounted so a short-lived admin client ``close()`` does
    not clear readiness for a still-open bridge client.
    """

    def __init__(self) -> None:
        """Create an unset (not live, not ready) health state."""
        self._lock = threading.Lock()
        self._last_heartbeat_monotonic: float | None = None
        self._fdb_owners: int = 0
        self._kafka_ready: bool = False
        self._liveness_timeout_seconds: float = DEFAULT_LIVENESS_TIMEOUT_SECONDS

    def configure(
        self, *, liveness_timeout_seconds: float = DEFAULT_LIVENESS_TIMEOUT_SECONDS
    ) -> None:
        """Configure health thresholds.

        Args:
            liveness_timeout_seconds: Max age of the last heartbeat for ``/healthz``.

        Raises:
            ValueError: If the timeout is not positive.
        """
        if liveness_timeout_seconds <= 0:
            raise ValueError("liveness_timeout_seconds must be positive")
        with self._lock:
            self._liveness_timeout_seconds = float(liveness_timeout_seconds)

    def heartbeat(self) -> None:
        """Mark the process loop as responsive."""
        with self._lock:
            self._last_heartbeat_monotonic = time.monotonic()

    def set_fdb_ready(self, ready: bool) -> None:
        """Acquire or release one FDB readiness owner.

        Args:
            ready: ``True`` to acquire; ``False`` to release one owner.
        """
        with self._lock:
            if ready:
                self._fdb_owners += 1
            else:
                self._fdb_owners = max(0, self._fdb_owners - 1)

    def set_kafka_ready(self, ready: bool) -> None:
        """Set Kafka producer readiness for ``/readyz``.

        Args:
            ready: Whether the producer is established and healthy.
        """
        with self._lock:
            self._kafka_ready = bool(ready)

    def is_live(self) -> bool:
        """Return whether a heartbeat has occurred within the timeout."""
        with self._lock:
            if self._last_heartbeat_monotonic is None:
                return False
            age = time.monotonic() - self._last_heartbeat_monotonic
            timeout = self._liveness_timeout_seconds
        return age <= timeout

    def is_ready(self) -> bool:
        """Return whether FDB and Kafka dependencies are both ready."""
        with self._lock:
            return self._fdb_owners > 0 and self._kafka_ready

    def reset(self) -> None:
        """Reset to the unset startup state (unit tests only)."""
        with self._lock:
            self._last_heartbeat_monotonic = None
            self._fdb_owners = 0
            self._kafka_ready = False
            self._liveness_timeout_seconds = DEFAULT_LIVENESS_TIMEOUT_SECONDS


# Process-wide default used by HTTP handlers and bridge wiring.
_STATE = HealthState()


def configure(
    *, liveness_timeout_seconds: float = DEFAULT_LIVENESS_TIMEOUT_SECONDS
) -> None:
    """Configure the process-wide health thresholds.

    Args:
        liveness_timeout_seconds: Max age of the last heartbeat for ``/healthz``.
    """
    _STATE.configure(liveness_timeout_seconds=liveness_timeout_seconds)


def heartbeat() -> None:
    """Mark the process loop as responsive (call after consume / ack)."""
    _STATE.heartbeat()


def set_fdb_ready(ready: bool) -> None:
    """Acquire or release one FDB readiness owner on the process-wide state.

    Args:
        ready: ``True`` to acquire; ``False`` to release.
    """
    _STATE.set_fdb_ready(ready)


def set_kafka_ready(ready: bool) -> None:
    """Set Kafka readiness on the process-wide state.

    Args:
        ready: Whether the producer is established and healthy.
    """
    _STATE.set_kafka_ready(ready)


def is_live() -> bool:
    """Return whether the process loop heartbeat is within the timeout."""
    return _STATE.is_live()


def is_ready() -> bool:
    """Return whether FDB and Kafka dependencies are both ready."""
    return _STATE.is_ready()


def reset_for_tests() -> None:
    """Reset process-wide health state (unit tests only)."""
    _STATE.reset()
