"""Live sustained-demo integration against FoundationDB CDC and Kafka.

Requires both live services::

    FDB_CDC_INTEGRATION=1 KAFKA_INTEGRATION=1 \\
      pytest tests/demo/test_sustained_integration.py -m integration -v
"""

from __future__ import annotations

import os

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("FDB_CDC_INTEGRATION") != "1"
        or os.environ.get("KAFKA_INTEGRATION") != "1",
        reason="set FDB_CDC_INTEGRATION=1 and KAFKA_INTEGRATION=1 for live demo",
    ),
]


def test_sustained_demo_catches_up_and_verifies() -> None:
    """Short concurrent run: catch up after writers stop; no missing indices."""
    # Import inside the test so collection does not require the FDB bindings.
    from demo.run_demo import run_sustained_demo

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    stats = run_sustained_demo(
        bootstrap_servers=bootstrap,
        duration=5.0,
        target_rate=50.0,
        seed=1,
        catchup_timeout=30.0,
        max_backlog=None,
        count_cap=250,
    )
    assert stats.written > 0
    assert stats.published >= stats.written
    assert stats.catchup_seconds >= 0.0
    assert stats.catchup_seconds < 30.0
