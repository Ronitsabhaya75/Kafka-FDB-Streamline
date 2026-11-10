"""Helpers for the sustained concurrent CDC demo mode.

Pure functions for backlog / catch-up math so unit tests need no FDB or Kafka.
"""

from __future__ import annotations

from dataclasses import dataclass, field


def default_max_backlog(target_rate: float) -> int:
    """Return the documented soft lag threshold for a target write rate.

    Uses five seconds of target throughput, floored at 1000 mutations, so a
    quiet demo still has headroom for CDC reply batching.

    Args:
        target_rate: Target mutations per second (0 means unthrottled).

    Returns:
        Soft maximum for ``written - published`` during the write window.
    """
    rate = max(float(target_rate), 0.0)
    return max(int(rate * 5), 1000)


def mutation_backlog(*, written: int, published: int) -> int:
    """Return in-flight mutations (committed but not yet published by the bridge).

    Args:
        written: Mutations committed to FoundationDB.
        published: Mutations the bridge reports as published to Kafka.

    Returns:
        Non-negative backlog count.
    """
    return max(0, int(written) - int(published))


def caught_up(*, written: int, published: int) -> bool:
    """Return whether the bridge has published at least every written mutation.

    Args:
        written: Mutations committed to FoundationDB.
        published: Mutations the bridge reports as published to Kafka.

    Returns:
        True when backlog is zero and at least one mutation was written.
    """
    return written > 0 and published >= written


@dataclass
class SustainedStats:
    """Accumulated stats for one sustained demo run.

    Attributes:
        written: Mutations committed by the writer.
        published: Bridge-reported published mutations at last sample.
        batches_acked: Bridge-reported CDC acknowledgements at last sample.
        max_backlog: Peak ``written - published`` observed while writing.
        write_duration_seconds: Wall time of the writer loop.
        catchup_seconds: Wall time from writer stop until backlog cleared.
        e2e_seconds: Wall time from writer start through verification.
        publish_latency_samples: Approximate seconds between write progress and
            matching publish progress samples.
    """

    written: int = 0
    published: int = 0
    batches_acked: int = 0
    max_backlog: int = 0
    write_duration_seconds: float = 0.0
    catchup_seconds: float = 0.0
    e2e_seconds: float = 0.0
    publish_latency_samples: list[float] = field(default_factory=list)

    def sample_backlog(self, *, written: int, published: int) -> int:
        """Record a backlog sample and update the peak.

        Args:
            written: Current committed mutation count.
            published: Current bridge published count.

        Returns:
            The backlog at this sample.
        """
        backlog = mutation_backlog(written=written, published=published)
        self.written = written
        self.published = published
        if backlog > self.max_backlog:
            self.max_backlog = backlog
        return backlog
