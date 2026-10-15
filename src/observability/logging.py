"""Structured JSON logging and VersionEnd audit events."""

from __future__ import annotations

import logging
from typing import Any

import structlog


def configure_logging(*, level: int = logging.INFO) -> None:
    """Configure structlog for JSON logs on stdout.

    Args:
        level: Root logging level.
    """
    logging.basicConfig(format="%(message)s", level=level)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True, key="timestamp"),
            structlog.processors.EventRenamer("event"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str = "streamline") -> Any:
    """Return a bound structlog logger.

    Args:
        name: Logger name.

    Returns:
        A structlog bound logger.
    """
    return structlog.get_logger(name)


def audit_version_end(
    *,
    fdb_version: int,
    mutation_count: int,
    duration_seconds: float,
) -> None:
    """Emit an audit line for one VersionEnd after durable ack.

    Idle watermarks (``mutation_count == 0``) log at DEBUG so quiet streams do
    not spam INFO. Non-zero VersionEnds log at INFO.

    Args:
        fdb_version: Commit version of the VersionEnd.
        mutation_count: Mutations for that version (0 for idle watermark).
        duration_seconds: Wall time from consume return through serialization.
    """
    payload: dict[str, Any] = {
        "event": "version_end_audit",
        "fdb_version": fdb_version,
        "mutation_count": mutation_count,
        "duration_seconds": duration_seconds,
    }
    logger = get_logger("streamline.audit")
    if mutation_count == 0:
        logger.debug(**payload)
    else:
        logger.info(**payload)
