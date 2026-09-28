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
    topic: str | None = None,
    partition: int | None = None,
) -> None:
    """Emit the INFO audit line for one VersionEnd watermark.

    Args:
        fdb_version: Commit version of the VersionEnd.
        mutation_count: Mutations flushed for that version (0 for idle watermark).
        duration_seconds: Wall time for the poll cycle that produced it.
        topic: Optional Kafka topic context.
        partition: Optional Kafka partition context.
    """
    payload: dict[str, Any] = {
        "event": "version_end_audit",
        "fdb_version": fdb_version,
        "mutation_count": mutation_count,
        "duration_seconds": duration_seconds,
    }
    if topic is not None:
        payload["topic"] = topic
    if partition is not None:
        payload["partition"] = partition
    get_logger("streamline.audit").info(**payload)
