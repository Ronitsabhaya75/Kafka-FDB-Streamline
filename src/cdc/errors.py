"""Exceptions raised by the FoundationDB CDC consumer module."""


class CDCError(Exception):
    """Base exception for all CDC consumer errors."""


class CDCNotSupportedError(CDCError):
    """Raised when native CDC is not supported by the FDB client or cluster.

    Typically occurs when FDB API version is less than 800 or the loaded
    FoundationDB C library does not export experimental CDC symbols.
    """


class CDCStreamNotFoundError(CDCError):
    """Raised when an operation targets a CDC stream that does not exist."""


class CDCConsumerClosedError(CDCError):
    """Raised when an operation is attempted on a closed CDC consumer."""


class CDCInvalidCursorError(CDCError):
    """Raised when a cursor has invalid coordinates (e.g. stream_id or version)."""


class CDCInvalidRangeError(CDCError):
    """Raised when an invalid key range is specified for CDC stream registration."""
