"""Exceptions raised by the FoundationDB CDC consumer module."""


class CDCError(Exception):
    """Base exception for all CDC consumer errors."""


class CDCNotSupportedError(CDCError):
    """Raised when native CDC is not supported by the FDB client or cluster.

    Typically occurs when FDB API version is less than 800 or the loaded
    FoundationDB C library does not export experimental CDC symbols.
    """


class CDCConsumerClosedError(CDCError):
    """Raised when an operation is attempted on a closed CDC consumer."""


class CDCInvalidCursorError(CDCError):
    """Raised when a cursor has invalid coordinates (e.g. stream_id or version)."""


class CDCInvalidRangeError(CDCError):
    """Raised when an invalid key range is specified for CDC stream registration."""


class CDCConsumeError(CDCError):
    """A native consume or acknowledge call failed.

    Attributes:
        code: The native FoundationDB error code when the failure came from the
            binding, otherwise ``None``.
    """

    def __init__(self, message: str, *, code: int | None = None) -> None:
        """Store the message and the originating native error code."""
        super().__init__(message)
        self.code = code


class CDCRetryableError(CDCConsumeError):
    """A consume/ack failure the caller should retry with bounded backoff.

    Raised for ``server_overloaded`` (1211). A conventional retry predicate treats
    this as non-retryable, so callers must apply their own bounded backoff. If it
    repeats at a single commit version it is the L1 poison pill; escalate rather
    than spin (see CDC-DEEP-DIVE §3).
    """


class CDCTerminalError(CDCConsumeError):
    """A consume/ack failure that will not succeed on a plain retry.

    Raised for ``transaction_too_old`` (1007) and ``client_invalid_operation``
    (2000). 1007 is unrecoverable data loss and must trigger a rebuild, not a
    retry. 2000 is terminal for the request apart from the narrow ≤5 s
    retry-once-past-the-lease case in CDC-DEEP-DIVE §3, which callers implement
    from ``code``.
    """
