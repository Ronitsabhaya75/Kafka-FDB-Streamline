"""Exceptions raised by the FoundationDB CDC consumer module."""


class CDCError(Exception):
    """Base exception for all CDC consumer errors."""


class CDCNotSupportedError(CDCError):
    """Raised when the local FDB client cannot provide native CDC.

    Raised for an API version below 800, missing Python bindings, or a binding
    ``RuntimeError`` reporting that native CDC is unsupported. A cluster without
    ``ENABLE_NATIVE_CDC`` is not detected here; it fails as a raw ``FDBError``.
    """


class CDCConsumerClosedError(CDCError):
    """Raised when an operation is attempted on a closed CDC consumer."""


class CDCInvalidCursorError(CDCError):
    """Raised when a resume cursor is malformed or cannot be reconciled.

    Covers a cursor without ``stream_id``/``last_consumed_version`` or with
    out-of-range values (permanent), and a resume whose read-version wait timed out
    before reaching the cursor version (possibly transient; a later resume from the
    same cursor may succeed).
    """


class CDCInvalidRangeError(CDCError):
    """Raised when an invalid key range is specified for CDC stream registration."""


class CDCConsumeError(CDCError):
    """A native consume or acknowledge call failed.

    Attributes:
        code: The native FoundationDB error code. Always set when this package
            raises the error; ``None`` only for instances constructed without it.
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
    (2000). 1007 means the cursor is behind the acknowledged watermark (a bridge
    bug) or the data it needs was popped (a retention-invariant violation); either
    way, alert and rebuild rather than retry. 2000 is terminal for the request apart
    from the narrow ≤5 s retry-once-past-the-lease case in CDC-DEEP-DIVE §3, which
    callers implement from ``code``.
    """
