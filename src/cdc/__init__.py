"""FoundationDB native Change Data Capture (CDC) consumer package.

Provides high-level client connection management and bounded Protobuf record
polling for FoundationDB 8.0 native CDC.
"""

from src.cdc.client import (
    DEFAULT_CDC_API_VERSION,
    MINIMUM_CDC_API_VERSION,
    FDBClient,
    init_fdb,
)
from src.cdc.errors import (
    CDCConsumeError,
    CDCConsumerClosedError,
    CDCError,
    CDCInvalidCursorError,
    CDCInvalidRangeError,
    CDCNotSupportedError,
    CDCRetryableError,
    CDCTerminalError,
)
from src.cdc.listener import (
    FDBMutationListener,
    strinc,
    subspace_to_key_range,
)

__all__ = [
    "CDCConsumeError",
    "CDCConsumerClosedError",
    "CDCError",
    "CDCInvalidCursorError",
    "CDCInvalidRangeError",
    "CDCNotSupportedError",
    "CDCRetryableError",
    "CDCTerminalError",
    "DEFAULT_CDC_API_VERSION",
    "FDBClient",
    "FDBMutationListener",
    "MINIMUM_CDC_API_VERSION",
    "init_fdb",
    "strinc",
    "subspace_to_key_range",
]
