"""FoundationDB native Change Data Capture (CDC) consumer package.

Provides high-level client connection management and bounded Protobuf record
polling for FoundationDB 8.0 native CDC.
"""

from fdb_kafka_bridge.cdc.checkpoint import (
    CheckpointCursor,
    CheckpointError,
    CheckpointFormatError,
    CheckpointOverlapError,
    CheckpointRegressionError,
    CheckpointSinkMismatchError,
    CheckpointStore,
    CheckpointStreamMismatchError,
    CheckpointWriteError,
    SinkIdentity,
)
from fdb_kafka_bridge.cdc.client import (
    DEFAULT_CDC_API_VERSION,
    MINIMUM_CDC_API_VERSION,
    FDBClient,
    init_fdb,
)
from fdb_kafka_bridge.cdc.errors import (
    CDCConsumeError,
    CDCConsumerClosedError,
    CDCError,
    CDCInvalidCursorError,
    CDCInvalidRangeError,
    CDCNotSupportedError,
    CDCRetryableError,
    CDCTerminalError,
)
from fdb_kafka_bridge.cdc.listener import (
    FDBMutationListener,
    strinc,
    subspace_to_key_range,
)

__all__ = [
    "CheckpointCursor",
    "CheckpointError",
    "CheckpointFormatError",
    "CheckpointOverlapError",
    "CheckpointRegressionError",
    "CheckpointSinkMismatchError",
    "CheckpointStore",
    "CheckpointStreamMismatchError",
    "CheckpointWriteError",
    "SinkIdentity",
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
