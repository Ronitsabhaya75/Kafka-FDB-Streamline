"""Error taxonomy of the serialization package."""

import enum


class SerializationError(Exception):
    """Base of every error this package raises deliberately."""


class InputTypeError(SerializationError, TypeError):
    """An argument or native-mutation attribute has the wrong Python type.

    Attributes:
        field: The offending parameter or attribute, as the public API spells it.
        index: Position in `mutations`, or `None` outside a batch.
    """

    def __init__(self, message: str, *, field: str, index: int | None = None) -> None:
        """Store the offending field and position."""
        super().__init__(message)
        self.field = field
        self.index = index


class InputValueError(SerializationError, ValueError):
    """An argument or native-mutation attribute is out of range.

    Attributes:
        field: The offending parameter or attribute, as the public API spells it.
        index: Position in `mutations`, or `None` outside a batch.
    """

    def __init__(self, message: str, *, field: str, index: int | None = None) -> None:
        """Store the offending field and position."""
        super().__init__(message)
        self.field = field
        self.index = index


class RecordTooLargeError(InputValueError):
    """One mutation, or the version end, cannot fit `max_record_bytes`.

    `index` is the mutation's position, or `None` when the version end is too big.

    Attributes:
        record_bytes: Size of the record that did not fit.
        max_record_bytes: The budget it exceeded.
    """

    def __init__(
        self,
        message: str,
        *,
        index: int | None,
        record_bytes: int,
        max_record_bytes: int,
    ) -> None:
        """Store the position and the two sizes."""
        super().__init__(message, field="max_record_bytes", index=index)
        self.record_bytes = record_bytes
        self.max_record_bytes = max_record_bytes


class DecodeFailure(enum.Enum):
    """Why bytes are not a decodable record. The value is the metric label."""

    MALFORMED_WIRE = "malformed_wire"
    NO_RECORD_ARM = "no_record_arm"
    NO_MUTATION_ARM = "no_mutation_arm"
    MISSING_VERSION_INDEX = "missing_version_index"
    TYPE_CODE_OUT_OF_RANGE = "type_code_out_of_range"
    VERSION_OUT_OF_RANGE = "version_out_of_range"
    EMPTY_BATCH = "empty_batch"
    INCONSISTENT_BATCH = "inconsistent_batch"


class RecordDecodeError(SerializationError, ValueError):
    """The bytes are an undecodable record.

    Attributes:
        reason: The check that failed.
        index: Position in the wire batch, or `None` outside a batch.
    """

    def __init__(
        self, message: str, *, reason: DecodeFailure, index: int | None = None
    ) -> None:
        """Store the failed check and the batch position."""
        super().__init__(message)
        self.reason = reason
        self.index = index
