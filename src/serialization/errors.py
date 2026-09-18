"""Error taxonomy of the serialization package."""

import enum


class SerializationError(Exception):
    """Base of every error this package raises deliberately."""


class InputTypeError(SerializationError, TypeError):
    """An argument or native-mutation attribute has the wrong Python type.

    Attributes:
        field: Offending parameter or attribute name, as spelled in the public API.
        index: Zero-based position in `mutations`; `None` outside a batch.
    """

    def __init__(self, message: str, *, field: str, index: int | None = None) -> None:
        """Initialise with a message and the offending field and position."""
        super().__init__(message)
        self.field = field
        self.index = index


class InputValueError(SerializationError, ValueError):
    """An argument or native-mutation attribute is outside its accepted domain.

    Attributes:
        field: Offending parameter or attribute name, as spelled in the public API.
        index: Zero-based position in `mutations`; `None` outside a batch.
    """

    def __init__(self, message: str, *, field: str, index: int | None = None) -> None:
        """Initialise with a message and the offending field and position."""
        super().__init__(message)
        self.field = field
        self.index = index


class RecordTooLargeError(InputValueError):
    """One mutation, or the version end, cannot fit `max_record_bytes`.

    Attributes:
        record_bytes: Size of the record that did not fit.
        max_record_bytes: The budget it was checked against.
    """

    def __init__(
        self,
        message: str,
        *,
        index: int | None,
        record_bytes: int,
        max_record_bytes: int,
    ) -> None:
        """Initialise with the unsplittable position and the two sizes."""
        super().__init__(message, field="max_record_bytes", index=index)
        self.record_bytes = record_bytes
        self.max_record_bytes = max_record_bytes


class DecodeFailure(enum.Enum):
    """Why bytes could not be interpreted as a record; doubles as a metric label."""

    MALFORMED_WIRE = "malformed_wire"
    NO_RECORD_ARM = "no_record_arm"
    NO_MUTATION_ARM = "no_mutation_arm"
    MISSING_VERSION_INDEX = "missing_version_index"
    TYPE_CODE_OUT_OF_RANGE = "type_code_out_of_range"
    VERSION_OUT_OF_RANGE = "version_out_of_range"
    EMPTY_BATCH = "empty_batch"
    INCONSISTENT_BATCH = "inconsistent_batch"


class RecordDecodeError(SerializationError, ValueError):
    """Bytes are not a record this package can interpret.

    Attributes:
        reason: The check that failed.
        index: Zero-based position in the wire batch; `None` outside a batch.
    """

    def __init__(
        self, message: str, *, reason: DecodeFailure, index: int | None = None
    ) -> None:
        """Initialise with a message, the failed check and the batch position."""
        super().__init__(message)
        self.reason = reason
        self.index = index
