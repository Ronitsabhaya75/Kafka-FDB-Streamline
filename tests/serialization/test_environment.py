import google.protobuf
from google.protobuf.internal import api_implementation


def test_backend_is_upb() -> None:
    assert api_implementation.Type() == "upb"


def test_runtime_version_in_supported_range() -> None:
    major, minor = (int(p) for p in google.protobuf.__version__.split(".")[:2])
    assert (7, 34) <= (major, minor) < (8, 0)
