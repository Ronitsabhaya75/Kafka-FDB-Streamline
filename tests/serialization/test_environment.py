import re
import tomllib
from pathlib import Path

import google.protobuf
from google.protobuf.internal import api_implementation

ROOT = Path(__file__).resolve().parents[2]
RUNTIME_RANGE = ((7, 34), (8, 0))


def _major_minor(version: str) -> tuple[int, int]:
    major, minor = (int(p) for p in version.split(".")[:2])
    return major, minor


def test_backend_is_upb() -> None:
    assert api_implementation.Type() == "upb"


# Pins INV-SER-11.
def test_runtime_version_in_supported_range() -> None:
    low, high = RUNTIME_RANGE
    assert low <= _major_minor(google.protobuf.__version__) < high


# Pins INV-SER-11.
def test_protobuf_version_pins_agree() -> None:
    (low_major, low_minor), (high_major, _) = RUNTIME_RANGE
    plugins = re.findall(
        r"buf\.build/protocolbuffers/\w+:v(\d+\.\d+)",
        (ROOT / "buf.gen.yaml").read_text(),
    )
    gencode = re.search(
        r"# Protobuf Python Version: (\S+)",
        (ROOT / "protobuf/gen/fdbkafka/cdc/v1/mutations_pb2.py").read_text(),
    )
    dependencies = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"][
        "dependencies"
    ]
    dev_pin = re.search(
        r"^protobuf==(\S+)$", (ROOT / "requirements-dev.txt").read_text(), re.M
    )

    assert gencode is not None and dev_pin is not None
    # Plugin vN.M emits Python gencode X.N.M.
    assert set(plugins) == {gencode.group(1).split(".", 1)[1]}
    # A runtime loads gencode at most one major older.
    assert int(gencode.group(1).split(".")[0]) == low_major - 1
    assert f"protobuf>={low_major}.{low_minor},<{high_major}" in dependencies
    low, high = RUNTIME_RANGE
    assert low <= _major_minor(dev_pin.group(1)) < high
