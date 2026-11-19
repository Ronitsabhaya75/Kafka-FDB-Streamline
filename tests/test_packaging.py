"""Wheel acceptance test: build, install into a fresh venv, import from elsewhere."""

import os
import re
import shutil
import subprocess
import venv
import zipfile
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

pytestmark = [
    pytest.mark.packaging,
    pytest.mark.skipif(
        os.environ.get("PACKAGING_INTEGRATION") != "1",
        reason="set PACKAGING_INTEGRATION=1 to build and install the wheel (network)",
    ),
]


@dataclass(frozen=True)
class InstalledWheel:
    wheel: Path
    python: Path
    outside_cwd: Path

    def run(self, code: str) -> str:
        # PYTHONPATH would let the repo satisfy imports the wheel fails to provide.
        env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
        result = subprocess.run(
            [str(self.python), "-c", code],
            cwd=self.outside_cwd,
            env=env,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        return result.stdout


def _copy_source_tree(dest: Path) -> None:
    # Tracked and new files only, so ignored local state cannot leak into the wheel.
    listed = subprocess.run(
        ["git", "ls-files", "-co", "--exclude-standard", "-z"],
        cwd=REPO_ROOT,
        capture_output=True,
        check=True,
        text=True,
    ).stdout
    for rel in filter(None, listed.split("\0")):
        src = REPO_ROOT / rel
        if src.is_file():
            target = dest / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, target)


@pytest.fixture(scope="module")
def installed(tmp_path_factory: pytest.TempPathFactory) -> InstalledWheel:
    root = tmp_path_factory.mktemp("packaging")
    source = root / "source"
    _copy_source_tree(source)

    venv.create(root / "venv", with_pip=True)
    python = root / "venv" / "bin" / "python"
    outside_cwd = root / "cwd"
    outside_cwd.mkdir()

    pip = [str(python), "-m", "pip", "--quiet", "--disable-pip-version-check"]
    subprocess.run(
        [*pip, "wheel", "--no-deps", "--wheel-dir", str(root / "dist"), str(source)],
        cwd=outside_cwd,
        check=True,
    )
    wheels = list((root / "dist").glob("*.whl"))
    assert len(wheels) == 1
    subprocess.run([*pip, "install", str(wheels[0])], cwd=outside_cwd, check=True)
    return InstalledWheel(wheels[0], python, outside_cwd)


@pytest.mark.parametrize(
    "module",
    [
        "fdb_kafka_bridge",
        "fdb_kafka_bridge.bridge",
        "fdb_kafka_bridge.cdc",
        "fdb_kafka_bridge.kafka",
        "fdb_kafka_bridge.serialization",
        "fdbkafka.cdc.v1",
        "fdbkafka.cdc.v1.mutations_pb2",
    ],
)
def test_module_imports_from_site_packages(
    installed: InstalledWheel, module: str
) -> None:
    location = installed.run(
        f"import importlib; print(importlib.import_module({module!r}).__file__)"
    )
    assert "site-packages" in location


def test_wheel_top_level_is_exactly_the_two_packages(
    installed: InstalledWheel,
) -> None:
    with zipfile.ZipFile(installed.wheel) as wheel:
        top_level = {name.split("/")[0] for name in wheel.namelist()}
    top_level = {name for name in top_level if not name.endswith(".dist-info")}
    assert top_level == {"fdb_kafka_bridge", "fdbkafka"}


def test_src_is_not_importable(installed: InstalledWheel) -> None:
    output = installed.run(
        "import importlib.util; print(importlib.util.find_spec('src'))"
    )
    assert output.strip() == "None"


def test_wheel_metadata_names_distribution_and_requires_foundationdb(
    installed: InstalledWheel,
) -> None:
    with zipfile.ZipFile(installed.wheel) as wheel:
        metadata_path = next(
            name for name in wheel.namelist() if name.endswith(".dist-info/METADATA")
        )
        metadata = wheel.read(metadata_path).decode()
    assert re.search(r"^Name: fdb[-_]kafka[-_]bridge$", metadata, re.MULTILINE)
    assert re.search(r"^Requires-Dist: foundationdb\b", metadata, re.MULTILINE)


def test_resolved_foundationdb_matches_dockerfile_minor(
    installed: InstalledWheel,
) -> None:
    dockerfile = (REPO_ROOT / "Dockerfile").read_text()
    docker_version = re.search(r"^ARG FDB_VERSION=(\S+)$", dockerfile, re.MULTILINE)
    assert docker_version is not None
    resolved = installed.run(
        "import importlib.metadata as m; print(m.version('foundationdb'))"
    ).strip()
    assert resolved.split(".")[:2] == docker_version[1].split(".")[:2]


def test_daemon_entrypoint_outside_checkout(installed: InstalledWheel) -> None:
    output = installed.run(
        "import pathlib, subprocess, sys; "
        "script = pathlib.Path(sys.executable).with_name('streamline-daemon'); "
        "result = subprocess.run([str(script), '--help'], "
        "capture_output=True, text=True); "
        "assert result.returncode == 0, result.stderr; print(result.stdout)"
    )
    assert "--checkpoint-path" in output
    assert "--transactional-id" in output
