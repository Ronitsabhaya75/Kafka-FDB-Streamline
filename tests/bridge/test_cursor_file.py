"""Unit tests for cursor-file persistence durability and validation."""

from __future__ import annotations

import errno
import json
import os
from pathlib import Path
from typing import Any

import pytest

from src.bridge.pipeline import _load_cursor_from_file, _save_cursor_to_file


class _Pos:
    def __init__(self, stream_id: Any = 7, last_consumed_version: Any = 100) -> None:
        self.stream_id = stream_id
        self.last_consumed_version = last_consumed_version


def _write(path: Path, payload: Any) -> str:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return str(path)


def _fail_dir_fsync(
    monkeypatch: pytest.MonkeyPatch, *, on_open: bool, err: int
) -> None:
    """Make opening or fsyncing a directory fd raise ``OSError(err)``."""
    real_open, real_fsync = os.open, os.fsync
    dir_fds: set[int] = set()

    def fake_open(path: Any, flags: int, *args: Any, **kwargs: Any) -> int:
        if os.path.isdir(path):
            if on_open:
                raise OSError(err, os.strerror(err))
            fd = real_open(path, flags, *args, **kwargs)
            dir_fds.add(fd)
            return fd
        return real_open(path, flags, *args, **kwargs)

    def fake_fsync(fd: int) -> None:
        if fd in dir_fds:
            raise OSError(err, os.strerror(err))
        real_fsync(fd)

    monkeypatch.setattr(os, "open", fake_open)
    monkeypatch.setattr(os, "fsync", fake_fsync)


def test_save_none_position_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="position"):
        _save_cursor_to_file(str(tmp_path / "c.cursor"), None)
    assert not (tmp_path / "c.cursor").exists()


@pytest.mark.parametrize("path", [None, ""])
def test_save_without_path_is_noop(path: str | None) -> None:
    _save_cursor_to_file(path, _Pos())
    _save_cursor_to_file(path, None)


@pytest.mark.parametrize("on_open", [True, False])
@pytest.mark.parametrize("err", [errno.EIO, errno.EACCES])
def test_save_dir_sync_failure_propagates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, on_open: bool, err: int
) -> None:
    _fail_dir_fsync(monkeypatch, on_open=on_open, err=err)
    with pytest.raises(OSError) as exc_info:
        _save_cursor_to_file(str(tmp_path / "c.cursor"), _Pos())
    assert exc_info.value.errno == err


@pytest.mark.parametrize("on_open", [True, False])
@pytest.mark.parametrize(
    "err",
    sorted({errno.EINVAL, errno.ENOTSUP, getattr(errno, "EOPNOTSUPP", errno.ENOTSUP)}),
)
def test_save_dir_sync_unsupported_is_tolerated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, on_open: bool, err: int
) -> None:
    _fail_dir_fsync(monkeypatch, on_open=on_open, err=err)
    path = str(tmp_path / "c.cursor")
    _save_cursor_to_file(path, _Pos())
    loaded = _load_cursor_from_file(path)
    assert loaded is not None
    assert loaded.stream_id == 7


def test_round_trip_with_stream_name(tmp_path: Path) -> None:
    path = str(tmp_path / "c.cursor")
    _save_cursor_to_file(path, _Pos(42, 123), stream_name="orders")
    with open(path, encoding="utf-8") as f:
        assert json.load(f)["stream_name"] == "orders"
    loaded = _load_cursor_from_file(path, stream_name="orders")
    assert loaded is not None
    assert (loaded.stream_id, loaded.last_consumed_version) == (42, 123)


def test_load_stream_name_mismatch_raises(tmp_path: Path) -> None:
    path = str(tmp_path / "c.cursor")
    _save_cursor_to_file(path, _Pos(), stream_name="orders")
    with pytest.raises(ValueError, match="stream"):
        _load_cursor_from_file(path, stream_name="payments")


def test_load_stream_name_absent_from_file_raises(tmp_path: Path) -> None:
    path = _write(tmp_path / "c.cursor", {"stream_id": 1, "last_consumed_version": 2})
    with pytest.raises(ValueError, match="stream"):
        _load_cursor_from_file(path, stream_name="orders")


def test_load_without_stream_name_skips_check(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "c.cursor",
        {"stream_name": "other", "stream_id": 1, "last_consumed_version": 2},
    )
    loaded = _load_cursor_from_file(path)
    assert loaded is not None
    assert loaded.last_consumed_version == 2


@pytest.mark.parametrize(
    "payload",
    [
        {"last_consumed_version": 2},
        {"stream_id": 1},
        {},
        {"stream_id": None, "last_consumed_version": 2},
        {"stream_id": 1, "last_consumed_version": None},
        {"stream_id": True, "last_consumed_version": 2},
        {"stream_id": 1, "last_consumed_version": False},
        {"stream_id": "1", "last_consumed_version": 2},
        {"stream_id": 1, "last_consumed_version": "2"},
        {"stream_id": 1.0, "last_consumed_version": 2},
    ],
)
def test_load_invalid_coordinates_raise(tmp_path: Path, payload: dict) -> None:
    path = _write(tmp_path / "c.cursor", payload)
    with pytest.raises(ValueError, match="Corrupt cursor file"):
        _load_cursor_from_file(path)


def test_load_null_document_raises(tmp_path: Path) -> None:
    path = _write(tmp_path / "c.cursor", None)
    with pytest.raises(ValueError, match="expected JSON object"):
        _load_cursor_from_file(path)


@pytest.mark.parametrize("name", [None, 5, ""])
def test_load_non_string_stream_name_in_file_raises(tmp_path: Path, name: Any) -> None:
    path = _write(
        tmp_path / "c.cursor",
        {"stream_name": name, "stream_id": 1, "last_consumed_version": 2},
    )
    with pytest.raises(ValueError, match="stream"):
        _load_cursor_from_file(path, stream_name="orders")
