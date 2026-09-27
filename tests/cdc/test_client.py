"""Unit tests for FDBClient and init_fdb."""

from unittest.mock import MagicMock, patch

import pytest

from src.cdc.client import FDBClient, init_fdb
from src.cdc.errors import CDCNotSupportedError


def test_init_fdb_version_too_low() -> None:
    with pytest.raises(CDCNotSupportedError, match="API version 800 or later"):
        init_fdb(api_version=730)


def test_init_fdb_version_too_low_without_fdb() -> None:
    with patch("src.cdc.client.fdb", None):
        with pytest.raises(CDCNotSupportedError, match="API version 800 or later"):
            init_fdb(api_version=730)


def test_init_fdb_missing_module() -> None:
    with patch("src.cdc.client.fdb", None):
        with pytest.raises(CDCNotSupportedError, match="bindings.*not installed"):
            init_fdb(800)


def test_init_fdb_idempotent() -> None:
    mock_fdb = MagicMock()
    mock_fdb.get_api_version.return_value = 800
    with patch("src.cdc.client.fdb", mock_fdb):
        init_fdb(800)
        mock_fdb.api_version.assert_not_called()


def test_init_fdb_conflicting_version() -> None:
    mock_fdb = MagicMock()
    mock_fdb.get_api_version.return_value = 740
    with patch("src.cdc.client.fdb", mock_fdb):
        with pytest.raises(RuntimeError, match="already initialized to 740"):
            init_fdb(800)


def test_fdb_client_open_and_close() -> None:
    mock_fdb = MagicMock()
    mock_fdb.get_api_version.return_value = 800
    mock_db = MagicMock()
    mock_fdb.open.return_value = mock_db

    with patch("src.cdc.client.fdb", mock_fdb):
        client = FDBClient(cluster_file="/etc/fdb.cluster", api_version=800)
        db = client.open()
        assert db is mock_db
        mock_fdb.open.assert_called_once_with("/etc/fdb.cluster")

        # Second access via property uses cached db
        assert client.db is mock_db
        assert mock_fdb.open.call_count == 1

        client.close()
        assert client._db is None


def test_fdb_client_context_manager() -> None:
    mock_fdb = MagicMock()
    mock_fdb.get_api_version.return_value = 800
    mock_db = MagicMock()
    mock_fdb.open.return_value = mock_db

    with patch("src.cdc.client.fdb", mock_fdb):
        with FDBClient(cluster_file=None) as client:
            assert client.db is mock_db
        assert client._db is None


def test_fdb_client_cdc_delegates() -> None:
    mock_fdb = MagicMock()
    mock_fdb.get_api_version.return_value = 800
    mock_db = MagicMock()
    mock_fdb.open.return_value = mock_db

    mock_db.register_cdc_stream.return_value.wait.return_value = 42
    mock_db.create_cdc_consumer.return_value.wait.return_value = MagicMock()
    mock_db.resume_cdc_consumer.return_value.wait.return_value = MagicMock()
    mock_db.remove_cdc_stream.return_value.wait.return_value = None
    mock_db.list_cdc_streams.return_value.wait.return_value = ["stream1", "stream2"]

    with patch("src.cdc.client.fdb", mock_fdb):
        client = FDBClient()

        # register_cdc_stream with str name
        stream_id = client.register_cdc_stream("test_stream", b"\x00", b"\xff")
        assert stream_id == 42
        mock_db.register_cdc_stream.assert_called_once_with(
            b"test_stream", b"\x00", b"\xff"
        )

        # create_cdc_consumer
        consumer = client.create_cdc_consumer("test_stream")
        assert consumer is not None
        mock_db.create_cdc_consumer.assert_called_once_with(b"test_stream")

        # resume_cdc_consumer
        resumed = client.resume_cdc_consumer((42, 100))
        assert resumed is not None
        mock_db.resume_cdc_consumer.assert_called_once_with((42, 100))

        # remove_cdc_stream
        client.remove_cdc_stream("test_stream")
        mock_db.remove_cdc_stream.assert_called_once_with(b"test_stream")

        # list_cdc_streams
        streams = client.list_cdc_streams()
        assert streams == ["stream1", "stream2"]
