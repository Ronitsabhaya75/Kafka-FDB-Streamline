"""CDC → Kafka bridge daemon run loop and signal handling."""

from __future__ import annotations

import signal
import threading
import time
from collections.abc import Callable
from types import FrameType
from typing import Any

from src.bridge.config import BridgeConfig
from src.cdc.client import FDBClient
from src.cdc.listener import FDBMutationListener
from src.kafka import MutationProducer
from src.observability import (
    configure_logging,
    get_logger,
    start_http_server,
    stop_http_server,
)


class BridgeDaemon:
    """Run the poll → produce → acknowledge loop until asked to stop.

    Durable FDB version checkpoints are owned by a separate module; this daemon
    finishes an in-flight produce and then calls CDC ``acknowledge()`` before
    tearing down handles.
    """

    def __init__(
        self,
        config: BridgeConfig,
        *,
        client: FDBClient | None = None,
        listener: FDBMutationListener | None = None,
        producer: MutationProducer | None = None,
        sleep: Callable[[float], None] = time.sleep,
        start_metrics: bool = True,
    ) -> None:
        """Create a daemon bound to ``config``.

        Args:
            config: Parsed bridge configuration.
            client: Optional pre-built FDB client (tests).
            listener: Optional pre-built listener (tests).
            producer: Optional pre-built producer (tests).
            sleep: Sleep callable used on empty polls.
            start_metrics: When False, skip binding the observability HTTP server.
        """
        self.config = config
        self._sleep = sleep
        self._start_metrics = start_metrics
        self._stop = threading.Event()
        self._client = client
        self._listener = listener
        self._producer = producer
        self._owns_client = client is None
        self._logger: Any = None

    def request_stop(self) -> None:
        """Signal the run loop to exit after the current iteration."""
        self._stop.set()

    def _install_signal_handlers(self) -> None:
        def _handler(signum: int, frame: FrameType | None) -> None:
            del signum, frame
            self.request_stop()

        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)

    def _ensure_dependencies(self) -> None:
        if self._client is None:
            self._client = FDBClient(cluster_file=self.config.cluster_file)
        if self._listener is None:
            subspace = self.config.subspace_prefix
            self._listener = FDBMutationListener(
                self._client,
                self.config.stream_name,
                subspace=subspace,
            )
        if self._producer is None:
            self._producer = MutationProducer(topic=self.config.topic)

    def run(self) -> None:
        """Start dependencies and block in the poll loop until stop.

        Raises:
            Exception: Propagates failures from start, poll, produce, or ack.
        """
        configure_logging()
        self._logger = get_logger("streamline.daemon")
        self._install_signal_handlers()
        if self._start_metrics:
            start_http_server(
                host=self.config.metrics_host,
                port=self.config.metrics_port,
            )

        self._ensure_dependencies()
        assert self._listener is not None
        assert self._producer is not None

        self._listener.start()
        self._logger.info(
            "daemon_started",
            stream_name=self.config.stream_name,
            topic=self.config.topic,
            poll_interval_ms=self.config.poll_interval_ms,
        )

        try:
            self._loop()
        finally:
            self.close()

    def _loop(self) -> None:
        assert self._listener is not None
        assert self._producer is not None
        poll_sleep = self.config.poll_interval_ms / 1000.0
        while not self._stop.is_set():
            records = self._listener.poll_records()
            if self._stop.is_set() and not records:
                break
            if not records:
                self._sleep(poll_sleep)
                continue
            self._producer.produce(records)
            self._listener.acknowledge()

    def close(self) -> None:
        """Close producer, listener, owned client, and metrics HTTP."""
        if self._producer is not None:
            try:
                self._producer.close()
            except Exception:
                if self._logger is not None:
                    self._logger.exception("producer_close_failed")
            self._producer = None
        if self._listener is not None:
            try:
                self._listener.close()
            except Exception:
                if self._logger is not None:
                    self._logger.exception("listener_close_failed")
            self._listener = None
        if self._client is not None and self._owns_client:
            try:
                self._client.close()
            except Exception:
                if self._logger is not None:
                    self._logger.exception("client_close_failed")
            self._client = None
        if self._start_metrics:
            stop_http_server()
        if self._logger is not None:
            self._logger.info("daemon_stopped")
