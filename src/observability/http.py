"""Background HTTP daemon for metrics and health endpoints."""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Final

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from src.observability import health
from src.observability.metrics import REGISTRY

DEFAULT_HOST: Final[str] = "0.0.0.0"
DEFAULT_PORT: Final[int] = 9102

_server: ThreadingHTTPServer | None = None
_thread: threading.Thread | None = None
_lock = threading.Lock()


class _ObservabilityHandler(BaseHTTPRequestHandler):
    """Serve ``/metrics``, ``/healthz``, and ``/readyz``."""

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        """Silence default request logging."""

    def do_GET(self) -> None:  # noqa: N802
        """Dispatch GET routes."""
        path = self.path.split("?", 1)[0]
        if path == "/metrics":
            payload = generate_latest(REGISTRY)
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        if path == "/healthz":
            code = 200 if health.is_live() else 503
            body = b"ok\n" if code == 200 else b"unhealthy\n"
            self._text(code, body)
            return
        if path == "/readyz":
            code = 200 if health.is_ready() else 503
            body = b"ready\n" if code == 200 else b"not ready\n"
            self._text(code, body)
            return
        self._text(404, b"not found\n")

    def _text(self, code: int, body: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def start_http_server(
    *, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT
) -> ThreadingHTTPServer:
    """Start the observability HTTP server on a background daemon thread.

    Args:
        host: Bind address.
        port: Bind port (card default ``9102``).

    Returns:
        The running HTTP server.

    Raises:
        RuntimeError: If the server is already running.
    """
    global _server, _thread
    with _lock:
        if _server is not None:
            raise RuntimeError("observability HTTP server is already running")
        server = ThreadingHTTPServer((host, port), _ObservabilityHandler)
        thread = threading.Thread(
            target=server.serve_forever,
            name="observability-http",
            daemon=True,
        )
        thread.start()
        _server = server
        _thread = thread
        return server


def stop_http_server() -> None:
    """Stop the observability HTTP server if it is running."""
    global _server, _thread
    with _lock:
        server = _server
        _server = None
        _thread = None
    if server is not None:
        server.shutdown()
        server.server_close()
