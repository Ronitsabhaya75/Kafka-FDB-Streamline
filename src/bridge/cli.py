"""CLI entrypoint for the FoundationDB CDC → Kafka bridge daemon."""

from __future__ import annotations

import sys

from src.bridge.config import load_config
from src.bridge.daemon import BridgeDaemon


def main(argv: list[str] | None = None) -> int:
    """Load config and run the bridge daemon.

    Args:
        argv: Optional argument list without the program name.

    Returns:
        Process exit code (``0`` clean, ``2`` config error, ``1`` runtime error).
    """
    try:
        config = load_config(argv)
    except SystemExit as exc:
        code = exc.code
        if code is None:
            return 0
        if isinstance(code, int):
            return code
        return 2
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    daemon = BridgeDaemon(config)
    try:
        daemon.run()
    except KeyboardInterrupt:
        daemon.request_stop()
        return 0
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
