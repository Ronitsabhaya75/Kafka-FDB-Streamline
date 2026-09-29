"""Allow ``python -m src.bridge`` to start the daemon."""

from src.bridge.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
