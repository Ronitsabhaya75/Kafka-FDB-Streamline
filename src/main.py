"""Allow ``python -m src.main`` to start the bridge daemon."""

from src.bridge.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
