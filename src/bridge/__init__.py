"""FoundationDB CDC → Kafka bridge daemon package.

Public contract: [`docs/DAEMON-SPEC.md`](../../docs/DAEMON-SPEC.md).
"""

from src.bridge.config import BridgeConfig, load_config
from src.bridge.daemon import BridgeDaemon

__all__ = ["BridgeConfig", "BridgeDaemon", "load_config"]
