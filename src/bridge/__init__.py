"""FoundationDB CDC → Kafka bridge pipeline.

Public contract: one CDC reply at a time, serialised to Protobuf,
published to Kafka (flushed or committed), then acknowledged back to FDB.
Both modes provide at-least-once delivery; neither provides cross-system
exactly-once.
"""

from src.bridge.pipeline import BridgePipeline, PublishFailedError

__all__ = ["BridgePipeline", "PublishFailedError"]
