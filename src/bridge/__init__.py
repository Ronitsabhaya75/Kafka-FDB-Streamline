"""FoundationDB CDC → Kafka bridge pipeline.

Public contract: one CDC reply at a time, serialised to Protobuf,
published inside a Kafka transaction, then acknowledged back to FDB.
"""

from src.bridge.pipeline import Bridge, BridgeMetrics, BridgePipeline

__all__ = ["Bridge", "BridgeMetrics", "BridgePipeline"]
