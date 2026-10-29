"""FoundationDB CDC → Kafka bridge pipeline.

Public contract: one CDC reply at a time, serialised to Protobuf, published
to Kafka, checkpointed, then acknowledged back to FDB. Both modes provide
at-least-once delivery; neither provides cross-system exactly-once.
"""

from src.bridge.config import (
    BridgeConfig,
    CDCConfig,
    FDBConfig,
    KafkaConfig,
    PipelineConfig,
    from_dict,
)
from src.bridge.pipeline import Bridge, BridgeMetrics, BridgePipeline, Checkpoint

__all__ = [
    "Bridge",
    "BridgeConfig",
    "BridgeMetrics",
    "BridgePipeline",
    "CDCConfig",
    "Checkpoint",
    "FDBConfig",
    "KafkaConfig",
    "PipelineConfig",
    "from_dict",
]
