"""FoundationDB CDC → Kafka bridge pipeline.

Public contract: one CDC reply at a time, serialised to Protobuf, published
to Kafka, checkpointed, then acknowledged back to FDB. Both modes provide
at-least-once delivery; neither provides cross-system exactly-once.
"""

from fdb_kafka_bridge.bridge.cli import build_parser, parse_config
from fdb_kafka_bridge.bridge.config import (
    BridgeConfig,
    CDCConfig,
    FDBConfig,
    KafkaConfig,
    PipelineConfig,
    from_dict,
    from_env,
)
from fdb_kafka_bridge.bridge.pipeline import (
    Bridge,
    BridgeMetrics,
    BridgePipeline,
    Checkpoint,
)

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
    "build_parser",
    "from_dict",
    "from_env",
    "parse_config",
]
