"""Kafka publish path for the CDC → Kafka bridge."""

from src.kafka.producer import MutationProducer

__all__ = ["MutationProducer"]
