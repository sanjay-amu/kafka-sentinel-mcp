"""kafka-sentinel-mcp: read-only Kafka observability tools for MCP clients.

Design rule: this module never imports any Kafka admin mutation API.
Every tool observes; nothing writes. The observer consumer never commits.
"""

from __future__ import annotations

import logging
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource
from mcp.server.fastmcp import FastMCP

logger = logging.getLogger("kafka-sentinel")

mcp = FastMCP("kafka-sentinel")

# Durability guardrails flagged by topic_audit
MIN_RF = 3
MIN_ISR = 2

_REQUEST_TIMEOUT = float(os.environ.get("KAFKA_SENTINEL_TIMEOUT", "10"))


class TopicNotFoundError(Exception):
    """Raised when a requested topic doesn't exist on the cluster.

    Every tool that takes a `topic` argument raises this instead of letting
    a raw KeyError leak out of a dict lookup — the message tells the caller
    (human or agent) to run list_topics() rather than guess.
    """


def _base_conf() -> dict:
    conf = {"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")}
    # SASL/SSL passthrough — values are read from env and never logged.
    for k in (
        "security.protocol",
        "sasl.mechanism",
        "sasl.username",
        "sasl.password",
        "ssl.ca.location",
    ):
        env = "KAFKA_" + k.upper().replace(".", "_")
        if env in os.environ:
            conf[k] = os.environ[env]
    return conf


def _admin() -> AdminClient:
    return AdminClient(_base_conf())


def _consumer(group: str = "kafka-sentinel-observer") -> Consumer:
    return Consumer({
        **_base_conf(),
        "group.id": group,
        "enable.auto.commit": False,  # observer never commits
    })


def _audit_log(tool: str, **params) -> None:
    """Log every tool invocation (no secrets ever pass through params)."""
    logger.info("tool=%s params=%s at=%s", tool, params,
                datetime.now(timezone.utc).isoformat())


def _topic_metadata(admin: AdminClient, topic: str):
    """Fetch metadata for one topic, raising TopicNotFoundError with a
    helpful message instead of a raw KeyError if it doesn't exist."""
    md = admin.list_topics(topic=topic, timeout=_REQUEST_TIMEOUT)
    entry = md.topics.get(topic)
    if entry is None or getattr(entry, "error", None) is not None:
        raise TopicNotFoundError(
            f"Topic '{topic}' not found on this cluster. "
            "Call list_topics() to see what's available."
        )
    return entry


@dataclass
class PartitionLag:
    topic: str
    partition: int
    committed: int
    end_offset: int
    lag: int


@mcp.tool()
def cluster_health() -> dict:
    """Broker count, controller, and under-replicated / offline partition summary."""
    _audit_log("cluster_health")
    md = _admin().list_topics(timeout=_REQUEST_TIMEOUT)
    urp = 0
    offline = 0
    for t in md.topics.values():
        for p in t.partitions.values():
            if len(p.isrs) < len(p.replicas):
                urp += 1
            if p.leader == -1:
                offline += 1
    return {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "brokers": len(md.brokers),
        "controller_id": md.controller_id,
        "topics": len(md.topics),
        "under_replicated_partitions": urp,
        "offline_partitions": offline,
    }


@mcp.tool()
def consumer_lag(group: str, topic: str) -> list[dict]:
    """Per-partition lag for a consumer group on a topic:
    committed offset vs log-end offset. committed = -1 means no offset stored."""
    _audit_log("consumer_lag", group=group, topic=topic)
    admin = _admin()
    entry = _topic_metadata(admin, topic)
    partitions = [TopicPartition(topic, p) for p in entry.partitions]

    c = _consumer(group)
    try:
        committed = c.committed(partitions, timeout=_REQUEST_TIMEOUT)
        out: list[PartitionLag] = []
        for tp in committed:
            _, end = c.get_watermark_offsets(tp, timeout=_REQUEST_TIMEOUT)
            has_offset = tp.offset >= 0
            out.append(PartitionLag(
                topic=topic,
                partition=tp.partition,
                committed=tp.offset if has_offset else -1,
                end_offset=end,
                lag=(end - tp.offset) if has_offset else -1,
            ))
        return [asdict(p) for p in out]
    finally:
        c.close()


@mcp.tool()
def topic_audit(topic: str) -> dict:
    """Replication and durability config audit for a topic.
    Flags settings that violate mission-critical best practice."""
    _audit_log("topic_audit", topic=topic)
    admin = _admin()
    t = _topic_metadata(admin, topic)
    rf = len(next(iter(t.partitions.values())).replicas) if t.partitions else 0

    cfg = admin.describe_configs([ConfigResource(ConfigResource.Type.TOPIC, topic)])
    resource = next(iter(cfg.values())).result(timeout=_REQUEST_TIMEOUT)
    min_isr = int(resource["min.insync.replicas"].value)

    flags = []
    if rf < MIN_RF:
        flags.append(
            f"replication.factor={rf} < {MIN_RF}: broker loss can mean data loss")
    if min_isr < MIN_ISR:
        flags.append(
            f"min.insync.replicas={min_isr} < {MIN_ISR}: acks=all is not durable")
    if rf > 0 and min_isr >= rf:
        flags.append(
            "min.insync.replicas >= replication.factor: "
            "one broker down blocks all writes")

    return {
        "topic": topic,
        "partitions": len(t.partitions),
        "replication_factor": rf,
        "min_insync_replicas": min_isr,
        "retention_ms": resource["retention.ms"].value,
        "durability_flags": flags,
    }


@mcp.tool()
def partition_state(topic: str) -> dict:
    """Leader/ISR state per partition, plus leader skew across brokers."""
    _audit_log("partition_state", topic=topic)
    t = _topic_metadata(_admin(), topic)
    leaders: dict[int, int] = {}
    parts = []
    for pid, p in sorted(t.partitions.items()):
        leaders[p.leader] = leaders.get(p.leader, 0) + 1
        parts.append({
            "partition": pid,
            "leader": p.leader,
            "replicas": list(p.replicas),
            "isr": list(p.isrs),
            "isr_shrunk": len(p.isrs) < len(p.replicas),
        })
    counts = list(leaders.values())
    return {
        "topic": topic,
        "partitions": parts,
        "leader_distribution": leaders,
        "leader_skew": (max(counts) - min(counts)) if counts else 0,
    }


@mcp.tool()
def replay_readiness(group: str, topic: str) -> dict:
    """Can this group still replay everything since its committed offsets,
    or has retention already deleted part of the gap?"""
    _audit_log("replay_readiness", group=group, topic=topic)
    admin = _admin()
    entry = _topic_metadata(admin, topic)
    partitions = [TopicPartition(topic, p) for p in entry.partitions]

    c = _consumer(group)
    try:
        committed = c.committed(partitions, timeout=_REQUEST_TIMEOUT)
        at_risk = []
        for tp in committed:
            earliest, _ = c.get_watermark_offsets(tp, timeout=_REQUEST_TIMEOUT)
            if 0 <= tp.offset < earliest:
                at_risk.append({
                    "partition": tp.partition,
                    "committed": tp.offset,
                    "earliest_available": earliest,
                    "messages_lost_to_retention": earliest - tp.offset,
                })
        return {
            "group": group,
            "topic": topic,
            "replay_safe": not at_risk,
            "partitions_at_risk": at_risk,
        }
    finally:
        c.close()


@mcp.tool()
def list_topics() -> list[dict]:
    """List all non-internal topics with partition count and replication
    factor. Use this first if you don't already know a topic name — every
    other tool needs one."""
    _audit_log("list_topics")
    md = _admin().list_topics(timeout=_REQUEST_TIMEOUT)
    out = []
    for name, t in sorted(md.topics.items()):
        if name.startswith("__"):
            continue  # internal topics (__consumer_offsets, etc.) — noise
        rf = len(next(iter(t.partitions.values())).replicas) if t.partitions else 0
        out.append({
            "topic": name,
            "partitions": len(t.partitions),
            "replication_factor": rf,
        })
    return out


@mcp.tool()
def list_consumer_groups() -> list[dict]:
    """List all consumer group IDs on the cluster with their state.
    Use this first if you don't already know a group name."""
    _audit_log("list_consumer_groups")
    future = _admin().list_consumer_groups(request_timeout=_REQUEST_TIMEOUT)
    result = future.result(timeout=_REQUEST_TIMEOUT)
    return [
        {
            "group": g.group_id,
            "state": getattr(g.state, "name", str(g.state)),
            "is_simple_consumer_group": g.is_simple_consumer_group,
        }
        for g in sorted(result.valid, key=lambda g: g.group_id)
    ]


@mcp.tool()
def incident_snapshot(group: str, topic: str) -> dict:
    """One-call bundle: cluster health + lag + audit + partition state +
    replay readiness. Timestamped for postmortems."""
    _audit_log("incident_snapshot", group=group, topic=topic)
    return {
        "cluster": cluster_health(),
        "lag": consumer_lag(group, topic),
        "topic_audit": topic_audit(topic),
        "partition_state": partition_state(topic),
        "replay": replay_readiness(group, topic),
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    mcp.run()


if __name__ == "__main__":
    main()
