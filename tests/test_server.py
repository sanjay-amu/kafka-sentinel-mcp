"""Unit tests for kafka-sentinel-mcp tools.

All Kafka interactions are mocked — tests verify tool logic, not librdkafka.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from kafka_sentinel_mcp import server


# ---------- fakes ----------

def fake_partition(leader=1, replicas=(1, 2, 3), isrs=(1, 2, 3)):
    return SimpleNamespace(leader=leader, replicas=list(replicas), isrs=list(isrs))


def fake_metadata(topics: dict, brokers=3, controller_id=1):
    return SimpleNamespace(
        topics={name: SimpleNamespace(partitions=parts) for name, parts in topics.items()},
        brokers={i: object() for i in range(brokers)},
        controller_id=controller_id,
    )


class FakeConsumer:
    """Mimics the subset of confluent_kafka.Consumer the server uses."""

    def __init__(self, committed_offsets, watermarks):
        self._committed = committed_offsets      # {partition_id: offset}
        self._watermarks = watermarks            # {partition_id: (earliest, end)}
        self.closed = False
        self.committed_called = False

    def committed(self, partitions, timeout=None):
        self.committed_called = True
        out = []
        for tp in partitions:
            tp.offset = self._committed.get(tp.partition, -1001)
            out.append(tp)
        return out

    def get_watermark_offsets(self, tp, timeout=None):
        return self._watermarks[tp.partition]

    def close(self):
        self.closed = True


# ---------- cluster_health ----------

def test_cluster_health_counts_urp_and_offline():
    md = fake_metadata({
        "t1": {
            0: fake_partition(leader=1, replicas=(1, 2, 3), isrs=(1, 2)),   # URP
            1: fake_partition(leader=-1, replicas=(1, 2, 3), isrs=(1, 2, 3)),  # offline
            2: fake_partition(),  # healthy
        },
    })
    admin = MagicMock()
    admin.list_topics.return_value = md
    with patch.object(server, "_admin", return_value=admin):
        out = server.cluster_health()
    assert out["brokers"] == 3
    assert out["under_replicated_partitions"] == 1
    assert out["offline_partitions"] == 1
    assert out["topics"] == 1


# ---------- consumer_lag ----------

def test_consumer_lag_computes_lag_and_handles_missing_offsets():
    md = fake_metadata({"orders": {0: fake_partition(), 1: fake_partition()}})
    admin = MagicMock()
    admin.list_topics.return_value = md
    consumer = FakeConsumer(
        committed_offsets={0: 90},          # partition 1 has no committed offset
        watermarks={0: (0, 100), 1: (0, 50)},
    )
    with patch.object(server, "_admin", return_value=admin), \
         patch.object(server, "_consumer", return_value=consumer):
        out = server.consumer_lag("g1", "orders")

    by_part = {row["partition"]: row for row in out}
    assert by_part[0]["lag"] == 10
    assert by_part[0]["committed"] == 90
    assert by_part[1]["committed"] == -1
    assert by_part[1]["lag"] == -1
    assert consumer.closed, "observer consumer must always be closed"


# ---------- topic_audit ----------

def make_admin_with_configs(md, configs: dict):
    admin = MagicMock()
    admin.list_topics.return_value = md
    entry = {k: SimpleNamespace(value=v) for k, v in configs.items()}
    fut = MagicMock()
    fut.result.return_value = entry
    admin.describe_configs.return_value = {"resource": fut}
    return admin


def test_topic_audit_flags_weak_durability():
    md = fake_metadata({"payments": {0: fake_partition(replicas=(1, 2), isrs=(1, 2))}})
    admin = make_admin_with_configs(
        md, {"min.insync.replicas": "1", "retention.ms": "604800000"})
    with patch.object(server, "_admin", return_value=admin):
        out = server.topic_audit("payments")

    assert out["replication_factor"] == 2
    assert out["min_insync_replicas"] == 1
    joined = " ".join(out["durability_flags"])
    assert "replication.factor=2" in joined
    assert "min.insync.replicas=1" in joined


def test_topic_audit_clean_config_has_no_flags():
    md = fake_metadata({"payments": {0: fake_partition()}})  # rf=3
    admin = make_admin_with_configs(
        md, {"min.insync.replicas": "2", "retention.ms": "604800000"})
    with patch.object(server, "_admin", return_value=admin):
        out = server.topic_audit("payments")
    assert out["durability_flags"] == []


def test_topic_audit_flags_min_isr_equal_to_rf():
    md = fake_metadata({"t": {0: fake_partition()}})  # rf=3
    admin = make_admin_with_configs(
        md, {"min.insync.replicas": "3", "retention.ms": "1"})
    with patch.object(server, "_admin", return_value=admin):
        out = server.topic_audit("t")
    assert any("blocks all writes" in f for f in out["durability_flags"])


# ---------- partition_state ----------

def test_partition_state_reports_skew_and_isr_shrink():
    md = fake_metadata({"t": {
        0: fake_partition(leader=1),
        1: fake_partition(leader=1),
        2: fake_partition(leader=2, isrs=(1, 2)),  # shrunk ISR
    }})
    admin = MagicMock()
    admin.list_topics.return_value = md
    with patch.object(server, "_admin", return_value=admin):
        out = server.partition_state("t")
    assert out["leader_skew"] == 1
    assert out["leader_distribution"] == {1: 2, 2: 1}
    shrunk = [p for p in out["partitions"] if p["isr_shrunk"]]
    assert [p["partition"] for p in shrunk] == [2]


# ---------- replay_readiness ----------

def test_replay_readiness_detects_retention_loss():
    md = fake_metadata({"t": {0: fake_partition(), 1: fake_partition()}})
    admin = MagicMock()
    admin.list_topics.return_value = md
    consumer = FakeConsumer(
        committed_offsets={0: 50, 1: 500},
        watermarks={0: (200, 1000), 1: (200, 1000)},  # earliest=200
    )
    with patch.object(server, "_admin", return_value=admin), \
         patch.object(server, "_consumer", return_value=consumer):
        out = server.replay_readiness("g1", "t")

    assert out["replay_safe"] is False
    assert len(out["partitions_at_risk"]) == 1
    risk = out["partitions_at_risk"][0]
    assert risk["partition"] == 0
    assert risk["messages_lost_to_retention"] == 150
    assert consumer.closed


def test_replay_readiness_safe_when_committed_within_retention():
    md = fake_metadata({"t": {0: fake_partition()}})
    admin = MagicMock()
    admin.list_topics.return_value = md
    consumer = FakeConsumer(committed_offsets={0: 300}, watermarks={0: (200, 1000)})
    with patch.object(server, "_admin", return_value=admin), \
         patch.object(server, "_consumer", return_value=consumer):
        out = server.replay_readiness("g1", "t")
    assert out["replay_safe"] is True
    assert out["partitions_at_risk"] == []


# ---------- topic-not-found guard ----------

def test_topic_audit_raises_clear_error_for_unknown_topic():
    md = fake_metadata({})  # no topics at all
    admin = MagicMock()
    admin.list_topics.return_value = md
    with patch.object(server, "_admin", return_value=admin):
        with pytest.raises(server.TopicNotFoundError, match="list_topics"):
            server.topic_audit("does-not-exist")


def test_consumer_lag_raises_clear_error_when_topic_metadata_has_error():
    # confluent_kafka represents "doesn't exist" as an entry present but
    # with .error set, not always a missing dict key — cover both shapes.
    md = fake_metadata({"ghost": {}})
    md.topics["ghost"].error = SimpleNamespace(code=lambda: 3)  # UNKNOWN_TOPIC
    admin = MagicMock()
    admin.list_topics.return_value = md
    with patch.object(server, "_admin", return_value=admin):
        with pytest.raises(server.TopicNotFoundError):
            server.consumer_lag("g1", "ghost")


def test_replay_readiness_raises_clear_error_for_unknown_topic():
    md = fake_metadata({})
    admin = MagicMock()
    admin.list_topics.return_value = md
    with patch.object(server, "_admin", return_value=admin):
        with pytest.raises(server.TopicNotFoundError):
            server.replay_readiness("g1", "does-not-exist")


# ---------- list_topics / list_consumer_groups ----------

def test_list_topics_excludes_internal_and_reports_rf():
    md = fake_metadata({
        "orders": {0: fake_partition(replicas=(1, 2, 3)), 1: fake_partition(replicas=(1, 2, 3))},
        "__consumer_offsets": {0: fake_partition()},
    })
    admin = MagicMock()
    admin.list_topics.return_value = md
    with patch.object(server, "_admin", return_value=admin):
        out = server.list_topics()
    names = [t["topic"] for t in out]
    assert "orders" in names
    assert "__consumer_offsets" not in names
    orders = next(t for t in out if t["topic"] == "orders")
    assert orders["partitions"] == 2
    assert orders["replication_factor"] == 3


def test_list_consumer_groups_returns_sorted_ids_and_state():
    group_a = SimpleNamespace(group_id="b-group", state=SimpleNamespace(name="STABLE"),
                               is_simple_consumer_group=False)
    group_b = SimpleNamespace(group_id="a-group", state=SimpleNamespace(name="EMPTY"),
                               is_simple_consumer_group=True)
    result = SimpleNamespace(valid=[group_a, group_b])
    future = MagicMock()
    future.result.return_value = result
    admin = MagicMock()
    admin.list_consumer_groups.return_value = future
    with patch.object(server, "_admin", return_value=admin):
        out = server.list_consumer_groups()
    assert [g["group"] for g in out] == ["a-group", "b-group"]
    assert out[0]["state"] == "EMPTY"
    assert out[0]["is_simple_consumer_group"] is True


# ---------- read-only invariants ----------

def test_server_module_imports_no_mutation_apis():
    """The whole point of the project: no write paths, ever."""
    import inspect
    src = inspect.getsource(server)
    for forbidden in ("NewTopic", "create_topics", "delete_topics",
                      "alter_configs", "incremental_alter_configs",
                      "Producer", "produce(", ".commit("):
        assert forbidden not in src, f"mutation API found in server: {forbidden}"


def test_observer_consumer_never_auto_commits():
    captured = {}
    class SpyConsumer(FakeConsumer):
        pass
    with patch.object(server, "Consumer") as consumer_cls:
        consumer_cls.side_effect = lambda conf: captured.update(conf) or MagicMock()
        server._consumer("g")
    assert captured["enable.auto.commit"] is False
