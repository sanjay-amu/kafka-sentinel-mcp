"""Seed local Kafka with synthetic demo data for kafka-sentinel-mcp recordings.

Not part of the installed package, and deliberately kept out of
src/kafka_sentinel_mcp — that module never imports topic/producer mutation
APIs, and this script needs both to set the stage for a demo.

Creates:
  - "orders"   (4 partitions, produced in full, consumed ~60% by a group
                that then stops — a realistic, non-alarming lag story)
  - "payments" (2 partitions, produced in full, no consumer group —
                surfaces real topic_audit flags on a single-broker cluster,
                since replication.factor=1 here is a true local-dev limitation,
                not a fabricated one)

All payloads are synthetic placeholders (fake order/customer ids) — no real
customer data. Safe to re-run; topic creation is idempotent.
"""

from __future__ import annotations

import json
import os
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
ORDERS_TOPIC = "orders"
PAYMENTS_TOPIC = "payments"
CONSUMER_GROUP = "orders-consumer-service"
ORDERS_MESSAGE_COUNT = 500
PAYMENTS_MESSAGE_COUNT = 200
CONSUME_FRACTION = 0.6  # leaves realistic lag on "orders"


def create_topics(admin: AdminClient) -> None:
    topics = [
        NewTopic(ORDERS_TOPIC, num_partitions=4, replication_factor=1),
        NewTopic(PAYMENTS_TOPIC, num_partitions=2, replication_factor=1),
    ]
    futures = admin.create_topics(topics)
    for name, fut in futures.items():
        try:
            fut.result()
            print(f"created topic: {name}")
        except Exception as e:  # topic already exists on re-run
            print(f"skip {name}: {e}")


def produce(topic: str, count: int) -> None:
    p = Producer({"bootstrap.servers": BOOTSTRAP})
    for i in range(count):
        payload = {
            "id": f"{topic}-demo-{i:05d}",
            "customer": f"fake-customer-{i % 37}",
            "amount_cents": (i * 137) % 9999,
            "ts": time.time(),
        }
        p.produce(topic, json.dumps(payload).encode("utf-8"))
        if i % 200 == 0:
            p.poll(0)
    p.flush()
    print(f"produced {count} messages to {topic}")


def simulate_partial_consumption(topic: str, group: str, fraction: float) -> None:
    from confluent_kafka import Consumer, TopicPartition

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    md = admin.list_topics(topic=topic, timeout=10)
    partitions = list(md.topics[topic].partitions.keys())

    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    c.assign([TopicPartition(topic, p) for p in partitions])

    to_commit = []
    for p in partitions:
        _, end = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        target = int(end * fraction)
        to_commit.append(TopicPartition(topic, p, target))

    c.commit(offsets=to_commit, asynchronous=False)
    c.close()
    print(f"group '{group}' committed to ~{int(fraction * 100)}% of {topic} — stalled there")


def main() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    create_topics(admin)
    time.sleep(2)  # let topic creation propagate
    produce(ORDERS_TOPIC, ORDERS_MESSAGE_COUNT)
    produce(PAYMENTS_TOPIC, PAYMENTS_MESSAGE_COUNT)
    simulate_partial_consumption(ORDERS_TOPIC, CONSUMER_GROUP, CONSUME_FRACTION)
    print("\nDemo data ready. Try asking your agent:")
    print(f'  "Why is the {CONSUMER_GROUP} group falling behind on {ORDERS_TOPIC}?"')
    print(f'  "Audit the {PAYMENTS_TOPIC} topic for durability risks."')


if __name__ == "__main__":
    main()
