# kafka-sentinel-mcp

**Give AI agents safe, read-only eyes on your Kafka clusters.**

An [MCP (Model Context Protocol)](https://modelcontextprotocol.io) server that exposes Kafka cluster health, consumer lag, partition state, and replay-readiness as structured tools — so LLM agents (Claude, or any MCP client) can diagnose streaming incidents without ever being able to break anything.

Built by an engineer who spent a decade running Kafka-based financial messaging at 99.999% availability, and got tired of every "AI + Kafka" demo assuming write access to production.

## Why this exists

When a consumer group stalls at 3 a.m., the questions are always the same: Is it lag? A stuck partition? A rebalance storm? An offset reset gone wrong? These are pattern-matching questions — exactly what LLM agents are good at — but no operator will hand an agent admin rights on a production cluster.

`kafka-sentinel-mcp` draws a hard line: **every tool is read-only by design**, enforced at the client-config level (no admin operations are even imported). The agent can observe, correlate, and recommend; a human executes.

## Tools

| Tool | What it returns |
|---|---|
| `cluster_health` | Broker count, controller status, under-replicated / offline partition counts |
| `consumer_lag` | Per-group, per-topic, per-partition lag with committed vs end offsets |
| `topic_audit` | Replication factor, min.insync.replicas, retention, and flags configs that violate durability best practice |
| `partition_state` | Leaders, ISR shrinkage, skew across brokers |
| `replay_readiness` | For a group + topic: earliest available offsets vs committed, i.e., "can we still replay what we missed?" |
| `incident_snapshot` | One-call bundle of all the above, timestamped — designed for pasting into a postmortem |

## Quick start

```bash
pip install kafka-sentinel-mcp   # (or: uv tool install)

# Run against your cluster (read-only credentials!)
KAFKA_BOOTSTRAP=localhost:9092 kafka-sentinel-mcp
```

Add to Claude Desktop / any MCP client:

```json
{
  "mcpServers": {
    "kafka-sentinel": {
      "command": "kafka-sentinel-mcp",
      "env": { "KAFKA_BOOTSTRAP": "broker1:9092,broker2:9092" }
    }
  }
}
```

Then ask your agent: *"Why is the payments-consumer group falling behind, and can we still replay from where it stalled?"*

## Security posture

- Read-only: no produce, no topic/config mutation, no offset commits, no ACL ops.
- Supports SASL/SSL; credentials via env only, never logged.
- Every tool call is logged with caller context for audit.
- Recommended: run with a Kafka principal that has `Describe`/`Read` ACLs only — the server degrades gracefully and reports what it can't see.

## Status

Early. See [ROADMAP.md](ROADMAP.md). Issues and PRs welcome — especially war stories about what you wish an agent could have told you during an incident.

## License

MIT

<!-- mcp-name: io.github.sanjay-amu/kafka-sentinel-mcp -->

