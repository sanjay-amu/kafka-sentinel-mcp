# Launch checklist

Everything below is copy for you to post yourself, from your own accounts —
I can't create HN/Reddit/Slack accounts or post on your behalf. This is
staged so the whole thing can go out in one coordinated window.

## Pre-launch (must be done before you post anywhere)

- [ ] `docs/demo.gif` recorded and embedded in README (see
      [demo_recording_script.md](demo_recording_script.md))
- [x] Published to PyPI (`pip install kafka-sentinel-mcp`)
- [x] Published to the MCP Registry (`io.github.sanjay-amu/kafka-sentinel-mcp`)
- [ ] Star the repo yourself first, watch the GitHub Actions badge is green,
      double check the README renders correctly on github.com (GIF, links)

## Timing

Post everything in a single ~2-hour window so momentum compounds instead of
trickling:

1. **Tuesday–Thursday, ~7:00-8:00am Pacific.** This is peak HN traffic and
   gives you a full US workday to reply to comments — replying fast in the
   first 1-2 hours matters more to HN's ranking than the post quality itself.
2. Post to **Hacker News first**. Wait ~30-45 min to see if it's gaining
   traction before cross-posting elsewhere (avoids splitting the same
   audience across platforms at once, and gives you a live link to reference).
3. Then **r/apachekafka**, then the **MCP community Slack/Discord**.
4. Reply to every comment on every platform for the first 2-3 hours. This is
   the single highest-leverage thing you can do post-launch — more than the
   copy below.

Do not ask for stars/upvotes directly in any post — it reads as needy on HN
specifically and can get flagged.

## Show HN (news.ycombinator.com)

**Title** (plain, factual, no hype — HN penalizes marketing language):
```
Show HN: Kafka-Sentinel – a read-only MCP server so AI agents can't touch prod
```

**Body** (first comment, posted immediately after submitting):
```
I spent a decade running Kafka-based financial messaging at 99.999%
availability. Every "AI + Kafka" demo I've seen assumes the agent gets
admin access to the cluster — create topics, reset offsets, the works.
No operator I've worked with would actually allow that against production.

So I built kafka-sentinel-mcp: an MCP server that gives an LLM agent eyes
on a Kafka cluster (broker health, consumer lag, partition/ISR state,
replay-readiness) with zero write paths. Not "the agent promises not to
write" — the admin/producer mutation APIs are never imported in the module,
and CI greps the source on every PR to enforce it
(https://github.com/sanjay-amu/kafka-sentinel-mcp/blob/main/tests/test_server.py).

The pitch: when a consumer group stalls at 3am, the diagnostic questions
(is it lag? a stuck partition? a rebalance storm? retention already ate
the gap?) are pattern-matching questions an LLM is good at. The fix should
still require a human with real access. This draws that line at the
protocol level instead of trusting a prompt.

Still early (v0.1) — read-only diagnostics only, single-cluster. Roadmap
and contributions welcome: https://github.com/sanjay-amu/kafka-sentinel-mcp

Curious what other "should never be automated" boundaries people have hit
building AI agents against production infra.
```

## r/apachekafka

```
Title: Built a read-only MCP server for Kafka observability, so AI agents
       can help diagnose incidents without write access

Body:
Sharing a side project: kafka-sentinel-mcp, an MCP (Model Context Protocol)
server exposing Kafka cluster health, consumer lag, partition/ISR state,
and replay-readiness as tools an LLM agent (Claude, or any MCP client) can
call.

The design constraint that actually matters here, regardless of the AI
angle: it never imports create_topics/produce/commit/alter_configs — the
admin client can only describe, never mutate. If you're the kind of person
who's nervous about giving anything (agent or otherwise) broad ACLs against
a production cluster, this is built around that nervousness rather than
around it.

Repo: https://github.com/sanjay-amu/kafka-sentinel-mcp
PyPI: pip install kafka-sentinel-mcp

Feedback especially welcome on what diagnostics you wish you'd had mid-
incident — that's basically the whole roadmap (rebalance-storm detection,
ISR-shrink timelines, broker skew reports are next).
```

## MCP community (Slack/Discord)

```
Published a new server to the registry: kafka-sentinel-mcp
(io.github.sanjay-amu/kafka-sentinel-mcp) — read-only Kafka observability
(cluster health, consumer lag, partition/ISR state, replay-readiness).

Built it read-only by construction (no mutation APIs imported at all,
enforced by a CI check) since that's a hard requirement for anyone who'd
actually run this against a production cluster. Would appreciate eyes on
it, especially if anyone's tried Resources/Prompts on top of a server like
this — that's the obvious next protocol-feature gap for me to fill.

https://github.com/sanjay-amu/kafka-sentinel-mcp
```

## After launch

- If it gets traction, the highest-value follow-up is closing the gaps
  identified in the original code review: a `list_topics`/`list_consumer_groups`
  discovery tool (right now every tool assumes you already know the topic/
  group name) and graceful "not found" errors instead of raw KeyErrors.
  Shipping that as a fast v0.1.1 while eyes are on the repo compounds the
  momentum.
- A short "what I learned" follow-up post 1-2 weeks later (what people
  actually asked for, what surprised you) tends to outperform the original
  launch post for sustained interest — worth planning for even now.
