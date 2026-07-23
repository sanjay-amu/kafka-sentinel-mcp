# Roadmap

## v0.1.2 (shipped) — Discovery + graceful errors
- [x] `list_topics` / `list_consumer_groups` — every other tool required
      already knowing a name; this closed that gap
- [x] Topic-not-found now raises a clear `TopicNotFoundError` pointing at
      `list_topics()` instead of a raw `KeyError`

## v0.1 (shipped) — Observe
- [x] cluster_health, consumer_lag, topic_audit, replay_readiness, incident_snapshot
- [ ] Tests against a dockerized Kafka (testcontainers)
- [x] CI (GitHub Actions: lint, type-check, test matrix)
- [x] PyPI release

## v0.2 — Correlate
- [ ] Rebalance-storm detector (group state history over a sampling window)
- [ ] ISR-shrink event timeline
- [ ] Broker skew report (partition/leader distribution)
- [ ] Structured "diagnosis hints" in every tool response (give the LLM priors, not just numbers)

## v0.3 — Drill (still read-only)
- [ ] Replay cost estimator: given a group + timestamp, how many messages / how much time to reprocess
- [ ] "Game day" mode: synthetic read-only scenarios for training on-call engineers with an AI copilot
- [ ] Prometheus/Dynatrace metric cross-reference (bring external observability into the same conversation)

## Ideas / community input wanted
- MSK / Confluent Cloud auth presets
- Schema-registry drift audit
- Multi-cluster federation view

## Positioning note (why read-only stays non-negotiable)
The moment this project gains a single write path, no regulated-industry operator can run it against production, and the entire differentiation dies. Mutations belong in a separate, human-gated tool.
