# Contributing

Thanks for your interest! This project has one non-negotiable rule:

**Every tool stays read-only.** PRs that add produce, topic/config mutation, offset commits, or ACL operations will be declined regardless of quality — that's the project's entire security posture. Mutation tooling belongs in a separate, human-gated project.

## Getting started

```bash
git clone https://github.com/sanjay-amu/kafka-sentinel-mcp
cd kafka-sentinel-mcp
pip install -e ".[dev]"   # or: pip install -e . pytest ruff
pytest
```

Tests are fully mocked — you don't need a running Kafka cluster to contribute.

## What's welcome

- New read-only diagnostics (see ROADMAP.md — rebalance detection, ISR timelines, skew reports)
- Auth presets for MSK / Confluent Cloud
- Better "diagnosis hints" in tool responses (structured priors for the LLM)
- War stories in issues: what do you wish an agent could have told you mid-incident?

## PR guidelines

- One logical change per PR; include tests
- `ruff check` and `pytest` must pass
- Describe the incident/use case your change serves — this project is use-case-driven
