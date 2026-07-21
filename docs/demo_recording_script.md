# Demo recording script

Goal: a 20-30 second GIF for the top of the README that proves the tool works
in one glance — not a full walkthrough.

## 1. Setup (one time)

```bash
cd "/Users/sanjay/Documents/New project/kafka-sentinel-mcp"
KAFKA_BOOTSTRAP=localhost:9092 uv run python examples/seed_demo_data.py
```

This creates `orders` (real lag on partition 0: 200 messages behind) and
`payments` (real durability flags: RF=1, min.insync.replicas=1 — genuine
single-broker findings, not staged). Re-run any time to reset state.

Make sure Claude Desktop's `kafka-sentinel` MCP server is connected (restart
the app if you just changed `claude_desktop_config.json`).

## 2. Recording setup

- Resize the Claude Desktop window to something screenshot-friendly (roughly
  900x600) and clear any unrelated chat history — start a fresh conversation.
- Use QuickTime Player → File → New Screen Recording → drag-select just the
  chat window (not the full screen).
- Zoom the app to a comfortable text size before recording (small text
  reads badly once downsized to a README-width GIF).

## 3. What to type (one message, let it fully resolve before stopping)

```
Why is the orders-consumer-service group falling behind on the orders topic,
and is it safe to replay everything it's missed?
```

Expected agent behavior: it should call `consumer_lag`, see partition 0 at
200 messages behind, then call `replay_readiness` and confirm nothing has
fallen out of retention yet — landing on a concrete, correct answer, not a
generic response.

Optional second beat (only if you want a second GIF / longer demo):

```
Audit the payments topic for durability risks.
```

This one is worth including precisely because the flags are real: RF=1 and
min.insync.replicas=1 on a single-broker dev cluster is an honest finding,
not a scripted one.

## 4. Stop recording

Stop as soon as the agent's answer finishes rendering — don't let it sit on
a static end screen for more than ~1 second. Trim in QuickTime
(Edit → Trim) if there's dead air at the start/end.

## 5. Convert to GIF

Once you have the `.mov` file, hand it to me (or run this yourself — I can
install `ffmpeg`/`gifski` for you first if you'd like):

```bash
brew install ffmpeg gifski

# .mov -> optimized gif, scaled to README width
ffmpeg -i demo.mov -vf "fps=12,scale=760:-1:flags=lanczos" -f gif - \
  | gifski --fps 12 -o demo.gif -
```

Target: under ~5MB so it loads fast on the README. If it's larger, drop fps
to 10 or scale width to 640.

## 6. Drop it in

Save as `docs/demo.gif`, then add near the top of README.md, right after the
tagline:

```markdown
![kafka-sentinel-mcp demo](docs/demo.gif)
```
