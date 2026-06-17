# Calling Opus 4.8 via `claude -p` with Max Thinking (Claude subscription)

How to run a one-shot, non-interactive Claude Code command (`claude -p`, aka `--print`)
that is **pinned to Claude Opus 4.8**, uses the **maximum reasoning/thinking effort**, and is
billed against your **Claude Max subscription** — **not** a per-token Anthropic API key.

---

## TL;DR (one-liner)

Bash (scopes the empty key to just this command):

```bash
ANTHROPIC_API_KEY= claude -p "<your question>" --model claude-opus-4-8 --effort max
```

That forces subscription auth, pins Opus 4.8, sets maximum thinking effort, and prints to stdout.

---

## 1. Force subscription billing, NOT the API key

If `ANTHROPIC_API_KEY` is present **and non-empty**, Claude Code uses it and bills per token —
silently.

### Fastest fix: blank the key for the call

An **empty** `ANTHROPIC_API_KEY` counts as "no key," so Claude Code falls back to your subscription:

```bash
# Bash — the VAR= prefix scopes the empty value to JUST this one command
ANTHROPIC_API_KEY= claude -p "<your prompt>" --model claude-opus-4-8 --effort max
```

### Verify which credential is actually used

```bash
claude auth status
```

Look for `"subscriptionType": "max"` and your email. If you see `"apiKeySource": "ANTHROPIC_API_KEY"`,
you're about to be billed per token — blank the key.

---

## 2. Pin the model to Opus 4.8 — use the full ID, not `opus`

> Do not use `--model opus`. A bare alias resolves to the *latest* Opus which may drift.
> Use the **full model name**:

```
claude-opus-4-8
```

Ways to set it (highest priority first):

| Method | How | Best for |
|---|---|---|
| `--model` flag | `claude -p "…" --model claude-opus-4-8` | **One-shot `-p` calls (recommended)** |
| `ANTHROPIC_MODEL` env var | `export ANTHROPIC_MODEL=claude-opus-4-8` | Pinning for a whole shell session |
| `settings.json` | `{ "model": "claude-opus-4-8" }` | Making it the persistent default |

---

## 3. Max thinking — use `--effort max`

Reasoning/thinking effort levels:

```
low  <  medium  <  high  <  xhigh  <  max
```

```bash
ANTHROPIC_API_KEY= claude -p "<your prompt>" --model claude-opus-4-8 --effort max
```

Optional extra nudge — add `ultrathink:` prefix in the prompt text. Composes with `--effort max`.

---

## 4. Full recipe

### One-shot review

```bash
ANTHROPIC_API_KEY= claude -p "$(cat /tmp/review-prompt.txt)" \
  --model claude-opus-4-8 --effort max --output-format text > /tmp/review-output.md
```

### Pipe a file as context

```bash
cat src/livebot/strategy.rs | ANTHROPIC_API_KEY= claude -p "review this for bugs" \
  --model claude-opus-4-8 --effort max
```

### JSON output (inspect modelUsage)

```bash
ANTHROPIC_API_KEY= claude -p "<prompt>" --model claude-opus-4-8 --effort max --output-format json
```

### Optional spend guard

```bash
ANTHROPIC_API_KEY= claude -p "<prompt>" --model claude-opus-4-8 --effort max --max-budget-usd 1.00
```

---

## 5. Gotchas checklist

- [ ] **Blank or remove `ANTHROPIC_API_KEY`** — otherwise you're billed per token.
- [ ] **Confirm with `claude auth status`** → look for `"subscriptionType": "max"`.
- [ ] **Use `claude-opus-4-8`, never the `opus` alias** — the alias drifts to the latest Opus.
- [ ] **`--effort max`** is the max thinking level.
- [ ] `total_cost_usd` in JSON output is notional — it does **not** prove which auth was used.
- [ ] **Opus + max effort burns Max-plan quota fastest** — consider `--max-budget-usd` for long runs.
