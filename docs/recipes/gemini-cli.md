# Gemini CLI Recipe

Run Gemini as a runkon agent using the built-in `cli` runtime. This gives you session resume, token counting via wildcard summation, and per-workflow approval-mode overrides — with zero Rust changes on your side.

For richer per-turn telemetry (stall detection, turn caps, stream-json event parsing), see [Phase 2: GeminiRuntime](#phase-2-geminiruntime-bespoke-stream-json-runtime) below.

## Prerequisites

- `gemini` CLI ≥ 0.41.2 on your `PATH`
- `GEMINI_API_KEY` set in your environment

## TOML recipe

Add this to your host config (`[runtimes.gemini]`):

```toml
[runtimes.gemini]
type = "cli"
binary = "gemini"
args = ["--prompt", "{{prompt}}", "--model", "{{model}}", "--output-format", "json"]
prompt_via = "arg"
default_model = "gemini-2.5-flash"
result_field = "response"
token_fields = "stats.models.*.tokens.total"
supported_models = ["gemini-2.5-flash", "gemini-2.5-pro", "gemini-2.5-flash-lite", "gemini-3-flash-preview", "gemini-3-pro-preview"]
env.GEMINI_API_KEY = "${GEMINI_API_KEY}"
```

`token_fields` uses a wildcard sum across `stats.models.*` so token counts are aggregated transparently across all routing roles (`main`, `subagent`, `utility_router`).

## Sample agent

```yaml
---
runtime: gemini
model: gemini-2.5-flash
---

You are a helpful assistant. Answer the user's question concisely.
```

Save as e.g. `.conductor/agents/my-gemini-agent.md`.

## Sample workflow

```yaml
name: gemini-demo
steps:
  - name: ask-gemini
    type: agent
    agent: my-gemini-agent
    inputs:
      prompt: "Explain quantum entanglement in one sentence."
```

## `-o json` output schemas

### Success shape

```jsonc
{
  "session_id": "uuid",
  "response": "string — the assistant's answer",
  "stats": {
    "models": {
      "<model-name>": {
        "api": { "totalRequests": 1, "totalErrors": 0, "totalLatencyMs": 1234 },
        "tokens": {
          "input": 42,        // = prompt (alias)
          "prompt": 42,
          "candidates": 80,   // output tokens
          "total": 122,
          "cached": 0,        // cache-read tokens
          "thoughts": 0,      // thinking tokens (Gemini-only)
          "tool": 0           // tool-call tokens
        },
        "roles": {
          // Observed: "main", "subagent", "utility_router"
          "<role>": { "totalRequests": 1, "tokens": { /* same shape */ } }
        }
      }
    },
    "tools": {
      "totalCalls": 0, "totalSuccess": 0, "totalFail": 0,
      "totalDurationMs": 0,
      "totalDecisions": { "accept": 0, "reject": 0, "modify": 0, "auto_accept": 0 }
    },
    "files": { "totalLinesAdded": 0, "totalLinesRemoved": 0 }
  }
}
```

### Error / cancellation shape

```jsonc
{
  "session_id": "uuid",
  "error": {
    "type": "FatalCancellationError",
    "message": "Operation cancelled.",
    "code": 130
  }
  // NO "response" field, NO "stats" block
}
```

Exit code 130 is returned on fatal cancellation (e.g. `--approval-mode plan` denying a write tool). `CliRuntime` detects the non-zero exit code and emits `RuntimeEvent::Failed`; the raw JSON (including `error.message`) is available as `result_text`.

## Session resume

`--resume <uuid|"latest"|index>` works in `-p` (non-interactive) mode:

```yaml
# In your workflow step:
- name: follow-up
  type: agent
  agent: my-gemini-agent
  resume_session_id: "latest"
```

Or pass via `extra_cli_args` for one-off overrides:

```yaml
  extra_cli_args:
    resume: "a1b2c3d4-..."
```

Key observations from live testing:
- Session history is stored independently of file persistence — no MEMORY.md write needed.
- Token count on resume is dramatically lower (caching effective).
- Resumed sessions skip internal strategic tools — overhead is established once per session.

## Approval mode / YOLO

The default recipe omits `--approval-mode` (Gemini's default is interactive). Override per workflow via `extra_cli_args`:

```yaml
  extra_cli_args:
    approval-mode: "yolo"        # equivalent to --yolo
    # approval-mode: "auto_edit"
    # approval-mode: "plan"
    # approval-mode: "default"
```

## Known limitations (Phase 1 `cli` runtime)

- **Token breakdown collapsed.** `token_fields` sums `stats.models.*.tokens.total` — a single number combining input, output, cached, thoughts, and tool tokens. Per-class breakdown requires Phase 2.
- **`cost_usd` always `None`.** Gemini reports tokens only; no pricing is returned in the JSON.
- **`thoughts` / `tool` token classes not surfaced.** Gemini emits separate token counts for thinking and tool invocations; these are absorbed into the wildcard total.
- **Single stall threshold.** The `cli` runtime polls via `try_wait` with 500 ms sleep; stall detection is not available.

## Phase 2: GeminiRuntime (bespoke stream-json runtime)

`GeminiRuntime` in `runkon-google` targets `--output-format stream-json` and provides:

- Per-event parsing (init, message, tool_use, tool_result, error, result)
- Turn-cap enforcement via `tool_use` event count
- Stall detection via keepalive
- Richer token mapping (`input_tokens`, `output_tokens`, `cache_read_input_tokens`)
- `--approval-mode` wired directly from `PermissionMode`

See [`sample-workflow-gemini-stream.yaml`](sample-workflow-gemini-stream.yaml) for a wiring example.
