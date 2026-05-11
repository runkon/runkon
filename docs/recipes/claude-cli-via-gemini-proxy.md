# Claude CLI via Anthropic-Compatible Proxy (e.g. Gemini through LiteLLM)

Point the Claude CLI runtime at any Anthropic-Messages-compatible proxy to run Gemini (or any other model) behind agents that already target the `claude` runtime — zero Rust changes. The plumbing is already in place: `RuntimeConfig.env` (`runkon-runtimes/src/config.rs`) merges env vars into the spawned subprocess environment, so Claude CLI picks up `ANTHROPIC_BASE_URL` and `ANTHROPIC_AUTH_TOKEN` automatically.

This is the simplest answer to "can I use Gemini models inside Claude Code workflows?" and pairs well with the [native Gemini CLI runtime](gemini-cli.md) for users who want to compare approaches.

## Prerequisites

- `claude` CLI on your `PATH`
- An Anthropic-Messages-compatible proxy running at a reachable URL (LiteLLM, Helicone, OpenRouter's Anthropic-compat endpoint, Helix, etc.) — runkon does not ship a proxy
- An API key for your chosen proxy

## TOML recipe

Add this to your host config:

```toml
[runtimes.claude-via-litellm]
type = "claude"
supported_models = ["gemini-2.5-pro", "gemini-2.5-flash"]
default_model = "gemini-2.5-flash"
env.ANTHROPIC_BASE_URL = "https://your-litellm-proxy.example.com"
env.ANTHROPIC_AUTH_TOKEN = "${LITELLM_API_KEY}"
```

The runtime name `claude-via-litellm` is illustrative; rename it to match your proxy. Any proxy that implements the Anthropic Messages API surface works — substitute its base URL and auth token variable accordingly.

## Sample agent

```yaml
---
runtime: claude-via-litellm
model: gemini-2.5-pro
---

You are a helpful assistant. Answer the user's question concisely.
```

Save as e.g. `.conductor/agents/my-gemini-proxy-agent.md`.

## Sample workflow

```yaml
name: gemini-via-proxy-demo
steps:
  - name: ask-gemini
    type: agent
    agent: my-gemini-proxy-agent
    inputs:
      prompt: "Explain quantum entanglement in one sentence."
```

## How it works

`RuntimeConfig.env` is merged into `RuntimeOptions.env` (`runkon-runtimes/src/runtime/mod.rs`) before the Claude CLI subprocess is spawned. Claude CLI reads `ANTHROPIC_BASE_URL` and `ANTHROPIC_AUTH_TOKEN` from its environment and routes all Messages API requests to your proxy instead of `api.anthropic.com`. The proxy is responsible for translating those requests to Gemini (or any other backend) and returning Anthropic-shaped responses.

## Caveats

- **Capability mismatch** — tool use, vision, prompt caching, long context, and computer use each survive translation only if your proxy implements them. Check your proxy's compatibility matrix before relying on these features.
- **Latency overhead** — the proxy adds a network hop on top of the model call. Expect higher p99 latencies than direct-API paths.
- **Spec drift** — proxies lag the Anthropic Messages API. New Claude features may not be available until the proxy catches up; consult your proxy's changelog.
- **Cost / metering** — `total_cost_usd` reported by Claude CLI may be missing or incorrect when routing through a proxy. Use your proxy's billing dashboard as the source of truth.

## Proxy setup

We don't ship a proxy or vendor its configuration. Each project's docs cover how to spin one up:

- **LiteLLM** — [docs.litellm.ai/docs/proxy/quick_start](https://docs.litellm.ai/docs/proxy/quick_start)
- **Helicone** — [docs.helicone.ai/getting-started/quick-start](https://docs.helicone.ai/getting-started/quick-start)
- **OpenRouter** — [openrouter.ai/docs#quick-start](https://openrouter.ai/docs#quick-start)

## Related

- [docs/recipes/gemini-cli.md](gemini-cli.md) — native Gemini CLI runtime via `type = "cli"` (no proxy, direct `gemini` subprocess)
- `runkon-google` crate — direct Gemini API path that bypasses the Claude CLI entirely
