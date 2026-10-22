# Changelog — runkon-runtimes

## 0.7.2 — Fixed

- **`spawn_headless` (the `claude` runtime) and the `script` runtime no longer inherit
  the host's stdin.** The child's stdin is `/dev/null`, so it cannot consume data meant
  for the host, such as an MCP server's JSON-RPC stream. A `script` command that read
  stdin now reads EOF immediately.
- **Added** `headless::TerminalEventCapture` and `headless::resolve_completed_run`
  (re-exported at the crate root). The Claude and Gemini runtimes use them to report a
  finished run from the agent's own result event when the host's row disagrees.

## 0.7.1 — Fixed

- **`cli` and `script` runtimes now apply `RuntimeConfig.env`** to the spawned process
  (added to the inherited environment; config entries win). It was silently ignored
  before, so settings such as `env.MY_API_BASE = "..."` never reached the process.
- **Migration:** configs that were silently ignored may now fail the spawn with
  `RuntimeError::Config`: an `env` key that is empty or contains `=` or NUL (or a value
  containing NUL), and, for `script`, an `env` key equal to `prompt_env`
  (`FLOW_PROMPT` by default). Remove or rename those entries.
- **Docs:** `RuntimeConfig::env` now says which runtime types apply it and how.

## 0.7.0 — Breaking

- **`script` runtimes pass the prompt as `FLOW_PROMPT` instead of `CONDUCTOR_PROMPT`.**
  The name comes from the new `RuntimeConfig::prompt_env`, which defaults to
  `DEFAULT_SCRIPT_PROMPT_ENV` (`"FLOW_PROMPT"`). To keep the old name, set
  `prompt_env = "CONDUCTOR_PROMPT"` in the runtime's config.
- **An invalid `prompt_env` fails the spawn** with `RuntimeError::Config`. The name must
  be a shell variable name: an ASCII letter or `_`, then letters, digits or `_`.
- **Added:** `RuntimeConfig::prompt_env` and `config::DEFAULT_SCRIPT_PROMPT_ENV`.

## 0.6.0 — Breaking

- **`RuntimeOptions` no longer carries `argv_builder`.** Hosts that constructed
  `ClaudeRuntime` through `RuntimeOptions` must now depend on `runkon-anthropic`
  and construct `ClaudeRuntime` directly.
- **`resolve_runtime` no longer resolves the built-in `"claude"` name**, nor does it
  handle `runtime_type = "claude"` in named-config entries. Wire `ClaudeRuntime`
  via your `RuntimeResolver` implementation.
- **`drain_stream_json` now requires a `LineEventParser` argument.** The Claude-shaped
  parser lives in `runkon_anthropic::ClaudeLineEventParser`.
- **Added:** `LineEventParser` trait and `ParseSignal` enum in `runkon_runtimes::headless`,
  re-exported from the crate root.
