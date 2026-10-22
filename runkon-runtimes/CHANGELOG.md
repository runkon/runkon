# Changelog — runkon-runtimes

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
