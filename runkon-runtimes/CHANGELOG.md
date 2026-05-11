# Changelog — runkon-runtimes

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
