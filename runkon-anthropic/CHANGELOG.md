# Changelog — runkon-anthropic

## 0.6.0 — Breaking

- **`ClaudeRuntime`, `ClaudeRuntimeOptions`, `ClaudeArgvRequest`, `ArgvBuilder`, and
  `ClaudeLineEventParser` are now defined in `runkon_anthropic::claude_runtime`** and
  re-exported from the crate root. Previously `ClaudeRuntime` and friends lived in
  `runkon_runtimes::runtime::claude`.
- **`ClaudeRuntime` no longer requires `argv_builder` to be threaded through
  `RuntimeOptions`**; callers construct `ClaudeRuntimeOptions` directly and pass it
  to `ClaudeRuntime::new`.
- **Added:** `ClaudeLineEventParser` — implements `runkon_runtimes::LineEventParser` and
  provides Claude's `system|assistant|result` event-shape interpretation for use with
  `drain_stream_json`.
