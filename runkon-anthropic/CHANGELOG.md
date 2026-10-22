# Changelog — runkon-anthropic

## 0.7.1 — Fixed

- **An agent whose structured output fails schema validation gets a chance to correct
  it.** When a completed run's `<<<FLOW_OUTPUT>>>` fails validation, the Claude executor
  resumes the same session with the validation error and asks for a corrected block
  only, up to `runkon_flow_executors::correction::MAX_OUTPUT_CORRECTION_ATTEMPTS` (2)
  times, before failing the step. The count is stored in the step metadata under
  `metadata_keys::OUTPUT_CORRECTION_ATTEMPTS`. Metrics are summed across attempts.
  Runs without a session id fail as before. `runkon-google`'s Gemini executor does
  the same.

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
