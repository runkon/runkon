# Changelog — runkon-anthropic

## 0.7.2 — Fixed

- **An agent that ends with a `result` event is reported as finished even if the host
  failed to record it.** `ClaudeRuntime::poll` used to return the row that
  `RunTracker::get_run` read back. If the host's `EventSink::on_event` write for the
  result failed, that row was still `running` and the step failed with
  "completed with status Running but no result text". `poll` now builds the run from
  the drained result event when the host's row is still running, missing or unreadable,
  and logs a warning. A row the host already finished is returned unchanged.
  `runkon-google`'s `GeminiRuntime` does the same.
- **Behaviour change:** with a tracker whose `get_run` returns `Ok(None)` (such as
  `NoopTracker`), `poll` after a result event now returns `Ok` with the drained result
  instead of `Err(PollError::Failed("run ... not found in DB after drain"))`. A `get_run`
  error after a result event no longer fails `poll` either.

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
