# Changelog — runkon-flow

## 0.7.1 — Fixed

- **A one-line `<<<FLOW_OUTPUT>>> {...}` with no end marker is no longer ignored.**
  `find_output_block_marker` now matches a line that starts with an opening marker,
  followed by anything. A script step that prints this form fails with "could not be
  parsed (invalid JSON or missing <<<END_FLOW_OUTPUT>>>)" instead of completing with
  `markers: []`, and an agent step gets an `OUTPUT_WARNING`. Scripts that used this form
  must print the block over three lines with `<<<END_FLOW_OUTPUT>>>`.
- **Output-block markers count only at the start of a line.** A `<<<FLOW_OUTPUT>>>` or
  `<<<END_FLOW_OUTPUT>>>` quoted inside the block (for example in a JSON string value)
  was taken for the real start or end, so valid output was rejected. Both markers may
  be indented, but nothing else may precede them on their line. This applies to
  `parse_flow_output` and to `runkon-flow-executors`' `extract_output_block` /
  `parse_structured_output`, which now share the new `helpers::find_flow_output_block`.
- **Added:** `helpers::find_flow_output_block` (the contents of the last complete block)
  and `helpers::find_block_start` (the offset of the last real start marker).
- **Deprecated:** `runkon_flow_executors::output::find_flow_output_start`. Use
  `runkon_flow::helpers::find_flow_output_block`, which also finds the end marker.
- **Deprecated:** `FlowEngineBuilder::script_env_provider()`. The engine never used it, and
  script steps always got the provider from `RunInput.script_env_provider` /
  `ChildRunInput.script_env_provider`. A host that set it only on the builder was getting
  `NoOpScriptEnvProvider` behaviour; pass the provider in `RunInput` instead. It will be
  removed in the next breaking release.

## 0.7.0 — Breaking

- **Script steps receive workflow inputs as `FLOW_<NAME>` instead of `CONDUCTOR_<NAME>`.**
  The prefix comes from the new `ScriptEnvProvider::input_env_prefix()`, which defaults
  to `DEFAULT_INPUT_ENV_PREFIX` (`"FLOW_"`). To keep the old names, override it to return
  `"CONDUCTOR_"`. Existing `ScriptEnvProvider` implementations still compile; only the
  runtime prefix changes.
- **An invalid input prefix refuses the run.** The prefix must be empty, or an ASCII
  letter or `_` followed by letters, digits or `_`. The engine checks it before taking
  the lease or running any step, and returns `EngineError::Workflow`.
- **Input-derived env var names can't set security-sensitive variables** (`PATH`,
  `GH_TOKEN`, `LD_PRELOAD`, …), matching the existing rule for the `env = { ... }` block.
- **Added:** `ScriptEnvProvider::input_env_prefix()`, `validate_input_env_prefix` and
  `DEFAULT_INPUT_ENV_PREFIX`, re-exported from the crate root.
- **Added:** `metadata_keys::OUTPUT_WARNING`. When a completed `call` or `parallel` step's
  executor sets it, the engine logs a warning and stores the message as the step's
  `step_error`. The agent executors in `runkon-anthropic` and `runkon-google` set it
  when an agent ends without a parseable `<<<FLOW_OUTPUT>>>` block.
