# Changelog — runkon-flow

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
