# Changelog — runkon-flow

## 0.11.0 — Breaking

- **Fixed:** a literal `{{` no longer pairs with the next `}}` and deletes the text
  between them. `{{` is a placeholder only when the text up to the next `}}` is a
  variable name (`[A-Za-z_][A-Za-z0-9_.-]*`, spaces trimmed). Text such as `{{...}}`,
  `{{<gate>.feedback}}`, `{{item.*}}` or a lone `{{` is now kept literally, where it used
  to be stripped along with everything up to the next `}}`. The rule applies in every
  path: agent prompts and snippets (runkon-flow-executors), script `run` and `env`, gate
  prompts, and `call workflow` and `foreach` inputs. An unknown well-formed `{{name}}`
  behaves as before.
- **Changed:** prompts that relied on the old stripping now show that text, and
  `{{ name }}` with spaces now resolves.
- **Added:** `{{#if name}} … {{else}} … {{/if}}` conditional blocks (nestable; true when
  the variable is set and non-empty; unbalanced tags are kept literally) in every
  substitution path, where the markers used to be stripped and the body always shown.
  Also added `prompt_builder::is_variable_name` and `prompt_builder::substitute_with`.

- **Fixed:** a run waiting at a gate now has status `waiting`, with `blocked_on`
  describing the gate (`human_approval`, `human_review`, `pr_approval`, `pr_checks`, or
  the new generic `gate` variant for other gate types). After approve, reject or timeout
  it returns to `running`, or to a terminal status, with `blocked_on` cleared. A resumed
  run no longer stays `waiting` after skipping an already-decided gate. A gate decision
  no longer overwrites a `cancelling` written while the gate waited. The in-memory
  backend's `update_run_status` now clears `blocked_on`, like SQLite.
- **Changed (breaking):** `WorkflowPersistence` gains the required methods
  `set_run_waiting` and `clear_run_waiting`; hosts with their own implementation must add
  them. `BlockedOn` gains the `Gate { gate_name, gate_type, prompt, options }` variant, so
  exhaustive `match`es on it must handle the new case.

- **Fixed:** a `parallel` branch whose action executor returns `Cancelled(UserRequested)`
  now cancels the run. Siblings stop before their next attempt, the block returns
  `Err(Cancelled)`, and the run ends `cancelled` instead of `failed`, without the
  `only N/M succeeded` and `finished with failures` warnings. An external cancel that
  lands while a parallel block is running also ends the run `cancelled`.

- **Fixed:** a script step's `env` that references a declared workflow input which should
  have a value (required, has a `default`, or boolean) but is missing from the run's inputs
  now fails the run at start with `unresolved variable {{name}} in env KEY of step <step>`.
  It used to reach the script as an empty string. Dry runs are checked too. Optional inputs
  with no default, and unknown names, behave as before.
- **Added:** `executors::script::check_script_env`, `check_workflow_script_env` and
  `UnresolvedEnvVar` let hosts run the check without executing anything, and
  `prompt_builder::placeholder_names` lists the `{{name}}` variables in a template.

## 0.10.0 — Breaking

- **Added:** `ScriptEnvProvider::script_log_path` (default `None`). When a host returns a
  path, a script step's stderr and the stdout outside its `FLOW_OUTPUT` block are written
  there as a log of about 64 KiB, with a `--- stderr ---` and a `--- stdout (outside
  FLOW_OUTPUT) ---` section (only non-empty ones). The oldest bytes are dropped and the
  section starts with `… [truncated N bytes]`. The log is written on success, non-zero
  exit, timeout and output-parse failure, once per attempt, and its path is recorded in
  the step row's `output_file` column. `{{prior_output_file}}` is unaffected.
- **Changed:** a failed script step's error now keeps the last 2000 characters of stderr
  instead of the first. Script output capture is bounded: stderr keeps the last 64 KiB
  and stdout the last 1 MiB, so a `FLOW_OUTPUT` block cut by that cap fails to parse.
- **Changed (breaking):** `StepUpdate` gains a public `output_file` field, persisted on
  terminal updates (`None` keeps the stored value). Hosts that build `StepUpdate` with a
  struct literal must add `output_file: None`.

- **Fixed:** a `script` step's `retries = N` and `on_fail = <agent>` were accepted and
  silently ignored. A failing script (non-zero exit, timeout, or an unparseable
  `FLOW_OUTPUT` block) is now retried up to N times, each attempt recorded as its own step
  row as for `call`. After the last failure the `on_fail` agent runs with `{{failed_step}}`,
  `{{failure_reason}}` and `{{retry_count}}` set. `on_fail = continue` is unchanged.

- **Fixed:** a `foreach` step's `inputs` values now substitute `{{item.id}}`, `{{item.ref}}`,
  `{{item.<key>}}` and the parent's `{{name}}` variables for each item before the child
  workflow starts. Before, the child got the placeholder text literally. An unknown
  `{{name}}` stays literal. The raw `item.*` input keys are still passed.

- **Fixed:** a gate's `prompt` now substitutes `{{name}}` workflow variables (inputs,
  engine variables, prior steps' FLOW_OUTPUT fields, gate feedback) when the gate starts,
  so the stored gate prompt, the resolver's `GateParams.prompt` and anything a host shows
  from them carry the resolved text. An unknown `{{name}}` stays literal.

- **Fixed:** if another engine resumed a run while the original engine was still polling a
  gate (as a host does when it approves and resumes from another process), the original
  engine could run the step after the gate a second time and leave its row `running`. A
  gate now stops polling once its lease is lost and checks that its engine still holds the
  run's lease before acting on a decision.
- **Fixed:** an engine that loses a run's lease no longer writes `Cancelled` over the
  terminal status the new owner set.

- **Fixed:** a gate decided without feedback (an approval, a rejection, a dry-run
  approval, or a timeout that continues) no longer leaves an earlier gate's feedback in
  `{{gate_feedback}}`. It is now always the most recent gate's value, also after a resume,
  and a resume now restores a gate's selections as a live run does.
- **Added:** `{{<gate-name>.feedback}}` reads a specific gate's feedback, or its joined
  selections, and is empty when that gate had neither.
- **Changed (breaking):** `ExecutionState` has a new public field, `gate_feedback_by_gate`.
  Hosts that build it with a struct literal must add `gate_feedback_by_gate:
  Default::default()`.

- **Added:** `meta { trigger = scheduled }` takes a schedule: `every = "<duration>"` or
  `cron = "<5-field cron>"` (exactly one), and an optional `timezone`. It is validated at
  parse time and exposed as `WorkflowDef::schedule` (`Schedule` / `ScheduleSpec`). runkon
  never runs it; the host does.
- **Changed:** a scheduled trigger without a schedule, with both `every` and `cron`, or a
  schedule field on a `manual` or `pr` workflow is a parse error naming the workflow. The
  "not implemented in v1" warning is no longer logged for `scheduled`.
- **Changed (breaking):** `WorkflowDef` gains a public `schedule: Option<Schedule>` field.
  Hosts that build `WorkflowDef` with a struct literal must add `schedule: None`.
  Serialised defs without the field still deserialise.

## 0.9.0 — Breaking

- **Changed (breaking):** `runkon-flow-executors`' `agent_loader` no longer hard-codes the
  `.conductor/` directory. Agents and prompt snippets are searched under a configurable
  root, the new validated `ConfigDir` type, which defaults to `.runkon`
  (`DEFAULT_CONFIG_DIR`). `load_agent` and `load_and_concat_snippets` take a `&ConfigDir`,
  and `BuildPromptParams` has a new required `config_dir` field. The search order is
  unchanged (workflow-local, then shared; worktree before repo). To keep the old layout,
  pass `ConfigDir::new(".conductor")?`. `ConfigDir::new` rejects empty, absolute, `.` and
  `..` paths.
- **Changed:** "agent not found" and "prompt snippet not found" errors list the paths
  actually searched under the configured root, and the base directories.

- **Changed (breaking):** removed `FlowEngineBuilder::script_env_provider()`, deprecated
  since 0.7.1-alpha. It had no effect; script steps take their provider from
  `RunInput.script_env_provider` / `ChildRunInput.script_env_provider`. To upgrade,
  delete the call.
- **Added:** `FlowEngine::validate_with(def, &ActionRegistry, &ItemProviderRegistry)`
  validates a workflow against the registries a host passes to `RunInput` /
  `ChildRunInput`, using the engine's gate resolvers and workflow resolver. It is the
  same check `run_workflow` / `run_child` apply, so executors no longer need to be
  registered on the builder just for pre-flight validation.
- **Changed (docs):** the ownership split is now documented. The builder holds the
  engine-wide hooks (gate resolvers, workflow resolver, event sinks); `RunInput` /
  `ChildRunInput` hold the per-run registries. The builder's `.action()`,
  `.action_fallback()` and `.item_provider()` serve `validate()` only.

- **Added:** a `require_output` option on `call` and on each `call` inside `parallel`
  (`require_output = true|false`). When it is in effect and the executor reports
  `metadata_keys::OUTPUT_WARNING` (the agent ended without a parseable `<<<FLOW_OUTPUT>>>`
  block), the attempt fails with the warning as its error, and `retries` / `on_fail`
  (or, in `parallel`, per-call `retries` and `fail_fast` / `min_success`) apply.
  `require_output` on a `script` step is a parse error.
- **Added:** `CallNode::require_output` and `ParallelNode::call_require_output`, both
  serde-defaulted. Code that builds these structs with literals must add
  `require_output: None` / `call_require_output: HashMap::new()`.
- **Changed (behaviour):** a step with an output schema (`output = …`, including one
  from a `do` block or a `parallel` block) now fails by default when its executor
  reports `OUTPUT_WARNING`. With the bundled executors that happens when the agent
  produced no result text; before, the step completed with empty markers and the
  warning as its `step_error`. Set `require_output = false` to keep the old behaviour.
  Hosts that worked around this in an adapter, such as conductor-ai's
  `schema_output_missing`, can drop the workaround.

- **Added:** `helpers::flow_output_block_line`, the 1-based line of a result text on
  which the `<<<FLOW_OUTPUT>>>` block that `find_flow_output_block` returns begins, so
  hosts no longer recompute it.
- **Added:** `runkon_flow_executors::{OutputError, AgentError}`. A FLOW_OUTPUT failure
  is typed as `OutputError::Missing`, `InvalidJson` or `Validation` (the last two carry
  `block_line`), and `AgentError::Output` wraps it with the number of correction
  attempts, so hosts can match on the failure instead of on the message. The message
  text is unchanged.
- **Changed (breaking):** `parse_structured_output` and `interpret_agent_output` return
  `OutputError`, and `run_agent_with_output_correction` and
  `runkon-google`'s `GeminiAgentExecutor::execute` return `AgentError`, instead of
  `String`. To keep the old behaviour, use `.map_err(|e| EngineError::Workflow(e.to_string()))`
  or `String::from(e)`.

- **Added:** on resume, the first attempt of an agent step whose earlier attempt failed
  carries `PriorStepAttempt` (the failed attempt's `child_run_id` and error) in
  `ActionParams.extensions`, so an executor can reuse that attempt's output instead of
  re-running it. A run whose only recorded steps failed now resumes with that context
  too. New metadata key `output_recovered_from_run`.
- **Fixed:** `InMemoryWorkflowPersistence::update_step` no longer clears a step's
  `child_run_id` when the update has none, matching the SQLite backend.

- **Fixed:** a `parallel` block now saves each branch's row (status, `ended_at`,
  `context_out`, `markers_out`, `structured_output`) when that branch finishes, instead
  of after the slowest sibling. A host recovery pass, such as conductor's
  `recover_stuck_steps`, can no longer mark finished branches as completed with NULL
  outputs and one shared `ended_at`. In addition, when
  `SqliteWorkflowPersistence::update_step` finds a step already `completed` with NULL
  outputs, it fills them in from the engine's write and logs a `warn`, where it used to
  drop the write at `debug`.

- **Fixed:** the canonical SQLite schema's `workflow_run_steps.role` CHECK now accepts
  every role the engine writes (`actor`, `reviewer`, `gate`, `workflow`, `script`,
  `foreach`, plus the legacy `agent`). Before, a host on runkon's own schema could not
  record a `call`, `parallel` or `script` step. The column default stays `'agent'`.
- **Breaking:** `create_canonical_schema` now creates `runkon_flow_schema_versions`. When
  it finds the pre-0.9 role CHECK, it rebuilds `workflow_run_steps` in place, keeping
  rows, indexes and host-added columns. That rebuild must run outside an open
  transaction and returns an error otherwise. Tables with a different CHECK (such as
  conductor's) are left untouched. `runkon_flow_schema_versions` is not part of
  `CANONICAL_SCHEMA_DDL`, so hosts that mirror the const get a different schema from hosts
  that call the function, and its name differs from the `runkon_flow_schema_history`
  table older conductor-ai databases carry from refinery, so the two never collide. Hosts
  that mirror `CANONICAL_SCHEMA_DDL` in their own migrations must widen the CHECK
  themselves. #131 will decide how canonical schema changes reach hosts.

## 0.8.2 — Breaking

- **Fixed:** a waiting gate now polls its registered `GateResolver`. `Approved` and
  `Rejected` results are recorded with `approve_gate` / `reject_gate` as
  `gate_resolver:<gate type>` and release or fail the gate like a manual decision. A
  recorded manual decision still wins. This makes `pr_checks` / `pr_approval` gates resolve
  on their own, including in resumed and child runs.
- **Added:** `GateResolver::poll_interval()` (default `None`, meaning the loop's
  `poll_interval`) to throttle resolver polls, and `GateResolverRegistry::get`.
- **Changed (breaking):** `ExecutionState` has a new `gate_resolvers` field. Struct literals
  need `gate_resolvers: Default::default()`; the engine fills it in on every run.

- **Fixed:** resuming a run whose `do … while` or `while` loop (with `on_max_iter = fail`)
  failed by reaching `max_iterations` now gives the loop a fresh budget of
  `max_iterations` iterations, continuing from the next iteration, instead of failing
  again at once. This also applies when the loop is in a `call workflow` child resumed
  through `ChildWorkflowRunner::resume_child`. Resumes after other failures keep the
  original budget, and `on_max_iter = continue` loops are unchanged.

## 0.8.1 — Fixed

- **Fixed:** `SqliteWorkflowPersistence::insert_step` stores `retry_count` 0 for a step
  inserted with `retry_count: None` (pending steps, including every waiting human gate),
  instead of NULL. In 0.8.0-alpha those inserts failed with
  `NOT NULL constraint failed: workflow_run_steps.retry_count` on any SQLite host, so a
  workflow that reached a gate could not continue.

## 0.8.0 — Breaking

- **Added:** a failed `call workflow` step now records the child's failing step and its
  error in its `step_error`/`result_text`, e.g.
  `Sub-workflow 'review-pr' failed: submit-review: <error>`. Nested calls chain, so the
  top-level step names the leaf step. The appended cause is truncated to 2000 characters.
  Hosts no longer need to walk `child_run_id` to show the cause.
- **Added:** `RunFailureCause { step_name, error }` is inserted into
  `WorkflowResult.extensions` when a run ends failed, and
  `engine::find_run_failure_cause` looks the cause up from a run's steps.
- **Changed:** hosts that match the exact text `Sub-workflow '<name>' failed` should match
  on the prefix instead.

- **Fixed:** human gate steps are stored with `gate_type`, `gate_prompt`, `gate_timeout`
  and the resolved `gate_options`, written in the same insert as the step. Hosts no longer
  need a persistence decorator for this, so conductor-ai's `GateMetadataPersistence` can be
  removed.
- **Fixed:** a gate whose `options` step reference cannot be resolved now fails before a
  waiting step or `GateWaiting` event is recorded, instead of leaving an orphaned row.
- **Changed (breaking):** `NewStep` has a new `gate: Option<GateStepInfo>` field. Struct
  literals must add `gate: None`. Custom `WorkflowPersistence` backends should store it:
  `gate_timeout` as `GateStepInfo::timeout_text()` (`"{secs}s"`) and `gate_options` as
  `GateStepInfo::options_json()` (a `[{"value","label"}]` JSON array). Both are derived
  by `NewStep::gate_columns()`, which backends can use directly.
- **Changed:** a `StepRef` gate option whose value is not a JSON string is now an error
  naming the gate, step, field and key, instead of being silently dropped.

## 0.7.2 — Fixed

- **`script` steps no longer inherit the host's stdin.** The step's stdin is `/dev/null`,
  so it cannot consume data meant for the host, such as an MCP server's JSON-RPC stream.
  A script that read stdin now reads EOF immediately.
- **Resume matches steps by occurrence within an iteration.** A workflow that runs the
  same `call`, `call workflow`, `gate`, `script` or `foreach` twice in one iteration
  used to skip the second run when interrupted between them, and conditions saw the
  first run's markers. The n-th time a step name runs in an iteration now matches the
  n-th recorded run of that name, so the second run executes and its markers are used.
  Existing workflows need no edits.
- **Changed:** `ResumeContext` is now `#[non_exhaustive]`, so struct-literal construction
  no longer compiles. Build it with `ResumeContext::from_steps` (pass the run's saved
  steps, of every status) or `ResumeContext::default()`.
- **Changed (breaking):** `ResumeContext::step_map` is no longer a public field, so
  `ctx.step_map[..]` no longer compiles. Call `ctx.step_map()` instead: a deprecated
  read-only accessor that returns an owned map of the latest completed row per
  `(name, iteration)`. Writing to it has no effect, and resume no longer reads it.
- **Added:** `engine::claim_completed_step`, `engine::restore_step_row` and
  `engine::restore_call_workflow_step_row`. An executor claims a step once per run of it,
  then restores the claimed row.
- **Deprecated:** `engine::should_skip`, `restore_step`, `restore_completed_step` and
  `restore_call_workflow_step`. They act on the next unclaimed run and never advance it.

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

- **Added:** `metadata_keys::OUTPUT_CORRECTION_ATTEMPTS`. Agent executors set it on a step
  when its structured output needed in-session corrections before passing validation.

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
