use std::io::Read;

use crate::dsl::ScriptNode;
use crate::engine::{handle_on_fail, record_step_success, ExecutionState};
use crate::engine_error::Result;
use crate::prompt_builder::build_variable_map;
use crate::traits::persistence::StepUpdate;

use wait_timeout::ChildExt;

/// How one run of the script ended.
enum ScriptAttempt {
    Succeeded {
        markers: Vec<String>,
        context: String,
        structured_output: Option<String>,
        duration_ms: i64,
    },
    /// A spawn error, timeout, non-zero exit or unparseable output block.
    Failed(String),
}

/// Join a pipe reader thread and decode its bytes as UTF-8. A panicked reader
/// or invalid UTF-8 is logged and yields an empty string.
fn join_pipe(handle: std::thread::JoinHandle<Vec<u8>>, stream_name: &str) -> String {
    match handle.join() {
        Ok(buf) => String::from_utf8(buf).unwrap_or_else(|e| {
            tracing::warn!("script: {stream_name} is not valid UTF-8: {e}");
            String::new()
        }),
        Err(_) => {
            tracing::warn!("script: {stream_name} reader thread panicked");
            String::new()
        }
    }
}

/// Spawn a reader thread BEFORE waiting for the child to avoid pipe deadlock.
/// If the child writes more than the OS pipe buffer size to either stream
/// while the parent is blocked in wait(), the child would hang forever.
fn spawn_reader_thread<R: Read + Send + 'static>(
    mut pipe: R,
    stream_name: &'static str,
) -> std::thread::JoinHandle<Vec<u8>> {
    std::thread::spawn(move || {
        let mut buf = Vec::new();
        if let Err(e) = pipe.read_to_end(&mut buf) {
            tracing::warn!("script: failed to read {stream_name} pipe: {e}");
        }
        buf
    })
}

/// Run the script once. Persists nothing and leaves `step_results` alone.
fn run_script_once(
    node: &ScriptNode,
    script_cmd: &str,
    env_vars: &std::collections::HashMap<String, String>,
    working_dir: &str,
) -> ScriptAttempt {
    let mut cmd = std::process::Command::new("sh");
    cmd.arg("-c").arg(script_cmd);
    cmd.current_dir(working_dir);
    for (k, v) in env_vars {
        cmd.env(k, v);
    }
    // The host's stdin may carry a protocol stream; a step must never read it.
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return ScriptAttempt::Failed(format!("Script '{}' failed to execute: {e}", node.name));
        }
    };

    let stdout_handle = match child.stdout.take() {
        Some(p) => spawn_reader_thread(p, "stdout"),
        None => {
            let _ = child.kill();
            let _ = child.wait();
            return ScriptAttempt::Failed(format!(
                "script '{}': stdout pipe unavailable after spawn",
                node.name
            ));
        }
    };
    let stderr_handle = match child.stderr.take() {
        Some(p) => spawn_reader_thread(p, "stderr"),
        None => {
            let _ = child.kill();
            let _ = child.wait();
            let _ = stdout_handle.join();
            return ScriptAttempt::Failed(format!(
                "script '{}': stderr pipe unavailable after spawn",
                node.name
            ));
        }
    };

    let start = std::time::Instant::now();

    let status = if let Some(timeout_secs) = node.timeout {
        let timeout = std::time::Duration::from_secs(timeout_secs);
        match child.wait_timeout(timeout) {
            Ok(Some(s)) => s,
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = stdout_handle.join();
                let _ = stderr_handle.join();
                return ScriptAttempt::Failed(format!(
                    "Script '{}' timed out after {}s",
                    node.name, timeout_secs
                ));
            }
            Err(e) => {
                let _ = stdout_handle.join();
                let _ = stderr_handle.join();
                return ScriptAttempt::Failed(format!("Script '{}' wait failed: {e}", node.name));
            }
        }
    } else {
        match child.wait() {
            Ok(s) => s,
            Err(e) => {
                let _ = stdout_handle.join();
                let _ = stderr_handle.join();
                return ScriptAttempt::Failed(format!("Script '{}' wait failed: {e}", node.name));
            }
        }
    };
    let duration_ms = start.elapsed().as_millis() as i64;

    let stdout = join_pipe(stdout_handle, "stdout");

    if !status.success() {
        let exit_code = status.code().unwrap_or(-1);

        let stderr = join_pipe(stderr_handle, "stderr");

        let captured_stderr = stderr.trim().chars().take(2000).collect::<String>();

        return ScriptAttempt::Failed(if captured_stderr.is_empty() {
            format!("Script '{}' exited with code {}", node.name, exit_code)
        } else {
            format!(
                "Script '{}' exited with code {}\n{}",
                node.name, exit_code, captured_stderr
            )
        });
    }

    // Discard stderr on success — join the thread so it doesn't leak,
    // but don't convert to String to avoid unnecessary allocation.
    let _ = stderr_handle.join();

    tracing::info!(
        "script '{}': completed successfully in {}ms",
        node.name,
        duration_ms
    );

    // Parse FLOW_OUTPUT once; keep the full struct so we can persist its
    // JSON shape (including any `extras` fields) as structured_output for
    // downstream variable injection — see prompt_builder::build_variable_map.
    let (markers, context, structured_output) = match crate::helpers::parse_flow_output(&stdout) {
        Some(out) => {
            let json = serde_json::to_string(&out)
                .map_err(|e| {
                    tracing::warn!(
                        step = %node.name,
                        error = %e,
                        "script: failed to re-serialize FlowOutput as structured_output \
                         — downstream `{{name}}` variable injection from this step's \
                         extras will be unavailable",
                    );
                })
                .ok();
            (out.markers, out.context, json)
        }
        None => {
            // An output block that didn't parse must fail the step: recording
            // `markers: []` would silently turn every downstream `if` false.
            if let Some(marker) = crate::helpers::find_output_block_marker(&stdout) {
                let detail = match marker {
                    "<<<FLOW_OUTPUT>>>" => "a <<<FLOW_OUTPUT>>> block that could not be \
                         parsed (invalid JSON or missing <<<END_FLOW_OUTPUT>>>)"
                        .to_string(),
                    other => format!(
                        "an unrecognised {other} block; expected <<<FLOW_OUTPUT>>> … \
                         <<<END_FLOW_OUTPUT>>>"
                    ),
                };
                return ScriptAttempt::Failed(format!(
                    "Script '{}' ({}) printed {detail}",
                    node.name, node.run
                ));
            }
            let ctx: String = stdout.chars().take(2000).collect();
            (vec![], ctx, None)
        }
    };

    ScriptAttempt::Succeeded {
        markers,
        context,
        structured_output,
        duration_ms,
    }
}

pub fn execute_script(state: &mut ExecutionState, node: &ScriptNode, iteration: u32) -> Result<()> {
    let pos = state.position;
    state.position += 1;

    // Skip completed script steps on resume
    if super::skip_if_already_completed(state, &node.name, iteration, &node.name) {
        return Ok(());
    }

    if state.exec_config.dry_run {
        let step_id =
            super::insert_step_record(state, &node.name, "script", pos, iteration, Some(0))?;
        tracing::info!("script '{}': dry-run, skipping execution", node.name);
        super::persist_completed_step(
            state,
            &step_id,
            None,
            Some("dry-run: script not executed".to_string()),
            None,
            None,
            0,
            None,
        )?;
        record_step_success(
            state,
            node.name.clone(),
            crate::types::StepSuccess {
                step_name: node.name.clone(),
                result_text: Some("dry-run: script not executed".to_string()),
                iteration,
                ..crate::types::StepSuccess::default()
            },
        );
        return Ok(());
    }

    // `run_inner` rejects an invalid prefix before the run starts; this covers
    // callers that execute a script step directly. The error is about the
    // configuration, so it is never retried.
    let input_prefix = state.script_env_provider.input_env_prefix().to_string();
    if let Err(e) = crate::traits::script_env_provider::validate_input_env_prefix(&input_prefix) {
        let step_id =
            super::insert_step_record(state, &node.name, "script", pos, iteration, Some(0))?;
        let err_msg = format!("Script '{}': {e}", node.name);
        tracing::warn!("{}", err_msg);
        mark_attempt_failed(state, &step_id, &err_msg, 0)?;
        return fail_step(state, node, err_msg, iteration, 1);
    }

    // Build variable map for substitution, shell-quoting all values to prevent
    // injection when they are interpolated into the sh -c command string.
    let vars = build_variable_map(state);
    let shell_safe_vars: std::collections::HashMap<String, String> = vars
        .iter()
        .map(|(k, v)| (k.clone(), crate::prompt_builder::shell_quote(v)))
        .collect();
    let script_cmd = crate::prompt_builder::substitute_variables(&node.run, &shell_safe_vars);

    tracing::info!("script '{}': executing command", node.name);

    // Build environment variables
    let mut env_vars: std::collections::HashMap<String, String> = std::collections::HashMap::new();

    // Inject PATH, GH_TOKEN (when `as = "..."` resolves to a host-known bot),
    // and other env from the script env provider. Falls back to the
    // workflow-level default bot when the step doesn't specify one.
    {
        let effective_bot = node
            .as_identity
            .as_deref()
            .or(state.default_as_identity.as_deref());
        let provider_env = state
            .script_env_provider
            .env(state.run_ctx.as_ref(), effective_bot);
        env_vars.extend(provider_env);
    }

    // Variables that neither workflow inputs nor the `env = { ... }` block may
    // set: overriding them would change what the script loads or who it acts as.
    const SENSITIVE_ENV_VARS: &[&str] = &[
        "LD_PRELOAD",
        "LD_LIBRARY_PATH",
        "DYLD_LIBRARY_PATH",
        "PATH",
        "DYLD_INSERT_LIBRARIES",
        "PYTHONPATH",
        "RUBYLIB",
        "NODE_PATH",
        // Protect the bot-identity token resolved by `as = "..."` from being
        // silently overwritten by a workflow-authored env block.
        "GH_TOKEN",
    ];

    // Inject all current workflow inputs as env vars, prefixed with the
    // provider's `input_env_prefix()` (`FLOW_` by default).
    // Validate that keys consist only of alphanumeric characters and underscores to
    // prevent malformed env var names if a key contains `=` or a null byte.
    for (k, v) in &state.inputs {
        if !k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            tracing::warn!(
                "script '{}': input key {:?} contains characters invalid in an env var name, skipping",
                node.name,
                k
            );
            continue;
        }
        let name = format!("{input_prefix}{}", k.to_uppercase());
        if SENSITIVE_ENV_VARS.contains(&name.as_str()) {
            tracing::warn!(
                "script '{}': input {:?} would set security-sensitive variable {:?} — skipping",
                node.name,
                k,
                name
            );
            continue;
        }
        env_vars.insert(name, v.clone());
    }

    // Inject explicit env vars from the workflow `env = { ... }` block.
    // Template variables (e.g. `{{prior_output}}`) are substituted using the raw
    // (non-shell-quoted) variable map because values are passed as discrete env
    // var values, not interpolated into a shell command string.
    for (k, v) in &node.env {
        if k.contains('=') || k.contains('\0') {
            tracing::warn!(
                "script '{}': env key {:?} contains '=' or null byte, skipping",
                node.name,
                k
            );
            continue;
        }
        if SENSITIVE_ENV_VARS.contains(&k.as_str()) {
            tracing::warn!(
                "script '{}': env block overrides security-sensitive variable {:?} — skipping",
                node.name,
                k
            );
            continue;
        }
        let resolved = crate::prompt_builder::substitute_variables(v, &vars);
        env_vars.insert(k.clone(), resolved);
    }

    let working_dir = state.run_ctx.working_dir_str();

    // One step row per attempt, as `call` records its retries. Retries are
    // immediate: `call` has no backoff either.
    let max_attempts = 1 + node.retries;
    let mut last_error = String::new();
    for attempt in 0..max_attempts {
        let step_id =
            super::begin_retry_attempt(state, &node.name, "script", pos, iteration, attempt)?;

        match run_script_once(node, &script_cmd, &env_vars, &working_dir) {
            ScriptAttempt::Succeeded {
                markers,
                context,
                structured_output,
                duration_ms,
            } => {
                let markers_json = crate::helpers::serialize_or_empty_array(
                    &markers,
                    &format!("script '{}'", node.name),
                );

                super::persist_completed_step(
                    state,
                    &step_id,
                    None,
                    Some(format!("Script '{}' completed", node.name)),
                    Some(context.clone()),
                    Some(markers_json),
                    attempt,
                    structured_output.clone(),
                )?;

                record_step_success(
                    state,
                    node.name.clone(),
                    crate::types::StepSuccess {
                        step_name: node.name.clone(),
                        result_text: Some(format!("Script '{}' completed", node.name)),
                        metadata: std::collections::HashMap::from([(
                            crate::constants::metadata_keys::DURATION_MS.to_string(),
                            duration_ms.to_string(),
                        )]),
                        markers,
                        context,
                        iteration,
                        structured_output,
                        ..crate::types::StepSuccess::default()
                    },
                );

                return Ok(());
            }
            ScriptAttempt::Failed(err_msg) => {
                tracing::warn!(
                    "script '{}' (attempt {}/{}): {}",
                    node.name,
                    attempt + 1,
                    max_attempts,
                    err_msg
                );
                mark_attempt_failed(state, &step_id, &err_msg, attempt)?;
                last_error = err_msg;
            }
        }
    }

    fail_step(state, node, last_error, iteration, max_attempts)
}

/// Record a failed attempt on its step row.
fn mark_attempt_failed(
    state: &ExecutionState,
    step_id: &str,
    err_msg: &str,
    retry_count: u32,
) -> Result<()> {
    let generation = state.expect_lease_generation();
    state.persistence.update_step(
        step_id,
        StepUpdate::failed(generation, err_msg.to_string(), retry_count),
    )?;
    Ok(())
}

/// Route a step that has failed for good through its `on_fail` policy.
fn fail_step(
    state: &mut ExecutionState,
    node: &ScriptNode,
    err_msg: String,
    iteration: u32,
    attempts: u32,
) -> Result<()> {
    handle_on_fail(
        state,
        node.name.clone(),
        &node.name,
        &node.on_fail,
        err_msg,
        node.retries,
        iteration,
        attempts,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::ScriptNode;
    use crate::engine::ExecutionState;
    use crate::persistence_memory::InMemoryWorkflowPersistence;
    use crate::status::WorkflowStepStatus;

    use crate::traits::persistence::{NewRun, WorkflowPersistence};
    use crate::traits::run_context::NoopRunContext;

    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_persistence() -> (Arc<InMemoryWorkflowPersistence>, String) {
        let p = Arc::new(InMemoryWorkflowPersistence::new());
        let run = p
            .create_run(NewRun {
                workflow_name: "wf".to_string(),
                parent_run_id: String::new(),
                dry_run: false,
                trigger: "manual".to_string(),
                definition_snapshot: None,
                parent_workflow_run_id: None,
            })
            .unwrap();
        (p, run.id)
    }

    fn make_state(persistence: Arc<InMemoryWorkflowPersistence>, run_id: String) -> ExecutionState {
        let mut state = crate::test_helpers::make_test_execution_state(persistence, run_id);
        state.run_ctx = Arc::new(NoopRunContext::default().with_working_dir(std::env::temp_dir()))
            as Arc<dyn crate::traits::run_context::RunContext>;
        state
    }

    fn make_node(name: &str, run: &str) -> ScriptNode {
        ScriptNode {
            name: name.to_string(),
            run: run.to_string(),
            env: Default::default(),
            timeout: None,
            retries: 0,
            on_fail: None,
            as_identity: None,
        }
    }

    /// When the script emits a valid FLOW_OUTPUT block, markers and context
    /// must be extracted and stored on the step record.
    #[test]
    fn flow_output_markers_propagate_to_step_record() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        // Use printf to avoid shell newline differences across platforms.
        let script = concat!(
            "printf '<<<FLOW_OUTPUT>>>\\n",
            r#"{"markers":["test_passed"],"context":"step ctx"}"#,
            "\\n<<<END_FLOW_OUTPUT>>>\\n'"
        );
        let node = make_node("check", script);
        execute_script(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Completed);
        let markers: Vec<String> = step
            .markers_out
            .as_deref()
            .and_then(|m| serde_json::from_str(m).ok())
            .unwrap_or_default();
        assert_eq!(markers, vec!["test_passed"]);
        assert_eq!(step.context_out.as_deref(), Some("step ctx"));
    }

    /// When the script produces no FLOW_OUTPUT block, context falls back to
    /// raw stdout truncated to 2000 characters.
    #[test]
    fn falls_back_to_raw_stdout_when_no_flow_output_block() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let node = make_node("info", "echo 'plain output'");
        execute_script(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Completed);
        let ctx = step.context_out.as_deref().unwrap_or("");
        assert!(
            ctx.contains("plain output"),
            "context should contain stdout: {ctx:?}"
        );
        let markers: Vec<String> = step
            .markers_out
            .as_deref()
            .and_then(|m| serde_json::from_str(m).ok())
            .unwrap_or_default();
        assert!(markers.is_empty(), "no markers expected for plain stdout");
    }

    /// A step reads nothing from the host's stdin, which keeps its pending data.
    #[test]
    fn script_step_does_not_read_host_stdin() {
        use runkon_test_support::*;

        if !is_stdin_probe_child() {
            run_in_child_with_piped_stdin(
                "executors::script::tests::script_step_does_not_read_host_stdin",
            );
            return;
        }

        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let node = make_node("probe", "printf '[%s]' \"$(cat)\"");
        execute_script(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps[0].status, WorkflowStepStatus::Completed);
        assert_eq!(steps[0].context_out.as_deref().unwrap_or("").trim(), "[]");

        assert_stdin_untouched_and_report();
    }

    /// A script that prints an unrecognised output block (e.g. from an older
    /// protocol) must fail the step instead of silently recording `markers: []`.
    #[test]
    fn unrecognised_output_block_fails_step() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let script = concat!(
            "printf '<<<LEGACY_OUTPUT>>>\\n",
            r#"{"markers":["has_code_changes"],"context":"Found 6 code files"}"#,
            "\\n<<<END_LEGACY_OUTPUT>>>\\n'"
        );
        let node = make_node("detect-file-types", script);
        let result = execute_script(&mut state, &node, 0);
        assert!(
            result.is_err(),
            "expected failure for unrecognised output block"
        );

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Failed);
        let err_text = step.result_text.as_deref().unwrap_or("");
        assert!(
            err_text.contains("detect-file-types") && err_text.contains("LEGACY_OUTPUT"),
            "error should name the step and the marker found: {err_text}"
        );
    }

    /// A `<<<FLOW_OUTPUT>>>` block with invalid JSON must fail the step.
    #[test]
    fn malformed_flow_output_block_fails_step() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let script =
            "printf '<<<FLOW_OUTPUT>>>\\n{\"markers\": [oops]}\\n<<<END_FLOW_OUTPUT>>>\\n'";
        let node = make_node("broken", script);
        let result = execute_script(&mut state, &node, 0);
        assert!(result.is_err(), "expected failure for unparseable block");

        let steps = persistence.get_steps(&run_id).unwrap();
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Failed);
        let err_text = step.result_text.as_deref().unwrap_or("");
        assert!(
            err_text.contains("could not be parsed"),
            "error should say the block could not be parsed: {err_text}"
        );
    }

    /// A one-line `<<<FLOW_OUTPUT>>> {…}` with no end marker must fail the step
    /// instead of completing with `markers: []` (#89).
    #[test]
    fn one_line_flow_output_without_end_marker_fails_step() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let script = "echo '<<<FLOW_OUTPUT>>> {\"markers\": [\"pushed_new_commits\"]}'";
        let node = make_node("push", script);
        let result = execute_script(&mut state, &node, 0);
        assert!(result.is_err(), "expected failure for one-line block");

        let steps = persistence.get_steps(&run_id).unwrap();
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Failed);
        let err_text = step.result_text.as_deref().unwrap_or("");
        assert!(
            err_text.contains("could not be parsed")
                && err_text.contains("missing <<<END_FLOW_OUTPUT>>>"),
            "error should point at the missing end marker: {err_text}"
        );
    }

    /// `on_fail = continue` still applies to an unparseable output block.
    #[test]
    fn unrecognised_output_block_respects_on_fail_continue() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let mut node = make_node(
            "lenient",
            "printf '<<<LEGACY_OUTPUT>>>\\n{}\\n<<<END_LEGACY_OUTPUT>>>\\n'",
        );
        node.on_fail = Some(crate::dsl::OnFail::Continue);
        execute_script(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps[0].status, WorkflowStepStatus::Failed);
        assert_eq!(
            state.step_results["lenient"].status,
            WorkflowStepStatus::Skipped
        );
    }

    /// When state.inputs contains a key with invalid env-var characters (e.g. `=`),
    /// that key must be silently dropped while a valid key is still injected.
    #[test]
    fn invalid_env_var_key_is_dropped_valid_key_is_injected() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());

        // One valid key and one invalid key (contains `=`).
        state
            .inputs
            .insert("VALID_KEY".to_string(), "hello".to_string());
        state
            .inputs
            .insert("INVALID=KEY".to_string(), "world".to_string());

        // The script prints the value of the env var for the valid key and exits 0.
        let node = make_node("env_test", "echo $FLOW_VALID_KEY");
        execute_script(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Completed);
        let ctx = step.context_out.as_deref().unwrap_or("");
        assert!(
            ctx.contains("hello"),
            "valid key should be injected as FLOW_VALID_KEY; context: {ctx:?}"
        );
    }

    /// A provider that only overrides the input env-var prefix.
    struct PrefixProvider(&'static str);

    impl crate::traits::script_env_provider::ScriptEnvProvider for PrefixProvider {
        fn env(
            &self,
            _ctx: &dyn crate::traits::run_context::RunContext,
            _as_identity: Option<&str>,
        ) -> HashMap<String, String> {
            HashMap::new()
        }

        fn input_env_prefix(&self) -> &str {
            self.0
        }
    }

    /// Run `script` with one input, `rk_env_probe = "T-1"`, and return the step's context.
    ///
    /// The key is one no host sets: script steps inherit the parent environment,
    /// and a test run inside a conductor workflow already has `CONDUCTOR_RK_ENV_PROBE`.
    fn run_with_ticket_input(state: &mut ExecutionState, run_id: &str, script: &str) -> String {
        state
            .inputs
            .insert("rk_env_probe".to_string(), "T-1".to_string());
        execute_script(state, &make_node("env_prefix", script), 0).unwrap();
        let steps = state.persistence.get_steps(run_id).unwrap();
        assert_eq!(steps[0].status, WorkflowStepStatus::Completed);
        steps[0].context_out.clone().unwrap_or_default()
    }

    const PRINT_BOTH_PREFIXES: &str =
        "echo \"flow=${FLOW_RK_ENV_PROBE:-unset} conductor=${CONDUCTOR_RK_ENV_PROBE:-unset}\"";

    /// With the default provider, inputs use the neutral `FLOW_` prefix only.
    #[test]
    fn inputs_use_flow_prefix_by_default() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(persistence, run_id.clone());
        let ctx = run_with_ticket_input(&mut state, &run_id, PRINT_BOTH_PREFIXES);
        assert!(ctx.contains("flow=T-1 conductor=unset"), "context: {ctx:?}");
    }

    /// A provider can override the prefix, e.g. conductor keeping `CONDUCTOR_`.
    #[test]
    fn provider_overrides_input_env_prefix() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(persistence, run_id.clone());
        state.script_env_provider = Arc::new(PrefixProvider("CONDUCTOR_"));
        let ctx = run_with_ticket_input(&mut state, &run_id, PRINT_BOTH_PREFIXES);
        assert!(ctx.contains("flow=unset conductor=T-1"), "context: {ctx:?}");
    }

    /// An invalid prefix fails the step without running the script, rather than
    /// injecting a variable the host didn't intend (`PATH=` + `RK_ENV_PROBE`).
    #[test]
    fn invalid_prefix_fails_the_step_without_running_it() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(persistence, run_id.clone());
        state.script_env_provider = Arc::new(PrefixProvider("PATH="));
        state
            .inputs
            .insert("rk_env_probe".to_string(), "T-1".to_string());
        // With fail_fast (the default) the failed step also fails the call.
        let res = execute_script(&mut state, &make_node("env_prefix", "echo ran"), 0);
        assert!(res.is_err(), "fail_fast should surface the step failure");
        let steps = state.persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps[0].status, WorkflowStepStatus::Failed);
        let result = steps[0].result_text.as_deref().unwrap_or("");
        assert!(
            result.contains("invalid script input env prefix"),
            "result: {result:?}"
        );
        assert!(
            !result.contains("ran"),
            "script must not run; result: {result:?}"
        );
    }

    /// With an empty prefix, an input must not be able to replace a
    /// security-sensitive variable such as `PATH`.
    #[test]
    fn empty_prefix_cannot_override_sensitive_vars() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(persistence, run_id.clone());
        state.script_env_provider = Arc::new(PrefixProvider(""));
        state
            .inputs
            .insert("path".to_string(), "/nonexistent".to_string());
        let ctx = run_with_ticket_input(
            &mut state,
            &run_id,
            "echo \"probe=$RK_ENV_PROBE path=$PATH\"",
        );
        assert!(ctx.contains("probe=T-1"), "context: {ctx:?}");
        assert!(!ctx.contains("path=/nonexistent"), "context: {ctx:?}");
    }

    /// env block vars from node.env are passed to the subprocess.
    #[test]
    fn node_env_vars_are_injected_into_subprocess() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());

        let mut env = HashMap::new();
        env.insert("MY_TEST_VAR".to_string(), "expected_value".to_string());
        let node = ScriptNode {
            name: "env-inject".to_string(),
            run: "echo $MY_TEST_VAR".to_string(),
            env,
            timeout: None,
            retries: 0,
            on_fail: None,
            as_identity: None,
        };
        execute_script(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Completed);
        let ctx = step.context_out.as_deref().unwrap_or("");
        assert!(
            ctx.contains("expected_value"),
            "MY_TEST_VAR should be injected from node.env; context: {ctx:?}"
        );
    }

    /// Template variables in node.env values are substituted from workflow state.
    #[test]
    fn node_env_vars_support_template_substitution() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        // prior_context comes from state.contexts.last().context in build_variable_map
        state.contexts.push(crate::types::ContextEntry {
            step: "prev-step".to_string(),
            iteration: 0,
            context: "substituted".to_string(),
            markers: vec![],
            structured_output: None,
            output_file: None,
        });

        let mut env = HashMap::new();
        env.insert("TEMPLATED_VAR".to_string(), "{{prior_context}}".to_string());
        let node = ScriptNode {
            name: "env-template".to_string(),
            run: "echo $TEMPLATED_VAR".to_string(),
            env,
            timeout: None,
            retries: 0,
            on_fail: None,
            as_identity: None,
        };
        execute_script(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Completed);
        let ctx = step.context_out.as_deref().unwrap_or("");
        assert!(
            ctx.contains("substituted"),
            "template in env value should be substituted; context: {ctx:?}"
        );
    }

    /// Security-sensitive env vars in node.env are skipped, not injected.
    #[test]
    fn sensitive_env_vars_are_blocked() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());

        let mut env = HashMap::new();
        env.insert("LD_PRELOAD".to_string(), "/malicious/lib.so".to_string());
        env.insert(
            "DYLD_LIBRARY_PATH".to_string(),
            "/malicious/lib".to_string(),
        );
        env.insert("SAFE_VAR".to_string(), "allowed_value".to_string());
        let node = ScriptNode {
            name: "sensitive-test".to_string(),
            run: "echo SAFE_VAR=[$SAFE_VAR] LD_PRELOAD=[$LD_PRELOAD] DYLD_LIBRARY_PATH=[$DYLD_LIBRARY_PATH]".to_string(),
            env,
            timeout: None,
            retries: 0,
            on_fail: None,
            as_identity: None,
        };
        execute_script(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Completed);
        let ctx = step.context_out.as_deref().unwrap_or("");
        assert!(
            ctx.contains("SAFE_VAR=[allowed_value]"),
            "SAFE_VAR should be injected; context: {ctx:?}"
        );
        assert!(
            !ctx.contains("/malicious/lib.so"),
            "LD_PRELOAD should be blocked; context: {ctx:?}"
        );
        assert!(
            !ctx.contains("/malicious/lib"),
            "DYLD_LIBRARY_PATH should be blocked; context: {ctx:?}"
        );
    }

    /// A script that exceeds its timeout is killed and the step is marked Failed.
    #[test]
    fn script_timeout_kills_long_running_process() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let node = ScriptNode {
            name: "sleepy".to_string(),
            run: "sleep 5".to_string(),
            env: Default::default(),
            timeout: Some(1), // 1 second timeout
            retries: 0,
            on_fail: None,
            as_identity: None,
        };
        let result = execute_script(&mut state, &node, 0);
        // on_fail is None, so failure should return Err.
        assert!(result.is_err(), "expected timeout error");

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Failed);
        let err_text = step.result_text.as_deref().unwrap_or("");
        assert!(
            err_text.contains("timed out"),
            "error should mention timeout: {err_text}"
        );
    }

    /// A script that writes more than the OS pipe buffer to stdout must not deadlock.
    /// This is a regression test for the pipe-deadlock fix (reader threads spawned
    /// before wait()).
    #[test]
    fn pipe_deadlock_regression_large_stdout() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        // Output 128KB of 'x' to stdout — well above the typical 64KB pipe buffer.
        let node = ScriptNode {
            name: "bigout".to_string(),
            run: "python3 -c \"import sys; sys.stdout.write('x' * 131072)\"".to_string(),
            env: Default::default(),
            timeout: None,
            retries: 0,
            on_fail: None,
            as_identity: None,
        };
        execute_script(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Completed);
    }

    /// Non-UTF-8 stdout is handled gracefully: the step still completes and the
    /// invalid bytes are dropped with a logged warning.
    #[test]
    fn non_utf8_stdout_is_handled_gracefully() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let node = ScriptNode {
            name: "binary".to_string(),
            run: "python3 -c \"import sys; sys.stdout.buffer.write(bytes([0x80,0x81,0x82]))\""
                .to_string(),
            env: Default::default(),
            timeout: None,
            retries: 0,
            on_fail: None,
            as_identity: None,
        };
        execute_script(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Completed);
    }

    /// A non-success exit code with stderr output must capture stderr in the step error.
    #[test]
    fn non_success_exit_with_stderr_capture() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let node = ScriptNode {
            name: "fails-with-stderr".to_string(),
            run: "echo 'error details' >&2 && exit 1".to_string(),
            env: Default::default(),
            timeout: None,
            retries: 0,
            on_fail: None,
            as_identity: None,
        };
        let result = execute_script(&mut state, &node, 0);
        assert!(result.is_err(), "expected failure for non-zero exit");

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Failed);
        let err_text = step.result_text.as_deref().unwrap_or("");
        assert!(
            err_text.contains("error details"),
            "stderr should be captured in error message: {err_text}"
        );
    }

    /// Non-UTF-8 stderr on a failed script must not panic; the step is marked Failed.
    #[test]
    fn non_utf8_stderr_is_handled_gracefully() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let node = ScriptNode {
            name: "bad-stderr".to_string(),
            run: "python3 -c \"import sys; sys.stderr.buffer.write(bytes([0x80,0x81,0x82])); sys.exit(1)\"".to_string(),
            env: Default::default(),
            timeout: None,
            retries: 0,
            on_fail: None,
            as_identity: None,
        };
        let result = execute_script(&mut state, &node, 0);
        assert!(result.is_err(), "expected failure for non-zero exit");

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Failed);
    }

    /// A unique marker path so retry tests can tell the first run from the next.
    fn marker_path(tag: &str) -> String {
        std::env::temp_dir()
            .join(format!(
                "runkon-script-retry-{tag}-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ))
            .to_string_lossy()
            .replace(['(', ')'], "")
    }

    /// Steps ordered by attempt; the in-memory store only sorts by position, which
    /// is shared by every attempt of a step.
    fn attempts(
        persistence: &Arc<InMemoryWorkflowPersistence>,
        run_id: &str,
    ) -> Vec<crate::types::WorkflowRunStep> {
        let mut steps = persistence.get_steps(run_id).unwrap();
        steps.sort_by_key(|s| s.retry_count);
        steps
    }

    /// Fails on the first run (creating the marker) and succeeds on the second.
    fn fail_once_script(marker: &str, fail_cmd: &str) -> String {
        format!("if [ -f '{marker}' ]; then echo ok; else touch '{marker}'; {fail_cmd}; fi")
    }

    #[test]
    fn retries_then_succeeds() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let marker = marker_path("succeeds");
        let _ = std::fs::remove_file(&marker);
        let mut node = make_node("flaky", &fail_once_script(&marker, "exit 1"));
        node.retries = 1;

        execute_script(&mut state, &node, 0).unwrap();
        let _ = std::fs::remove_file(&marker);

        let steps = attempts(&persistence, &run_id);
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].status, WorkflowStepStatus::Failed);
        assert_eq!(steps[0].retry_count, 0);
        assert_eq!(steps[1].status, WorkflowStepStatus::Completed);
        assert_eq!(steps[1].retry_count, 1);
        assert_eq!(
            state.step_results["flaky"].status,
            WorkflowStepStatus::Completed
        );
    }

    #[test]
    fn unparseable_output_is_retried() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let marker = marker_path("unparseable");
        let _ = std::fs::remove_file(&marker);
        let bad = "echo '<<<FLOW_OUTPUT>>> not json <<<END_FLOW_OUTPUT>>>'";
        let mut node = make_node("bad_output_once", &fail_once_script(&marker, bad));
        node.retries = 1;

        execute_script(&mut state, &node, 0).unwrap();
        let _ = std::fs::remove_file(&marker);

        let steps = attempts(&persistence, &run_id);
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].status, WorkflowStepStatus::Failed);
        assert!(steps[0]
            .result_text
            .as_deref()
            .unwrap_or("")
            .contains("could not be parsed"));
        assert_eq!(steps[1].status, WorkflowStepStatus::Completed);
    }

    #[test]
    fn timeout_is_retried() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let marker = marker_path("timeout");
        let _ = std::fs::remove_file(&marker);
        let mut node = make_node("slow_once", &fail_once_script(&marker, "sleep 5"));
        node.timeout = Some(1);
        node.retries = 1;

        execute_script(&mut state, &node, 0).unwrap();
        let _ = std::fs::remove_file(&marker);

        let steps = attempts(&persistence, &run_id);
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].status, WorkflowStepStatus::Failed);
        assert!(steps[0]
            .result_text
            .as_deref()
            .unwrap_or("")
            .contains("timed out"));
        assert_eq!(steps[1].status, WorkflowStepStatus::Completed);
    }

    #[test]
    fn on_fail_continue_after_retries_skips() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let mut node = make_node("always_bad", "exit 1");
        node.retries = 1;
        node.on_fail = Some(crate::dsl::OnFail::Continue);

        execute_script(&mut state, &node, 0).unwrap();

        let steps = attempts(&persistence, &run_id);
        assert_eq!(steps.len(), 2);
        assert!(steps.iter().all(|s| s.status == WorkflowStepStatus::Failed));
        assert_eq!(
            state.step_results["always_bad"].status,
            WorkflowStepStatus::Skipped
        );
    }

    #[test]
    fn exhausted_retries_run_the_on_fail_agent_once() {
        use crate::traits::action_executor::{
            ActionExecutor, ActionOutput, ActionParams, ActionRegistry, StepInfo,
        };
        use std::sync::Mutex;

        struct Recorder(Arc<Mutex<Vec<HashMap<String, String>>>>);
        impl ActionExecutor for Recorder {
            fn name(&self) -> &str {
                "fixer"
            }
            fn execute(
                &self,
                _ctx: &dyn crate::traits::run_context::RunContext,
                _info: &StepInfo,
                params: &ActionParams,
            ) -> std::result::Result<ActionOutput, crate::engine_error::EngineError> {
                self.0.lock().unwrap().push((*params.inputs).clone());
                Ok(ActionOutput::default())
            }
        }

        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut named: HashMap<String, Box<dyn ActionExecutor>> = HashMap::new();
        named.insert(
            "fixer".to_string(),
            Box::new(Recorder(Arc::clone(&calls))) as Box<dyn ActionExecutor>,
        );

        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        state.action_registry = Arc::new(ActionRegistry::new(named, None));
        let mut node = make_node("doomed", "exit 3");
        node.retries = 2;
        node.on_fail = Some(crate::dsl::OnFail::Agent(crate::dsl::AgentRef::Name(
            "fixer".to_string(),
        )));

        // fail_fast is on, so the exhausted step still fails the call.
        assert!(execute_script(&mut state, &node, 0).is_err());

        let steps = attempts(&persistence, &run_id);
        let script_rows: Vec<_> = steps.iter().filter(|s| s.role == "script").collect();
        assert_eq!(script_rows.len(), 3);
        assert!(script_rows
            .iter()
            .all(|s| s.status == WorkflowStepStatus::Failed));
        let counts: Vec<i64> = script_rows.iter().map(|s| s.retry_count).collect();
        assert_eq!(counts, vec![0, 1, 2]);

        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "on_fail agent should run exactly once");
        assert_eq!(calls[0]["failed_step"], "doomed");
        assert!(calls[0]["failure_reason"].contains("exited with code 3"));
        assert_eq!(calls[0]["retry_count"], "2");
    }
}
