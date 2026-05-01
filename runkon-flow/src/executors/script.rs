use std::io::Read;
use std::path::Path;

use crate::dsl::ScriptNode;
use crate::engine::{
    record_step_failure, record_step_skipped, record_step_success, restore_step, should_skip,
    ExecutionState,
};
use crate::engine_error::Result;
use crate::prompt_builder::build_variable_map;
use crate::traits::persistence::StepUpdate;
use crate::traits::run_context::RunContext;

use super::p_err;

use wait_timeout::ChildExt;

fn apply_script_on_fail(
    state: &mut ExecutionState,
    step_name: &str,
    on_fail: &Option<crate::dsl::OnFail>,
    err_msg: String,
) -> Result<()> {
    match on_fail {
        Some(crate::dsl::OnFail::Continue) => {
            record_step_skipped(state, step_name.to_string(), step_name);
            Ok(())
        }
        _ => record_step_failure(state, step_name.to_string(), step_name, err_msg, 1, true),
    }
}

/// Persist a script step failure and apply on_fail logic in one call.
fn fail_script_step(
    state: &mut ExecutionState,
    step_id: &str,
    node: &ScriptNode,
    err_msg: String,
) -> Result<()> {
    tracing::warn!("{}", err_msg);
    state
        .persistence
        .update_step(step_id, StepUpdate::failed(err_msg.clone(), 0))
        .map_err(p_err)?;
    apply_script_on_fail(state, &node.name, &node.on_fail, err_msg)
}

pub fn execute_script(state: &mut ExecutionState, node: &ScriptNode, iteration: u32) -> Result<()> {
    let pos = state.position;
    state.position += 1;

    // Skip completed script steps on resume
    if should_skip(state, &node.name, iteration) {
        tracing::info!("Skipping completed script step '{}'", node.name);
        restore_step(state, &node.name, iteration);
        return Ok(());
    }

    let step_id = super::insert_step_record(state, &node.name, "script", pos, iteration, Some(0))?;

    if state.exec_config.dry_run {
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
        struct ScriptRunCtx<'a> {
            working_dir: &'a str,
            repo_path: &'a str,
        }
        impl RunContext for ScriptRunCtx<'_> {
            fn injected_variables(&self) -> std::collections::HashMap<&'static str, String> {
                std::collections::HashMap::new()
            }
            fn working_dir(&self) -> &Path {
                Path::new(self.working_dir)
            }
            fn repo_path(&self) -> &Path {
                Path::new(self.repo_path)
            }
            fn worktree_id(&self) -> Option<&str> {
                None
            }
            fn ticket_id(&self) -> Option<&str> {
                None
            }
            fn repo_id(&self) -> Option<&str> {
                None
            }
        }
        let run_ctx = ScriptRunCtx {
            working_dir: &state.worktree_ctx.working_dir,
            repo_path: &state.worktree_ctx.repo_path,
        };
        let effective_bot = node
            .bot_name
            .as_deref()
            .or(state.default_bot_name.as_deref());
        let provider_env = state.script_env_provider.env(&run_ctx, effective_bot);
        env_vars.extend(provider_env);
    }

    // Inject all current workflow inputs as env vars (prefixed with CONDUCTOR_).
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
        env_vars.insert(format!("CONDUCTOR_{}", k.to_uppercase()), v.clone());
    }

    // Inject explicit env vars from the workflow `env = { ... }` block.
    // Template variables (e.g. `{{prior_output}}`) are substituted using the raw
    // (non-shell-quoted) variable map because values are passed as discrete env
    // var values, not interpolated into a shell command string.
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

    // Execute the script
    let working_dir = &state.worktree_ctx.working_dir;

    let mut cmd = std::process::Command::new("sh");
    cmd.arg("-c").arg(&script_cmd);
    cmd.current_dir(working_dir);
    for (k, v) in &env_vars {
        cmd.env(k, v);
    }
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return fail_script_step(
                state,
                &step_id,
                node,
                format!("Script '{}' failed to execute: {e}", node.name),
            );
        }
    };

    // Spawn reader threads BEFORE waiting for the child to avoid pipe deadlock.
    // If the child writes more than the OS pipe buffer size to either stream
    // while the parent is blocked in wait(), the child would hang forever.
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

    let stdout_handle = {
        let pipe = match child.stdout.take() {
            Some(p) => p,
            None => {
                return fail_script_step(
                    state,
                    &step_id,
                    node,
                    format!(
                        "script '{}': stdout pipe unavailable after spawn",
                        node.name
                    ),
                );
            }
        };
        spawn_reader_thread(pipe, "stdout")
    };
    let stderr_handle = {
        let pipe = match child.stderr.take() {
            Some(p) => p,
            None => {
                return fail_script_step(
                    state,
                    &step_id,
                    node,
                    format!(
                        "script '{}': stderr pipe unavailable after spawn",
                        node.name
                    ),
                );
            }
        };
        spawn_reader_thread(pipe, "stderr")
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
                return fail_script_step(
                    state,
                    &step_id,
                    node,
                    format!("Script '{}' timed out after {}s", node.name, timeout_secs),
                );
            }
            Err(e) => {
                let _ = stdout_handle.join();
                let _ = stderr_handle.join();
                return fail_script_step(
                    state,
                    &step_id,
                    node,
                    format!("Script '{}' wait failed: {e}", node.name),
                );
            }
        }
    } else {
        match child.wait() {
            Ok(s) => s,
            Err(e) => {
                let _ = stdout_handle.join();
                let _ = stderr_handle.join();
                return fail_script_step(
                    state,
                    &step_id,
                    node,
                    format!("Script '{}' wait failed: {e}", node.name),
                );
            }
        }
    };
    let duration_ms = start.elapsed().as_millis() as i64;

    let stdout = match stdout_handle.join() {
        Ok(buf) => match String::from_utf8(buf) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("script: stdout is not valid UTF-8: {e}");
                String::new()
            }
        },
        Err(_) => {
            tracing::warn!("script: stdout reader thread panicked");
            String::new()
        }
    };

    if status.success() {
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
        let parsed = crate::helpers::parse_flow_output(&stdout);
        let (markers, context, structured_output) = match parsed {
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
                let ctx: String = stdout.chars().take(2000).collect();
                (vec![], ctx, None)
            }
        };

        let markers_json =
            crate::helpers::serialize_or_empty_array(&markers, &format!("script '{}'", node.name));

        super::persist_completed_step(
            state,
            &step_id,
            None,
            Some(format!("Script '{}' completed", node.name)),
            Some(context.clone()),
            Some(markers_json),
            0,
            structured_output.clone(),
        )?;

        record_step_success(
            state,
            node.name.clone(),
            crate::types::StepSuccess {
                step_name: node.name.clone(),
                result_text: Some(format!("Script '{}' completed", node.name)),
                duration_ms: Some(duration_ms),
                markers,
                context,
                iteration,
                structured_output,
                ..crate::types::StepSuccess::default()
            },
        );

        Ok(())
    } else {
        let exit_code = status.code().unwrap_or(-1);

        let stderr = match stderr_handle.join() {
            Ok(buf) => match String::from_utf8(buf) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("script: stderr is not valid UTF-8: {e}");
                    String::new()
                }
            },
            Err(_) => {
                tracing::warn!("script: stderr reader thread panicked");
                String::new()
            }
        };

        let captured_stderr = stderr.trim().chars().take(2000).collect::<String>();

        let err_msg = if captured_stderr.is_empty() {
            format!("Script '{}' exited with code {}", node.name, exit_code)
        } else {
            format!(
                "Script '{}' exited with code {}\n{}",
                node.name, exit_code, captured_stderr
            )
        };

        fail_script_step(state, &step_id, node, err_msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::ScriptNode;
    use crate::engine::{ExecutionState, WorktreeContext};
    use crate::persistence_memory::InMemoryWorkflowPersistence;
    use crate::status::WorkflowStepStatus;
    use crate::traits::action_executor::ActionRegistry;
    use crate::traits::item_provider::ItemProviderRegistry;
    use crate::traits::persistence::{NewRun, WorkflowPersistence};
    use crate::traits::script_env_provider::NoOpScriptEnvProvider;
    use crate::types::WorkflowExecConfig;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_persistence() -> (Arc<InMemoryWorkflowPersistence>, String) {
        let p = Arc::new(InMemoryWorkflowPersistence::new());
        let run = p
            .create_run(NewRun {
                workflow_name: "wf".to_string(),
                worktree_id: None,
                ticket_id: None,
                repo_id: None,
                parent_run_id: String::new(),
                dry_run: false,
                trigger: "manual".to_string(),
                definition_snapshot: None,
                parent_workflow_run_id: None,
                target_label: None,
            })
            .unwrap();
        (p, run.id)
    }

    fn make_state(persistence: Arc<InMemoryWorkflowPersistence>, run_id: String) -> ExecutionState {
        ExecutionState {
            persistence,
            action_registry: Arc::new(ActionRegistry::new(HashMap::new(), None)),
            script_env_provider: Arc::new(NoOpScriptEnvProvider),
            workflow_run_id: run_id,
            workflow_name: "wf".to_string(),
            worktree_ctx: WorktreeContext {
                worktree_id: None,
                working_dir: std::env::temp_dir().to_string_lossy().to_string(),
                repo_path: String::new(),
                ticket_id: None,
                repo_id: None,
                extra_plugin_dirs: vec![],
            },
            model: None,
            exec_config: WorkflowExecConfig::default(),
            inputs: HashMap::new(),
            parent_run_id: String::new(),
            depth: 0,
            target_label: None,
            step_results: HashMap::new(),
            contexts: vec![],
            position: 0,
            all_succeeded: true,
            total_cost: 0.0,
            total_turns: 0,
            total_duration_ms: 0,
            total_input_tokens: 0,
            total_output_tokens: 0,
            total_cache_read_input_tokens: 0,
            total_cache_creation_input_tokens: 0,
            last_gate_feedback: None,
            block_output: None,
            block_with: vec![],
            resume_ctx: None,
            default_bot_name: None,
            triggered_by_hook: false,
            schema_resolver: None,
            child_runner: None,
            last_heartbeat_at: ExecutionState::new_heartbeat(),
            registry: Arc::new(ItemProviderRegistry::new()),
            event_sinks: Arc::from(vec![]),
            cancellation: crate::cancellation::CancellationToken::new(),
            current_execution_id: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    fn make_node(name: &str, run: &str) -> ScriptNode {
        ScriptNode {
            name: name.to_string(),
            run: run.to_string(),
            env: Default::default(),
            timeout: None,
            retries: 0,
            on_fail: None,
            bot_name: None,
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
        let node = make_node("env_test", "echo $CONDUCTOR_VALID_KEY");
        execute_script(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Completed);
        let ctx = step.context_out.as_deref().unwrap_or("");
        assert!(
            ctx.contains("hello"),
            "valid key should be injected as CONDUCTOR_VALID_KEY; context: {ctx:?}"
        );
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
            bot_name: None,
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
            bot_name: None,
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
            bot_name: None,
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
            bot_name: None,
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
            bot_name: None,
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
            bot_name: None,
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
            bot_name: None,
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
            bot_name: None,
        };
        let result = execute_script(&mut state, &node, 0);
        assert!(result.is_err(), "expected failure for non-zero exit");

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Failed);
    }
}
