use std::path::Path;

use crate::dsl::ScriptNode;
use crate::engine::{
    record_step_failure, record_step_skipped, record_step_success, restore_step, should_skip,
    ExecutionState,
};
use crate::engine_error::{EngineError, Result};
use crate::prompt_builder::build_variable_map;
use crate::status::WorkflowStepStatus;
use crate::traits::persistence::{NewStep, StepUpdate};
use crate::traits::run_context::RunContext;

pub fn execute_script(state: &mut ExecutionState, node: &ScriptNode, iteration: u32) -> Result<()> {
    let pos = state.position;
    state.position += 1;

    // Skip completed script steps on resume
    if should_skip(state, &node.name, iteration) {
        tracing::info!("Skipping completed script step '{}'", node.name);
        restore_step(state, &node.name, iteration);
        return Ok(());
    }

    let step_id = state
        .persistence
        .insert_step(NewStep {
            workflow_run_id: state.workflow_run_id.clone(),
            step_name: node.name.clone(),
            role: "script".to_string(),
            can_commit: false,
            position: pos,
            iteration: iteration as i64,
            retry_count: Some(0),
        })
        .map_err(|e| EngineError::Persistence(e.to_string()))?;

    if state.exec_config.dry_run {
        tracing::info!("script '{}': dry-run, skipping execution", node.name);
        state
            .persistence
            .update_step(
                &step_id,
                StepUpdate {
                    status: WorkflowStepStatus::Completed,
                    child_run_id: None,
                    result_text: Some("dry-run: script not executed".to_string()),
                    context_out: None,
                    markers_out: None,
                    retry_count: Some(0),
                    structured_output: None,
                    step_error: None,
                },
            )
            .map_err(|e| EngineError::Persistence(e.to_string()))?;

        record_step_success(
            state,
            node.name.clone(),
            &node.name,
            Some("dry-run: script not executed".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            vec![],
            String::new(),
            None,
            iteration,
            None,
            None,
        );
        return Ok(());
    }

    // Build variable map for substitution, shell-quoting all values to prevent
    // injection when they are interpolated into the sh -c command string.
    let vars = build_variable_map(state);
    let shell_safe_vars: std::collections::HashMap<&str, String> = vars
        .iter()
        .map(|(k, v)| (*k, crate::prompt_builder::shell_quote(v)))
        .collect();
    let script_cmd = crate::prompt_builder::substitute_variables(&node.run, &shell_safe_vars);

    tracing::info!("script '{}': executing command", node.name);

    // Build environment variables
    let mut env_vars: std::collections::HashMap<String, String> = std::collections::HashMap::new();

    // Inject PATH and other env from the script env provider
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
        let provider_env = state.script_env_provider.env(&run_ctx);
        env_vars.extend(provider_env);
    }

    // Inject all current workflow inputs as env vars (prefixed with CONDUCTOR_)
    for (k, v) in &state.inputs {
        env_vars.insert(format!("CONDUCTOR_{}", k.to_uppercase()), v.clone());
    }

    // Execute the script
    let working_dir = &state.worktree_ctx.working_dir;
    let output_file = match tempfile::NamedTempFile::new() {
        Ok(f) => {
            let path = f.path().to_path_buf();
            match f.keep() {
                Ok(_) => Some(path),
                Err(e) => {
                    tracing::warn!(
                        "script '{}': failed to persist temp output file: {e}",
                        node.name
                    );
                    None
                }
            }
        }
        Err(e) => {
            tracing::warn!(
                "script '{}': failed to create temp output file, stdout will not be captured: {e}",
                node.name
            );
            None
        }
    };

    let mut cmd = std::process::Command::new("sh");
    cmd.arg("-c").arg(&script_cmd);
    cmd.current_dir(working_dir);
    for (k, v) in &env_vars {
        cmd.env(k, v);
    }

    // Optionally capture stdout to output file
    if let Some(ref out_path) = output_file {
        if let Ok(file) = std::fs::File::create(out_path) {
            cmd.stdout(file);
        }
    }

    let start = std::time::Instant::now();
    let result = cmd.status();
    let duration_ms = start.elapsed().as_millis() as i64;

    match result {
        Ok(status) if status.success() => {
            tracing::info!(
                "script '{}': completed successfully in {}ms",
                node.name,
                duration_ms
            );

            let output_file_path = output_file
                .as_ref()
                .map(|p| p.to_string_lossy().to_string());

            state
                .persistence
                .update_step(
                    &step_id,
                    StepUpdate {
                        status: WorkflowStepStatus::Completed,
                        child_run_id: None,
                        result_text: Some(format!("Script '{}' completed", node.name)),
                        context_out: output_file_path.clone(),
                        markers_out: None,
                        retry_count: Some(0),
                        structured_output: None,
                        step_error: None,
                    },
                )
                .map_err(|e| EngineError::Persistence(e.to_string()))?;

            record_step_success(
                state,
                node.name.clone(),
                &node.name,
                Some(format!("Script '{}' completed", node.name)),
                None,
                None,
                Some(duration_ms),
                None,
                None,
                None,
                None,
                vec![],
                String::new(),
                None,
                iteration,
                None,
                output_file_path,
            );

            Ok(())
        }
        Ok(status) => {
            let exit_code = status.code().unwrap_or(-1);
            let err_msg = format!("Script '{}' exited with code {}", node.name, exit_code);
            tracing::warn!("{}", err_msg);

            state
                .persistence
                .update_step(
                    &step_id,
                    StepUpdate {
                        status: WorkflowStepStatus::Failed,
                        child_run_id: None,
                        result_text: Some(err_msg.clone()),
                        context_out: None,
                        markers_out: None,
                        retry_count: Some(0),
                        structured_output: None,
                        step_error: Some(err_msg.clone()),
                    },
                )
                .map_err(|e| EngineError::Persistence(e.to_string()))?;

            // Clean up output file on failure
            if let Some(ref p) = output_file {
                let _ = std::fs::remove_file(p);
            }

            match &node.on_fail {
                Some(crate::dsl::OnFail::Continue) => {
                    record_step_skipped(state, node.name.clone(), &node.name);
                    Ok(())
                }
                _ => record_step_failure(state, node.name.clone(), &node.name, err_msg, 1, true),
            }
        }
        Err(e) => {
            let err_msg = format!("Script '{}' failed to execute: {e}", node.name);
            tracing::warn!("{}", err_msg);

            state
                .persistence
                .update_step(
                    &step_id,
                    StepUpdate {
                        status: WorkflowStepStatus::Failed,
                        child_run_id: None,
                        result_text: Some(err_msg.clone()),
                        context_out: None,
                        markers_out: None,
                        retry_count: Some(0),
                        structured_output: None,
                        step_error: Some(err_msg.clone()),
                    },
                )
                .map_err(|e2| EngineError::Persistence(e2.to_string()))?;

            match &node.on_fail {
                Some(crate::dsl::OnFail::Continue) => {
                    record_step_skipped(state, node.name.clone(), &node.name);
                    Ok(())
                }
                _ => record_step_failure(state, node.name.clone(), &node.name, err_msg, 1, true),
            }
        }
    }
}
