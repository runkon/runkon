use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::cancellation_reason::CancellationReason;
use crate::dsl::CallNode;
use crate::engine::{
    emit_event, handle_on_fail, record_step_success, resolve_schema, restore_step, should_skip,
    ExecutionState,
};
use crate::engine_error::{EngineError, Result};
use crate::events::EngineEvent;
use crate::prompt_builder::build_variable_map;
use crate::status::WorkflowStepStatus;
use crate::traits::action_executor::{ActionParams, ExecutionContext};
use crate::traits::persistence::{NewStep, StepUpdate};

fn parse_duration(s: &str) -> std::result::Result<std::time::Duration, String> {
    if let Some(n) = s.strip_suffix("ms") {
        let ms = n
            .parse::<u64>()
            .map_err(|e| format!("invalid timeout '{s}': {e}"))?;
        return Ok(std::time::Duration::from_millis(ms));
    }
    if let Some(n) = s.strip_suffix('h') {
        let h = n
            .parse::<u64>()
            .map_err(|e| format!("invalid timeout '{s}': {e}"))?;
        return Ok(std::time::Duration::from_secs(h * 3600));
    }
    if let Some(n) = s.strip_suffix('m') {
        let m = n
            .parse::<u64>()
            .map_err(|e| format!("invalid timeout '{s}': {e}"))?;
        return Ok(std::time::Duration::from_secs(m * 60));
    }
    if let Some(n) = s.strip_suffix('s') {
        let sec = n
            .parse::<u64>()
            .map_err(|e| format!("invalid timeout '{s}': {e}"))?;
        return Ok(std::time::Duration::from_secs(sec));
    }
    let sec = s
        .parse::<u64>()
        .map_err(|e| format!("invalid timeout '{s}': {e}"))?;
    Ok(std::time::Duration::from_secs(sec))
}

pub fn execute_call(state: &mut ExecutionState, node: &CallNode, iteration: u32) -> Result<()> {
    // Call-level output overrides block-level; if neither is set, use None.
    let effective_output: Option<String> = match (&node.output, &state.block_output) {
        (Some(o), _) => Some(o.clone()),
        (None, Some(b)) => Some(b.clone()),
        (None, None) => None,
    };
    let effective_with: Vec<String> = if state.block_with.is_empty() {
        node.with.clone()
    } else if node.with.is_empty() {
        state.block_with.clone()
    } else {
        state
            .block_with
            .iter()
            .chain(node.with.iter())
            .cloned()
            .collect()
    };
    execute_call_inner(
        state,
        node,
        iteration,
        effective_output.as_deref(),
        &effective_with,
    )
}

fn execute_call_inner(
    state: &mut ExecutionState,
    node: &CallNode,
    iteration: u32,
    schema_name: Option<&str>,
    with_refs: &[String],
) -> Result<()> {
    let pos = state.position;
    state.position += 1;

    let step_key_check = node.agent.step_key();
    if should_skip(state, &step_key_check, iteration) {
        tracing::info!(
            "Skipping completed step '{}' (iteration {})",
            step_key_check,
            iteration
        );
        restore_step(state, &step_key_check, iteration);
        return Ok(());
    }

    let agent_label = node.agent.label();
    let step_key = node.agent.step_key();

    // Load output schema if specified
    let schema = schema_name
        .map(|name| resolve_schema(state, name))
        .transpose()?;

    // Retry loop
    let max_attempts = 1 + node.retries;
    let mut last_error = String::new();

    for attempt in 0..max_attempts {
        if attempt > 0 {
            emit_event(
                state,
                EngineEvent::StepRetrying {
                    step_name: agent_label.to_string(),
                    attempt,
                },
            );
        }

        // Insert step record as running
        let step_id = state
            .persistence
            .insert_step(NewStep {
                workflow_run_id: state.workflow_run_id.clone(),
                step_name: agent_label.to_string(),
                role: "actor".to_string(),
                can_commit: false,
                position: pos,
                iteration: iteration as i64,
                retry_count: Some(attempt as i64),
            })
            .map_err(|e| EngineError::Persistence(e.to_string()))?;

        emit_event(
            state,
            EngineEvent::StepStarted {
                step_name: agent_label.to_string(),
            },
        );

        // Build variable map and inputs for this attempt
        let inputs: HashMap<String, String> = {
            let var_map = build_variable_map(state);
            var_map
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect()
        };

        let effective_bot_name = node
            .bot_name
            .as_deref()
            .or(state.default_bot_name.as_deref())
            .map(String::from);

        let mut merged_plugin_dirs = state.worktree_ctx.extra_plugin_dirs.clone();
        for dir in &node.plugin_dirs {
            if !merged_plugin_dirs.contains(dir) {
                merged_plugin_dirs.push(dir.clone());
            }
        }

        let ectx = ExecutionContext {
            run_id: step_id.clone(),
            working_dir: std::path::PathBuf::from(&state.worktree_ctx.working_dir),
            repo_path: state.worktree_ctx.repo_path.clone(),
            step_timeout: state.exec_config.step_timeout,
            shutdown: state.exec_config.shutdown.clone(),
            model: state.model.clone(),
            bot_name: effective_bot_name,
            plugin_dirs: merged_plugin_dirs,
            workflow_name: state.workflow_name.clone(),
            worktree_id: state.worktree_ctx.worktree_id.clone(),
            parent_run_id: state.parent_run_id.clone(),
            step_id: step_id.clone(),
        };

        let params = ActionParams {
            name: agent_label.to_string(),
            inputs,
            retries_remaining: max_attempts - attempt - 1,
            retry_error: if attempt == 0 {
                None
            } else {
                Some(last_error.clone())
            },
            snippets: with_refs.to_vec(),
            dry_run: state.exec_config.dry_run,
            gate_feedback: state.last_gate_feedback.clone(),
            schema: schema.clone(),
        };

        // Per-step timeout: spawn a timer thread that cancels a child token after
        // the configured duration. Checked after dispatch to override the result.
        // The `timer_done` flag lets the timer exit early when the step completes
        // before the timeout fires, preventing thread leaks.
        let timer_done = Arc::new(AtomicBool::new(false));
        let step_token = node
            .timeout
            .as_deref()
            .map(|t| -> Result<_> {
                let duration = parse_duration(t).map_err(EngineError::Workflow)?;
                let tok = state.cancellation.child();
                let tok2 = tok.clone();
                let done = Arc::clone(&timer_done);
                std::thread::spawn(move || {
                    let start = std::time::Instant::now();
                    let poll_ms = std::time::Duration::from_millis(10);
                    while start.elapsed() < duration {
                        if done.load(Ordering::Relaxed) {
                            return;
                        }
                        std::thread::sleep(poll_ms.min(duration - start.elapsed()));
                    }
                    if !done.load(Ordering::Relaxed) {
                        tok2.cancel(CancellationReason::Timeout);
                    }
                });
                Ok(tok)
            })
            .transpose()?;

        // Record the active executor so cancel_run() can fire-and-forget executor.cancel().
        {
            let mut cur = state
                .current_execution_id
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *cur = Some((agent_label.to_string(), step_id.clone()));
        }
        // Clone the Arc before dispatch so we hold no borrow on `state` while
        // the executor runs.
        let registry = Arc::clone(&state.action_registry);
        let dispatch_result = registry.dispatch(&params.name, &ectx, &params);
        // Clear the active executor record and signal the timer thread to exit.
        {
            let mut cur = state
                .current_execution_id
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *cur = None;
        }
        timer_done.store(true, Ordering::Relaxed);

        // Timeout check: if the step token was cancelled while dispatch ran,
        // the step exceeded its DSL-level time limit.
        if let Some(ref tok) = step_token {
            if tok.is_cancelled() {
                tracing::warn!(
                    "Step '{}' timed out (timeout={:?})",
                    agent_label,
                    node.timeout,
                );
                state
                    .persistence
                    .update_step(
                        &step_id,
                        StepUpdate {
                            status: WorkflowStepStatus::TimedOut,
                            child_run_id: None,
                            result_text: Some(format!(
                                "timed out after {}",
                                node.timeout.as_deref().unwrap_or("?")
                            )),
                            context_out: None,
                            markers_out: None,
                            retry_count: Some(attempt as i64),
                            structured_output: None,
                            step_error: Some("step timed out".to_string()),
                        },
                    )
                    .map_err(|e| EngineError::Persistence(e.to_string()))?;
                return Err(EngineError::Cancelled(CancellationReason::Timeout));
            }
        }

        match dispatch_result {
            Ok(output) => {
                let markers_json = serde_json::to_string(&output.markers).unwrap_or_default();
                let context = output.context.clone().unwrap_or_default();

                tracing::info!(
                    "Step '{}' completed: cost=${:.4}, {} turns, markers={:?}",
                    agent_label,
                    output.cost_usd.unwrap_or(0.0),
                    output.num_turns.unwrap_or(0),
                    output.markers,
                );

                // Update step to completed
                state
                    .persistence
                    .update_step(
                        &step_id,
                        StepUpdate {
                            status: WorkflowStepStatus::Completed,
                            child_run_id: output.child_run_id.clone(),
                            result_text: output.result_text.clone(),
                            context_out: Some(context.clone()),
                            markers_out: Some(markers_json),
                            retry_count: Some(attempt as i64),
                            structured_output: output.structured_output.clone(),
                            step_error: None,
                        },
                    )
                    .map_err(|e| EngineError::Persistence(e.to_string()))?;

                emit_event(
                    state,
                    EngineEvent::StepCompleted {
                        step_name: agent_label.to_string(),
                        succeeded: true,
                    },
                );

                record_step_success(
                    state,
                    step_key.clone(),
                    agent_label,
                    output.result_text,
                    output.cost_usd,
                    output.num_turns,
                    output.duration_ms,
                    output.input_tokens,
                    output.output_tokens,
                    output.cache_read_input_tokens,
                    output.cache_creation_input_tokens,
                    output.markers,
                    context,
                    output.child_run_id,
                    iteration,
                    output.structured_output,
                    None,
                );

                return Ok(());
            }
            Err(EngineError::Cancelled(reason)) => {
                state
                    .persistence
                    .update_step(
                        &step_id,
                        StepUpdate {
                            status: WorkflowStepStatus::Failed,
                            child_run_id: None,
                            result_text: Some("executor shutdown requested".to_string()),
                            context_out: None,
                            markers_out: None,
                            retry_count: Some(attempt as i64),
                            structured_output: None,
                            step_error: None,
                        },
                    )
                    .map_err(|e| EngineError::Persistence(e.to_string()))?;
                return Err(EngineError::Cancelled(reason));
            }
            Err(e) => {
                let err_msg = e.to_string();
                tracing::warn!(
                    "Step '{}' (attempt {}/{}): {err_msg}",
                    agent_label,
                    attempt + 1,
                    max_attempts,
                );
                // Mark step failed
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
                            retry_count: Some(attempt as i64),
                            structured_output: None,
                            step_error: Some(err_msg.clone()),
                        },
                    )
                    .map_err(|persist_err| EngineError::Persistence(persist_err.to_string()))?;
                last_error = err_msg;
                continue;
            }
        }
    }

    handle_on_fail(
        state,
        step_key,
        agent_label,
        &node.on_fail,
        last_error,
        node.retries,
        iteration,
        max_attempts,
    )
}
