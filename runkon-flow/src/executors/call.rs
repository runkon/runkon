use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::cancellation_reason::CancellationReason;
use crate::dsl::CallNode;
use crate::engine::{emit_event, handle_on_fail, resolve_schema, ExecutionState};
use crate::engine_error::{EngineError, Result};
use crate::events::EngineEvent;
use crate::status::WorkflowStepStatus;
use crate::traits::persistence::StepUpdate;

use super::{build_action_params, p_err, record_dispatch_success};

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
    if super::skip_if_already_completed(state, &step_key_check, iteration, &step_key_check) {
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
        // Insert step record as running (also emits StepRetrying when attempt > 0)
        let step_id =
            super::begin_retry_attempt(state, agent_label, "actor", pos, iteration, attempt)?;

        emit_event(
            state,
            EngineEvent::StepStarted {
                step_name: agent_label.to_string(),
            },
        );

        // Build variable map and inputs for this attempt
        let inputs = super::build_inputs_map(state);

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

        let ectx =
            super::build_execution_context(state, &step_id, effective_bot_name, merged_plugin_dirs);

        let params = build_action_params(
            agent_label,
            inputs,
            with_refs.to_vec(),
            state.exec_config.dry_run,
            state.last_gate_feedback.clone(),
            schema.clone(),
            max_attempts - attempt - 1,
            if attempt == 0 {
                None
            } else {
                Some(last_error.clone())
            },
        );

        // Per-step timeout: spawn a timer thread that cancels a child token after
        // the configured duration. Checked after dispatch to override the result.
        // The `timer_done` flag lets the timer exit early when the step completes
        // before the timeout fires, preventing thread leaks.
        let timer_done = Arc::new(AtomicBool::new(false));
        let step_token = node
            .timeout
            .as_deref()
            .map(|t| -> Result<_> {
                let duration = crate::helpers::parse_duration(t).map_err(EngineError::Workflow)?;
                let tok = state.cancellation.child();
                let tok2 = tok.clone();
                let done = Arc::clone(&timer_done);
                std::thread::spawn(move || {
                    let start = std::time::Instant::now();
                    // 100 ms poll interval gives ~10× fewer wake-ups than the
                    // previous 10 ms while still keeping timeout precision well
                    // below typical step durations (seconds–minutes).
                    let poll = std::time::Duration::from_millis(100);
                    while start.elapsed() < duration {
                        if done.load(Ordering::Relaxed) {
                            return;
                        }
                        std::thread::sleep(poll.min(duration - start.elapsed()));
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
                            step_error: Some(format!(
                                "step '{}' timed out after {}",
                                agent_label,
                                node.timeout.as_deref().unwrap_or("?"),
                            )),
                        },
                    )
                    .map_err(p_err)?;
                return Err(EngineError::Cancelled(CancellationReason::Timeout));
            }
        }

        match dispatch_result {
            Ok(output) => {
                tracing::info!(
                    "Step '{}' completed: cost=${:.4}, {} turns, markers={:?}",
                    agent_label,
                    output.cost_usd.unwrap_or(0.0),
                    output.num_turns.unwrap_or(0),
                    output.markers,
                );
                record_dispatch_success(
                    state,
                    &step_id,
                    &step_key,
                    agent_label,
                    &output,
                    iteration,
                    attempt,
                    None,
                )?;
                emit_event(
                    state,
                    EngineEvent::StepCompleted {
                        step_name: agent_label.to_string(),
                        succeeded: true,
                    },
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
                    .map_err(p_err)?;
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
                state
                    .persistence
                    .update_step(&step_id, StepUpdate::failed(err_msg.clone(), attempt))
                    .map_err(p_err)?;
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
