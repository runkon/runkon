use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::cancellation_reason::CancellationReason;
use crate::dsl::CallNode;
use crate::engine::{emit_event, handle_on_fail, resolve_schema, ExecutionState};
use crate::engine_error::{EngineError, Result};
use crate::events::EngineEvent;
use crate::status::WorkflowStepStatus;
use crate::traits::persistence::StepUpdate;

use super::{build_action_params, record_dispatch_success};

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

        // Heartbeat keeper: polls every 500 ms and ticks last_heartbeat while the
        // executor blocks in registry.dispatch(). Without this, a long-running agent
        // leaves the heartbeat stale and the watchdog reaper incorrectly claims the
        // run as stuck — the same hazard fixed for parallel/foreach in #2731.
        let heartbeat_done = Arc::new(AtomicBool::new(false));
        {
            let done = Arc::clone(&heartbeat_done);
            let persistence = Arc::clone(&state.persistence);
            let run_id = state.workflow_run_id.clone();
            let last_hb = Arc::clone(&state.last_heartbeat_at);
            let cancellation = state.cancellation.clone();
            std::thread::spawn(move || {
                let poll = std::time::Duration::from_millis(500);
                loop {
                    std::thread::sleep(poll);
                    if done.load(Ordering::Relaxed) {
                        return;
                    }
                    let now_secs = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as i64;
                    let last = last_hb.load(Ordering::Relaxed);
                    if now_secs - last < 5 {
                        continue;
                    }
                    last_hb.store(now_secs, Ordering::Relaxed);
                    match persistence.is_run_cancelled(&run_id) {
                        Ok(true) => {
                            tracing::info!("run {run_id} cancelled externally");
                            cancellation.cancel(CancellationReason::UserRequested(None));
                            return;
                        }
                        Ok(false) => {}
                        Err(e) => {
                            tracing::warn!("cancellation check failed for {run_id}: {e}");
                        }
                    }
                    if let Err(e) = persistence.tick_heartbeat(&run_id) {
                        tracing::warn!("heartbeat tick failed for {run_id}: {e}");
                    }
                }
            });
        }

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
        // Clear the active executor record and signal the timer and heartbeat threads to exit.
        {
            let mut cur = state
                .current_execution_id
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *cur = None;
        }
        timer_done.store(true, Ordering::Relaxed);
        heartbeat_done.store(true, Ordering::Relaxed);

        // Timeout check: if the step token was cancelled while dispatch ran,
        // the step exceeded its DSL-level time limit.
        if let Some(ref tok) = step_token {
            if tok.is_cancelled() {
                tracing::warn!(
                    "Step '{}' timed out (timeout={:?})",
                    agent_label,
                    node.timeout,
                );
                let generation = state.expect_lease_generation();
                state.persistence.update_step(
                    &step_id,
                    StepUpdate {
                        generation,
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
                )?;
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
                let generation = state.expect_lease_generation();
                state.persistence.update_step(
                    &step_id,
                    StepUpdate {
                        generation,
                        status: WorkflowStepStatus::Failed,
                        child_run_id: None,
                        result_text: Some("executor shutdown requested".to_string()),
                        context_out: None,
                        markers_out: None,
                        retry_count: Some(attempt as i64),
                        structured_output: None,
                        step_error: None,
                    },
                )?;
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
                let generation = state.expect_lease_generation();
                state.persistence.update_step(
                    &step_id,
                    StepUpdate::failed(generation, err_msg.clone(), attempt),
                )?;
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::dsl::{AgentRef, CallNode};
    use crate::traits::action_executor::{ActionExecutor, ActionOutput, ActionParams};
    use crate::traits::persistence::WorkflowPersistence;

    use super::execute_call;

    /// Regression for the watchdog double-spawn bug: the heartbeat keeper thread
    /// must tick `last_heartbeat` while a long-running agent blocks inside
    /// `registry.dispatch()`. Prior to this fix, the heartbeat went stale during
    /// any multi-minute sequential call step and the watchdog reaper raced the
    /// engine, spawning a duplicate executor on the same step.
    #[test]
    fn call_step_ticks_heartbeat_during_dispatch() {
        struct SleepingExecutor;
        impl ActionExecutor for SleepingExecutor {
            fn name(&self) -> &str {
                "sleeping_exec"
            }
            fn execute(
                &self,
                _ectx: &crate::traits::action_executor::ExecutionContext,
                _params: &ActionParams,
            ) -> std::result::Result<ActionOutput, crate::engine_error::EngineError> {
                // Long enough for the 500 ms keeper poll to fire at least once.
                std::thread::sleep(std::time::Duration::from_millis(1300));
                Ok(ActionOutput {
                    cost_usd: Some(0.0),
                    ..Default::default()
                })
            }
        }

        let mut named: HashMap<String, Box<dyn ActionExecutor>> = HashMap::new();
        named.insert(
            "sleeping_exec".to_string(),
            Box::new(SleepingExecutor) as Box<dyn ActionExecutor>,
        );
        let registry = crate::traits::action_executor::ActionRegistry::new(named, None);

        let cp = Arc::new(crate::test_helpers::CountingPersistence::new());
        let run_id = cp
            .create_run(crate::traits::persistence::NewRun {
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
            .unwrap()
            .id;
        let cp_for_state: Arc<dyn WorkflowPersistence> = Arc::clone(&cp) as _;

        let mut state = crate::test_helpers::make_test_execution_state(cp_for_state, run_id);
        state.action_registry = Arc::new(registry);
        // Run was created with generation=0; use that so update_step generation check passes.
        state.lease_generation = Some(0);

        let node = CallNode {
            agent: AgentRef::Name("sleeping_exec".to_string()),
            output: None,
            with: vec![],
            retries: 0,
            on_fail: None,
            timeout: None,
            bot_name: None,
            plugin_dirs: vec![],
        };

        execute_call(&mut state, &node, 0).unwrap();

        // last_heartbeat_at starts at 0, so the first 500 ms keeper poll sees
        // now_secs - 0 >> 5 and fires immediately. Expect ≥1 tick.
        assert!(
            cp.tick_count() >= 1,
            "expected ≥1 heartbeat tick during call step dispatch, got {}; \
             without this fix the keeper thread is absent and the watchdog can \
             race the engine after >60 s of agent execution.",
            cp.tick_count()
        );
    }
}
