use std::sync::Arc;

use crate::cancellation_reason::CancellationReason;
use crate::dsl::ParallelNode;
use crate::engine::{record_step_success, resolve_schema, ExecutionState};
use crate::engine_error::{EngineError, Result};
use crate::status::WorkflowStepStatus;
use crate::traits::action_executor::{ActionOutput, ActionParams, ExecutionContext};
use crate::traits::persistence::StepUpdate;
use crate::types::{StepResult, StepSuccess};

pub fn execute_parallel(
    state: &mut ExecutionState,
    node: &ParallelNode,
    iteration: u32,
) -> Result<()> {
    let group_id = ulid::Ulid::new().to_string();
    let pos_base = state.position;

    tracing::info!(
        "parallel: spawning {} agents (fail_fast={}, min_success={:?})",
        node.calls.len(),
        node.fail_fast,
        node.min_success,
    );

    // Load block-level schema (if any)
    let block_schema = node
        .output
        .as_deref()
        .map(|name| resolve_schema(state, name))
        .transpose()?;

    struct ParallelCallResult {
        agent_name: String,
        step_id: String,
        agent_step_key: String,
        result: std::result::Result<ActionOutput, EngineError>,
        attempt: u32,
    }

    struct DispatchInput {
        step_id: String,
        agent_name: String,
        agent_step_key: String,
        ectx: ExecutionContext,
        params: ActionParams,
        retries: u32,
    }

    struct CallInput {
        idx: usize,
        agent_step_key: String,
        call_schema: Option<crate::output_schema::OutputSchema>,
        effective_with: Vec<String>,
        retries: u32,
    }

    let mut skipped_count = 0u32;
    let mut call_inputs: Vec<CallInput> = Vec::new();

    // First pass: skip any already-completed agents on resume
    for (i, agent_ref) in node.calls.iter().enumerate() {
        let pos = pos_base + i as i64;
        state.position = pos + 1;
        let agent_label = agent_ref.label();
        let agent_step_key = agent_ref.step_key();

        if super::skip_if_already_completed(state, &agent_step_key, iteration, agent_label) {
            skipped_count += 1;
            continue;
        }

        // Determine schema for this call: per-call override > block-level
        let call_schema = node
            .call_outputs
            .get(&i.to_string())
            .map(|name| resolve_schema(state, name))
            .transpose()?;
        let effective_schema = call_schema.as_ref().or(block_schema.as_ref()).cloned();

        // Combine block-level `with` + per-call `with` additions. Only clone when
        // there are per-call extras to avoid N × snippet_total allocations.
        let effective_with = if let Some(extra) = node.call_with.get(&i.to_string()) {
            let mut w = node.with.clone();
            w.extend(extra.iter().cloned());
            w
        } else {
            node.with.clone()
        };

        let retries = node.call_retries.get(&i.to_string()).copied().unwrap_or(0);
        call_inputs.push(CallInput {
            idx: i,
            agent_step_key: agent_step_key.clone(),
            call_schema: effective_schema,
            effective_with,
            retries,
        });
    }

    // Pre-dispatch pass: evaluate per-call `if` conditions, create step records, and build
    // the dispatch queue. All records are created before any thread is spawned so the DB
    // reflects the full parallel block immediately (important for UI and resume).
    let mut dispatch_queue: Vec<DispatchInput> = Vec::new();

    // Build the variable map once — state doesn't change between branches so there is
    // no need to re-serialize state.contexts for every parallel branch.
    let shared_inputs = super::build_inputs_map(state);

    // Parallel-scope token: child of the run root. Cancelling it signals running branches
    // to exit early when fail_fast fires.
    let scope_token = state.cancellation.child();

    for call_input in call_inputs {
        let CallInput {
            idx: i,
            agent_step_key,
            call_schema,
            effective_with,
            retries,
        } = call_input;
        let pos = pos_base + i as i64;
        let agent_ref = &node.calls[i];
        let agent_label = agent_ref.label();

        // Check per-call `if` condition
        if let Some((cond_step, cond_marker)) = node.call_if.get(&i.to_string()) {
            let has_marker = state
                .step_results
                .get(cond_step)
                .map(|r| r.markers.iter().any(|m| m == cond_marker))
                .unwrap_or(false);
            if !has_marker {
                tracing::info!(
                    "parallel: skipping '{}' (if={}.{} not satisfied)",
                    agent_label,
                    cond_step,
                    cond_marker
                );
                super::insert_step_with_status(
                    state,
                    agent_label,
                    "actor",
                    pos,
                    iteration,
                    None,
                    WorkflowStepStatus::Skipped,
                    Some(format!("skipped: {cond_step}.{cond_marker} not emitted")),
                )?;
                skipped_count += 1;
                continue;
            }
        }

        let step_id =
            super::insert_step_record(state, agent_label, "actor", pos, iteration, Some(0))?;

        let inputs = Arc::clone(&shared_inputs);

        let ectx = super::build_execution_context(
            state,
            &step_id,
            state.default_bot_name.clone(),
            state.worktree_ctx.extra_plugin_dirs.clone(),
        );

        let params = super::build_action_params(
            agent_label,
            inputs,
            effective_with,
            state.exec_config.dry_run,
            state.last_gate_feedback.clone(),
            call_schema,
            retries,
            None,
        );

        dispatch_queue.push(DispatchInput {
            step_id,
            agent_name: agent_label.to_string(),
            agent_step_key,
            ectx,
            params,
            retries,
        });
    }

    // Spawn all agents concurrently. Each thread checks the scope token before dispatching
    // so that a fail_fast cancellation from a result that arrives while threads are still
    // starting will prevent those threads from doing any work.
    let (completion_tx, completion_rx) = std::sync::mpsc::channel::<(
        String,
        String,
        String,
        std::result::Result<ActionOutput, EngineError>,
        u32,
    )>();

    for dispatch_input in dispatch_queue {
        let tx = completion_tx.clone();
        let registry = Arc::clone(&state.action_registry);
        let scope = scope_token.clone();
        std::thread::spawn(move || {
            let max_attempts = 1 + dispatch_input.retries;
            let mut last_error = String::new();
            let mut params = dispatch_input.params;
            let mut final_attempt = 0u32;
            let mut result: std::result::Result<ActionOutput, EngineError> =
                Err(EngineError::Workflow("no attempts made".into()));

            for attempt in 0..max_attempts {
                if scope.is_cancelled() {
                    result = Err(EngineError::Cancelled(CancellationReason::FailFast));
                    break;
                }
                params.retries_remaining = max_attempts - attempt - 1;
                params.retry_error = if attempt == 0 {
                    None
                } else {
                    Some(last_error.clone())
                };
                final_attempt = attempt;

                result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    registry.dispatch(&params.name, &dispatch_input.ectx, &params)
                }))
                .unwrap_or_else(|payload| {
                    let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                        format!("executor '{}' panicked: {s}", params.name)
                    } else if let Some(s) = payload.downcast_ref::<String>() {
                        format!("executor '{}' panicked: {s}", params.name)
                    } else {
                        format!("executor '{}' panicked", params.name)
                    };
                    Err(EngineError::Workflow(msg))
                });

                match &result {
                    Ok(_) => break,
                    Err(EngineError::Cancelled(_)) => break,
                    Err(e) => {
                        last_error = e.to_string();
                    }
                }
            }

            if let Err(e) = tx.send((
                dispatch_input.step_id,
                dispatch_input.agent_name,
                dispatch_input.agent_step_key,
                result,
                final_attempt,
            )) {
                tracing::warn!("parallel: result channel broken (receiver dropped): {}", e);
            }
        });
    }
    // Drop the sender so the receiver knows when all threads have completed.
    drop(completion_tx);

    // Collect results as threads complete, triggering fail_fast cancellation as needed.
    // The timeout-based recv lets us tick the heartbeat and poll for cross-process
    // cancellation while waiting on long-running agents — without it the engine
    // sits silent here for the full duration of the slowest branch and the
    // watchdog reaper races us after >60 s. #2731.
    let mut results: Vec<ParallelCallResult> = Vec::new();
    loop {
        match completion_rx.recv_timeout(std::time::Duration::from_millis(500)) {
            Ok((step_id, agent_name, agent_step_key, result, attempt)) => {
                let failed = result.is_err();
                results.push(ParallelCallResult {
                    agent_name,
                    step_id,
                    agent_step_key,
                    result,
                    attempt,
                });
                if failed && node.fail_fast {
                    scope_token.cancel(CancellationReason::FailFast);
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                // Tick heartbeat + check for external cancel. Best-effort: on
                // external cancel, propagate to scope_token so worker threads
                // see it via their pre-dispatch check; keep draining the rest
                // of the channel so in-flight workers' results land in `results`.
                if state.tick_heartbeat_throttled().is_err() {
                    scope_token.cancel(CancellationReason::UserRequested(None));
                }
            }
        }
    }

    // Process results
    let mut merged_markers: Vec<String> = Vec::new();
    let mut successes = 0u32;
    let mut failures = 0u32;

    let results_count = results.len();
    for pr in results {
        match pr.result {
            Ok(output) => {
                let markers_json = crate::helpers::serialize_or_empty_array(
                    &output.markers,
                    &format!("parallel: '{}'", pr.agent_name),
                );
                let context = output.context.clone().unwrap_or_default();

                super::persist_completed_step(
                    state,
                    &pr.step_id,
                    output.child_run_id.clone(),
                    output.result_text.clone(),
                    Some(context.clone()),
                    Some(markers_json),
                    pr.attempt,
                    output.structured_output.clone(),
                )?;

                tracing::info!(
                    "parallel: '{}' completed (cost=${:.4})",
                    pr.agent_name,
                    output.cost_usd.unwrap_or(0.0),
                );

                successes += 1;
                merged_markers.extend(output.markers.iter().cloned());

                record_step_success(
                    state,
                    pr.agent_step_key.clone(),
                    StepSuccess::from_action_output(
                        &output,
                        pr.agent_name.clone(),
                        context,
                        iteration,
                        None,
                    ),
                );
            }
            Err(e) => {
                tracing::warn!("parallel: '{}' failed: {e}", pr.agent_name);
                let generation = state.expect_lease_generation();
                state.persistence.update_step(
                    &pr.step_id,
                    StepUpdate::failed(generation, e.to_string(), pr.attempt),
                )?;
                failures += 1;
            }
        }
    }

    // Apply min_success policy (skipped-on-resume agents count as successes)
    let effective_successes = successes + skipped_count;
    let total_agents = results_count as u32 + skipped_count;
    let min_required = node.min_success.unwrap_or(total_agents);
    tracing::info!(
        "parallel: {successes} succeeded, {failures} failed, {skipped_count} skipped out of {total_agents} agents",
    );
    if effective_successes < min_required {
        tracing::warn!(
            "parallel: only {}/{} succeeded (min_success={})",
            effective_successes,
            total_agents,
            min_required
        );
        state.all_succeeded = false;
    }

    // Store merged markers as a synthetic result
    let synthetic_result = StepResult {
        step_name: format!("parallel:{}", group_id),
        status: if effective_successes >= min_required {
            WorkflowStepStatus::Completed
        } else {
            WorkflowStepStatus::Failed
        },
        result_text: None,
        cost_usd: None,
        num_turns: None,
        duration_ms: None,
        markers: merged_markers,
        context: String::new(),
        child_run_id: None,
        structured_output: None,
        output_file: None,
    };
    state
        .step_results
        .insert(format!("parallel:{}", group_id), synthetic_result);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::{AgentRef, ParallelNode};
    use crate::engine::{ExecutionState, WorktreeContext};
    use crate::engine_error::EngineError;
    use crate::persistence_memory::InMemoryWorkflowPersistence;
    use crate::status::WorkflowStepStatus;
    use crate::traits::action_executor::{ActionExecutor, ActionOutput, ActionParams};
    use crate::traits::item_provider::ItemProviderRegistry;
    use crate::traits::persistence::WorkflowPersistence;
    use crate::traits::script_env_provider::NoOpScriptEnvProvider;
    use crate::types::WorkflowExecConfig;
    use std::collections::HashMap;
    use std::sync::Arc;

    struct MarkersExecutor {
        markers: Vec<String>,
        context: String,
    }

    impl ActionExecutor for MarkersExecutor {
        fn name(&self) -> &str {
            "markers_exec"
        }
        fn execute(
            &self,
            _ectx: &crate::traits::action_executor::ExecutionContext,
            _params: &ActionParams,
        ) -> Result<ActionOutput, EngineError> {
            Ok(ActionOutput {
                markers: self.markers.clone(),
                context: Some(self.context.clone()),
                cost_usd: Some(0.01),
                num_turns: Some(2),
                ..Default::default()
            })
        }
    }

    fn make_persistence_with_run() -> (Arc<InMemoryWorkflowPersistence>, String) {
        let p = Arc::new(InMemoryWorkflowPersistence::new());
        let run = p
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
            .unwrap();
        (p, run.id)
    }

    fn make_state(
        persistence: Arc<InMemoryWorkflowPersistence>,
        run_id: String,
        registry: crate::traits::action_executor::ActionRegistry,
    ) -> ExecutionState {
        ExecutionState {
            persistence,
            action_registry: Arc::new(registry),
            script_env_provider: Arc::new(NoOpScriptEnvProvider),
            workflow_run_id: run_id,
            workflow_name: "wf".to_string(),
            worktree_ctx: WorktreeContext {
                worktree_id: None,
                working_dir: String::new(),
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
            owner_token: None,
            // Run was created with generation=0; use that so update_step check passes.
            lease_generation: Some(0),
        }
    }

    /// Verifies the ActionOutput dispatch path: markers, context, and metrics from
    /// the executor are correctly extracted and stored in the step record and state.
    #[test]
    fn parallel_actionoutput_dispatch_path_records_markers_and_context() {
        let mut named = HashMap::new();
        named.insert(
            "markers_exec".to_string(),
            Box::new(MarkersExecutor {
                markers: vec!["m1".to_string(), "m2".to_string()],
                context: "step context".to_string(),
            }) as Box<dyn ActionExecutor>,
        );
        let registry = crate::traits::action_executor::ActionRegistry::new(named, None);

        let (persistence, run_id) = make_persistence_with_run();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone(), registry);

        let node = ParallelNode {
            fail_fast: false,
            min_success: None,
            calls: vec![AgentRef::Name("markers_exec".to_string())],
            output: None,
            call_outputs: HashMap::new(),
            with: vec![],
            call_with: HashMap::new(),
            call_if: HashMap::new(),
            call_retries: HashMap::new(),
        };

        execute_parallel(&mut state, &node, 0).unwrap();

        // The step record in the DB should be Completed with correct markers.
        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1, "expected one step record");
        let step = &steps[0];
        assert_eq!(
            step.status,
            WorkflowStepStatus::Completed,
            "step should be Completed; got {:?}",
            step.status
        );
        let markers: Vec<String> = step
            .markers_out
            .as_deref()
            .and_then(|m| serde_json::from_str(m).ok())
            .unwrap_or_default();
        assert_eq!(
            markers,
            vec!["m1", "m2"],
            "markers should match executor output"
        );
        assert_eq!(
            step.context_out.as_deref(),
            Some("step context"),
            "context should match executor output"
        );

        // The context entry should be pushed to state.contexts.
        assert!(
            state.contexts.iter().any(|c| c.context == "step context"),
            "executor context should be in state.contexts"
        );

        // Metrics should be accumulated.
        assert!(
            state.total_cost > 0.0,
            "cost should be accumulated from ActionOutput"
        );
    }

    /// Verifies that a panicking executor is caught by `catch_unwind` and recorded as a
    /// Failed step rather than crashing the whole process.
    #[test]
    fn parallel_panicking_executor_is_caught_and_step_is_failed() {
        struct PanicExec;
        impl ActionExecutor for PanicExec {
            fn name(&self) -> &str {
                "panic_exec"
            }
            fn execute(
                &self,
                _ectx: &crate::traits::action_executor::ExecutionContext,
                _params: &ActionParams,
            ) -> Result<ActionOutput, EngineError> {
                panic!("deliberate panic in test executor");
            }
        }

        let mut named = HashMap::new();
        named.insert(
            "panic_exec".to_string(),
            Box::new(PanicExec) as Box<dyn ActionExecutor>,
        );
        let registry = crate::traits::action_executor::ActionRegistry::new(named, None);

        let (persistence, run_id) = make_persistence_with_run();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone(), registry);

        let node = ParallelNode {
            fail_fast: false,
            min_success: None,
            calls: vec![AgentRef::Name("panic_exec".to_string())],
            output: None,
            call_outputs: HashMap::new(),
            with: vec![],
            call_with: HashMap::new(),
            call_if: HashMap::new(),
            call_retries: HashMap::new(),
        };

        // execute_parallel should succeed (the panic is caught internally).
        execute_parallel(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1, "expected one step record");
        let step = &steps[0];
        assert_eq!(
            step.status,
            WorkflowStepStatus::Failed,
            "panicking executor should produce a Failed step; got {:?}",
            step.status
        );
        let error_msg = step.step_error.as_deref().unwrap_or("");
        assert!(
            error_msg.contains("panic_exec"),
            "step_error should name the executor; got: {error_msg:?}"
        );
        assert!(
            error_msg.contains("deliberate panic in test executor"),
            "step_error should include the panic payload; got: {error_msg:?}"
        );
    }

    /// Verifies that a panicking executor with a `String` payload is caught and the
    /// message is surfaced in the step error.
    #[test]
    fn parallel_panicking_executor_string_payload_is_surfaced() {
        struct PanicStringExec;
        impl ActionExecutor for PanicStringExec {
            fn name(&self) -> &str {
                "panic_string_exec"
            }
            fn execute(
                &self,
                _ectx: &crate::traits::action_executor::ExecutionContext,
                _params: &ActionParams,
            ) -> Result<ActionOutput, EngineError> {
                panic!("{}", "string payload panic".to_string())
            }
        }

        let mut named = HashMap::new();
        named.insert(
            "panic_string_exec".to_string(),
            Box::new(PanicStringExec) as Box<dyn ActionExecutor>,
        );
        let registry = crate::traits::action_executor::ActionRegistry::new(named, None);

        let (persistence, run_id) = make_persistence_with_run();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone(), registry);

        let node = ParallelNode {
            fail_fast: false,
            min_success: None,
            calls: vec![AgentRef::Name("panic_string_exec".to_string())],
            output: None,
            call_outputs: HashMap::new(),
            with: vec![],
            call_with: HashMap::new(),
            call_if: HashMap::new(),
            call_retries: HashMap::new(),
        };

        execute_parallel(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        let error_msg = steps[0].step_error.as_deref().unwrap_or("");
        assert!(
            error_msg.contains("panic_string_exec"),
            "step_error should name the executor; got: {error_msg:?}"
        );
        assert!(
            error_msg.contains("string payload panic"),
            "step_error should include the String panic payload; got: {error_msg:?}"
        );
    }

    /// Verifies that a panicking executor with an unknown payload type (neither `&str`
    /// nor `String`) falls back to a generic panic message.
    #[test]
    fn parallel_panicking_executor_unknown_payload_fallback() {
        struct PanicUnknownExec;
        impl ActionExecutor for PanicUnknownExec {
            fn name(&self) -> &str {
                "panic_unknown_exec"
            }
            fn execute(
                &self,
                _ectx: &crate::traits::action_executor::ExecutionContext,
                _params: &ActionParams,
            ) -> Result<ActionOutput, EngineError> {
                std::panic::panic_any(42i32)
            }
        }

        let mut named = HashMap::new();
        named.insert(
            "panic_unknown_exec".to_string(),
            Box::new(PanicUnknownExec) as Box<dyn ActionExecutor>,
        );
        let registry = crate::traits::action_executor::ActionRegistry::new(named, None);

        let (persistence, run_id) = make_persistence_with_run();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone(), registry);

        let node = ParallelNode {
            fail_fast: false,
            min_success: None,
            calls: vec![AgentRef::Name("panic_unknown_exec".to_string())],
            output: None,
            call_outputs: HashMap::new(),
            with: vec![],
            call_with: HashMap::new(),
            call_if: HashMap::new(),
            call_retries: HashMap::new(),
        };

        execute_parallel(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        let error_msg = steps[0].step_error.as_deref().unwrap_or("");
        assert!(
            error_msg.contains("panic_unknown_exec"),
            "step_error should name the executor; got: {error_msg:?}"
        );
        // Unknown payload should produce the fallback message without a payload description.
        assert!(
            !error_msg.contains("42"),
            "step_error should NOT contain the unknown payload value; got: {error_msg:?}"
        );
    }

    /// Verifies that fail_fast marks the workflow as not-all-succeeded after the first failure.
    ///
    /// With true parallel execution all branches are spawned before any result is processed.
    /// The scope token is cancelled only when the first failure result is dequeued by the
    /// main thread; branches that already called `dispatch()` before the cancel fires will
    /// complete normally (Ok or Err depending on the executor). The exact count of Failed
    /// steps is therefore non-deterministic: it is at least 1 (the failing branch) but may
    /// be higher if racing branches also see the cancellation check. The meaningful invariant
    /// is that `all_succeeded` becomes false and at least one step is recorded as Failed.
    #[test]
    fn parallel_fail_fast_stops_after_first_failure() {
        struct FailExec;
        impl ActionExecutor for FailExec {
            fn name(&self) -> &str {
                "fail_exec"
            }
            fn execute(
                &self,
                _ectx: &crate::traits::action_executor::ExecutionContext,
                _params: &ActionParams,
            ) -> Result<ActionOutput, EngineError> {
                Err(EngineError::Workflow("intentional failure".to_string()))
            }
        }

        let mut named = HashMap::new();
        named.insert(
            "fail_exec".to_string(),
            Box::new(FailExec) as Box<dyn ActionExecutor>,
        );
        named.insert(
            "markers_exec".to_string(),
            Box::new(MarkersExecutor {
                markers: vec!["ok".to_string()],
                context: String::new(),
            }) as Box<dyn ActionExecutor>,
        );
        let registry = crate::traits::action_executor::ActionRegistry::new(named, None);

        let (persistence, run_id) = make_persistence_with_run();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone(), registry);

        let node = ParallelNode {
            fail_fast: true,
            min_success: None,
            calls: vec![
                AgentRef::Name("fail_exec".to_string()),
                AgentRef::Name("markers_exec".to_string()),
                AgentRef::Name("markers_exec".to_string()),
            ],
            output: None,
            call_outputs: HashMap::new(),
            with: vec![],
            call_with: HashMap::new(),
            call_if: HashMap::new(),
            call_retries: HashMap::new(),
        };

        execute_parallel(&mut state, &node, 0).ok();

        let steps = persistence.get_steps(&run_id).unwrap();
        let failed = steps
            .iter()
            .filter(|s| s.status == WorkflowStepStatus::Failed)
            .count();
        assert!(
            failed >= 1,
            "at least one branch should be Failed; steps: {:?}",
            steps
        );
        // The overall workflow should be marked as not all-succeeded
        assert!(
            !state.all_succeeded,
            "all_succeeded should be false when fail_fast fires"
        );
    }

    /// Regression for #2731: the parallel wait loop must keep `tick_heartbeat`
    /// firing while children are running. Prior to the fix, the wait loop was
    /// `for ... in completion_rx { ... }` — blocking on the receiver for the
    /// whole duration of the slowest branch — so `last_heartbeat` went stale
    /// and the watchdog reaper claimed the run after >60 s, double-running it.
    #[test]
    fn parallel_wait_loop_ticks_heartbeat_during_long_branches() {
        struct SleepingExecutor;
        impl ActionExecutor for SleepingExecutor {
            fn name(&self) -> &str {
                "sleeping_exec"
            }
            fn execute(
                &self,
                _ectx: &crate::traits::action_executor::ExecutionContext,
                _params: &ActionParams,
            ) -> std::result::Result<ActionOutput, EngineError> {
                // Long enough to trigger several recv_timeout (500 ms) iterations
                // in the wait loop so the heartbeat tick has a chance to fire.
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

        let node = ParallelNode {
            fail_fast: false,
            min_success: None,
            calls: vec![AgentRef::Name("sleeping_exec".to_string())],
            output: None,
            call_outputs: HashMap::new(),
            with: vec![],
            call_with: HashMap::new(),
            call_if: HashMap::new(),
            call_retries: HashMap::new(),
        };

        execute_parallel(&mut state, &node, 0).unwrap();

        // last_heartbeat_at starts at 0 → first recv_timeout iteration's
        // tick_heartbeat_throttled() fires immediately. With a 1300 ms sleep
        // and 500 ms recv_timeout, expect at least one Timeout iteration → ≥1 tick.
        assert!(
            cp.tick_count() >= 1,
            "expected ≥1 heartbeat tick during parallel wait loop, got {}; \
             without #2731 fix this would be 0 because the receiver blocks \
             for the whole duration of the slowest branch.",
            cp.tick_count()
        );
    }

    /// Verifies that a failing parallel branch with `retries = 1` is dispatched twice
    /// (first attempt + one retry) and ultimately succeeds when the second attempt succeeds.
    #[test]
    fn parallel_retries_on_failed_branch_succeeds_on_second_attempt() {
        use std::sync::atomic::{AtomicU32, Ordering};

        struct FailOnceThenSucceed {
            call_count: Arc<AtomicU32>,
        }
        impl ActionExecutor for FailOnceThenSucceed {
            fn name(&self) -> &str {
                "fail_once"
            }
            fn execute(
                &self,
                _ectx: &crate::traits::action_executor::ExecutionContext,
                _params: &ActionParams,
            ) -> Result<ActionOutput, EngineError> {
                let n = self.call_count.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    Err(EngineError::Workflow("first attempt fails".to_string()))
                } else {
                    Ok(ActionOutput {
                        markers: vec!["retried_ok".to_string()],
                        ..Default::default()
                    })
                }
            }
        }

        let call_count = Arc::new(AtomicU32::new(0));
        let mut named = HashMap::new();
        named.insert(
            "fail_once".to_string(),
            Box::new(FailOnceThenSucceed {
                call_count: Arc::clone(&call_count),
            }) as Box<dyn ActionExecutor>,
        );
        let registry = crate::traits::action_executor::ActionRegistry::new(named, None);

        let (persistence, run_id) = make_persistence_with_run();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone(), registry);

        let mut call_retries = HashMap::new();
        call_retries.insert("0".to_string(), 1u32);

        let node = ParallelNode {
            fail_fast: false,
            min_success: None,
            calls: vec![AgentRef::Name("fail_once".to_string())],
            output: None,
            call_outputs: HashMap::new(),
            with: vec![],
            call_with: HashMap::new(),
            call_if: HashMap::new(),
            call_retries,
        };

        execute_parallel(&mut state, &node, 0).unwrap();

        // The executor should have been called exactly twice (attempt 0 + retry 1).
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            2,
            "executor should be dispatched twice (initial + 1 retry)"
        );

        // The step should be Completed, not Failed.
        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(
            steps[0].status,
            WorkflowStepStatus::Completed,
            "step should be Completed after successful retry; got {:?}",
            steps[0].status
        );
    }

    /// Verifies that a branch with `retries = 1` that always fails is marked Failed
    /// after exactly two dispatch attempts (initial + 1 retry).
    #[test]
    fn parallel_retries_exhausted_marks_step_failed() {
        use std::sync::atomic::{AtomicU32, Ordering};

        struct AlwaysFail {
            call_count: Arc<AtomicU32>,
        }
        impl ActionExecutor for AlwaysFail {
            fn name(&self) -> &str {
                "always_fail"
            }
            fn execute(
                &self,
                _ectx: &crate::traits::action_executor::ExecutionContext,
                _params: &ActionParams,
            ) -> Result<ActionOutput, EngineError> {
                self.call_count.fetch_add(1, Ordering::SeqCst);
                Err(EngineError::Workflow("always fails".to_string()))
            }
        }

        let call_count = Arc::new(AtomicU32::new(0));
        let mut named = HashMap::new();
        named.insert(
            "always_fail".to_string(),
            Box::new(AlwaysFail {
                call_count: Arc::clone(&call_count),
            }) as Box<dyn ActionExecutor>,
        );
        let registry = crate::traits::action_executor::ActionRegistry::new(named, None);

        let (persistence, run_id) = make_persistence_with_run();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone(), registry);

        let mut call_retries = HashMap::new();
        call_retries.insert("0".to_string(), 1u32);

        let node = ParallelNode {
            fail_fast: false,
            min_success: None,
            calls: vec![AgentRef::Name("always_fail".to_string())],
            output: None,
            call_outputs: HashMap::new(),
            with: vec![],
            call_with: HashMap::new(),
            call_if: HashMap::new(),
            call_retries,
        };

        execute_parallel(&mut state, &node, 0).unwrap();

        // Should be dispatched retries+1 = 2 times total.
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            2,
            "executor should be dispatched twice (initial + 1 retry) before giving up"
        );

        // The step should be Failed after exhausting all retries.
        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(
            steps[0].status,
            WorkflowStepStatus::Failed,
            "step should be Failed after all retries exhausted; got {:?}",
            steps[0].status
        );
    }
}
