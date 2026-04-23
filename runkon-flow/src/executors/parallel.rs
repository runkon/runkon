use std::sync::Arc;

use crate::cancellation_reason::CancellationReason;
use crate::dsl::ParallelNode;
use crate::engine::{resolve_schema, restore_step, should_skip, ExecutionState};
use crate::engine_error::{EngineError, Result};
use crate::status::WorkflowStepStatus;
use crate::traits::action_executor::{ActionOutput, ActionParams, ExecutionContext};
use crate::traits::persistence::{NewStep, StepUpdate};
use crate::types::{ContextEntry, StepResult};

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
        result: std::result::Result<ActionOutput, EngineError>,
        attempt: u32,
    }

    let mut skipped_count = 0u32;
    let mut call_inputs: Vec<(
        usize,
        String,
        Option<crate::output_schema::OutputSchema>,
        Vec<String>,
    )> = Vec::new();

    // First pass: skip any already-completed agents on resume
    for (i, agent_ref) in node.calls.iter().enumerate() {
        let pos = pos_base + i as i64;
        state.position = pos + 1;
        let agent_label = agent_ref.label();
        let agent_step_key = agent_ref.step_key();

        if should_skip(state, &agent_step_key, iteration) {
            tracing::info!("parallel: skipping completed agent '{}'", agent_label);
            restore_step(state, &agent_step_key, iteration);
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

        // Combine block-level `with` + per-call `with` additions
        let mut effective_with = node.with.clone();
        if let Some(extra) = node.call_with.get(&i.to_string()) {
            effective_with.extend(extra.iter().cloned());
        }

        call_inputs.push((i, agent_step_key.clone(), effective_schema, effective_with));
    }

    // Second pass: execute each non-skipped agent synchronously via action registry
    // Note: in the full implementation these would be parallel; here we serialize for simplicity
    // The conductor-core parallel executor handles true parallelism via headless subprocesses.
    let mut results: Vec<ParallelCallResult> = Vec::new();
    let mut merged_markers: Vec<String> = Vec::new();
    let mut successes = 0u32;
    let mut failures = 0u32;

    // Parallel-scope token: child of the run root. Cancelling it prevents later branches
    // from executing when fail_fast fires.
    let scope_token = state.cancellation.child();

    for (i, _agent_step_key, call_schema, effective_with) in call_inputs {
        // Check scope token before dispatching each branch (fail_fast from a prior branch).
        if scope_token.is_cancelled() {
            tracing::info!(
                "parallel: scope token cancelled (fail_fast), skipping remaining branches"
            );
            break;
        }

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
                let step_id = state
                    .persistence
                    .insert_step(NewStep {
                        workflow_run_id: state.workflow_run_id.clone(),
                        step_name: agent_label.to_string(),
                        role: "actor".to_string(),
                        can_commit: false,
                        position: pos,
                        iteration: iteration as i64,
                        retry_count: None,
                    })
                    .map_err(|e| EngineError::Persistence(e.to_string()))?;
                state
                    .persistence
                    .update_step(
                        &step_id,
                        StepUpdate {
                            status: WorkflowStepStatus::Skipped,
                            child_run_id: None,
                            result_text: Some(format!(
                                "skipped: {cond_step}.{cond_marker} not emitted"
                            )),
                            context_out: None,
                            markers_out: None,
                            retry_count: None,
                            structured_output: None,
                            step_error: None,
                        },
                    )
                    .map_err(|e| EngineError::Persistence(e.to_string()))?;
                skipped_count += 1;
                continue;
            }
        }

        let step_id = state
            .persistence
            .insert_step(NewStep {
                workflow_run_id: state.workflow_run_id.clone(),
                step_name: agent_label.to_string(),
                role: "actor".to_string(),
                can_commit: false,
                position: pos,
                iteration: iteration as i64,
                retry_count: Some(0),
            })
            .map_err(|e| EngineError::Persistence(e.to_string()))?;

        let inputs: std::collections::HashMap<String, String> = {
            let var_map = crate::prompt_builder::build_variable_map(state);
            var_map
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect()
        };

        let ectx = ExecutionContext {
            run_id: step_id.clone(),
            working_dir: std::path::PathBuf::from(&state.worktree_ctx.working_dir),
            repo_path: state.worktree_ctx.repo_path.clone(),
            step_timeout: state.exec_config.step_timeout,
            shutdown: state.exec_config.shutdown.clone(),
            model: state.model.clone(),
            bot_name: state.default_bot_name.clone(),
            plugin_dirs: state.worktree_ctx.extra_plugin_dirs.clone(),
            workflow_name: state.workflow_name.clone(),
            worktree_id: state.worktree_ctx.worktree_id.clone(),
            parent_run_id: state.parent_run_id.clone(),
            step_id: step_id.clone(),
        };

        let params = ActionParams {
            name: agent_label.to_string(),
            inputs,
            retries_remaining: 0,
            retry_error: None,
            snippets: effective_with,
            dry_run: state.exec_config.dry_run,
            gate_feedback: state.last_gate_feedback.clone(),
            schema: call_schema.clone(),
        };

        let registry = Arc::clone(&state.action_registry);
        let result = registry.dispatch(&params.name, &ectx, &params);

        // If fail_fast and this branch failed, cancel the scope token to stop remaining branches.
        let failed = result.is_err();
        results.push(ParallelCallResult {
            agent_name: agent_label.to_string(),
            step_id,
            result,
            attempt: 0,
        });
        if failed && node.fail_fast {
            scope_token.cancel(CancellationReason::FailFast);
        }
    }

    // Process results
    for pr in &results {
        match &pr.result {
            Ok(output) => {
                let markers_json = serde_json::to_string(&output.markers).unwrap_or_default();
                let context = output.context.clone().unwrap_or_default();

                state
                    .persistence
                    .update_step(
                        &pr.step_id,
                        StepUpdate {
                            status: WorkflowStepStatus::Completed,
                            child_run_id: output.child_run_id.clone(),
                            result_text: output.result_text.clone(),
                            context_out: Some(context.clone()),
                            markers_out: Some(markers_json),
                            retry_count: Some(pr.attempt as i64),
                            structured_output: output.structured_output.clone(),
                            step_error: None,
                        },
                    )
                    .map_err(|e| EngineError::Persistence(e.to_string()))?;

                tracing::info!(
                    "parallel: '{}' completed (cost=${:.4})",
                    pr.agent_name,
                    output.cost_usd.unwrap_or(0.0),
                );

                successes += 1;
                merged_markers.extend(output.markers.iter().cloned());

                // Push parallel agent context
                state.contexts.push(ContextEntry {
                    step: pr.agent_name.clone(),
                    iteration,
                    context: context.clone(),
                    markers: output.markers.clone(),
                    structured_output: output.structured_output.clone(),
                    output_file: None,
                });

                state.accumulate_metrics(
                    output.cost_usd,
                    output.num_turns,
                    output.duration_ms,
                    output.input_tokens,
                    output.output_tokens,
                    output.cache_read_input_tokens,
                    output.cache_creation_input_tokens,
                );

                if let Err(e) = state.flush_metrics() {
                    tracing::warn!("Failed to flush mid-run metrics after parallel agent: {e}");
                }
            }
            Err(e) => {
                tracing::warn!("parallel: '{}' failed: {e}", pr.agent_name);
                state
                    .persistence
                    .update_step(
                        &pr.step_id,
                        StepUpdate {
                            status: WorkflowStepStatus::Failed,
                            child_run_id: None,
                            result_text: Some(e.to_string()),
                            context_out: None,
                            markers_out: None,
                            retry_count: Some(pr.attempt as i64),
                            structured_output: None,
                            step_error: Some(e.to_string()),
                        },
                    )
                    .map_err(|e2| EngineError::Persistence(e2.to_string()))?;
                failures += 1;
            }
        }
    }

    // Apply min_success policy (skipped-on-resume agents count as successes)
    let effective_successes = successes + skipped_count;
    let total_agents = results.len() as u32 + skipped_count;
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
                worktree_slug: String::new(),
                repo_path: String::new(),
                ticket_id: None,
                repo_id: None,
                conductor_bin_dir: None,
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

    /// Verifies that fail_fast cancels remaining branches after the first failure.
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
        };

        execute_parallel(&mut state, &node, 0).ok();

        let steps = persistence.get_steps(&run_id).unwrap();
        let failed = steps
            .iter()
            .filter(|s| s.status == WorkflowStepStatus::Failed)
            .count();
        assert_eq!(
            failed, 1,
            "only the first (failing) branch should be Failed; steps: {:?}",
            steps
        );
    }
}
