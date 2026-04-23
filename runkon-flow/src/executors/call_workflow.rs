use crate::dsl::CallWorkflowNode;
use crate::engine::{
    fetch_child_completion_data, handle_on_fail, record_step_success, resolve_child_inputs,
    restore_step, should_skip, ExecutionState,
};
use crate::engine_error::{EngineError, Result};
use crate::prompt_builder::build_variable_map;
use crate::status::WorkflowStepStatus;
use crate::traits::persistence::{NewStep, StepUpdate};

pub fn execute_call_workflow(
    state: &mut ExecutionState,
    node: &CallWorkflowNode,
    iteration: u32,
) -> Result<()> {
    let pos = state.position;
    state.position += 1;

    // Skip completed sub-workflow steps on resume
    let wf_step_name = format!("workflow:{}", node.workflow);
    if should_skip(state, &wf_step_name, iteration) {
        tracing::info!("Skipping completed sub-workflow '{}'", node.workflow);
        restore_step(state, &wf_step_name, iteration);
        return Ok(());
    }

    let child_depth = state.depth + 1;
    if child_depth > crate::dsl::MAX_WORKFLOW_DEPTH {
        let msg = format!(
            "Workflow nesting depth exceeds maximum of {}: parent '{}' calling '{}'",
            crate::dsl::MAX_WORKFLOW_DEPTH,
            state.workflow_name,
            node.workflow,
        );
        state.all_succeeded = false;
        if state.exec_config.fail_fast {
            return Err(EngineError::Workflow(msg));
        }
        tracing::error!("{msg}");
        return Ok(());
    }

    let step_key = node.workflow.clone();

    // Require a child runner to be configured
    let child_runner = match &state.child_runner {
        Some(r) => r.clone(),
        None => {
            return Err(EngineError::Workflow(format!(
                "call_workflow '{}': no ChildWorkflowRunner configured — cannot execute sub-workflow",
                node.workflow
            )));
        }
    };

    // Check for resumable child run first
    match child_runner.find_resumable_child(&state.workflow_run_id, &node.workflow) {
        Ok(Some(prior_child)) => {
            // Resume the prior child run
            let step_id = state
                .persistence
                .insert_step(NewStep {
                    workflow_run_id: state.workflow_run_id.clone(),
                    step_name: wf_step_name.clone(),
                    role: "workflow".to_string(),
                    can_commit: false,
                    position: pos,
                    iteration: iteration as i64,
                    retry_count: Some(0),
                })
                .map_err(|e| EngineError::Persistence(e.to_string()))?;

            tracing::info!(
                "Step 'workflow:{}': resuming prior child run '{}'",
                node.workflow,
                prior_child.id,
            );

            let msg = match child_runner.resume_child(&prior_child.id, state.model.as_deref()) {
                Ok(result) if result.all_succeeded => {
                    tracing::info!(
                        "Sub-workflow '{}' resumed and completed: cost=${:.4}, {} turns",
                        node.workflow,
                        result.total_cost,
                        result.total_turns,
                    );

                    let ((markers, context), child_steps) = fetch_child_completion_data(
                        state.persistence.as_ref(),
                        &result.workflow_run_id,
                    );

                    let markers_json = serde_json::to_string(&markers).unwrap_or_default();

                    state
                        .persistence
                        .update_step(
                            &step_id,
                            StepUpdate {
                                status: WorkflowStepStatus::Completed,
                                child_run_id: Some(result.workflow_run_id.clone()),
                                result_text: Some(format!(
                                    "Sub-workflow '{}' completed",
                                    node.workflow
                                )),
                                context_out: Some(context.clone()),
                                markers_out: Some(markers_json),
                                retry_count: Some(0),
                                structured_output: None,
                                step_error: None,
                            },
                        )
                        .map_err(|e| EngineError::Persistence(e.to_string()))?;

                    record_step_success(
                        state,
                        step_key.clone(),
                        &node.workflow,
                        Some(format!(
                            "Sub-workflow '{}' completed successfully",
                            node.workflow
                        )),
                        Some(result.total_cost),
                        Some(result.total_turns),
                        Some(result.total_duration_ms),
                        Some(result.total_input_tokens),
                        Some(result.total_output_tokens),
                        Some(result.total_cache_read_input_tokens),
                        Some(result.total_cache_creation_input_tokens),
                        markers,
                        context,
                        Some(result.workflow_run_id.clone()),
                        iteration,
                        None,
                        None,
                    );

                    for (key, value) in child_steps {
                        state.step_results.insert(key, value);
                    }

                    return Ok(());
                }
                Ok(result) => {
                    let msg = format!("Sub-workflow '{}' failed (resume)", node.workflow);
                    tracing::warn!(
                        "'{}': resume of child run '{}' did not succeed (all_succeeded=false)",
                        node.workflow,
                        result.workflow_run_id,
                    );
                    state
                        .persistence
                        .update_step(
                            &step_id,
                            StepUpdate {
                                status: WorkflowStepStatus::Failed,
                                child_run_id: Some(result.workflow_run_id),
                                result_text: Some(msg.clone()),
                                context_out: None,
                                markers_out: None,
                                retry_count: Some(0),
                                structured_output: None,
                                step_error: Some(msg.clone()),
                            },
                        )
                        .map_err(|e| EngineError::Persistence(e.to_string()))?;
                    msg
                }
                Err(e) => {
                    let msg = format!("Sub-workflow '{}' resume error: {e}", node.workflow);
                    tracing::warn!(
                        "'{}': error resuming child run '{}': {e}",
                        node.workflow,
                        prior_child.id,
                    );
                    state
                        .persistence
                        .update_step(
                            &step_id,
                            StepUpdate {
                                status: WorkflowStepStatus::Failed,
                                child_run_id: Some(prior_child.id),
                                result_text: Some(msg.clone()),
                                context_out: None,
                                markers_out: None,
                                retry_count: Some(0),
                                structured_output: None,
                                step_error: Some(msg.clone()),
                            },
                        )
                        .map_err(|e2| EngineError::Persistence(e2.to_string()))?;
                    msg
                }
            };
            return handle_on_fail(
                state,
                step_key,
                &node.workflow,
                &node.on_fail,
                msg,
                1,
                iteration,
                1,
            );
        }
        Ok(None) => {}
        Err(e) => {
            tracing::warn!(
                "call_workflow '{}': failed to find resumable child run: {e}",
                node.workflow
            );
        }
    }

    // No resumable child — load the workflow definition and execute fresh
    // We can't load the workflow definition directly here since that's conductor-core's job.
    // Instead, we need to get the child_def from somewhere.
    // For now, we need an empty inputs map from the DSL reference.
    // The child runner is responsible for loading the workflow def.

    let max_attempts = 1 + node.retries;
    let mut last_error = String::new();

    for attempt in 0..max_attempts {
        let step_id = state
            .persistence
            .insert_step(NewStep {
                workflow_run_id: state.workflow_run_id.clone(),
                step_name: wf_step_name.clone(),
                role: "workflow".to_string(),
                can_commit: false,
                position: pos,
                iteration: iteration as i64,
                retry_count: Some(attempt as i64),
            })
            .map_err(|e| EngineError::Persistence(e.to_string()))?;

        tracing::info!(
            "Step 'workflow:{}' (attempt {}/{}): executing sub-workflow",
            node.workflow,
            attempt + 1,
            max_attempts,
        );

        // Build inputs for the child workflow via variable substitution
        let vars = build_variable_map(state);
        // We'll pass the raw inputs along with vars to the child runner
        // which will resolve them with the actual input decls
        let raw_child_inputs = node.inputs.clone();
        let mut child_inputs = std::collections::HashMap::new();
        for (k, v) in &raw_child_inputs {
            child_inputs.insert(
                k.clone(),
                crate::prompt_builder::substitute_variables_keep_literal(v, &vars),
            );
        }

        let effective_bot_name = node
            .bot_name
            .clone()
            .or_else(|| state.default_bot_name.clone());

        // Create a minimal child workflow definition stub for passing inputs.
        // The child runner (conductor-core adapter) will load the actual def.
        // For the runkon-flow engine, we use a placeholder approach:
        // The child_runner.execute_child accepts a pre-loaded WorkflowDef.
        // Since we can't load files here, we need a different approach.
        //
        // SOLUTION: Store the workflow path in worktree_ctx and let the runner handle loading.
        // We pass an empty WorkflowDef as a placeholder and let the runner resolve it by name.
        //
        // Actually, the better approach is: child_runner gets the workflow name via the step
        // and loads it itself. We'll pass inputs as-is.

        // Build a minimal dummy WorkflowDef with the name only — the runner must load the real one.
        let placeholder_def = crate::dsl::WorkflowDef {
            name: node.workflow.clone(),
            title: None,
            description: String::new(),
            trigger: crate::dsl::WorkflowTrigger::Manual,
            targets: vec![],
            group: None,
            inputs: vec![],
            body: vec![],
            always: vec![],
            source_path: String::new(),
        };

        // Resolve child inputs against the placeholder (no decls → just pass through substituted vars)
        let resolved_inputs =
            match resolve_child_inputs(&raw_child_inputs, &vars, &placeholder_def.inputs) {
                Ok(inputs) => inputs,
                Err(missing) => {
                    let msg = format!(
                        "Sub-workflow '{}' requires input '{}' but it was not provided",
                        node.workflow, missing,
                    );
                    tracing::warn!("{msg}");
                    state
                        .persistence
                        .update_step(
                            &step_id,
                            StepUpdate {
                                status: WorkflowStepStatus::Failed,
                                child_run_id: None,
                                result_text: Some(msg.clone()),
                                context_out: None,
                                markers_out: None,
                                retry_count: Some(attempt as i64),
                                structured_output: None,
                                step_error: Some(msg.clone()),
                            },
                        )
                        .map_err(|e| EngineError::Persistence(e.to_string()))?;
                    last_error = msg;
                    continue;
                }
            };

        // Use the child_runner to execute — it knows about conductor-core types
        let _ = child_inputs; // raw inputs already resolved above
        match child_runner.execute_child(
            &placeholder_def,
            state,
            crate::engine::ChildWorkflowInput {
                inputs: resolved_inputs,
                iteration,
                bot_name: effective_bot_name.clone(),
                depth: child_depth,
                parent_step_id: Some(step_id.clone()),
                cancellation: state.cancellation.child(),
            },
        ) {
            Ok(result) => {
                if result.all_succeeded {
                    tracing::info!(
                        "Sub-workflow '{}' completed: cost=${:.4}, {} turns",
                        node.workflow,
                        result.total_cost,
                        result.total_turns,
                    );

                    let ((markers, context), child_steps) = fetch_child_completion_data(
                        state.persistence.as_ref(),
                        &result.workflow_run_id,
                    );

                    let markers_json = serde_json::to_string(&markers).unwrap_or_default();

                    state
                        .persistence
                        .update_step(
                            &step_id,
                            StepUpdate {
                                status: WorkflowStepStatus::Completed,
                                child_run_id: Some(result.workflow_run_id.clone()),
                                result_text: Some(format!(
                                    "Sub-workflow '{}' completed",
                                    node.workflow
                                )),
                                context_out: Some(context.clone()),
                                markers_out: Some(markers_json),
                                retry_count: Some(attempt as i64),
                                structured_output: None,
                                step_error: None,
                            },
                        )
                        .map_err(|e| EngineError::Persistence(e.to_string()))?;

                    record_step_success(
                        state,
                        step_key.clone(),
                        &node.workflow,
                        Some(format!(
                            "Sub-workflow '{}' completed successfully",
                            node.workflow
                        )),
                        Some(result.total_cost),
                        Some(result.total_turns),
                        Some(result.total_duration_ms),
                        Some(result.total_input_tokens),
                        Some(result.total_output_tokens),
                        Some(result.total_cache_read_input_tokens),
                        Some(result.total_cache_creation_input_tokens),
                        markers,
                        context,
                        Some(result.workflow_run_id.clone()),
                        iteration,
                        None,
                        None,
                    );

                    for (key, value) in child_steps {
                        state.step_results.insert(key, value);
                    }

                    return Ok(());
                } else {
                    let msg = format!("Sub-workflow '{}' failed", node.workflow);
                    tracing::warn!("{} (attempt {}/{})", msg, attempt + 1, max_attempts);
                    state
                        .persistence
                        .update_step(
                            &step_id,
                            StepUpdate {
                                status: WorkflowStepStatus::Failed,
                                child_run_id: Some(result.workflow_run_id),
                                result_text: Some(msg.clone()),
                                context_out: None,
                                markers_out: None,
                                retry_count: Some(attempt as i64),
                                structured_output: None,
                                step_error: Some(msg.clone()),
                            },
                        )
                        .map_err(|e| EngineError::Persistence(e.to_string()))?;
                    last_error = msg;
                    continue;
                }
            }
            Err(e) => {
                let msg = format!("Sub-workflow '{}' error: {e}", node.workflow);
                tracing::warn!("{} (attempt {}/{})", msg, attempt + 1, max_attempts);
                state
                    .persistence
                    .update_step(
                        &step_id,
                        StepUpdate {
                            status: WorkflowStepStatus::Failed,
                            child_run_id: None,
                            result_text: Some(msg.clone()),
                            context_out: None,
                            markers_out: None,
                            retry_count: Some(attempt as i64),
                            structured_output: None,
                            step_error: Some(msg.clone()),
                        },
                    )
                    .map_err(|e2| EngineError::Persistence(e2.to_string()))?;
                last_error = msg;
                continue;
            }
        }
    }

    handle_on_fail(
        state,
        step_key,
        &node.workflow,
        &node.on_fail,
        last_error,
        node.retries,
        iteration,
        max_attempts,
    )
}
