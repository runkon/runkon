use crate::dsl::CallWorkflowNode;
use crate::engine::{
    fetch_child_completion_data, handle_on_fail, record_step_success, resolve_child_inputs,
    ExecutionState,
};
use crate::engine_error::{EngineError, Result};
use crate::prompt_builder::build_variable_map;
use crate::traits::persistence::StepUpdate;

pub fn execute_call_workflow(
    state: &mut ExecutionState,
    node: &CallWorkflowNode,
    iteration: u32,
) -> Result<()> {
    let pos = state.position;
    state.position += 1;

    // Skip completed sub-workflow steps on resume
    let wf_step_name = format!("workflow:{}", node.workflow);
    if super::skip_if_already_completed(state, &wf_step_name, iteration, &node.workflow) {
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
    let mut last_error = String::new();

    // Helper: persist success and bubble up child step results.
    // Used by both the resume-success path and the fresh-success path.
    let record_child_success = |state: &mut ExecutionState,
                                step_id: &str,
                                result: &crate::types::WorkflowResult,
                                attempt: u32|
     -> Result<()> {
        let ((markers, context), child_steps) =
            fetch_child_completion_data(state.persistence.as_ref(), &result.workflow_run_id);

        let markers_json = crate::helpers::serialize_or_empty_array(
            &markers,
            &format!("call_workflow '{}'", node.workflow),
        );

        super::persist_completed_step(
            state,
            step_id,
            Some(result.workflow_run_id.clone()),
            Some(format!("Sub-workflow '{}' completed", node.workflow)),
            Some(context.clone()),
            Some(markers_json),
            attempt,
            None,
        )?;

        record_step_success(
            state,
            step_key.clone(),
            crate::types::StepSuccess {
                step_name: node.workflow.clone(),
                result_text: Some(format!(
                    "Sub-workflow '{}' completed successfully",
                    node.workflow
                )),
                cost_usd: Some(result.total_cost),
                num_turns: Some(result.total_turns),
                duration_ms: Some(result.total_duration_ms),
                input_tokens: Some(result.total_input_tokens),
                output_tokens: Some(result.total_output_tokens),
                cache_read_input_tokens: Some(result.total_cache_read_input_tokens),
                cache_creation_input_tokens: Some(result.total_cache_creation_input_tokens),
                markers,
                context,
                child_run_id: Some(result.workflow_run_id.clone()),
                iteration,
                structured_output: None,
                output_file: None,
            },
        );

        for (key, value) in child_steps {
            state.step_results.insert(key, value);
        }

        Ok(())
    };

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
            let step_id = super::insert_step_record(
                state,
                &wf_step_name,
                "workflow",
                pos,
                iteration,
                Some(0),
            )?;

            tracing::info!(
                "Step 'workflow:{}': resuming prior child run '{}'",
                node.workflow,
                prior_child.id,
            );

            let msg = match child_runner.resume_child(
                &prior_child.id,
                state.model.as_deref(),
                &state.child_workflow_context(),
            ) {
                Ok(result) if result.all_succeeded => {
                    tracing::info!(
                        "Sub-workflow '{}' resumed and completed: cost=${:.4}, {} turns",
                        node.workflow,
                        result.total_cost,
                        result.total_turns,
                    );
                    record_child_success(state, &step_id, &result, 0)?;
                    return Ok(());
                }
                Ok(result) => {
                    let msg = format!("Sub-workflow '{}' failed (resume)", node.workflow);
                    tracing::warn!(
                        "'{}': resume of child run '{}' did not succeed (all_succeeded=false)",
                        node.workflow,
                        result.workflow_run_id,
                    );
                    let generation = state.expect_lease_generation();
                    state.persistence.update_step(
                        &step_id,
                        StepUpdate::failed_with_child(
                            generation,
                            msg.clone(),
                            0,
                            Some(result.workflow_run_id),
                        ),
                    )?;
                    msg
                }
                Err(e) => {
                    let msg = format!("Sub-workflow '{}' resume error: {e}", node.workflow);
                    tracing::warn!(
                        "'{}': error resuming child run '{}': {e}",
                        node.workflow,
                        prior_child.id,
                    );
                    let generation = state.expect_lease_generation();
                    state.persistence.update_step(
                        &step_id,
                        StepUpdate::failed_with_child(
                            generation,
                            msg.clone(),
                            0,
                            Some(prior_child.id),
                        ),
                    )?;
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
            last_error = format!("failed to find resumable child run: {e}");
            tracing::warn!("call_workflow '{}': {last_error}", node.workflow);
        }
    }

    // No resumable child — load the workflow definition and execute fresh
    // We can't load the workflow definition directly here since that's conductor-core's job.
    // Instead, we need to get the child_def from somewhere.
    // For now, we need an empty inputs map from the DSL reference.
    // The child runner is responsible for loading the workflow def.

    let max_attempts = 1 + node.retries;

    for attempt in 0..max_attempts {
        // Insert step record as running (also emits StepRetrying when attempt > 0)
        let step_id =
            super::begin_retry_attempt(state, &wf_step_name, "workflow", pos, iteration, attempt)?;

        tracing::info!(
            "Step 'workflow:{}' (attempt {}/{}): executing sub-workflow",
            node.workflow,
            attempt + 1,
            max_attempts,
        );

        // Build inputs for the child workflow via variable substitution
        let vars = build_variable_map(state);
        let raw_child_inputs = node.inputs.clone();

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

        // Resolve child inputs against an empty inputs map (no decls → just pass through substituted vars)
        let resolved_inputs = match resolve_child_inputs(&raw_child_inputs, &vars, &[]) {
            Ok(inputs) => inputs,
            Err(missing) => {
                let msg = format!(
                    "Sub-workflow '{}' requires input '{}' but it was not provided",
                    node.workflow, missing,
                );
                tracing::warn!("{msg}");
                let generation = state.expect_lease_generation();
                state.persistence.update_step(
                    &step_id,
                    StepUpdate::failed(generation, msg.clone(), attempt),
                )?;
                last_error = msg;
                continue;
            }
        };

        // Use the child_runner to execute — it resolves the real def by name
        match child_runner.execute_child(
            &node.workflow,
            &state.child_workflow_context(),
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
                    record_child_success(state, &step_id, &result, attempt)?;
                    return Ok(());
                } else {
                    let msg = format!("Sub-workflow '{}' failed", node.workflow);
                    tracing::warn!(
                        "{} (attempt {}/{}) [child_run_id={}]",
                        msg,
                        attempt + 1,
                        max_attempts,
                        result.workflow_run_id,
                    );
                    let generation = state.expect_lease_generation();
                    state.persistence.update_step(
                        &step_id,
                        StepUpdate::failed_with_child(
                            generation,
                            msg.clone(),
                            attempt,
                            Some(result.workflow_run_id),
                        ),
                    )?;
                    last_error = msg;
                    continue;
                }
            }
            Err(e) => {
                let msg = format!("Sub-workflow '{}' error: {e}", node.workflow);
                tracing::warn!("{} (attempt {}/{})", msg, attempt + 1, max_attempts);
                let generation = state.expect_lease_generation();
                state.persistence.update_step(
                    &step_id,
                    StepUpdate::failed(generation, msg.clone(), attempt),
                )?;
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
