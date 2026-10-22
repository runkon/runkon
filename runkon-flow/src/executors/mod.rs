pub mod call;
pub mod call_workflow;
pub mod control_flow;
pub mod foreach;
pub mod gate;
pub mod parallel;
pub mod script;

use std::collections::HashMap;
use std::sync::Arc;

use crate::engine_error::EngineError;

#[inline]
#[track_caller]
pub(super) fn p_err(e: impl std::fmt::Display) -> EngineError {
    let loc = std::panic::Location::caller();
    EngineError::Persistence(format!("{}:{} — {e}", loc.file(), loc.line()))
}

/// Insert a workflow step record. Returns the new step_id.
///
/// `retry_count` semantics: `Some(n)` → status='running' + started_at=now (set by DB);
/// `None` → status='pending'.
pub(super) fn insert_step_record(
    state: &crate::engine::ExecutionState,
    step_name: &str,
    role: &str,
    pos: i64,
    iteration: u32,
    retry_count: Option<i64>,
) -> crate::engine_error::Result<String> {
    use crate::traits::persistence::NewStep;
    state
        .persistence
        .insert_step(NewStep {
            workflow_run_id: state.workflow_run_id.clone(),
            step_name: step_name.to_string(),
            role: role.to_string(),
            can_commit: false,
            position: pos,
            iteration: iteration as i64,
            retry_count,
        })
        .map_err(p_err)
}

/// Insert a step then immediately apply a non-Running status update.
///
/// Used for terminal/transitional statuses set at insert time:
/// gate dry-run (Completed), gate waiting (Waiting), parallel conditional-skip (Skipped).
/// Returns the new step_id.
#[allow(clippy::too_many_arguments)]
pub(super) fn insert_step_with_status(
    state: &crate::engine::ExecutionState,
    step_name: &str,
    role: &str,
    pos: i64,
    iteration: u32,
    retry_count: Option<i64>,
    status: crate::status::WorkflowStepStatus,
    result_text: Option<String>,
) -> crate::engine_error::Result<String> {
    use crate::traits::persistence::StepUpdate;
    let step_id = insert_step_record(state, step_name, role, pos, iteration, retry_count)?;
    let generation = state.expect_lease_generation();
    state.persistence.update_step(
        &step_id,
        StepUpdate {
            generation,
            status,
            child_run_id: None,
            result_text,
            context_out: None,
            markers_out: None,
            retry_count,
            structured_output: None,
            step_error: None,
        },
    )?;
    Ok(step_id)
}

/// Insert a step record and emit a StepRetrying event when `attempt > 0`.
/// Returns the new step_id.
pub(super) fn begin_retry_attempt(
    state: &mut crate::engine::ExecutionState,
    step_name: &str,
    role: &str,
    pos: i64,
    iteration: u32,
    attempt: u32,
) -> crate::engine_error::Result<String> {
    use crate::engine::emit_event;
    use crate::events::EngineEvent;
    if attempt > 0 {
        emit_event(
            state,
            EngineEvent::StepRetrying {
                step_name: step_name.to_string(),
                attempt,
            },
        );
    }
    insert_step_record(state, step_name, role, pos, iteration, Some(attempt as i64))
}

/// Build the inputs map (`Arc<HashMap<String, String>>`) from the current execution state.
///
/// Serializes `state.contexts` into a flat variable map once so callers do not
/// duplicate the `build_variable_map` → collect pattern.
pub(super) fn build_inputs_map(
    state: &crate::engine::ExecutionState,
) -> Arc<HashMap<String, String>> {
    let var_map = crate::prompt_builder::build_variable_map(state);
    Arc::new(
        var_map
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    )
}

/// Construct a `StepInfo` from shared state fields.
///
/// Centralises the repeated initialisation in `call.rs` and `parallel.rs`.
pub(super) fn build_step_info(
    state: &crate::engine::ExecutionState,
    step_id: &str,
) -> crate::traits::action_executor::StepInfo {
    crate::traits::action_executor::StepInfo {
        step_id: step_id.to_string(),
        step_timeout: state.exec_config.step_timeout,
    }
}

/// Persist a successfully completed step via `state.persistence.update_step`.
///
/// Wraps the `StepUpdate::completed(...)` pattern used in `call.rs`, `parallel.rs`,
/// and `call_workflow.rs`. Propagates `Cancelled(LeaseLost)` without wrapping.
#[allow(clippy::too_many_arguments)]
pub(super) fn persist_completed_step(
    state: &crate::engine::ExecutionState,
    step_id: &str,
    child_run_id: Option<String>,
    result_text: Option<String>,
    context_out: Option<String>,
    markers_out: Option<String>,
    attempt: u32,
    structured_output: Option<String>,
) -> crate::engine_error::Result<()> {
    use crate::traits::persistence::StepUpdate;
    let generation = state.expect_lease_generation();
    state.persistence.update_step(
        step_id,
        StepUpdate::completed(
            generation,
            child_run_id,
            result_text,
            context_out,
            markers_out,
            attempt,
            structured_output,
        ),
    )
}

/// Build [`ActionParams`] from the fields that are identical in `call.rs` and `parallel.rs`.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_action_params(
    name: &str,
    inputs: Arc<HashMap<String, String>>,
    snippets: Vec<String>,
    dry_run: bool,
    gate_feedback: Option<String>,
    schema: Option<crate::output_schema::OutputSchema>,
    retries_remaining: u32,
    retry_error: Option<String>,
    model: Option<String>,
    as_identity: Option<String>,
    plugin_dirs: Vec<String>,
    max_turns: Option<u32>,
) -> crate::traits::action_executor::ActionParams {
    let mut extensions = crate::extensions::Extensions::default();
    if let Some(s) = schema {
        extensions.insert(s);
    }
    if let Some(mt) = max_turns {
        extensions.insert(crate::extensions::ClaudeActionParams {
            max_turns: Some(mt),
        });
    }
    crate::traits::action_executor::ActionParams {
        name: name.to_string(),
        inputs,
        retries_remaining,
        retry_error,
        snippets,
        dry_run,
        gate_feedback,
        extensions,
        model,
        as_identity,
        plugin_dirs,
    }
}

/// Persist a completed agent step from its [`ActionOutput`].
///
/// If the executor set [`metadata_keys::OUTPUT_WARNING`] (for example, the agent
/// ended without a `<<<FLOW_OUTPUT>>>` block, so its markers are empty), the
/// warning is logged and stored as the step's `step_error`: the step still
/// completes, but the run shows why its markers are missing.
///
/// [`metadata_keys::OUTPUT_WARNING`]: crate::constants::metadata_keys::OUTPUT_WARNING
pub(super) fn persist_completed_action_step(
    state: &crate::engine::ExecutionState,
    step_id: &str,
    agent_label: &str,
    output: &crate::traits::action_executor::ActionOutput,
    context: String,
    markers_json: String,
    attempt: u32,
) -> crate::engine_error::Result<()> {
    use crate::traits::persistence::StepUpdate;
    let warning = output
        .metadata
        .get(crate::constants::metadata_keys::OUTPUT_WARNING)
        .cloned();
    if let Some(ref w) = warning {
        tracing::warn!(step = %agent_label, "agent '{agent_label}': {w}");
    }
    let mut update = StepUpdate::completed(
        state.expect_lease_generation(),
        output.child_run_id.clone(),
        output.result_text.clone(),
        Some(context),
        Some(markers_json),
        attempt,
        output.structured_output.clone(),
    );
    update.step_error = warning;
    state.persistence.update_step(step_id, update)
}

/// Persist a completed step and record its success result in one call.
/// Centralises the `persist_completed_step` + `record_step_success` pair used
/// by `call.rs` after a successful agent dispatch.
#[allow(clippy::too_many_arguments)]
pub(super) fn record_dispatch_success(
    state: &mut crate::engine::ExecutionState,
    step_id: &str,
    step_key: &str,
    agent_label: &str,
    output: &crate::traits::action_executor::ActionOutput,
    iteration: u32,
    attempt: u32,
    output_file: Option<String>,
) -> crate::engine_error::Result<()> {
    let markers_json = crate::helpers::serialize_or_empty_array(
        &output.markers,
        &format!("agent '{agent_label}'"),
    );
    let context = output.context.clone().unwrap_or_default();
    persist_completed_action_step(
        state,
        step_id,
        agent_label,
        output,
        context.clone(),
        markers_json,
        attempt,
    )?;
    crate::engine::record_step_success(
        state,
        step_key.to_string(),
        crate::types::StepSuccess::from_action_output(
            output,
            agent_label.to_string(),
            context,
            iteration,
            output_file,
        ),
    );
    Ok(())
}

/// Returns `true` and performs skip cleanup if the step has already completed.
/// Callers should `return Ok(())` (or `continue`) immediately when this returns `true`.
pub(super) fn skip_if_already_completed(
    state: &mut crate::engine::ExecutionState,
    step_key: &str,
    iteration: u32,
    label: &str,
) -> bool {
    skip_if_already_completed_with(state, step_key, iteration, label, |state| {
        crate::engine::restore_step(state, step_key, iteration);
        Ok(())
    })
    .unwrap_or(false)
}

/// Like [`skip_if_already_completed`], but with a custom `restore` for steps
/// (such as `call workflow`) that restore more than their own row. A failed
/// restore is returned as an error rather than skipping silently.
pub(super) fn skip_if_already_completed_with(
    state: &mut crate::engine::ExecutionState,
    step_key: &str,
    iteration: u32,
    label: &str,
    restore: impl FnOnce(&mut crate::engine::ExecutionState) -> crate::engine_error::Result<()>,
) -> crate::engine_error::Result<bool> {
    if !crate::engine::should_skip(state, step_key, iteration) {
        return Ok(false);
    }
    tracing::info!(
        "Skipping completed step '{}' (iteration {})",
        label,
        iteration
    );
    restore(state)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extensions::ClaudeActionParams;
    use crate::traits::action_executor::ActionParams;

    fn make_inputs() -> Arc<HashMap<String, String>> {
        Arc::new(HashMap::new())
    }

    fn make_test_params(max_turns: Option<u32>) -> ActionParams {
        build_action_params(
            "test",
            make_inputs(),
            vec![],
            false,
            None,
            None,
            0,
            None,
            None,
            None,
            vec![],
            max_turns,
        )
    }

    #[test]
    fn build_action_params_inserts_claude_action_params_when_max_turns_some() {
        let params = make_test_params(Some(50));
        let cap = params
            .extensions
            .get::<ClaudeActionParams>()
            .expect("ClaudeActionParams should be present");
        assert_eq!(cap.max_turns, Some(50));
    }

    #[test]
    fn build_action_params_no_claude_action_params_when_max_turns_none() {
        let params = make_test_params(None);
        assert!(params.extensions.get::<ClaudeActionParams>().is_none());
    }
}
