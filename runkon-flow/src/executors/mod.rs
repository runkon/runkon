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
    use crate::traits::persistence::NewStep;
    if attempt > 0 {
        emit_event(
            state,
            EngineEvent::StepRetrying {
                step_name: step_name.to_string(),
                attempt,
            },
        );
    }
    let step_id = state
        .persistence
        .insert_step(NewStep {
            workflow_run_id: state.workflow_run_id.clone(),
            step_name: step_name.to_string(),
            role: role.to_string(),
            can_commit: false,
            position: pos,
            iteration: iteration as i64,
            retry_count: Some(attempt as i64),
        })
        .map_err(p_err)?;
    Ok(step_id)
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

/// Construct an `ExecutionContext` from shared state fields.
///
/// Centralises the repeated struct-literal initialisation in `call.rs` and
/// `parallel.rs`.  Both sites differ only in `bot_name` and `plugin_dirs`;
/// all other fields come directly from `state`.
pub(super) fn build_execution_context(
    state: &crate::engine::ExecutionState,
    step_id: &str,
    bot_name: Option<String>,
    plugin_dirs: Vec<String>,
) -> crate::traits::action_executor::ExecutionContext {
    crate::traits::action_executor::ExecutionContext {
        run_id: step_id.to_string(),
        working_dir: std::path::PathBuf::from(&state.worktree_ctx.working_dir),
        repo_path: state.worktree_ctx.repo_path.clone(),
        step_timeout: state.exec_config.step_timeout,
        shutdown: state.exec_config.shutdown.clone(),
        model: state.model.clone(),
        bot_name,
        plugin_dirs,
        workflow_name: state.workflow_name.clone(),
        worktree_id: state.worktree_ctx.worktree_id.clone(),
        parent_run_id: state.parent_run_id.clone(),
        step_id: step_id.to_string(),
    }
}

/// Persist a successfully completed step via `state.persistence.update_step`.
///
/// Wraps the `StepUpdate::completed(...)` + `.map_err(p_err)` pattern that is
/// duplicated in `call.rs`, `parallel.rs`, and `call_workflow.rs`.
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
    state
        .persistence
        .update_step(
            step_id,
            StepUpdate::completed(
                child_run_id,
                result_text,
                context_out,
                markers_out,
                attempt,
                structured_output,
            ),
        )
        .map_err(p_err)
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
) -> crate::traits::action_executor::ActionParams {
    crate::traits::action_executor::ActionParams {
        name: name.to_string(),
        inputs,
        retries_remaining,
        retry_error,
        snippets,
        dry_run,
        gate_feedback,
        schema,
    }
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
    persist_completed_step(
        state,
        step_id,
        output.child_run_id.clone(),
        output.result_text.clone(),
        Some(context.clone()),
        Some(markers_json),
        attempt,
        output.structured_output.clone(),
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
    use crate::engine::{restore_step, should_skip};
    if should_skip(state, step_key, iteration) {
        tracing::info!(
            "Skipping completed step '{}' (iteration {})",
            label,
            iteration
        );
        restore_step(state, step_key, iteration);
        true
    } else {
        false
    }
}
