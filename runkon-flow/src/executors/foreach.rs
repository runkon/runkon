use std::collections::HashSet;

use crate::dsl::{ForEachNode, OnChildFail};
use crate::engine::{
    emit_event, record_step_failure, record_step_success, restore_step, should_skip, ExecutionState,
};
use crate::engine_error::{EngineError, Result};
use crate::events::EngineEvent;
use crate::status::WorkflowStepStatus;
use crate::traits::item_provider::ProviderContext;
use crate::traits::persistence::{FanOutItemStatus, FanOutItemUpdate, NewStep, StepUpdate};

/// Execute a `foreach` step: fan out a child workflow over a collection of items.
pub fn execute_foreach(
    state: &mut ExecutionState,
    node: &ForEachNode,
    iteration: u32,
) -> Result<()> {
    let pos = state.position;
    state.position += 1;

    let step_key = format!("foreach:{}", node.name);

    // Skip on resume if already completed.
    if should_skip(state, &step_key, iteration) {
        tracing::info!("foreach '{}': skipping completed step", node.name);
        restore_step(state, &step_key, iteration);
        return Ok(());
    }

    // Insert the step record
    let step_id = state
        .persistence
        .insert_step(NewStep {
            workflow_run_id: state.workflow_run_id.clone(),
            step_name: step_key.clone(),
            role: "foreach".to_string(),
            can_commit: false,
            position: pos,
            iteration: iteration as i64,
            retry_count: Some(0),
        })
        .map_err(|e| EngineError::Persistence(e.to_string()))?;

    state
        .persistence
        .update_step(
            &step_id,
            StepUpdate {
                status: WorkflowStepStatus::Running,
                child_run_id: None,
                result_text: None,
                context_out: None,
                markers_out: None,
                retry_count: Some(0),
                structured_output: None,
                step_error: None,
            },
        )
        .map_err(|e| EngineError::Persistence(e.to_string()))?;

    // Validate the provider exists
    let provider = state.registry.get(&node.over).ok_or_else(|| {
        EngineError::Workflow(format!(
            "foreach '{}': unknown provider '{}' — no ItemProvider registered for this name",
            node.name, node.over
        ))
    })?;

    // Require a child runner for dispatching child workflows
    let child_runner = match &state.child_runner {
        Some(r) => r.clone(),
        None => {
            return Err(EngineError::Workflow(format!(
                "foreach '{}': no ChildWorkflowRunner configured — cannot dispatch child workflows",
                node.name
            )));
        }
    };

    // Build provider context
    let provider_ctx = ProviderContext {
        repo_id: state.worktree_ctx.repo_id.clone(),
        worktree_id: state.worktree_ctx.worktree_id.clone(),
    };

    // Phase 1: Item collection
    let existing_items = state
        .persistence
        .get_fan_out_items(&step_id, None)
        .unwrap_or_default();
    let existing_set: HashSet<String> = existing_items.iter().map(|i| i.item_id.clone()).collect();

    let provider_items = provider.items(
        &provider_ctx,
        node.scope.as_ref(),
        &node.filter,
        &existing_set,
    )?;

    let items: Vec<(String, String, String)> = provider_items
        .into_iter()
        .map(|i| (i.item_type, i.item_id, i.item_ref))
        .collect();

    // Write pending rows for newly discovered items.
    for (item_type, item_id, item_ref) in &items {
        if !existing_set.contains(item_id) {
            state
                .persistence
                .insert_fan_out_item(&step_id, item_type, item_id, item_ref)
                .map_err(|e| EngineError::Persistence(e.to_string()))?;
        }
    }

    let all_items = state
        .persistence
        .get_fan_out_items(&step_id, None)
        .map_err(|e| EngineError::Persistence(e.to_string()))?;
    let total_items = all_items.len();

    tracing::info!(
        "foreach '{}': {} items to process (over={}, max_parallel={})",
        node.name,
        total_items,
        node.over,
        node.max_parallel,
    );

    emit_event(
        state,
        EngineEvent::FanOutItemsCollected { count: total_items },
    );

    if total_items == 0 {
        let context = format!("foreach {}: no items to process", node.name);
        state
            .persistence
            .update_step(
                &step_id,
                StepUpdate {
                    status: WorkflowStepStatus::Completed,
                    child_run_id: None,
                    result_text: Some(context.clone()),
                    context_out: Some(context.clone()),
                    markers_out: None,
                    retry_count: Some(0),
                    structured_output: None,
                    step_error: None,
                },
            )
            .map_err(|e| EngineError::Persistence(e.to_string()))?;

        record_step_success(
            state,
            step_key,
            &node.name,
            Some(context.clone()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            vec![],
            context,
            None,
            iteration,
            None,
            None,
        );
        return Ok(());
    }

    // Phase 2: Dispatch loop (simplified sequential dispatch)

    let pending_items = state
        .persistence
        .get_fan_out_items(&step_id, Some(FanOutItemStatus::Pending))
        .map_err(|e| EngineError::Persistence(e.to_string()))?;

    for item in pending_items {
        emit_event(
            state,
            EngineEvent::FanOutItemStarted {
                item_id: item.item_id.clone(),
            },
        );

        // Mark as running
        state
            .persistence
            .update_fan_out_item(
                &item.id,
                FanOutItemUpdate::Running {
                    child_run_id: "dispatching".to_string(),
                },
            )
            .map_err(|e| EngineError::Persistence(e.to_string()))?;

        // Build child inputs with item-specific variables
        let mut child_inputs = node.inputs.clone();
        child_inputs.insert("item.id".to_string(), item.item_id.clone());
        child_inputs.insert("item.ref".to_string(), item.item_ref.clone());

        // Build placeholder workflow def with the name
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

        match child_runner.execute_child(
            &placeholder_def,
            state,
            crate::engine::ChildWorkflowInput {
                inputs: child_inputs,
                iteration,
                bot_name: None,
                depth: state.depth + 1,
                parent_step_id: None,
                cancellation: state.cancellation.child(),
            },
        ) {
            Ok(result) => {
                let terminal = if result.all_succeeded {
                    "completed"
                } else {
                    "failed"
                };
                state
                    .persistence
                    .update_fan_out_item(
                        &item.id,
                        FanOutItemUpdate::Terminal {
                            status: if result.all_succeeded {
                                FanOutItemStatus::Completed
                            } else {
                                FanOutItemStatus::Failed
                            },
                        },
                    )
                    .map_err(|e| EngineError::Persistence(e.to_string()))?;
                emit_event(
                    state,
                    EngineEvent::FanOutItemCompleted {
                        item_id: item.item_id.clone(),
                        succeeded: result.all_succeeded,
                    },
                );
                if result.all_succeeded {
                    tracing::info!(
                        "foreach '{}': item '{}' → {terminal}",
                        node.name,
                        item.item_ref
                    );
                } else {
                    tracing::warn!(
                        "foreach '{}': item '{}' → {terminal}",
                        node.name,
                        item.item_ref
                    );

                    match node.on_child_fail {
                        OnChildFail::Halt => {
                            tracing::warn!("foreach '{}': on_child_fail=halt, stopping", node.name);
                            break;
                        }
                        OnChildFail::Continue | OnChildFail::SkipDependents => {}
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    "foreach '{}': item '{}' error: {e}",
                    node.name,
                    item.item_ref
                );
                state
                    .persistence
                    .update_fan_out_item(
                        &item.id,
                        FanOutItemUpdate::Terminal {
                            status: FanOutItemStatus::Failed,
                        },
                    )
                    .map_err(|e2| EngineError::Persistence(e2.to_string()))?;
                emit_event(
                    state,
                    EngineEvent::FanOutItemCompleted {
                        item_id: item.item_id.clone(),
                        succeeded: false,
                    },
                );

                match node.on_child_fail {
                    OnChildFail::Halt => {
                        tracing::warn!("foreach '{}': on_child_fail=halt, stopping", node.name);
                        break;
                    }
                    OnChildFail::Continue | OnChildFail::SkipDependents => {}
                }
            }
        }
    }

    // Phase 3: Step completion
    let fan_out_items = state
        .persistence
        .get_fan_out_items(&step_id, None)
        .unwrap_or_default();
    let completed_count = fan_out_items
        .iter()
        .filter(|i| i.status == "completed")
        .count();
    let failed_count = fan_out_items
        .iter()
        .filter(|i| i.status == "failed")
        .count();
    let skipped_count = fan_out_items
        .iter()
        .filter(|i| i.status == "skipped")
        .count();
    let total = fan_out_items.len();

    let context = format!(
        "foreach {}: {completed_count} completed, {failed_count} failed, {skipped_count} skipped of {total} {}",
        node.name, node.over,
    );

    let step_succeeded = failed_count == 0;

    if step_succeeded {
        state
            .persistence
            .update_step(
                &step_id,
                StepUpdate {
                    status: WorkflowStepStatus::Completed,
                    child_run_id: None,
                    result_text: Some(context.clone()),
                    context_out: Some(context.clone()),
                    markers_out: None,
                    retry_count: Some(0),
                    structured_output: None,
                    step_error: None,
                },
            )
            .map_err(|e| EngineError::Persistence(e.to_string()))?;

        record_step_success(
            state,
            step_key,
            &node.name,
            Some(context.clone()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            vec![],
            context,
            None,
            iteration,
            None,
            None,
        );
    } else {
        let error_msg = format!(
            "foreach '{}': {failed_count} of {total} items failed",
            node.name
        );

        state
            .persistence
            .update_step(
                &step_id,
                StepUpdate {
                    status: WorkflowStepStatus::Failed,
                    child_run_id: None,
                    result_text: Some(error_msg.clone()),
                    context_out: Some(context),
                    markers_out: None,
                    retry_count: Some(0),
                    structured_output: None,
                    step_error: Some(error_msg.clone()),
                },
            )
            .map_err(|e| EngineError::Persistence(e.to_string()))?;

        return record_step_failure(state, step_key, &node.name, error_msg, 1, true);
    }

    Ok(())
}
