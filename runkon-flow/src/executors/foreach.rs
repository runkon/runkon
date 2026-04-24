use std::collections::{HashMap, HashSet};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::cancellation::CancellationToken;
use crate::dsl::{ForEachNode, OnChildFail};
use crate::engine::{
    emit_event, record_step_failure, record_step_success, restore_step, should_skip,
    ChildWorkflowInput, ExecutionState, WorktreeContext,
};
use crate::engine_error::{EngineError, Result};
use crate::events::EngineEvent;
use crate::status::WorkflowStepStatus;
use crate::traits::action_executor::ActionRegistry;
use crate::traits::item_provider::{ItemProviderRegistry, ProviderContext};
use crate::traits::persistence::{
    FanOutItemStatus, FanOutItemUpdate, NewStep, StepUpdate, WorkflowPersistence,
};
use crate::traits::script_env_provider::ScriptEnvProvider;

#[inline]
fn p_err(e: EngineError) -> EngineError {
    EngineError::Persistence(e.to_string())
}

/// Shared parent-state snapshot captured before thread spawning.
///
/// All fields are either `Arc` clones or cheap `Clone` copies — no borrows into
/// the parent `ExecutionState` are kept, so the main thread retains full `&mut`
/// access throughout the dispatch loop.
struct ForeachParentCtx {
    persistence: Arc<dyn WorkflowPersistence>,
    action_registry: Arc<ActionRegistry>,
    script_env_provider: Arc<dyn ScriptEnvProvider>,
    registry: Arc<ItemProviderRegistry>,
    event_sinks: Arc<[Arc<dyn crate::events::EventSink>]>,
    child_runner: Arc<dyn crate::engine::ChildWorkflowRunner>,
    workflow_run_id: String,
    workflow_name: String,
    model: Option<String>,
    exec_config: crate::types::WorkflowExecConfig,
    parent_run_id: String,
    depth: u32,
    target_label: Option<String>,
    default_bot_name: Option<String>,
    worktree_ctx: WorktreeContext,
}

impl ForeachParentCtx {
    fn from_state(
        state: &ExecutionState,
        child_runner: Arc<dyn crate::engine::ChildWorkflowRunner>,
    ) -> Self {
        Self {
            persistence: Arc::clone(&state.persistence),
            action_registry: Arc::clone(&state.action_registry),
            script_env_provider: Arc::clone(&state.script_env_provider),
            registry: Arc::clone(&state.registry),
            event_sinks: Arc::clone(&state.event_sinks),
            child_runner,
            workflow_run_id: state.workflow_run_id.clone(),
            workflow_name: state.workflow_name.clone(),
            model: state.model.clone(),
            exec_config: state.exec_config.clone(),
            parent_run_id: state.parent_run_id.clone(),
            depth: state.depth,
            target_label: state.target_label.clone(),
            default_bot_name: state.default_bot_name.clone(),
            worktree_ctx: state.worktree_ctx.clone(),
        }
    }

    fn make_child_state(&self, cancellation: CancellationToken) -> ExecutionState {
        ExecutionState {
            persistence: Arc::clone(&self.persistence),
            action_registry: Arc::clone(&self.action_registry),
            script_env_provider: Arc::clone(&self.script_env_provider),
            workflow_run_id: self.workflow_run_id.clone(),
            workflow_name: self.workflow_name.clone(),
            worktree_ctx: self.worktree_ctx.clone(),
            model: self.model.clone(),
            exec_config: self.exec_config.clone(),
            inputs: HashMap::new(),
            parent_run_id: self.parent_run_id.clone(),
            depth: self.depth,
            target_label: self.target_label.clone(),
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
            default_bot_name: self.default_bot_name.clone(),
            triggered_by_hook: false,
            schema_resolver: None,
            child_runner: Some(Arc::clone(&self.child_runner)),
            last_heartbeat_at: ExecutionState::new_heartbeat(),
            registry: Arc::clone(&self.registry),
            event_sinks: Arc::clone(&self.event_sinks),
            cancellation,
            current_execution_id: Arc::new(Mutex::new(None)),
        }
    }
}

/// DFS over `dependents_map` starting from `start`, collecting all transitively
/// reachable item IDs that are not yet terminal.
fn collect_transitive_dependents(
    start: &str,
    dependents_map: &HashMap<String, HashSet<String>>,
    terminal_ids: &HashSet<String>,
) -> HashSet<String> {
    let mut result = HashSet::new();
    let mut queue = vec![start.to_string()];
    while let Some(current) = queue.pop() {
        if let Some(children) = dependents_map.get(&current) {
            for child in children {
                if !terminal_ids.contains(child) && result.insert(child.clone()) {
                    queue.push(child.clone());
                }
            }
        }
    }
    result
}

/// Returns true if all declared dependencies of `item_id` are in `terminal_ids`.
fn is_eligible(
    item_id: &str,
    dep_map: &HashMap<String, HashSet<String>>,
    terminal_ids: &HashSet<String>,
) -> bool {
    dep_map
        .get(item_id)
        .map(|deps| deps.iter().all(|d| terminal_ids.contains(d)))
        .unwrap_or(true)
}

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
        .map_err(p_err)?;

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
        .map_err(p_err)?;

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
        .map_err(p_err)?;
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
                .map_err(p_err)?;
        }
    }

    let new_item_count = items
        .iter()
        .filter(|(_, id, _)| !existing_set.contains(id))
        .count();
    let total_items = existing_items.len() + new_item_count;

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
            .map_err(p_err)?;

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

    // Phase 2: Parallel dispatch loop

    let max_slots = if node.max_parallel == 0 {
        1
    } else {
        node.max_parallel as usize
    };

    let pending_items = state
        .persistence
        .get_fan_out_items(&step_id, Some(FanOutItemStatus::Pending))
        .map_err(p_err)?;

    // Build dependency maps when ordered execution is requested.
    // edges: (blocker_item_id, dependent_item_id) — blocker must finish before dependent starts.
    let (dep_map, dependents_map): (
        HashMap<String, HashSet<String>>,
        HashMap<String, HashSet<String>>,
    ) = if node.ordered && provider.supports_ordered() {
        let edges = provider.dependencies(&step_id)?;
        let mut dep: HashMap<String, HashSet<String>> = HashMap::new();
        let mut rev: HashMap<String, HashSet<String>> = HashMap::new();
        for (blocker, dependent) in edges {
            rev.entry(blocker.clone())
                .or_default()
                .insert(dependent.clone());
            dep.entry(dependent).or_default().insert(blocker);
        }
        (dep, rev)
    } else {
        (HashMap::new(), HashMap::new())
    };

    // Lookup maps built once from the initial pending set.
    let db_id_to_item_id: HashMap<String, String> = pending_items
        .iter()
        .map(|i| (i.id.clone(), i.item_id.clone()))
        .collect();
    let item_id_to_db_id: HashMap<String, String> = pending_items
        .iter()
        .map(|i| (i.item_id.clone(), i.id.clone()))
        .collect();
    let item_ref_map: HashMap<String, String> = pending_items
        .iter()
        .map(|i| (i.item_id.clone(), i.item_ref.clone()))
        .collect();

    // Channel: spawned threads send (fan_out_item_db_id, succeeded).
    let (tx, rx) = mpsc::channel::<(String, bool)>();

    // Snapshot the parent context once; shared via Arc across all thread spawns.
    let parent_ctx = Arc::new(ForeachParentCtx::from_state(
        state,
        Arc::clone(&child_runner),
    ));

    // Build the placeholder WorkflowDef once — same for every item.
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

    // Dispatch-loop tracking state
    let mut pending: Vec<crate::types::FanOutItemRow> = pending_items;
    let mut in_flight: usize = 0;
    let mut halt = false;
    // item_ids that have reached a terminal state (completed, failed, or skipped)
    let mut terminal_ids: HashSet<String> = HashSet::new();

    tracing::info!(
        "foreach '{}': starting parallel dispatch (max_slots={}, items={})",
        node.name,
        max_slots,
        pending.len(),
    );

    loop {
        // 1. When threads are in-flight, block briefly on the first result to yield
        //    the CPU instead of spinning. Then drain any additional ready results.
        let mut completed: Vec<(String, bool)> = Vec::new();
        if in_flight > 0 {
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(m) => completed.push(m),
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
            }
        }
        while let Ok(m) = rx.try_recv() {
            completed.push(m);
        }

        for (item_db_id, succeeded) in completed {
            in_flight -= 1;

            let item_id = db_id_to_item_id
                .get(&item_db_id)
                .cloned()
                .unwrap_or_default();
            let item_ref = item_ref_map.get(&item_id).cloned().unwrap_or_default();

            state
                .persistence
                .update_fan_out_item(
                    &item_db_id,
                    FanOutItemUpdate::Terminal {
                        status: if succeeded {
                            FanOutItemStatus::Completed
                        } else {
                            FanOutItemStatus::Failed
                        },
                    },
                )
                .map_err(p_err)?;

            emit_event(
                state,
                EngineEvent::FanOutItemCompleted {
                    item_id: item_id.clone(),
                    succeeded,
                },
            );

            if succeeded {
                tracing::info!("foreach '{}': item '{}' → completed", node.name, item_ref);
            } else {
                tracing::warn!("foreach '{}': item '{}' → failed", node.name, item_ref);
            }

            terminal_ids.insert(item_id.clone());

            if !succeeded {
                match node.on_child_fail {
                    OnChildFail::Halt => {
                        tracing::warn!(
                            "foreach '{}': on_child_fail=halt, stopping dispatch",
                            node.name
                        );
                        halt = true;
                    }
                    OnChildFail::SkipDependents => {
                        let to_skip =
                            collect_transitive_dependents(&item_id, &dependents_map, &terminal_ids);
                        for skip_id in &to_skip {
                            if let Some(skip_db_id) = item_id_to_db_id.get(skip_id) {
                                state
                                    .persistence
                                    .update_fan_out_item(
                                        skip_db_id,
                                        FanOutItemUpdate::Terminal {
                                            status: FanOutItemStatus::Skipped,
                                        },
                                    )
                                    .map_err(p_err)?;
                            }
                            terminal_ids.insert(skip_id.clone());
                        }
                        pending.retain(|i| !to_skip.contains(&i.item_id));
                        if !to_skip.is_empty() {
                            tracing::info!(
                                "foreach '{}': skipped {} dependents of '{}'",
                                node.name,
                                to_skip.len(),
                                item_id
                            );
                        }
                    }
                    OnChildFail::Continue => {}
                }
            }
        }

        // 2. Check parent cancellation.
        if state.cancellation.is_cancelled() {
            tracing::info!(
                "foreach '{}': cancelled — draining {} in-flight",
                node.name,
                in_flight
            );
            break;
        }

        // 3. Dispatch new items while slots are available and we're not halted.
        if !halt {
            while in_flight < max_slots {
                let eligible_pos = pending
                    .iter()
                    .position(|item| is_eligible(&item.item_id, &dep_map, &terminal_ids));

                let item = match eligible_pos {
                    Some(pos) => pending.swap_remove(pos),
                    None => break,
                };

                emit_event(
                    state,
                    EngineEvent::FanOutItemStarted {
                        item_id: item.item_id.clone(),
                    },
                );

                state
                    .persistence
                    .update_fan_out_item(
                        &item.id,
                        FanOutItemUpdate::Running {
                            child_run_id: "dispatching".to_string(),
                        },
                    )
                    .map_err(p_err)?;

                let mut child_inputs = node.inputs.clone();
                child_inputs.insert("item.id".to_string(), item.item_id.clone());
                child_inputs.insert("item.ref".to_string(), item.item_ref.clone());

                let ctx = Arc::clone(&parent_ctx);
                let def = placeholder_def.clone();
                let inputs = child_inputs;
                let item_db_id = item.id.clone();
                let child_cancellation = state.cancellation.child();
                let tx_clone = tx.clone();
                let depth = state.depth;

                thread::spawn(move || {
                    let child_state = ctx.make_child_state(child_cancellation.clone());
                    let succeeded = ctx
                        .child_runner
                        .execute_child(
                            &def,
                            &child_state,
                            ChildWorkflowInput {
                                inputs,
                                iteration,
                                bot_name: None,
                                depth: depth + 1,
                                parent_step_id: None,
                                cancellation: child_cancellation,
                            },
                        )
                        .map(|r| r.all_succeeded)
                        .unwrap_or_else(|e| {
                            tracing::error!(
                                item_db_id = %item_db_id,
                                error = %e,
                                "foreach: child workflow execution error; treating item as failed"
                            );
                            false
                        });

                    if let Err(e) = tx_clone.send((item_db_id, succeeded)) {
                        tracing::error!(
                            "foreach: result channel broken (main thread dropped): {}",
                            e
                        );
                    }
                });

                in_flight += 1;
            }
        }

        // 4. Exit when all work is done.
        let has_eligible = !halt
            && pending
                .iter()
                .any(|item| is_eligible(&item.item_id, &dep_map, &terminal_ids));

        if in_flight == 0 && !has_eligible {
            break;
        }
    }

    // Phase 3: Step completion
    let fan_out_items = state
        .persistence
        .get_fan_out_items(&step_id, None)
        .map_err(p_err)?;
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
            .map_err(p_err)?;

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
            .map_err(p_err)?;

        return record_step_failure(state, step_key, &node.name, error_msg, 1, true);
    }

    Ok(())
}
