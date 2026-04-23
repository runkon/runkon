use std::collections::HashSet;

use crate::dsl::{Condition, DoNode, DoWhileNode, IfNode, UnlessNode, WhileNode};
use crate::engine::{
    check_max_iterations, check_stuck, execute_nodes, execute_single_node, ExecutionState,
};
use crate::engine_error::Result;
use crate::helpers::find_max_completed_while_iteration;

pub fn eval_condition(state: &ExecutionState, condition: &Condition) -> bool {
    match condition {
        Condition::StepMarker { step, marker } => state
            .step_results
            .get(step)
            .map(|r| r.markers.iter().any(|m| m == marker))
            .unwrap_or(false),
        Condition::BoolInput { input } => state
            .inputs
            .get(input)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false),
    }
}

pub fn execute_if(state: &mut ExecutionState, node: &IfNode) -> Result<()> {
    let condition_met = eval_condition(state, &node.condition);

    if condition_met {
        tracing::info!(condition = ?node.condition, "if — condition met, executing body");
        execute_nodes(state, &node.body, true)?;
    } else {
        tracing::info!(condition = ?node.condition, "if — condition not met, skipping");
    }

    Ok(())
}

pub fn execute_unless(state: &mut ExecutionState, node: &UnlessNode) -> Result<()> {
    let condition_met = eval_condition(state, &node.condition);

    if !condition_met {
        tracing::info!(condition = ?node.condition, "unless — condition not met, executing body");
        execute_nodes(state, &node.body, true)?;
    } else {
        tracing::info!(condition = ?node.condition, "unless — condition met, skipping");
    }

    Ok(())
}

pub fn execute_while(state: &mut ExecutionState, node: &WhileNode) -> Result<()> {
    // On resume, determine the last completed iteration so we can fast-forward
    let start_iteration = if state.resume_ctx.is_some() {
        find_max_completed_while_iteration(state, node)
    } else {
        0u32
    };
    let mut iteration = start_iteration;
    let mut prev_marker_sets: Vec<HashSet<String>> = Vec::new();

    loop {
        // Check condition
        let has_marker = state
            .step_results
            .get(&node.step)
            .map(|r| r.markers.iter().any(|m| m == &node.marker))
            .unwrap_or(false);

        if !has_marker {
            tracing::info!(
                "while {}.{} — condition no longer met after {} iterations",
                node.step,
                node.marker,
                iteration
            );
            break;
        }

        if check_max_iterations(
            state,
            iteration,
            node.max_iterations,
            &node.on_max_iter,
            &node.step,
            &node.marker,
            "while",
        )? {
            break;
        }

        tracing::info!(
            "while {}.{} — iteration {}/{}",
            node.step,
            node.marker,
            iteration + 1,
            node.max_iterations
        );

        // Execute body
        for body_node in &node.body {
            execute_single_node(state, body_node, iteration)?;

            if !state.all_succeeded && state.exec_config.fail_fast {
                return Ok(());
            }
        }

        // Stuck detection
        if let Some(stuck_after) = node.stuck_after {
            check_stuck(
                state,
                &mut prev_marker_sets,
                &node.step,
                &node.marker,
                stuck_after,
                "while",
            )?;
        }

        iteration += 1;
    }

    Ok(())
}

pub fn execute_do_while(state: &mut ExecutionState, node: &DoWhileNode) -> Result<()> {
    let mut iteration = 0u32;
    let mut prev_marker_sets: Vec<HashSet<String>> = Vec::new();

    loop {
        if check_max_iterations(
            state,
            iteration,
            node.max_iterations,
            &node.on_max_iter,
            &node.step,
            &node.marker,
            "do",
        )? {
            break;
        }

        tracing::info!(
            "do {}.{} — iteration {}/{}",
            node.step,
            node.marker,
            iteration + 1,
            node.max_iterations
        );

        // Execute body first (do-while: body always runs before condition check)
        for body_node in &node.body {
            execute_single_node(state, body_node, iteration)?;

            if !state.all_succeeded && state.exec_config.fail_fast {
                return Ok(());
            }
        }

        // Check condition after body
        let has_marker = state
            .step_results
            .get(&node.step)
            .map(|r| r.markers.iter().any(|m| m == &node.marker))
            .unwrap_or(false);

        // Stuck detection
        if let Some(stuck_after) = node.stuck_after {
            check_stuck(
                state,
                &mut prev_marker_sets,
                &node.step,
                &node.marker,
                stuck_after,
                "do",
            )?;
        }

        if !has_marker {
            tracing::info!(
                "do {}.{} — condition no longer met after {} iterations",
                node.step,
                node.marker,
                iteration + 1
            );
            break;
        }

        iteration += 1;
    }

    Ok(())
}

pub fn execute_do(state: &mut ExecutionState, node: &DoNode) -> Result<()> {
    tracing::info!(
        "do block: executing {} body nodes sequentially",
        node.body.len()
    );

    // Save and apply block-level output/with so nested calls can inherit them
    let saved_output = state.block_output.clone();
    let saved_with = state.block_with.clone();

    if node.output.is_some() {
        state.block_output = node.output.clone();
    }
    if !node.with.is_empty() {
        let mut combined = node.with.clone();
        combined.extend(saved_with.iter().cloned());
        state.block_with = combined;
    }

    for body_node in &node.body {
        if let Err(e) = execute_single_node(state, body_node, 0) {
            state.block_output = saved_output;
            state.block_with = saved_with;
            return Err(e);
        }
        if !state.all_succeeded && state.exec_config.fail_fast {
            break;
        }
    }

    // Restore block-level context
    state.block_output = saved_output;
    state.block_with = saved_with;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{atomic::AtomicI64, Arc};

    use crate::dsl::{Condition, IfNode, UnlessNode};
    use crate::engine::{ExecutionState, WorktreeContext};
    use crate::persistence_memory::InMemoryWorkflowPersistence;
    use crate::traits::action_executor::ActionRegistry;
    use crate::traits::item_provider::ItemProviderRegistry;
    use crate::traits::persistence::{NewRun, WorkflowPersistence};
    use crate::traits::script_env_provider::NoOpScriptEnvProvider;
    use crate::types::{StepResult, WorkflowExecConfig};

    use super::{eval_condition, execute_if, execute_unless};

    fn make_state() -> ExecutionState {
        let persistence = Arc::new(InMemoryWorkflowPersistence::default());
        let new_run = NewRun {
            workflow_name: "test-wf".to_string(),
            worktree_id: None,
            ticket_id: None,
            repo_id: None,
            parent_run_id: "parent-1".to_string(),
            dry_run: false,
            trigger: "test".to_string(),
            definition_snapshot: None,
            parent_workflow_run_id: None,
            target_label: None,
        };
        let run = persistence.create_run(new_run).unwrap();
        ExecutionState {
            persistence,
            action_registry: Arc::new(ActionRegistry::new(Default::default(), None)),
            script_env_provider: Arc::new(NoOpScriptEnvProvider),
            workflow_run_id: run.id,
            workflow_name: "test-wf".to_string(),
            worktree_ctx: WorktreeContext {
                worktree_id: None,
                working_dir: "/tmp".to_string(),
                worktree_slug: "test".to_string(),
                repo_path: "/tmp".to_string(),
                ticket_id: None,
                repo_id: None,
                conductor_bin_dir: None,
                extra_plugin_dirs: vec![],
            },
            model: None,
            exec_config: WorkflowExecConfig::default(),
            inputs: Default::default(),
            parent_run_id: "parent-1".to_string(),
            depth: 0,
            target_label: None,
            step_results: Default::default(),
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
            last_heartbeat_at: Arc::new(AtomicI64::new(0)),
            registry: Arc::new(ItemProviderRegistry::default()),
            event_sinks: Arc::from(vec![]),
            cancellation: crate::cancellation::CancellationToken::new(),
            current_execution_id: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    fn make_step_result_with_marker(marker: &str) -> StepResult {
        StepResult {
            step_name: "step1".to_string(),
            status: crate::status::WorkflowStepStatus::Completed,
            result_text: None,
            cost_usd: None,
            num_turns: None,
            duration_ms: None,
            markers: vec![marker.to_string()],
            context: String::new(),
            child_run_id: None,
            structured_output: None,
            output_file: None,
        }
    }

    // ---- eval_condition tests ----

    #[test]
    fn eval_condition_step_marker_present_returns_true() {
        let mut state = make_state();
        state
            .step_results
            .insert("step1".to_string(), make_step_result_with_marker("done"));

        let condition = Condition::StepMarker {
            step: "step1".to_string(),
            marker: "done".to_string(),
        };
        assert!(eval_condition(&state, &condition));
    }

    #[test]
    fn eval_condition_step_marker_absent_returns_false() {
        let state = make_state();
        let condition = Condition::StepMarker {
            step: "step1".to_string(),
            marker: "done".to_string(),
        };
        assert!(!eval_condition(&state, &condition));
    }

    #[test]
    fn eval_condition_step_marker_wrong_marker_returns_false() {
        let mut state = make_state();
        state
            .step_results
            .insert("step1".to_string(), make_step_result_with_marker("other"));

        let condition = Condition::StepMarker {
            step: "step1".to_string(),
            marker: "done".to_string(),
        };
        assert!(!eval_condition(&state, &condition));
    }

    #[test]
    fn eval_condition_bool_input_true_returns_true() {
        let mut state = make_state();
        state.inputs.insert("flag".to_string(), "true".to_string());

        let condition = Condition::BoolInput {
            input: "flag".to_string(),
        };
        assert!(eval_condition(&state, &condition));
    }

    #[test]
    fn eval_condition_bool_input_case_insensitive_true() {
        let mut state = make_state();
        state.inputs.insert("flag".to_string(), "TRUE".to_string());

        let condition = Condition::BoolInput {
            input: "flag".to_string(),
        };
        assert!(eval_condition(&state, &condition));
    }

    #[test]
    fn eval_condition_bool_input_false_returns_false() {
        let mut state = make_state();
        state.inputs.insert("flag".to_string(), "false".to_string());

        let condition = Condition::BoolInput {
            input: "flag".to_string(),
        };
        assert!(!eval_condition(&state, &condition));
    }

    #[test]
    fn eval_condition_bool_input_missing_returns_false() {
        let state = make_state();
        let condition = Condition::BoolInput {
            input: "flag".to_string(),
        };
        assert!(!eval_condition(&state, &condition));
    }

    // ---- execute_if tests ----

    #[test]
    fn execute_if_condition_not_met_does_nothing() {
        let mut state = make_state();
        // No step results — condition will be false
        let node = IfNode {
            condition: Condition::StepMarker {
                step: "nonexistent".to_string(),
                marker: "done".to_string(),
            },
            body: vec![],
        };
        let result = execute_if(&mut state, &node);
        assert!(result.is_ok());
        // all_succeeded unchanged
        assert!(state.all_succeeded);
    }

    #[test]
    fn execute_if_condition_met_with_empty_body_succeeds() {
        let mut state = make_state();
        state
            .step_results
            .insert("step1".to_string(), make_step_result_with_marker("done"));

        let node = IfNode {
            condition: Condition::StepMarker {
                step: "step1".to_string(),
                marker: "done".to_string(),
            },
            body: vec![],
        };
        let result = execute_if(&mut state, &node);
        assert!(result.is_ok());
    }

    #[test]
    fn execute_if_bool_input_not_set_skips_body() {
        let mut state = make_state();
        let node = IfNode {
            condition: Condition::BoolInput {
                input: "run_extra".to_string(),
            },
            body: vec![],
        };
        let result = execute_if(&mut state, &node);
        assert!(result.is_ok());
        assert!(state.all_succeeded);
    }

    // ---- execute_unless tests ----

    #[test]
    fn execute_unless_condition_not_met_runs_body() {
        let mut state = make_state();
        // No step results — condition is false, so unless body should run
        let node = UnlessNode {
            condition: Condition::StepMarker {
                step: "nonexistent".to_string(),
                marker: "done".to_string(),
            },
            body: vec![],
        };
        let result = execute_unless(&mut state, &node);
        assert!(result.is_ok());
    }

    #[test]
    fn execute_unless_condition_met_skips_body() {
        let mut state = make_state();
        state
            .step_results
            .insert("step1".to_string(), make_step_result_with_marker("done"));

        let node = UnlessNode {
            condition: Condition::StepMarker {
                step: "step1".to_string(),
                marker: "done".to_string(),
            },
            body: vec![],
        };
        let result = execute_unless(&mut state, &node);
        assert!(result.is_ok());
        assert!(state.all_succeeded);
    }

    #[test]
    fn execute_unless_bool_input_true_skips_body() {
        let mut state = make_state();
        state
            .inputs
            .insert("skip_me".to_string(), "true".to_string());

        let node = UnlessNode {
            condition: Condition::BoolInput {
                input: "skip_me".to_string(),
            },
            body: vec![],
        };
        let result = execute_unless(&mut state, &node);
        assert!(result.is_ok());
        assert!(state.all_succeeded);
    }

    #[test]
    fn execute_unless_bool_input_false_runs_body() {
        let mut state = make_state();
        state
            .inputs
            .insert("skip_me".to_string(), "false".to_string());

        let node = UnlessNode {
            condition: Condition::BoolInput {
                input: "skip_me".to_string(),
            },
            body: vec![],
        };
        let result = execute_unless(&mut state, &node);
        assert!(result.is_ok());
        assert!(state.all_succeeded);
    }

    // ---- execute_script dry-run tests ----
    // These are in executors/script.rs but exercised via execute_if for coverage.
    // Direct dry-run tests live in script.rs.

    // Verify that inputs are correctly evaluated for bool conditions via HashMap lookup.
    #[test]
    fn eval_condition_bool_input_uses_inputs_map() {
        let mut inputs = HashMap::new();
        inputs.insert("enabled".to_string(), "true".to_string());
        let mut state = make_state();
        state.inputs = inputs;

        let condition = Condition::BoolInput {
            input: "enabled".to_string(),
        };
        assert!(eval_condition(&state, &condition));
    }
}
