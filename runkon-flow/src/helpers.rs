use crate::dsl::{WhileNode, WorkflowNode};
use crate::status::WorkflowStepStatus;

// Forward declaration for ExecutionState — defined in engine.rs
use crate::engine::ExecutionState;

/// Build a human-readable summary of a workflow execution.
pub fn build_workflow_summary(state: &ExecutionState) -> String {
    let steps = state
        .persistence
        .get_steps(&state.workflow_run_id)
        .unwrap_or_default();

    let total = steps.len();
    let count_status =
        |status: WorkflowStepStatus| steps.iter().filter(|s| s.status == status).count();
    let completed = count_status(WorkflowStepStatus::Completed);
    let failed = count_status(WorkflowStepStatus::Failed);
    let skipped = count_status(WorkflowStepStatus::Skipped);
    let timed_out = count_status(WorkflowStepStatus::TimedOut);

    let mut lines = Vec::new();
    lines.push(format!(
        "Workflow '{}': {completed}/{total} steps completed{}{}{}",
        state.workflow_name,
        if failed > 0 {
            format!(", {failed} failed")
        } else {
            String::new()
        },
        if skipped > 0 {
            format!(", {skipped} skipped")
        } else {
            String::new()
        },
        if timed_out > 0 {
            format!(", {timed_out} timed out")
        } else {
            String::new()
        },
    ));

    for step in &steps {
        let marker = step.status.short_label();
        let iter_label = if step.iteration > 0 {
            format!(" (iter {})", step.iteration)
        } else {
            String::new()
        };
        let never_executed = step.status == WorkflowStepStatus::Failed && step.started_at.is_none();
        let step_note = if never_executed {
            " (never executed)"
        } else {
            ""
        };
        lines.push(format!(
            "  [{marker}] {}{iter_label}{step_note}",
            step.step_name
        ));
    }

    if state.all_succeeded {
        lines.push("Status: SUCCESS".to_string());
    } else {
        lines.push("Status: FAILED".to_string());
    }

    lines.join("\n")
}

/// Extract all leaf-node step keys from a workflow node.
///
/// Recurses into control-flow nodes (if/unless/while/always) and collects
/// keys from all trackable leaves: `Call`, `Parallel` agents, `Gate`, and
/// `CallWorkflow`.
pub fn collect_leaf_step_keys(node: &WorkflowNode) -> Vec<String> {
    match node {
        WorkflowNode::Call(c) => vec![c.agent.step_key()],
        WorkflowNode::Parallel(p) => p.calls.iter().map(|a| a.step_key()).collect(),
        WorkflowNode::Gate(g) => vec![g.name.clone()],
        WorkflowNode::CallWorkflow(cw) => vec![format!("workflow:{}", cw.workflow)],
        WorkflowNode::Script(s) => vec![s.name.clone()],
        WorkflowNode::If(n) => n.body.iter().flat_map(collect_leaf_step_keys).collect(),
        WorkflowNode::Unless(n) => n.body.iter().flat_map(collect_leaf_step_keys).collect(),
        WorkflowNode::While(n) => n.body.iter().flat_map(collect_leaf_step_keys).collect(),
        WorkflowNode::DoWhile(n) => n.body.iter().flat_map(collect_leaf_step_keys).collect(),
        WorkflowNode::Do(n) => n.body.iter().flat_map(collect_leaf_step_keys).collect(),
        WorkflowNode::Always(n) => n.body.iter().flat_map(collect_leaf_step_keys).collect(),
        WorkflowNode::ForEach(n) => vec![format!("foreach:{}", n.name)],
    }
}

/// Find the starting iteration for a while loop on resume.
///
/// Looks at the skip_completed set for step keys that match body nodes of the
/// while loop. Returns the max iteration that has all body nodes completed,
/// so the loop resumes from the iteration where it failed.
pub fn find_max_completed_while_iteration(state: &ExecutionState, node: &WhileNode) -> u32 {
    let skip_set = match state.resume_ctx {
        Some(ref ctx) => &ctx.skip_completed,
        None => return 0,
    };

    // Collect step keys from all trackable body nodes
    let body_keys: Vec<String> = node.body.iter().flat_map(collect_leaf_step_keys).collect();

    if body_keys.is_empty() {
        return 0;
    }

    // Find the highest iteration where all body nodes are completed
    let mut iter = 0u32;
    loop {
        let all_done = body_keys
            .iter()
            .all(|k| skip_set.contains(&(k.clone(), iter)));
        if !all_done {
            break;
        }
        iter += 1;
    }
    // iter is now the first incomplete iteration — start there
    iter
}

#[cfg(test)]
mod tests {
    use super::collect_leaf_step_keys;
    use crate::dsl::{
        AgentRef, AlwaysNode, CallNode, CallWorkflowNode, Condition, DoNode, ForEachNode, GateNode,
        GateType, IfNode, OnMaxIter, ParallelNode, ScriptNode, UnlessNode, WhileNode, WorkflowNode,
    };

    fn call_node(name: &str) -> WorkflowNode {
        WorkflowNode::Call(CallNode {
            agent: AgentRef::Name(name.to_string()),
            retries: 0,
            on_fail: None,
            output: None,
            with: vec![],
            bot_name: None,
            plugin_dirs: vec![],
            timeout: None,
        })
    }

    fn script_node(name: &str) -> WorkflowNode {
        WorkflowNode::Script(ScriptNode {
            name: name.to_string(),
            run: "echo hello".to_string(),
            env: Default::default(),
            timeout: None,
            retries: 0,
            on_fail: None,
            bot_name: None,
        })
    }

    // ---- collect_leaf_step_keys ----

    #[test]
    fn leaf_keys_from_call_node() {
        let node = call_node("plan");
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["plan".to_string()]);
    }

    #[test]
    fn leaf_keys_from_script_node() {
        let node = script_node("lint");
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["lint".to_string()]);
    }

    #[test]
    fn leaf_keys_from_call_workflow_node() {
        let node = WorkflowNode::CallWorkflow(CallWorkflowNode {
            workflow: "child-wf".to_string(),
            inputs: Default::default(),
            retries: 0,
            on_fail: None,
            bot_name: None,
        });
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["workflow:child-wf".to_string()]);
    }

    #[test]
    fn leaf_keys_from_parallel_node() {
        let node = WorkflowNode::Parallel(ParallelNode {
            fail_fast: true,
            min_success: None,
            calls: vec![
                AgentRef::Name("agent_a".to_string()),
                AgentRef::Name("agent_b".to_string()),
            ],
            output: None,
            call_outputs: Default::default(),
            with: vec![],
            call_with: Default::default(),
            call_if: Default::default(),
        });
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["agent_a".to_string(), "agent_b".to_string()]);
    }

    #[test]
    fn leaf_keys_from_gate_node() {
        let node = WorkflowNode::Gate(GateNode {
            name: "human_approval".to_string(),
            gate_type: GateType::HumanApproval,
            prompt: None,
            min_approvals: 1,
            approval_mode: Default::default(),
            timeout_secs: 0,
            on_timeout: crate::dsl::OnTimeout::Fail,
            bot_name: None,
            quality_gate: None,
            options: None,
        });
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["human_approval".to_string()]);
    }

    #[test]
    fn leaf_keys_from_foreach_node() {
        let node = WorkflowNode::ForEach(ForEachNode {
            name: "fan".to_string(),
            over: "tickets".to_string(),
            scope: None,
            filter: Default::default(),
            ordered: false,
            on_cycle: crate::dsl::OnCycle::Fail,
            max_parallel: 4,
            workflow: "child".to_string(),
            inputs: Default::default(),
            on_child_fail: crate::dsl::OnChildFail::Continue,
        });
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["foreach:fan".to_string()]);
    }

    #[test]
    fn leaf_keys_from_if_node_recurses_into_body() {
        let node = WorkflowNode::If(IfNode {
            condition: Condition::BoolInput {
                input: "flag".to_string(),
            },
            body: vec![call_node("inner_agent")],
        });
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["inner_agent".to_string()]);
    }

    #[test]
    fn leaf_keys_from_unless_node_recurses_into_body() {
        let node = WorkflowNode::Unless(UnlessNode {
            condition: Condition::StepMarker {
                step: "s".to_string(),
                marker: "m".to_string(),
            },
            body: vec![call_node("fallback")],
        });
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["fallback".to_string()]);
    }

    #[test]
    fn leaf_keys_from_while_node_recurses_into_body() {
        let node = WorkflowNode::While(WhileNode {
            step: "s".to_string(),
            marker: "m".to_string(),
            max_iterations: 5,
            stuck_after: None,
            on_max_iter: OnMaxIter::Fail,
            body: vec![call_node("loop_agent")],
        });
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["loop_agent".to_string()]);
    }

    #[test]
    fn leaf_keys_from_do_node_recurses_into_body() {
        let node = WorkflowNode::Do(DoNode {
            output: None,
            with: vec![],
            body: vec![call_node("step_a"), script_node("step_b")],
        });
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["step_a".to_string(), "step_b".to_string()]);
    }

    #[test]
    fn leaf_keys_from_always_node_recurses_into_body() {
        let node = WorkflowNode::Always(AlwaysNode {
            body: vec![call_node("cleanup")],
        });
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["cleanup".to_string()]);
    }

    #[test]
    fn leaf_keys_empty_body_returns_empty() {
        let node = WorkflowNode::If(IfNode {
            condition: Condition::BoolInput {
                input: "x".to_string(),
            },
            body: vec![],
        });
        let keys = collect_leaf_step_keys(&node);
        assert!(keys.is_empty());
    }

    #[test]
    fn leaf_keys_path_agent_uses_file_stem() {
        let node = WorkflowNode::Call(CallNode {
            agent: AgentRef::Path(".claude/agents/plan.md".to_string()),
            retries: 0,
            on_fail: None,
            output: None,
            with: vec![],
            bot_name: None,
            plugin_dirs: vec![],
            timeout: None,
        });
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["plan".to_string()]);
    }
}
