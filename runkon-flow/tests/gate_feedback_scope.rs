//! #188: `{{gate_feedback}}` is always the most recent gate's value, live and after a
//! resume, and `{{<gate>.feedback}}` reads a named gate's value.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use runkon_flow::dsl::{WorkflowDef, WorkflowNode};
use runkon_flow::engine::ExecutionState;
use runkon_flow::engine_error::EngineError;
use runkon_flow::persistence_memory::InMemoryWorkflowPersistence;
use runkon_flow::prompt_builder::build_variable_map;
use runkon_flow::traits::action_executor::{
    ActionExecutor, ActionOutput, ActionParams, ActionRegistry, StepInfo,
};
use runkon_flow::traits::gate_resolver::{GateParams, GatePoll, GateResolver};
use runkon_flow::traits::persistence::{GateStepInfo, NewRun, NewStep, WorkflowPersistence};
use runkon_flow::traits::run_context::RunContext;
use runkon_flow::traits::GateApprovalStore;
use runkon_flow::types::WorkflowExecConfig;
use runkon_flow::FlowEngineBuilder;

use common::{call_node, gate_node, make_def, make_persistence, named_executors};

/// Decides each gate by name: `Ok(feedback)` approves, `Err(reason)` rejects.
struct ByNameResolver {
    decisions: HashMap<String, Result<Option<String>, String>>,
}

impl GateResolver for ByNameResolver {
    fn gate_type(&self) -> &str {
        "human_approval"
    }
    fn poll(
        &self,
        _run_id: &str,
        params: &GateParams,
        _ctx: &dyn RunContext,
    ) -> Result<GatePoll, EngineError> {
        Ok(match self.decisions.get(&params.gate_name) {
            Some(Ok(fb)) => GatePoll::Approved(fb.clone()),
            Some(Err(reason)) => GatePoll::Rejected(reason.clone()),
            None => GatePoll::Pending,
        })
    }
}

/// Records the `gate_feedback` each dispatch received, keyed by step name.
struct Recorder {
    name: &'static str,
    seen: Arc<Mutex<HashMap<String, Option<String>>>>,
}

impl ActionExecutor for Recorder {
    fn name(&self) -> &str {
        self.name
    }
    fn execute(
        &self,
        _: &dyn RunContext,
        _: &StepInfo,
        params: &ActionParams,
    ) -> Result<ActionOutput, EngineError> {
        self.seen
            .lock()
            .unwrap()
            .insert(self.name.to_string(), params.gate_feedback.clone());
        Ok(ActionOutput::default())
    }
}

type Seen = Arc<Mutex<HashMap<String, Option<String>>>>;

fn waiting_gate(name: &str) -> WorkflowNode {
    let WorkflowNode::Gate(mut g) = gate_node(name) else {
        unreachable!()
    };
    g.timeout_secs = 60;
    WorkflowNode::Gate(g)
}

/// first, `between`, second, `after`.
fn def() -> WorkflowDef {
    make_def(
        "wf",
        vec![
            waiting_gate("first"),
            call_node("between"),
            waiting_gate("second"),
            call_node("after"),
        ],
    )
}

fn engine(decisions: HashMap<String, Result<Option<String>, String>>) -> runkon_flow::FlowEngine {
    FlowEngineBuilder::new()
        .gate_resolver(ByNameResolver { decisions })
        .build()
        .unwrap()
}

fn state_for(p: &Arc<InMemoryWorkflowPersistence>, run_id: &str, seen: &Seen) -> ExecutionState {
    let mut state = runkon_flow::test_helpers::make_test_execution_state(
        Arc::clone(p) as Arc<dyn WorkflowPersistence>,
        run_id.to_string(),
    );
    let execs: Vec<Box<dyn ActionExecutor>> = ["between", "after"]
        .into_iter()
        .map(|name| {
            Box::new(Recorder {
                name,
                seen: Arc::clone(seen),
            }) as Box<dyn ActionExecutor>
        })
        .collect();
    state.action_registry = Arc::new(ActionRegistry::from_executors(named_executors(execs), None));
    state.workflow_name = "wf".to_string();
    state.exec_config = WorkflowExecConfig {
        poll_interval: Duration::from_millis(1),
        ..WorkflowExecConfig::default()
    };
    state
}

fn new_run(p: &InMemoryWorkflowPersistence) -> String {
    p.create_run(NewRun {
        workflow_name: "wf".to_string(),
        parent_run_id: String::new(),
        parent_workflow_run_id: None,
        dry_run: false,
        trigger: "test".to_string(),
        definition_snapshot: None,
    })
    .unwrap()
    .id
}

/// Seed a gate row already decided, as a run parked before this point would have it.
fn seed_decided_gate(
    p: &InMemoryWorkflowPersistence,
    run_id: &str,
    name: &str,
    pos: i64,
    feedback: Option<&str>,
    selections: Option<&[String]>,
) {
    let id = p
        .insert_step(NewStep {
            workflow_run_id: run_id.to_string(),
            step_name: name.to_string(),
            role: "gate".to_string(),
            can_commit: false,
            position: pos,
            iteration: 0,
            retry_count: None,
            gate: Some(GateStepInfo {
                gate_type: "human_approval".to_string(),
                prompt: None,
                timeout_secs: 60,
                options: vec![],
            }),
        })
        .unwrap();
    p.approve_gate(&id, "tester", feedback, selections).unwrap();
}

fn decisions(pairs: &[(&str, Option<&str>)]) -> HashMap<String, Result<Option<String>, String>> {
    pairs
        .iter()
        .map(|(n, fb)| (n.to_string(), Ok(fb.map(String::from))))
        .collect()
}

#[test]
fn second_gate_without_feedback_clears_the_first_gates_value() {
    let p = make_persistence();
    let run_id = new_run(&p);
    let seen: Seen = Arc::default();
    let mut state = state_for(&p, &run_id, &seen);

    let result = engine(decisions(&[
        ("first", Some("edited draft")),
        ("second", None),
    ]))
    .run(&def(), &mut state)
    .unwrap();
    assert!(result.all_succeeded);

    let seen = seen.lock().unwrap();
    assert_eq!(seen["between"].as_deref(), Some("edited draft"));
    assert_eq!(seen["after"], None);
    assert_eq!(state.last_gate_feedback, None);
    let vars = build_variable_map(&state);
    assert!(!vars.contains_key("gate_feedback"));
    assert_eq!(vars["first.feedback"], "edited draft");
    assert_eq!(vars["second.feedback"], "");
}

#[test]
fn resume_between_the_gates_keeps_the_scope() {
    let p = make_persistence();
    let run_id = new_run(&p);
    seed_decided_gate(&p, &run_id, "first", 0, Some("edited draft"), None);
    let seen: Seen = Arc::default();
    let mut state = state_for(&p, &run_id, &seen);

    let result = engine(decisions(&[("second", None)]))
        .resume(&def(), &mut state)
        .unwrap();
    assert!(result.all_succeeded);

    assert_eq!(seen.lock().unwrap()["after"], None);
    let vars = build_variable_map(&state);
    assert!(!vars.contains_key("gate_feedback"));
    assert_eq!(vars["first.feedback"], "edited draft");
    assert_eq!(vars["second.feedback"], "");
}

#[test]
fn resume_past_both_decided_gates_lets_the_last_row_decide() {
    let p = make_persistence();
    let run_id = new_run(&p);
    seed_decided_gate(&p, &run_id, "first", 0, Some("edited draft"), None);
    seed_decided_gate(&p, &run_id, "second", 2, None, None);
    let seen: Seen = Arc::default();
    let mut state = state_for(&p, &run_id, &seen);

    // `between` has no row, so it runs; `after` must still see no feedback.
    let result = engine(HashMap::new()).resume(&def(), &mut state).unwrap();
    assert!(result.all_succeeded);

    assert_eq!(seen.lock().unwrap()["after"], None);
    let vars = build_variable_map(&state);
    assert_eq!(vars["first.feedback"], "edited draft");
    assert_eq!(vars["second.feedback"], "");
}

#[test]
fn resumed_gate_selections_are_restored_joined() {
    let p = make_persistence();
    let run_id = new_run(&p);
    let selections = vec!["a".to_string(), "b".to_string()];
    seed_decided_gate(&p, &run_id, "first", 0, Some("typed"), Some(&selections));
    seed_decided_gate(&p, &run_id, "second", 2, None, None);
    let seen: Seen = Arc::default();
    let mut state = state_for(&p, &run_id, &seen);

    engine(HashMap::new()).resume(&def(), &mut state).unwrap();

    assert_eq!(seen.lock().unwrap()["between"].as_deref(), Some("a, b"));
    assert_eq!(build_variable_map(&state)["first.feedback"], "a, b");
}

#[test]
fn rejection_reason_replaces_an_earlier_gates_value() {
    let p = make_persistence();
    let run_id = new_run(&p);
    let seen: Seen = Arc::default();
    let mut state = state_for(&p, &run_id, &seen);
    let mut d = decisions(&[("first", Some("edited draft"))]);
    d.insert("second".to_string(), Err("not good enough".to_string()));

    let result = engine(d).run(&def(), &mut state).unwrap();
    assert!(!result.all_succeeded);

    assert_eq!(state.last_gate_feedback.as_deref(), Some("not good enough"));
    let vars = build_variable_map(&state);
    assert_eq!(vars["second.feedback"], "not good enough");
    assert_eq!(vars["first.feedback"], "edited draft");
}

/// Seed a completed gate row with no gate_type, as a dry-run auto-approval writes it.
fn seed_dry_run_gate_row(p: &InMemoryWorkflowPersistence, run_id: &str, name: &str, pos: i64) {
    let id = p
        .insert_step(NewStep {
            workflow_run_id: run_id.to_string(),
            step_name: name.to_string(),
            role: "reviewer".to_string(),
            can_commit: false,
            position: pos,
            iteration: 0,
            retry_count: None,
            gate: None,
        })
        .unwrap();
    p.approve_gate(&id, "dry-run", None, None).unwrap();
}

fn timeout_def(on_timeout: runkon_flow::dsl::OnTimeout) -> WorkflowDef {
    let WorkflowNode::Gate(mut g) = gate_node("second") else {
        unreachable!()
    };
    g.timeout_secs = 0;
    g.on_timeout = on_timeout;
    make_def(
        "wf",
        vec![
            waiting_gate("first"),
            call_node("between"),
            WorkflowNode::Gate(g),
            call_node("after"),
        ],
    )
}

#[test]
fn timeout_continue_clears_the_earlier_gates_value() {
    let p = make_persistence();
    let run_id = new_run(&p);
    let seen: Seen = Arc::default();
    let mut state = state_for(&p, &run_id, &seen);

    let result = engine(decisions(&[("first", Some("edited draft"))]))
        .run(
            &timeout_def(runkon_flow::dsl::OnTimeout::Continue),
            &mut state,
        )
        .unwrap();
    assert!(result.all_succeeded);

    assert_eq!(seen.lock().unwrap()["after"], None);
    let vars = build_variable_map(&state);
    assert!(!vars.contains_key("gate_feedback"));
    assert_eq!(vars["second.feedback"], "");
}

#[test]
fn timeout_fail_clears_the_earlier_gates_value() {
    let p = make_persistence();
    let run_id = new_run(&p);
    let seen: Seen = Arc::default();
    let mut state = state_for(&p, &run_id, &seen);

    let result = engine(decisions(&[("first", Some("edited draft"))]))
        .run(&timeout_def(runkon_flow::dsl::OnTimeout::Fail), &mut state)
        .unwrap();
    assert!(!result.all_succeeded);

    assert_eq!(state.last_gate_feedback, None);
    assert_eq!(build_variable_map(&state)["second.feedback"], "");
}

#[test]
fn dry_run_gate_clears_the_earlier_gates_value() {
    let p = make_persistence();
    let run_id = new_run(&p);
    let seen: Seen = Arc::default();
    let mut state = state_for(&p, &run_id, &seen);
    state.exec_config.dry_run = true;
    state.last_gate_feedback = Some("stale".to_string());

    let d = make_def("wf", vec![waiting_gate("second"), call_node("after")]);
    let result = engine(HashMap::new()).run(&d, &mut state).unwrap();
    assert!(result.all_succeeded);

    assert_eq!(seen.lock().unwrap()["after"], None);
    let vars = build_variable_map(&state);
    assert!(!vars.contains_key("gate_feedback"));
    assert_eq!(vars["second.feedback"], "");
}

#[test]
fn resume_past_a_dry_run_gate_row_clears_the_earlier_gates_value() {
    let p = make_persistence();
    let run_id = new_run(&p);
    seed_decided_gate(&p, &run_id, "first", 0, Some("edited draft"), None);
    seed_dry_run_gate_row(&p, &run_id, "second", 2);
    let seen: Seen = Arc::default();
    let mut state = state_for(&p, &run_id, &seen);

    let result = engine(HashMap::new()).resume(&def(), &mut state).unwrap();
    assert!(result.all_succeeded);

    assert_eq!(seen.lock().unwrap()["after"], None);
    let vars = build_variable_map(&state);
    assert!(!vars.contains_key("gate_feedback"));
    assert_eq!(vars["first.feedback"], "edited draft");
    assert_eq!(vars["second.feedback"], "");
}
