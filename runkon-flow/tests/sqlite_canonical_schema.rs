//! End-to-end runs against the canonical SQLite schema with CHECK constraints
//! enforced (#169): every role the executors write must pass the role CHECK.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use runkon_flow::dsl::{
    AgentRef, ApprovalMode, GateNode, OnTimeout, ParallelNode, ScriptNode, WorkflowNode,
};
use runkon_flow::status::WorkflowStepStatus;
use runkon_flow::traits::action_executor::ActionRegistry;
use runkon_flow::traits::gate_resolver::{GateParams, GatePoll, GateResolver};
use runkon_flow::traits::persistence::{NewRun, WorkflowPersistence};
use runkon_flow::traits::run_context::RunContext;
use runkon_flow::types::WorkflowExecConfig;
use runkon_flow::{create_canonical_schema, FlowEngineBuilder, SqliteWorkflowPersistence};

use common::{call_node, make_def, named_executors, MockExecutor};

struct ApprovingResolver;

impl GateResolver for ApprovingResolver {
    fn gate_type(&self) -> &str {
        "human_approval"
    }
    fn poll(
        &self,
        _run_id: &str,
        _params: &GateParams,
        _ctx: &dyn RunContext,
    ) -> Result<GatePoll, runkon_flow::engine_error::EngineError> {
        Ok(GatePoll::Approved(None))
    }
}

fn approval_gate() -> WorkflowNode {
    WorkflowNode::Gate(GateNode {
        name: "approval".to_string(),
        gate_type: "human_approval".to_string(),
        prompt: None,
        min_approvals: 1,
        approval_mode: ApprovalMode::default(),
        timeout_secs: 30,
        on_timeout: OnTimeout::Fail,
        as_identity: None,
        quality_gate: None,
        options: None,
    })
}

fn script_node() -> WorkflowNode {
    WorkflowNode::Script(ScriptNode {
        name: "say".to_string(),
        run: "echo ok".to_string(),
        env: HashMap::new(),
        timeout: None,
        retries: 0,
        on_fail: None,
        as_identity: None,
    })
}

fn parallel_node(calls: &[&str]) -> WorkflowNode {
    WorkflowNode::Parallel(ParallelNode {
        fail_fast: true,
        min_success: None,
        calls: calls
            .iter()
            .map(|c| AgentRef::Name((*c).to_string()))
            .collect(),
        output: None,
        call_outputs: HashMap::new(),
        with: vec![],
        call_with: HashMap::new(),
        call_if: HashMap::new(),
        call_retries: HashMap::new(),
        call_require_output: Default::default(),
    })
}

/// Canonical schema on an in-memory connection with foreign keys on and CHECK
/// constraints enforced.
fn sqlite_persistence() -> Arc<SqliteWorkflowPersistence> {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
    create_canonical_schema(&conn).unwrap();
    let ignoring: i64 = conn
        .query_row("PRAGMA ignore_check_constraints", [], |r| r.get(0))
        .unwrap();
    assert_eq!(ignoring, 0, "CHECK constraints must be enforced");
    Arc::new(SqliteWorkflowPersistence::from_shared_connection(Arc::new(
        Mutex::new(conn),
    )))
}

fn run_workflow(
    wf_name: &str,
    body: Vec<WorkflowNode>,
    dry_run: bool,
) -> (
    runkon_flow::types::WorkflowResult,
    Vec<runkon_flow::types::WorkflowRunStep>,
) {
    let persistence = sqlite_persistence();
    let run = persistence
        .create_run(NewRun {
            workflow_name: wf_name.to_string(),
            parent_run_id: String::new(),
            dry_run,
            trigger: "test".to_string(),
            definition_snapshot: None,
            parent_workflow_run_id: None,
        })
        .unwrap();
    let mut state = runkon_flow::test_helpers::make_test_execution_state(
        Arc::clone(&persistence) as Arc<dyn WorkflowPersistence>,
        run.id,
    );
    state.action_registry = Arc::new(ActionRegistry::from_executors(
        named_executors(
            ["alpha", "beta", "gamma"]
                .map(|n| Box::new(MockExecutor::new(n)) as Box<dyn common::ActionExecutor>),
        ),
        None,
    ));
    state.workflow_name = wf_name.to_string();
    state.exec_config = WorkflowExecConfig {
        dry_run,
        ..WorkflowExecConfig::default()
    };

    let engine = FlowEngineBuilder::new()
        .gate_resolver(ApprovingResolver)
        .build()
        .expect("engine build failed");
    let def = make_def(wf_name, body);
    let result = engine.run(&def, &mut state).expect("run should succeed");
    let steps = persistence.get_steps(&result.workflow_run_id).unwrap();
    (result, steps)
}

#[test]
fn call_script_parallel_gate_run_against_canonical_sqlite() {
    let (result, steps) = run_workflow(
        "canonical-roles",
        vec![
            call_node("alpha"),
            script_node(),
            parallel_node(&["beta", "gamma"]),
            approval_gate(),
        ],
        false,
    );
    assert!(result.all_succeeded);
    assert!(
        steps
            .iter()
            .all(|s| s.status == WorkflowStepStatus::Completed),
        "all steps should complete: {steps:?}"
    );
    let mut roles: Vec<&str> = steps.iter().map(|s| s.role.as_str()).collect();
    roles.sort_unstable();
    assert_eq!(roles, ["actor", "actor", "actor", "gate", "script"]);
}

#[test]
fn dry_run_gate_records_reviewer_role_against_canonical_sqlite() {
    let (result, steps) = run_workflow("canonical-dry-gate", vec![approval_gate()], true);
    assert!(result.all_succeeded);
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].role, "reviewer");
}
