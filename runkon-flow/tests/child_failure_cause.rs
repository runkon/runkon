//! A failed `call workflow` step records the child's failing step and its error.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use runkon_flow::cancellation_reason::CancellationReason;
use runkon_flow::dsl::{CallWorkflowNode, WorkflowDef, WorkflowNode};
use runkon_flow::engine::ExecutionState;
use runkon_flow::engine::{ChildWorkflowContext, ChildWorkflowInput, ChildWorkflowRunner};
use runkon_flow::engine_error::Result as EngineResult;
use runkon_flow::persistence_memory::InMemoryWorkflowPersistence;
use runkon_flow::status::{WorkflowRunStatus, WorkflowStepStatus};
use runkon_flow::traits::action_executor::ActionRegistry;
use runkon_flow::traits::persistence::{NewRun, NewStep, StepUpdate, WorkflowPersistence};
use runkon_flow::types::{WorkflowResult, WorkflowRun};
use runkon_flow::{FlowEngine, FlowEngineBuilder, RunFailureCause};

use common::{
    call_node, make_def, make_persistence, named_executors, ActionExecutor, FailingExecutor,
    MockExecutor,
};

/// The executors every run in this file can call.
fn test_executors() -> Vec<Box<dyn ActionExecutor>> {
    vec![
        Box::new(FailingExecutor),
        Box::new(MockExecutor::new("ok-agent")),
    ]
}

fn test_registry() -> Arc<ActionRegistry> {
    Arc::new(ActionRegistry::from_executors(
        named_executors(test_executors()),
        None,
    ))
}

fn test_engine() -> Arc<FlowEngine> {
    let builder = test_executors()
        .into_iter()
        .fold(FlowEngineBuilder::new(), |b, e| b.action(e));
    Arc::new(builder.build().expect("engine build"))
}

/// Create a run record; `parent` is the parent run id for child runs.
fn new_run(
    p: &InMemoryWorkflowPersistence,
    workflow_name: &str,
    parent: Option<&str>,
) -> WorkflowRun {
    p.create_run(NewRun {
        workflow_name: workflow_name.to_string(),
        parent_run_id: parent.unwrap_or_default().to_string(),
        parent_workflow_run_id: parent.map(str::to_string),
        dry_run: false,
        trigger: "test".to_string(),
        definition_snapshot: None,
    })
    .expect("create_run")
}

/// An `ExecutionState` for `run_id` with the shared test registry.
fn test_state(
    p: &Arc<InMemoryWorkflowPersistence>,
    run_id: &str,
    workflow_name: &str,
) -> ExecutionState {
    let mut state = runkon_flow::test_helpers::make_test_execution_state(
        Arc::clone(p) as Arc<dyn WorkflowPersistence>,
        run_id.to_string(),
    );
    state.action_registry = test_registry();
    state.workflow_name = workflow_name.to_string();
    state
}

/// Runs child workflows by name against one shared persistence; nested
/// `call workflow` steps recurse through the same runner.
struct RecursiveChildRunner {
    engine: Arc<FlowEngine>,
    persistence: Arc<InMemoryWorkflowPersistence>,
    defs: HashMap<String, WorkflowDef>,
}

impl ChildWorkflowRunner for RecursiveChildRunner {
    fn execute_child(
        &self,
        workflow_name: &str,
        parent_ctx: &ChildWorkflowContext,
        params: ChildWorkflowInput,
    ) -> EngineResult<WorkflowResult> {
        let def = self.defs.get(workflow_name).expect("unknown child def");
        let child_run = new_run(
            &self.persistence,
            workflow_name,
            Some(&parent_ctx.workflow_run_id),
        );
        let mut state = test_state(&self.persistence, &child_run.id, workflow_name);
        state.inputs = params.inputs;
        state.parent_run_id = parent_ctx.workflow_run_id.clone();
        state.depth = params.depth;
        state.cancellation = params.cancellation;
        state.child_runner = Some(Arc::new(RecursiveChildRunner {
            engine: Arc::clone(&self.engine),
            persistence: Arc::clone(&self.persistence),
            defs: self.defs.clone(),
        }));
        self.engine.run(def, &mut state)
    }

    fn resume_child(
        &self,
        _workflow_run_id: &str,
        _model: Option<&str>,
        _parent_ctx: &ChildWorkflowContext,
    ) -> EngineResult<WorkflowResult> {
        unimplemented!()
    }

    fn find_resumable_child(
        &self,
        _parent_run_id: &str,
        _workflow_name: &str,
    ) -> EngineResult<Option<WorkflowRun>> {
        Ok(None)
    }
}

fn call_wf(name: &str) -> WorkflowNode {
    WorkflowNode::CallWorkflow(CallWorkflowNode {
        workflow: name.to_string(),
        inputs: HashMap::new(),
        retries: 0,
        on_fail: None,
        as_identity: None,
    })
}

/// Run `top` (with `defs` as callable children); return the top-level result and the
/// persistence holding every run.
fn run_top(
    top: &WorkflowDef,
    defs: Vec<WorkflowDef>,
) -> (WorkflowResult, Arc<InMemoryWorkflowPersistence>) {
    let persistence = make_persistence();
    let engine = test_engine();
    let runner = RecursiveChildRunner {
        engine: Arc::clone(&engine),
        persistence: Arc::clone(&persistence),
        defs: defs.into_iter().map(|d| (d.name.clone(), d)).collect(),
    };
    let run = new_run(&persistence, &top.name, None);
    let mut state = test_state(&persistence, &run.id, &top.name);
    state.child_runner = Some(Arc::new(runner));
    let result = engine.run(top, &mut state).expect("run returns a result");
    (result, persistence)
}

fn workflow_step_error(p: &InMemoryWorkflowPersistence, run_id: &str, name: &str) -> String {
    p.get_steps(run_id)
        .unwrap()
        .into_iter()
        .find(|s| s.step_name == name && s.status == WorkflowStepStatus::Failed)
        .unwrap_or_else(|| panic!("no failed step {name}"))
        .step_error
        .expect("step_error set")
}

#[test]
fn parent_step_error_names_child_failing_step() {
    let child = make_def("child", vec![call_node("failing")]);
    let top = make_def("top", vec![call_wf("child")]);
    let (result, p) = run_top(&top, vec![child]);
    assert!(!result.all_succeeded);

    let err = workflow_step_error(&p, &result.workflow_run_id, "workflow:child");
    assert!(
        err.starts_with("Sub-workflow 'child' failed: failing: "),
        "{err}"
    );
    assert!(err.contains("intentional test failure"), "{err}");
}

#[test]
fn three_level_failure_chains_to_leaf_step() {
    let submit = make_def("submit-review", vec![call_node("failing")]);
    let review = make_def("review-pr", vec![call_wf("submit-review")]);
    let top = make_def("iterate-pr", vec![call_wf("review-pr")]);
    let (result, p) = run_top(&top, vec![submit, review]);

    let err = workflow_step_error(&p, &result.workflow_run_id, "workflow:review-pr");
    assert!(
        err.starts_with("Sub-workflow 'review-pr' failed: "),
        "{err}"
    );
    assert!(err.contains("submit-review"), "{err}");
    assert!(err.contains("failing"), "{err}");
    assert!(err.contains("intentional test failure"), "{err}");
}

#[test]
fn failed_run_carries_cause_extension_and_success_does_not() {
    let top = make_def("top", vec![call_node("failing")]);
    let (result, _) = run_top(&top, vec![]);
    let cause = result
        .extensions
        .get::<RunFailureCause>()
        .expect("cause on failed run");
    assert_eq!(cause.step_name.as_deref(), Some("failing"));
    assert!(cause.error.contains("intentional test failure"));

    let ok = make_def("ok", vec![call_node("ok-agent")]);
    let (result, _) = run_top(&ok, vec![]);
    assert!(result.all_succeeded);
    assert!(result.extensions.get::<RunFailureCause>().is_none());
}

// ---------------------------------------------------------------------------
// Engine fallbacks: body error and cancellation
// ---------------------------------------------------------------------------

/// Run `top` with no child runner, so a `call workflow` step aborts the body with an
/// `EngineError` before any step row is written.
fn run_without_child_runner(
    top: &WorkflowDef,
    cancel: Option<CancellationReason>,
) -> WorkflowResult {
    let persistence = make_persistence();
    let run = new_run(&persistence, &top.name, None);
    let mut state = test_state(&persistence, &run.id, &top.name);
    if let Some(reason) = cancel {
        state.cancellation.cancel(reason);
    }
    test_engine()
        .run(top, &mut state)
        .expect("run returns a result")
}

#[test]
fn body_error_without_failed_step_yields_cause_without_step_name() {
    let top = make_def("top", vec![call_wf("child")]);
    let result = run_without_child_runner(&top, None);
    assert!(!result.all_succeeded);
    let cause = result
        .extensions
        .get::<RunFailureCause>()
        .expect("cause from body error");
    assert_eq!(cause.step_name, None);
    assert!(
        cause.error.contains("no ChildWorkflowRunner"),
        "{}",
        cause.error
    );
}

#[test]
fn cancelled_run_carries_no_cause_extension() {
    let top = make_def("top", vec![call_wf("child")]);
    let result = run_without_child_runner(&top, Some(CancellationReason::UserRequested(None)));
    assert!(!result.all_succeeded);
    assert!(result.extensions.get::<RunFailureCause>().is_none());
}

// ---------------------------------------------------------------------------
// resolve_cause fallbacks and the resume path
// ---------------------------------------------------------------------------

/// A runner that returns a failed result with no extensions, as a runner that is not
/// the built-in engine would, for either a fresh or a resumed child.
struct StubFailingRunner {
    persistence: Arc<InMemoryWorkflowPersistence>,
    child_run_id: String,
    resumable: Option<WorkflowRun>,
}

impl StubFailingRunner {
    fn failed_result(&self) -> WorkflowResult {
        WorkflowResult {
            workflow_run_id: self.child_run_id.clone(),
            workflow_name: "child".to_string(),
            all_succeeded: false,
            total_duration_ms: 0,
            extensions: Default::default(),
        }
    }
}

impl ChildWorkflowRunner for StubFailingRunner {
    fn execute_child(
        &self,
        _workflow_name: &str,
        _parent_ctx: &ChildWorkflowContext,
        _params: ChildWorkflowInput,
    ) -> EngineResult<WorkflowResult> {
        Ok(self.failed_result())
    }

    fn resume_child(
        &self,
        _workflow_run_id: &str,
        _model: Option<&str>,
        _parent_ctx: &ChildWorkflowContext,
    ) -> EngineResult<WorkflowResult> {
        Ok(self.failed_result())
    }

    fn find_resumable_child(
        &self,
        _parent_run_id: &str,
        _workflow_name: &str,
    ) -> EngineResult<Option<WorkflowRun>> {
        let _ = &self.persistence;
        Ok(self.resumable.clone())
    }
}

/// Seed a failed step row on `run_id`.
fn seed_failed_step(p: &InMemoryWorkflowPersistence, run_id: &str, name: &str, err: &str) {
    let id = p
        .insert_step(NewStep {
            workflow_run_id: run_id.to_string(),
            step_name: name.to_string(),
            role: "actor".to_string(),
            can_commit: false,
            position: 0,
            iteration: 0,
            retry_count: Some(0),
            gate: None,
        })
        .unwrap();
    p.update_step(&id, StepUpdate::failed(0, err.to_string(), 0))
        .unwrap();
}

/// Run a top workflow calling `child` through a [`StubFailingRunner`] whose child run
/// `child_id` lives in `p`; return the parent's `workflow:child` step error.
fn stub_parent_error(
    p: Arc<InMemoryWorkflowPersistence>,
    child_run_id: &str,
    resumable: Option<WorkflowRun>,
) -> String {
    let top = make_def("top", vec![call_wf("child")]);
    let run = new_run(&p, "top", None);
    let mut state = test_state(&p, &run.id, "top");
    state.child_runner = Some(Arc::new(StubFailingRunner {
        persistence: Arc::clone(&p),
        child_run_id: child_run_id.to_string(),
        resumable,
    }));
    let result = test_engine()
        .run(&top, &mut state)
        .expect("run returns a result");
    assert!(!result.all_succeeded);
    workflow_step_error(&p, &result.workflow_run_id, "workflow:child")
}

#[test]
fn resolve_cause_falls_back_to_failed_step_in_store() {
    let p = make_persistence();
    let child = new_run(&p, "child", None);
    seed_failed_step(&p, &child.id, "leaf", "leaf exploded");
    let err = stub_parent_error(p, &child.id, None);
    assert_eq!(err, "Sub-workflow 'child' failed: leaf: leaf exploded");
}

#[test]
fn resolve_cause_falls_back_to_run_error() {
    let p = make_persistence();
    let child = new_run(&p, "child", None);
    p.update_run_status(
        &child.id,
        WorkflowRunStatus::Failed,
        None,
        Some("run level error"),
    )
    .unwrap();
    let err = stub_parent_error(p, &child.id, None);
    assert_eq!(err, "Sub-workflow 'child' failed: run level error");
}

#[test]
fn resolve_cause_none_gives_bare_message() {
    let p = make_persistence();
    let child = new_run(&p, "child", None);
    let err = stub_parent_error(p, &child.id, None);
    assert_eq!(err, "Sub-workflow 'child' failed");
}

#[test]
fn resolve_cause_unknown_child_run_gives_bare_message() {
    let err = stub_parent_error(make_persistence(), "no-such-run", None);
    assert_eq!(err, "Sub-workflow 'child' failed");
}

#[test]
fn resume_path_failure_appends_child_cause() {
    let p = make_persistence();
    let child = new_run(&p, "child", None);
    seed_failed_step(&p, &child.id, "leaf", "leaf exploded");
    let err = stub_parent_error(p, &child.id, Some(child.clone()));
    assert_eq!(
        err,
        "Sub-workflow 'child' failed (resume): leaf: leaf exploded"
    );
}
