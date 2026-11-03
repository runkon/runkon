//! A failed `call workflow` step records the child's failing step and its error.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use runkon_flow::dsl::{CallWorkflowNode, WorkflowDef, WorkflowNode};
use runkon_flow::engine::{ChildWorkflowContext, ChildWorkflowInput, ChildWorkflowRunner};
use runkon_flow::engine_error::Result as EngineResult;
use runkon_flow::persistence_memory::InMemoryWorkflowPersistence;
use runkon_flow::status::WorkflowStepStatus;
use runkon_flow::traits::action_executor::ActionRegistry;
use runkon_flow::traits::persistence::{NewRun, WorkflowPersistence};
use runkon_flow::types::{WorkflowResult, WorkflowRun};
use runkon_flow::{FlowEngine, FlowEngineBuilder, RunFailureCause};

use common::{
    call_node, make_def, make_persistence, named_executors, ActionExecutor, FailingExecutor,
    MockExecutor,
};

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
        let child_run = self.persistence.create_run(NewRun {
            workflow_name: workflow_name.to_string(),
            parent_run_id: parent_ctx.workflow_run_id.clone(),
            parent_workflow_run_id: Some(parent_ctx.workflow_run_id.clone()),
            dry_run: false,
            trigger: "test".to_string(),
            definition_snapshot: None,
        })?;
        let mut state = runkon_flow::test_helpers::make_test_execution_state(
            Arc::clone(&self.persistence) as Arc<dyn WorkflowPersistence>,
            child_run.id.clone(),
        );
        state.action_registry = Arc::new(ActionRegistry::from_executors(
            named_executors([
                Box::new(FailingExecutor) as Box<dyn ActionExecutor>,
                Box::new(MockExecutor::new("ok-agent")) as Box<dyn ActionExecutor>,
            ]),
            None,
        ));
        state.workflow_name = workflow_name.to_string();
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
    let engine = Arc::new(
        FlowEngineBuilder::new()
            .action(Box::new(FailingExecutor))
            .action(Box::new(MockExecutor::new("ok-agent")))
            .build()
            .expect("engine build"),
    );
    let runner = RecursiveChildRunner {
        engine: Arc::clone(&engine),
        persistence: Arc::clone(&persistence),
        defs: defs.into_iter().map(|d| (d.name.clone(), d)).collect(),
    };
    let run = persistence
        .create_run(NewRun {
            workflow_name: top.name.clone(),
            parent_run_id: String::new(),
            parent_workflow_run_id: None,
            dry_run: false,
            trigger: "test".to_string(),
            definition_snapshot: None,
        })
        .unwrap();
    let mut state = runkon_flow::test_helpers::make_test_execution_state(
        Arc::clone(&persistence) as Arc<dyn WorkflowPersistence>,
        run.id.clone(),
    );
    state.action_registry = Arc::new(ActionRegistry::from_executors(
        named_executors([
            Box::new(FailingExecutor) as Box<dyn ActionExecutor>,
            Box::new(MockExecutor::new("ok-agent")) as Box<dyn ActionExecutor>,
        ]),
        None,
    ));
    state.workflow_name = top.name.clone();
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
