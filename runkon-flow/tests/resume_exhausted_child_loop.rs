//! A parent resumed after its `call workflow` child exhausted a `do … while` loop
//! re-runs the child's loop with a fresh budget and continues past it (#133).

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use runkon_flow::dsl::{CallWorkflowNode, DoWhileNode, OnMaxIter, WorkflowDef, WorkflowNode};
use runkon_flow::engine::ExecutionState;
use runkon_flow::engine::{ChildWorkflowContext, ChildWorkflowInput, ChildWorkflowRunner};
use runkon_flow::engine_error::{EngineError, Result as EngineResult};
use runkon_flow::persistence_memory::InMemoryWorkflowPersistence;
use runkon_flow::status::WorkflowStepStatus;
use runkon_flow::traits::action_executor::{
    ActionExecutor, ActionOutput, ActionParams, ActionRegistry, StepInfo,
};
use runkon_flow::traits::persistence::{NewRun, WorkflowPersistence};
use runkon_flow::traits::run_context::RunContext;
use runkon_flow::types::{WorkflowResult, WorkflowRun};
use runkon_flow::{FlowEngine, FlowEngineBuilder};

use common::{call_node, make_def, make_persistence, named_executors, MockExecutor};

/// Emits `has_errors` while `broken` is set and counts its runs.
struct AnalyzeExecutor {
    broken: Arc<AtomicBool>,
    runs: Arc<AtomicUsize>,
}

impl ActionExecutor for AnalyzeExecutor {
    fn name(&self) -> &str {
        "analyze"
    }

    fn execute(
        &self,
        _: &dyn RunContext,
        _: &StepInfo,
        _: &ActionParams,
    ) -> Result<ActionOutput, EngineError> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        let markers = if self.broken.load(Ordering::SeqCst) {
            vec!["has_errors".to_string()]
        } else {
            vec![]
        };
        Ok(ActionOutput {
            markers,
            ..Default::default()
        })
    }
}

struct Harness {
    engine: Arc<FlowEngine>,
    persistence: Arc<InMemoryWorkflowPersistence>,
    broken: Arc<AtomicBool>,
    analyze_runs: Arc<AtomicUsize>,
    child_def: WorkflowDef,
    child_run_id: Mutex<Option<String>>,
}

impl Harness {
    fn executors(&self) -> Vec<Box<dyn ActionExecutor>> {
        vec![
            Box::new(AnalyzeExecutor {
                broken: Arc::clone(&self.broken),
                runs: Arc::clone(&self.analyze_runs),
            }),
            Box::new(MockExecutor::new("after")),
        ]
    }

    fn state(&self, run_id: &str, workflow_name: &str) -> ExecutionState {
        let mut state = runkon_flow::test_helpers::make_test_execution_state(
            Arc::clone(&self.persistence) as Arc<dyn WorkflowPersistence>,
            run_id.to_string(),
        );
        state.action_registry = Arc::new(ActionRegistry::from_executors(
            named_executors(self.executors()),
            None,
        ));
        state.workflow_name = workflow_name.to_string();
        state
    }
}

struct ResumingRunner(Arc<Harness>);

impl ChildWorkflowRunner for ResumingRunner {
    fn execute_child(
        &self,
        workflow_name: &str,
        parent_ctx: &ChildWorkflowContext,
        _params: ChildWorkflowInput,
    ) -> EngineResult<WorkflowResult> {
        let h = &self.0;
        let run = h.persistence.create_run(NewRun {
            workflow_name: workflow_name.to_string(),
            parent_run_id: parent_ctx.workflow_run_id.clone(),
            parent_workflow_run_id: Some(parent_ctx.workflow_run_id.clone()),
            dry_run: false,
            trigger: "test".to_string(),
            definition_snapshot: None,
        })?;
        *h.child_run_id.lock().unwrap() = Some(run.id.clone());
        let mut state = h.state(&run.id, workflow_name);
        h.engine.run(&h.child_def, &mut state)
    }

    fn resume_child(
        &self,
        workflow_run_id: &str,
        _model: Option<&str>,
        _parent_ctx: &ChildWorkflowContext,
    ) -> EngineResult<WorkflowResult> {
        let h = &self.0;
        let mut state = h.state(workflow_run_id, "child");
        h.engine.resume(&h.child_def, &mut state)
    }

    fn find_resumable_child(
        &self,
        _parent_run_id: &str,
        _workflow_name: &str,
    ) -> EngineResult<Option<WorkflowRun>> {
        let id = self.0.child_run_id.lock().unwrap().clone();
        match id {
            Some(id) => self.0.persistence.get_run(&id),
            None => Ok(None),
        }
    }
}

#[test]
fn parent_resumed_after_child_exhausted_loop_reruns_it_and_continues() {
    let broken = Arc::new(AtomicBool::new(true));
    let analyze_runs = Arc::new(AtomicUsize::new(0));
    let persistence = make_persistence();

    let child_def = make_def(
        "child",
        vec![WorkflowNode::DoWhile(DoWhileNode {
            step: "analyze".to_string(),
            marker: "has_errors".to_string(),
            max_iterations: 2,
            stuck_after: None,
            on_max_iter: OnMaxIter::Fail,
            body: vec![call_node("analyze")],
        })],
    );
    let parent_def = make_def(
        "parent",
        vec![
            WorkflowNode::CallWorkflow(CallWorkflowNode {
                workflow: "child".to_string(),
                inputs: HashMap::new(),
                retries: 0,
                on_fail: None,
                as_identity: None,
            }),
            call_node("after"),
        ],
    );

    let executors = |b: &Arc<AtomicBool>, r: &Arc<AtomicUsize>| -> Vec<Box<dyn ActionExecutor>> {
        vec![
            Box::new(AnalyzeExecutor {
                broken: Arc::clone(b),
                runs: Arc::clone(r),
            }),
            Box::new(MockExecutor::new("after")),
        ]
    };
    let engine = Arc::new(
        executors(&broken, &analyze_runs)
            .into_iter()
            .fold(FlowEngineBuilder::new(), |b, e| b.action(e))
            .build()
            .expect("engine build"),
    );
    let harness = Arc::new(Harness {
        engine: Arc::clone(&engine),
        persistence: Arc::clone(&persistence),
        broken: Arc::clone(&broken),
        analyze_runs: Arc::clone(&analyze_runs),
        child_def,
        child_run_id: Mutex::new(None),
    });

    let parent_run = persistence
        .create_run(NewRun {
            workflow_name: "parent".to_string(),
            parent_run_id: String::new(),
            parent_workflow_run_id: None,
            dry_run: false,
            trigger: "test".to_string(),
            definition_snapshot: None,
        })
        .unwrap();
    let mut state = harness.state(&parent_run.id, "parent");
    state.child_runner = Some(Arc::new(ResumingRunner(Arc::clone(&harness))));
    let result = engine.run(&parent_def, &mut state).unwrap();
    assert!(!result.all_succeeded, "child exhausts max_iterations");
    assert_eq!(analyze_runs.load(Ordering::SeqCst), 2);
    let child_id = harness.child_run_id.lock().unwrap().clone().unwrap();

    // The manual fix, then a resume of the parent.
    broken.store(false, Ordering::SeqCst);
    persistence.expire_lease(&parent_run.id);
    persistence.expire_lease(&child_id);
    let mut state = harness.state(&parent_run.id, "parent");
    state.child_runner = Some(Arc::new(ResumingRunner(Arc::clone(&harness))));
    let result = engine.resume(&parent_def, &mut state).unwrap();

    assert!(result.all_succeeded, "resumed parent completes");
    assert_eq!(
        analyze_runs.load(Ordering::SeqCst),
        3,
        "the child's loop ran once more"
    );
    assert_eq!(
        harness.child_run_id.lock().unwrap().clone().unwrap(),
        child_id,
        "the prior child was resumed, not replaced"
    );
    let steps = persistence.get_steps(&parent_run.id).unwrap();
    assert!(steps
        .iter()
        .any(|s| s.step_name == "after" && s.status == WorkflowStepStatus::Completed));
    assert!(steps
        .iter()
        .any(|s| s.step_name == "workflow:child" && s.status == WorkflowStepStatus::Completed));
}
