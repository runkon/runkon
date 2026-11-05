//! On resume, the first attempt of a step whose earlier attempt failed carries a
//! `PriorStepAttempt` in `ActionParams.extensions`, so an agent executor can reuse that
//! attempt's output instead of re-running it (#143).

mod common;

use std::sync::{Arc, Mutex};

use runkon_flow::dsl::{AgentRef, ParallelNode, WorkflowNode};
use runkon_flow::engine_error::EngineError;
use runkon_flow::extensions::PriorStepAttempt;
use runkon_flow::persistence_memory::InMemoryWorkflowPersistence;
use runkon_flow::status::WorkflowStepStatus;
use runkon_flow::traits::action_executor::{
    ActionExecutor, ActionOutput, ActionParams, ActionRegistry, StepInfo,
};
use runkon_flow::traits::persistence::{NewRun, NewStep, StepUpdate, WorkflowPersistence};
use runkon_flow::traits::run_context::RunContext;
use runkon_flow::FlowEngineBuilder;

use common::{call_node, make_def, make_persistence, named_executors};

/// Records the `PriorStepAttempt` (if any) of every dispatch; fails the first
/// `fail_times` dispatches.
struct Recorder {
    name: &'static str,
    seen: Arc<Mutex<Vec<Option<String>>>>,
    fail_times: usize,
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
        let mut seen = self.seen.lock().unwrap();
        seen.push(
            params
                .extensions
                .get::<PriorStepAttempt>()
                .map(|p| p.child_run_id.clone()),
        );
        if seen.len() <= self.fail_times {
            return Err(EngineError::Workflow("boom".to_string()));
        }
        Ok(ActionOutput::default())
    }
}

fn new_run(p: &InMemoryWorkflowPersistence, name: &str) -> String {
    p.create_run(NewRun {
        workflow_name: name.to_string(),
        parent_run_id: String::new(),
        parent_workflow_run_id: None,
        dry_run: false,
        trigger: "test".to_string(),
        definition_snapshot: None,
    })
    .unwrap()
    .id
}

/// Seed a step row for `step` at `pos` with the given terminal status.
fn seed(
    p: &InMemoryWorkflowPersistence,
    run_id: &str,
    step: &str,
    pos: i64,
    status: WorkflowStepStatus,
    child: Option<&str>,
) {
    let id = p
        .insert_step(NewStep {
            workflow_run_id: run_id.to_string(),
            step_name: step.to_string(),
            role: "actor".to_string(),
            can_commit: false,
            position: pos,
            iteration: 0,
            retry_count: Some(0),
            gate: None,
        })
        .unwrap();
    p.update_step(
        &id,
        StepUpdate {
            generation: 0,
            status,
            child_run_id: child.map(String::from),
            result_text: Some("structured output validation: bad".to_string()),
            context_out: None,
            markers_out: None,
            retry_count: None,
            structured_output: None,
            step_error: None,
        },
    )
    .unwrap();
}

fn resume(
    p: &Arc<InMemoryWorkflowPersistence>,
    run_id: &str,
    def: &runkon_flow::dsl::WorkflowDef,
    execs: Vec<Box<dyn ActionExecutor>>,
) -> runkon_flow::types::WorkflowResult {
    let mut state = runkon_flow::test_helpers::make_test_execution_state(
        Arc::clone(p) as Arc<dyn WorkflowPersistence>,
        run_id.to_string(),
    );
    state.action_registry = Arc::new(ActionRegistry::from_executors(named_executors(execs), None));
    state.workflow_name = def.name.clone();
    let engine = FlowEngineBuilder::new().build().unwrap();
    engine.resume(def, &mut state).unwrap()
}

#[test]
fn resumed_step_gets_prior_attempt_on_first_dispatch_only() {
    let p = make_persistence();
    let run_id = new_run(&p, "wf");
    seed(
        &p,
        &run_id,
        "plan",
        0,
        WorkflowStepStatus::Failed,
        Some("agent-run-1"),
    );
    let seen = Arc::new(Mutex::new(vec![]));
    let def = make_def(
        "wf",
        vec![WorkflowNode::Call({
            let WorkflowNode::Call(mut c) = call_node("plan") else {
                unreachable!()
            };
            c.retries = 1;
            c
        })],
    );
    let exec = Recorder {
        name: "plan",
        seen: Arc::clone(&seen),
        fail_times: 1,
    };
    let result = resume(&p, &run_id, &def, vec![Box::new(exec)]);
    assert!(result.all_succeeded);
    assert_eq!(
        *seen.lock().unwrap(),
        vec![Some("agent-run-1".to_string()), None],
        "the retry attempt must run fresh"
    );
}

#[test]
fn fresh_run_and_unlinked_failure_get_no_prior_attempt() {
    // A failed row with no child run: nothing to recover from.
    let p = make_persistence();
    let run_id = new_run(&p, "wf");
    seed(&p, &run_id, "plan", 0, WorkflowStepStatus::Failed, None);
    seed(&p, &run_id, "other", 1, WorkflowStepStatus::Completed, None);
    let seen = Arc::new(Mutex::new(vec![]));
    let def = make_def("wf", vec![call_node("plan"), call_node("other")]);
    let result = resume(
        &p,
        &run_id,
        &def,
        vec![
            Box::new(Recorder {
                name: "plan",
                seen: Arc::clone(&seen),
                fail_times: 0,
            }),
            Box::new(Recorder {
                name: "other",
                seen: Arc::clone(&seen),
                fail_times: 0,
            }),
        ],
    );
    assert!(result.all_succeeded);
    // `other` completed earlier, so it is skipped; `plan` ran once with no extension.
    assert_eq!(*seen.lock().unwrap(), vec![None]);

    // A non-resume run never carries it.
    let p = make_persistence();
    let seen = Arc::new(Mutex::new(vec![]));
    let mut state = common::make_state(
        "wf",
        Arc::clone(&p),
        named_executors([Box::new(Recorder {
            name: "plan",
            seen: Arc::clone(&seen),
            fail_times: 0,
        }) as Box<dyn ActionExecutor>]),
    );
    let engine = FlowEngineBuilder::new().build().unwrap();
    engine
        .run(&make_def("wf", vec![call_node("plan")]), &mut state)
        .unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![None]);
}

#[test]
fn parallel_branch_gets_prior_attempt_on_resume() {
    let p = make_persistence();
    let run_id = new_run(&p, "wf");
    seed(&p, &run_id, "a", 0, WorkflowStepStatus::Completed, None);
    seed(
        &p,
        &run_id,
        "b",
        1,
        WorkflowStepStatus::Failed,
        Some("agent-run-b"),
    );
    let seen_a = Arc::new(Mutex::new(vec![]));
    let seen_b = Arc::new(Mutex::new(vec![]));
    let def = make_def(
        "wf",
        vec![WorkflowNode::Parallel(ParallelNode {
            fail_fast: true,
            min_success: None,
            calls: vec![AgentRef::Name("a".into()), AgentRef::Name("b".into())],
            output: None,
            call_outputs: Default::default(),
            with: vec![],
            call_with: Default::default(),
            call_if: Default::default(),
            call_retries: Default::default(),
        })],
    );
    let result = resume(
        &p,
        &run_id,
        &def,
        vec![
            Box::new(Recorder {
                name: "a",
                seen: Arc::clone(&seen_a),
                fail_times: 0,
            }),
            Box::new(Recorder {
                name: "b",
                seen: Arc::clone(&seen_b),
                fail_times: 0,
            }),
        ],
    );
    assert!(result.all_succeeded);
    assert!(
        seen_a.lock().unwrap().is_empty(),
        "completed branch skipped"
    );
    assert_eq!(
        *seen_b.lock().unwrap(),
        vec![Some("agent-run-b".to_string())]
    );
}
