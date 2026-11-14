//! #187: the step after an approved `human_approval` gate must run exactly once.
//!
//! Covers approval while the engine polls, approval followed by `resume`, a concurrent
//! `resume` refused by the lease, and a host that clears the lease owner and resumes a run
//! whose original engine is still polling the gate (conductor's `mark_run_resuming`).

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use runkon_flow::cancellation_reason::CancellationReason;
use runkon_flow::dsl::{GateNode, WorkflowNode};
use runkon_flow::engine_error::EngineError;
use runkon_flow::persistence_memory::InMemoryWorkflowPersistence;
use runkon_flow::status::{WorkflowRunStatus, WorkflowStepStatus};
use runkon_flow::traits::action_executor::{ActionExecutor, ActionOutput, ActionParams, StepInfo};
use runkon_flow::traits::gate_resolver::{GateParams, GatePoll, GateResolver};
use runkon_flow::traits::persistence::{GateStepInfo, NewStep, WorkflowPersistence};
use runkon_flow::traits::run_context::RunContext;
use runkon_flow::traits::GateApprovalStore;
use runkon_flow::types::WorkflowExecConfig;
use runkon_flow::{FlowEngine, FlowEngineBuilder};

use common::{call_node, gate_node, make_def, make_persistence, make_state, named_executors};

const NEXT_STEP: &str = "notify";
const GATE: &str = "approval";

/// Counts executions of the step that follows the gate.
struct CountingExecutor {
    count: Arc<AtomicUsize>,
}

impl ActionExecutor for CountingExecutor {
    fn name(&self) -> &str {
        NEXT_STEP
    }

    fn execute(
        &self,
        _ctx: &dyn RunContext,
        _info: &StepInfo,
        _params: &ActionParams,
    ) -> Result<ActionOutput, EngineError> {
        self.count.fetch_add(1, Ordering::SeqCst);
        Ok(ActionOutput::default())
    }
}

/// Registers `human_approval` so validation passes; stays Pending, so only manual
/// decisions release the gate.
struct StubApprovalResolver;

impl GateResolver for StubApprovalResolver {
    fn gate_type(&self) -> &str {
        "human_approval"
    }
    fn poll(
        &self,
        _run_id: &str,
        _params: &GateParams,
        _ctx: &dyn RunContext,
    ) -> Result<GatePoll, EngineError> {
        Ok(GatePoll::Pending)
    }
}

fn build_engine(count: &Arc<AtomicUsize>) -> FlowEngine {
    FlowEngineBuilder::new()
        .action(Box::new(CountingExecutor {
            count: Arc::clone(count),
        }))
        .gate_resolver(StubApprovalResolver)
        .build()
        .expect("engine build failed")
}

fn def() -> runkon_flow::dsl::WorkflowDef {
    let mut gate: GateNode = match gate_node(GATE) {
        WorkflowNode::Gate(g) => g,
        _ => unreachable!(),
    };
    // `gate_node` times out immediately; these tests need a gate that waits.
    gate.timeout_secs = 60;
    make_def("wf", vec![WorkflowNode::Gate(gate), call_node(NEXT_STEP)])
}

/// A state for `run_id` (or a fresh run when `None`) with the counting executor registered.
fn state_for(
    persistence: &Arc<InMemoryWorkflowPersistence>,
    count: &Arc<AtomicUsize>,
    run_id: Option<&str>,
    poll_interval: Duration,
) -> runkon_flow::engine::ExecutionState {
    let mut state = make_state(
        "wf",
        Arc::clone(persistence),
        named_executors([Box::new(CountingExecutor {
            count: Arc::clone(count),
        }) as Box<dyn ActionExecutor>]),
    );
    if let Some(id) = run_id {
        state.workflow_run_id = id.to_string();
    }
    state.exec_config = WorkflowExecConfig {
        poll_interval,
        ..WorkflowExecConfig::default()
    };
    state
}

/// Wait (against a deadline, not a fixed sleep) until the run's gate row is `Waiting`.
fn wait_for_waiting_gate(persistence: &InMemoryWorkflowPersistence, run_id: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let steps = persistence.get_steps(run_id).unwrap_or_default();
        if let Some(g) = steps
            .iter()
            .find(|s| s.step_name == GATE && s.status == WorkflowStepStatus::Waiting)
        {
            return g.id.clone();
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    panic!("gate never reached Waiting");
}

/// The next step ran once, has one `Completed` row, nothing is left in flight, and the run
/// and the gate are `Completed`.
fn assert_ran_once(persistence: &InMemoryWorkflowPersistence, run_id: &str, count: &AtomicUsize) {
    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "the step after the gate must execute exactly once"
    );
    let steps = persistence.get_steps(run_id).unwrap();
    let notify: Vec<_> = steps.iter().filter(|s| s.step_name == NEXT_STEP).collect();
    assert_eq!(notify.len(), 1, "exactly one row for the step: {notify:?}");
    assert_eq!(notify[0].status, WorkflowStepStatus::Completed);
    let in_flight: Vec<_> = steps
        .iter()
        .filter(|s| {
            matches!(
                s.status,
                WorkflowStepStatus::Running | WorkflowStepStatus::Pending
            )
        })
        .collect();
    assert!(
        in_flight.is_empty(),
        "no step row left in flight: {in_flight:?}"
    );
    let gate = steps.iter().find(|s| s.step_name == GATE).unwrap();
    assert_eq!(gate.status, WorkflowStepStatus::Completed);
    let run = persistence.get_run(run_id).unwrap().unwrap();
    assert_eq!(run.status, WorkflowRunStatus::Completed);
}

/// Run `def()` on its own thread; returns the run id and the join handle.
fn spawn_engine_a(
    persistence: &Arc<InMemoryWorkflowPersistence>,
    count: &Arc<AtomicUsize>,
    poll_interval: Duration,
) -> (
    String,
    std::thread::JoinHandle<Result<runkon_flow::types::WorkflowResult, EngineError>>,
) {
    let (tx, rx) = mpsc::channel();
    let p = Arc::clone(persistence);
    let c = Arc::clone(count);
    let handle = std::thread::spawn(move || {
        let engine = build_engine(&c);
        let mut state = state_for(&p, &c, None, poll_interval);
        tx.send(state.workflow_run_id.clone()).unwrap();
        engine.run(&def(), &mut state)
    });
    let run_id = rx.recv_timeout(Duration::from_secs(10)).unwrap();
    (run_id, handle)
}

#[test]
fn approval_while_polling_runs_next_step_once() {
    let persistence = make_persistence();
    let count = Arc::new(AtomicUsize::new(0));
    let (run_id, handle) = spawn_engine_a(&persistence, &count, Duration::from_millis(1));

    let gate_id = wait_for_waiting_gate(&persistence, &run_id);
    persistence
        .approve_gate(&gate_id, "tester", None, None)
        .unwrap();

    let result = handle.join().unwrap().expect("run should succeed");
    assert!(result.all_succeeded);
    assert_ran_once(&persistence, &run_id, &count);
}

#[test]
fn approval_then_resume_runs_next_step_once() {
    let persistence = make_persistence();
    let count = Arc::new(AtomicUsize::new(0));
    let engine = build_engine(&count);

    // A run whose gate was approved while no engine was polling: seed the run and the gate
    // row, then approve through the store, as a host does for a parked run.
    let mut state = state_for(&persistence, &count, None, Duration::from_millis(1));
    let run_id = state.workflow_run_id.clone();
    let gate_id = persistence
        .insert_step(NewStep {
            workflow_run_id: run_id.clone(),
            step_name: GATE.to_string(),
            role: "gate".to_string(),
            can_commit: false,
            position: 0,
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
    persistence
        .approve_gate(&gate_id, "tester", None, None)
        .unwrap();
    persistence
        .update_run_status(&run_id, WorkflowRunStatus::Running, None, None)
        .unwrap();

    let result = engine
        .resume(&def(), &mut state)
        .expect("resume should succeed");
    assert!(result.all_succeeded);
    assert_ran_once(&persistence, &run_id, &count);
}

#[test]
fn resume_while_another_engine_polls_is_refused() {
    let persistence = make_persistence();
    let count = Arc::new(AtomicUsize::new(0));
    let (run_id, handle) = spawn_engine_a(&persistence, &count, Duration::from_millis(1));
    let gate_id = wait_for_waiting_gate(&persistence, &run_id);

    let engine_b = build_engine(&count);
    let mut state_b = state_for(
        &persistence,
        &count,
        Some(&run_id),
        Duration::from_millis(1),
    );
    let refused = engine_b.resume(&def(), &mut state_b);
    assert!(
        matches!(refused, Err(EngineError::AlreadyOwned(_))),
        "resume must be refused while the lease is held: {refused:?}"
    );

    persistence
        .approve_gate(&gate_id, "tester", None, None)
        .unwrap();
    handle.join().unwrap().expect("run should succeed");
    assert_ran_once(&persistence, &run_id, &count);
}

/// conductor's `mark_run_resuming` clears the lease owner, so a resume can take the run from
/// an engine that is still waiting on the gate. The original engine must not act on the
/// approval it then sees.
#[test]
fn resume_after_lease_owner_cleared_runs_next_step_once() {
    let persistence = make_persistence();
    let count = Arc::new(AtomicUsize::new(0));
    // Slow poll: engine A is asleep while B resumes and finishes. The default 10 s lease
    // refresh never fires, so A can only notice the loss when it acts on the decision.
    let (run_id, handle_a) = spawn_engine_a(&persistence, &count, Duration::from_millis(500));
    let gate_id = wait_for_waiting_gate(&persistence, &run_id);

    persistence
        .approve_gate(&gate_id, "tester", None, None)
        .unwrap();
    persistence.clear_lease_owner(&run_id, 60);

    let engine_b = build_engine(&count);
    let mut state_b = state_for(
        &persistence,
        &count,
        Some(&run_id),
        Duration::from_millis(1),
    );
    let result_b = engine_b
        .resume(&def(), &mut state_b)
        .expect("resume should succeed");
    assert!(result_b.all_succeeded);

    let result_a = handle_a.join().unwrap();
    assert!(
        matches!(
            result_a,
            Err(EngineError::Cancelled(CancellationReason::LeaseLost))
        ),
        "engine A lost the lease and must abort: {result_a:?}"
    );
    assert_ran_once(&persistence, &run_id, &count);
}
