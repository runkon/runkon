//! #207: a run waiting at a gate has status `Waiting` with `blocked_on` naming the gate,
//! and returns to `Running` or a terminal status with `blocked_on` cleared afterwards.
//!
//! Workflow: script-like step -> `human_review` gate -> script-like step.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use runkon_flow::dsl::{GateNode, OnTimeout, WorkflowNode};
use runkon_flow::engine_error::EngineError;
use runkon_flow::persistence_memory::InMemoryWorkflowPersistence;
use runkon_flow::status::{WorkflowRunStatus, WorkflowStepStatus};
use runkon_flow::traits::action_executor::{ActionExecutor, ActionOutput, ActionParams, StepInfo};
use runkon_flow::traits::gate_resolver::{GateParams, GatePoll, GateResolver};
use runkon_flow::traits::persistence::WorkflowPersistence;
use runkon_flow::traits::run_context::RunContext;
use runkon_flow::traits::GateApprovalStore;
use runkon_flow::types::{BlockedOn, WorkflowExecConfig};
use runkon_flow::FlowEngineBuilder;

use common::{call_node, gate_node, make_def, make_persistence, make_state, named_executors};

const GATE: &str = "review";

struct Counting {
    name: &'static str,
    count: Arc<AtomicUsize>,
}

impl ActionExecutor for Counting {
    fn name(&self) -> &str {
        self.name
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

/// Registers `human_review` so validation passes; stays Pending, so only manual decisions
/// release the gate.
struct StubReviewResolver;

impl GateResolver for StubReviewResolver {
    fn gate_type(&self) -> &str {
        "human_review"
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

type RunHandle = std::thread::JoinHandle<Result<runkon_flow::types::WorkflowResult, EngineError>>;

fn spawn_run(
    persistence: &Arc<InMemoryWorkflowPersistence>,
    first: &Arc<AtomicUsize>,
    second: &Arc<AtomicUsize>,
    timeout_secs: u64,
    on_timeout: OnTimeout,
) -> (String, RunHandle) {
    let (tx, rx) = mpsc::channel();
    let p = Arc::clone(persistence);
    let (c1, c2) = (Arc::clone(first), Arc::clone(second));
    let handle = std::thread::spawn(move || {
        let engine = FlowEngineBuilder::new()
            .action(Box::new(Counting {
                name: "first",
                count: Arc::clone(&c1),
            }))
            .action(Box::new(Counting {
                name: "second",
                count: Arc::clone(&c2),
            }))
            .gate_resolver(StubReviewResolver)
            .build()
            .expect("engine build failed");
        let mut gate: GateNode = match gate_node(GATE) {
            WorkflowNode::Gate(g) => g,
            _ => unreachable!(),
        };
        gate.gate_type = "human_review".to_string();
        gate.timeout_secs = timeout_secs;
        gate.on_timeout = on_timeout;
        gate.prompt = Some("looks good?".to_string());
        let def = make_def(
            "wf",
            vec![
                call_node("first"),
                WorkflowNode::Gate(gate),
                call_node("second"),
            ],
        );
        let mut state = make_state(
            "wf",
            Arc::clone(&p),
            named_executors([
                Box::new(Counting {
                    name: "first",
                    count: c1,
                }) as Box<dyn ActionExecutor>,
                Box::new(Counting {
                    name: "second",
                    count: c2,
                }) as Box<dyn ActionExecutor>,
            ]),
        );
        state.exec_config = WorkflowExecConfig {
            poll_interval: Duration::from_millis(1),
            ..WorkflowExecConfig::default()
        };
        tx.send(state.workflow_run_id.clone()).unwrap();
        engine.run(&def, &mut state)
    });
    (rx.recv_timeout(Duration::from_secs(10)).unwrap(), handle)
}

fn wait_for_waiting_gate(p: &InMemoryWorkflowPersistence, run_id: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let steps = p.get_steps(run_id).unwrap_or_default();
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

fn assert_waiting_on_gate(p: &InMemoryWorkflowPersistence, run_id: &str) {
    let run = p.get_run(run_id).unwrap().unwrap();
    assert_eq!(run.status, WorkflowRunStatus::Waiting);
    match run.blocked_on {
        Some(BlockedOn::HumanReview {
            gate_name, prompt, ..
        }) => {
            assert_eq!(gate_name, GATE);
            assert_eq!(prompt.as_deref(), Some("looks good?"));
        }
        other => panic!("unexpected blocked_on: {other:?}"),
    }
}

#[test]
fn approved_gate_waits_then_completes() {
    let p = make_persistence();
    let (first, second) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (run_id, handle) = spawn_run(&p, &first, &second, 60, OnTimeout::Fail);

    let gate_id = wait_for_waiting_gate(&p, &run_id);
    assert_waiting_on_gate(&p, &run_id);
    p.approve_gate(&gate_id, "tester", None, None).unwrap();

    handle.join().unwrap().expect("run should succeed");
    let run = p.get_run(&run_id).unwrap().unwrap();
    assert_eq!(run.status, WorkflowRunStatus::Completed);
    assert!(run.blocked_on.is_none());
    assert_eq!(second.load(Ordering::SeqCst), 1);
}

#[test]
fn rejected_gate_fails_the_run_and_clears_blocked_on() {
    let p = make_persistence();
    let (first, second) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (run_id, handle) = spawn_run(&p, &first, &second, 60, OnTimeout::Fail);

    let gate_id = wait_for_waiting_gate(&p, &run_id);
    assert_waiting_on_gate(&p, &run_id);
    p.reject_gate(&gate_id, "tester", Some("no")).unwrap();

    let _ = handle.join().unwrap();
    let run = p.get_run(&run_id).unwrap().unwrap();
    assert_eq!(run.status, WorkflowRunStatus::Failed);
    assert!(run.blocked_on.is_none());
    assert_eq!(second.load(Ordering::SeqCst), 0);
}

#[test]
fn timed_out_gate_with_continue_completes_and_clears_blocked_on() {
    let p = make_persistence();
    let (first, second) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (run_id, handle) = spawn_run(&p, &first, &second, 1, OnTimeout::Continue);

    wait_for_waiting_gate(&p, &run_id);
    handle.join().unwrap().expect("run should succeed");
    let run = p.get_run(&run_id).unwrap().unwrap();
    assert_eq!(run.status, WorkflowRunStatus::Completed);
    assert!(run.blocked_on.is_none());
    assert_eq!(second.load(Ordering::SeqCst), 1);
}
