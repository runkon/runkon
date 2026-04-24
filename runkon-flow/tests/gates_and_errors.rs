mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use runkon_flow::dsl::OnTimeout;
use runkon_flow::status::WorkflowStepStatus;
use runkon_flow::traits::gate_resolver::{GateContext, GateParams, GatePoll, GateResolver};
use runkon_flow::traits::persistence::WorkflowPersistence;
use runkon_flow::types::WorkflowExecConfig;
use runkon_flow::CancellationReason;
use runkon_flow::FlowEngineBuilder;

use common::{
    call_node, gate_node, make_def, make_def_with_always, make_persistence, make_state,
    named_executors, timeout_gate, ActionExecutor, FailingExecutor, ForwardSink, MockExecutor,
    VecSink,
};

// ---------------------------------------------------------------------------
// Stub gate resolver — only used to satisfy validation; execution polls DB.
// ---------------------------------------------------------------------------

struct StubApprovalResolver;

impl GateResolver for StubApprovalResolver {
    fn gate_type(&self) -> &str {
        "human_approval"
    }
    fn poll(
        &self,
        _run_id: &str,
        _params: &GateParams,
        _ctx: &GateContext,
    ) -> Result<GatePoll, runkon_flow::engine_error::EngineError> {
        Ok(GatePoll::Approved(None))
    }
}

// ---------------------------------------------------------------------------
// Gate dry-run auto-approve
// ---------------------------------------------------------------------------

#[test]
fn gate_dry_run_auto_approve() {
    let engine = FlowEngineBuilder::new()
        .gate_resolver(StubApprovalResolver)
        .build()
        .expect("engine build failed");

    let def = make_def("gate-dry-run", vec![gate_node("approval")]);

    let persistence = make_persistence();
    let mut state = make_state("gate-dry-run", Arc::clone(&persistence), HashMap::new());
    // dry_run = true auto-approves all gates
    state.exec_config = WorkflowExecConfig {
        dry_run: true,
        ..WorkflowExecConfig::default()
    };

    let result = engine.run(&def, &mut state).expect("run should succeed");

    assert!(result.all_succeeded, "dry-run gate should auto-approve");

    let steps = persistence
        .get_steps(&result.workflow_run_id)
        .expect("get_steps failed");
    assert_eq!(steps.len(), 1, "one step for the gate");
    assert_eq!(
        steps[0].status,
        WorkflowStepStatus::Completed,
        "gate step should be Completed in dry-run"
    );
}

// ---------------------------------------------------------------------------
// Gate timeout → fail
// ---------------------------------------------------------------------------

#[test]
fn gate_timeout_fail() {
    let engine = FlowEngineBuilder::new()
        .gate_resolver(StubApprovalResolver)
        .build()
        .expect("engine build failed");

    // timeout_secs = 0 with a very short poll_interval causes the gate to time
    // out after a single poll cycle. on_timeout = Fail so the run fails.
    let def = make_def("gate-timeout", vec![timeout_gate(OnTimeout::Fail)]);

    let persistence = make_persistence();
    let mut state = make_state("gate-timeout", Arc::clone(&persistence), HashMap::new());
    state.exec_config = WorkflowExecConfig {
        poll_interval: Duration::from_millis(1),
        ..WorkflowExecConfig::default()
    };

    // run may return Ok(result) with all_succeeded=false or Err on timeout
    let run_id = state.workflow_run_id.clone();
    let _ = engine.run(&def, &mut state);

    let steps = persistence.get_steps(&run_id).expect("get_steps failed");

    // on_timeout=Fail marks the step as Failed (not TimedOut); TimedOut is used
    // only by on_timeout=Continue so the distinction is observable in the DB.
    let timed_out_or_failed = steps.iter().any(|s| {
        matches!(
            s.status,
            WorkflowStepStatus::Failed | WorkflowStepStatus::TimedOut
        )
    });
    assert!(
        timed_out_or_failed,
        "gate step should be marked Failed or TimedOut on timeout; got: {:?}",
        steps
    );
    // Verify the gate step was recorded with the timeout error text
    let gate_step = steps.iter().find(|s| s.step_name == "approval");
    assert!(gate_step.is_some(), "gate step record should exist");
    let result_text = gate_step.unwrap().result_text.as_deref().unwrap_or("");
    assert!(
        result_text.contains("timed out"),
        "result_text should mention timeout; got: {result_text}"
    );
}

// ---------------------------------------------------------------------------
// Gate timeout → continue
// ---------------------------------------------------------------------------

#[test]
fn gate_timeout_continue_succeeds() {
    let engine = FlowEngineBuilder::new()
        .gate_resolver(StubApprovalResolver)
        .build()
        .expect("engine build failed");

    let def = make_def(
        "gate-timeout-continue",
        vec![timeout_gate(OnTimeout::Continue)],
    );

    let persistence = make_persistence();
    let mut state = make_state(
        "gate-timeout-continue",
        Arc::clone(&persistence),
        HashMap::new(),
    );
    state.exec_config = WorkflowExecConfig {
        poll_interval: Duration::from_millis(1),
        ..WorkflowExecConfig::default()
    };

    let result = engine
        .run(&def, &mut state)
        .expect("run should succeed with on_timeout=continue");

    assert!(
        result.all_succeeded,
        "on_timeout=continue should let the workflow succeed"
    );
}

// ---------------------------------------------------------------------------
// Step failure propagates — WorkflowResult.all_succeeded = false
// ---------------------------------------------------------------------------

#[test]
fn step_failure_marks_run_failed() {
    let engine = FlowEngineBuilder::new()
        .action(Box::new(FailingExecutor))
        .build()
        .expect("engine build failed");

    let def = make_def("step-fail", vec![call_node("failing")]);

    let persistence = make_persistence();
    let mut state = make_state(
        "step-fail",
        Arc::clone(&persistence),
        named_executors([Box::new(FailingExecutor) as Box<dyn ActionExecutor>]),
    );

    let result = engine
        .run(&def, &mut state)
        .expect("run returns Ok even on step failure");

    assert!(
        !result.all_succeeded,
        "failed step should set all_succeeded = false"
    );

    let steps = persistence
        .get_steps(&result.workflow_run_id)
        .expect("get_steps failed");
    let failed_step = steps.iter().find(|s| s.step_name == "failing");
    assert!(failed_step.is_some(), "failing step should be recorded");
    assert_eq!(
        failed_step.unwrap().status,
        WorkflowStepStatus::Failed,
        "failing step status should be Failed"
    );
}

// ---------------------------------------------------------------------------
// Always block runs even when body step fails
// ---------------------------------------------------------------------------

#[test]
fn always_block_runs_on_failure() {
    let engine = FlowEngineBuilder::new()
        .action(Box::new(FailingExecutor))
        .action(Box::new(MockExecutor::new("cleanup")))
        .build()
        .expect("engine build failed");

    let def = make_def_with_always(
        "always-fail",
        vec![call_node("failing")],
        vec![call_node("cleanup")],
    );

    let persistence = make_persistence();
    let mut state = make_state(
        "always-fail",
        Arc::clone(&persistence),
        named_executors([
            Box::new(FailingExecutor) as Box<dyn ActionExecutor>,
            Box::new(MockExecutor::new("cleanup")) as Box<dyn ActionExecutor>,
        ]),
    );

    let result = engine.run(&def, &mut state).expect("run returns Ok");

    assert!(
        !result.all_succeeded,
        "body failure should set all_succeeded = false"
    );

    let run_id = &result.workflow_run_id;
    let steps = persistence.get_steps(run_id).expect("get_steps failed");

    let cleanup_ran = steps.iter().any(|s| s.step_name == "cleanup");
    assert!(
        cleanup_ran,
        "always-block cleanup step should run even when body fails"
    );
}

// ---------------------------------------------------------------------------
// Pre-cancelled token stops execution
// ---------------------------------------------------------------------------

#[test]
fn pre_cancelled_token_stops_run() {
    let engine = FlowEngineBuilder::new()
        .action(Box::new(MockExecutor::new("should-not-run")))
        .build()
        .expect("engine build failed");

    let def = make_def("cancel-test", vec![call_node("should-not-run")]);

    let persistence = make_persistence();
    let mut state = make_state(
        "cancel-test",
        Arc::clone(&persistence),
        named_executors([Box::new(MockExecutor::new("should-not-run")) as Box<dyn ActionExecutor>]),
    );

    // Cancel before run starts
    state
        .cancellation
        .cancel(CancellationReason::UserRequested(None));

    let outcome = engine.run(&def, &mut state);
    let did_not_succeed = match outcome {
        Ok(wr) => !wr.all_succeeded,
        Err(_) => true,
    };
    assert!(
        did_not_succeed,
        "run with pre-cancelled token should not complete successfully"
    );
}

// ---------------------------------------------------------------------------
// ChannelEventSink — ordered event assertions
// ---------------------------------------------------------------------------

#[test]
fn channel_event_sink_records_events_in_order() {
    use runkon_flow::events::{EngineEventData, EventSink};
    use runkon_flow::EngineEvent;
    use std::sync::mpsc;

    struct TestChannelSink(mpsc::Sender<EngineEventData>);
    impl EventSink for TestChannelSink {
        fn emit(&self, event: &EngineEventData) {
            let _ = self.0.send(event.clone());
        }
    }

    let (tx, rx) = mpsc::channel();
    let engine = FlowEngineBuilder::new()
        .action(Box::new(MockExecutor::new("worker")))
        .event_sink(Box::new(TestChannelSink(tx)))
        .build()
        .expect("engine build failed");

    let def = make_def("channel-sink", vec![call_node("worker")]);

    let persistence = make_persistence();
    let mut state = make_state(
        "channel-sink",
        Arc::clone(&persistence),
        named_executors([Box::new(MockExecutor::new("worker")) as Box<dyn ActionExecutor>]),
    );

    engine.run(&def, &mut state).expect("run should succeed");

    let events: Vec<_> = rx.try_iter().collect();
    assert!(
        !events.is_empty(),
        "channel event sink should receive events"
    );

    let first = &events[0].event;
    assert!(
        matches!(first, EngineEvent::RunStarted { .. }),
        "first event should be RunStarted; got: {:?}",
        first
    );
    let last = &events.last().unwrap().event;
    assert!(
        matches!(last, EngineEvent::RunCompleted { .. }),
        "last event should be RunCompleted; got: {:?}",
        last
    );
}

// ---------------------------------------------------------------------------
// Multi-step with failure: fail_fast stops subsequent steps
// ---------------------------------------------------------------------------

#[test]
fn fail_fast_stops_after_first_failure() {
    let engine = FlowEngineBuilder::new()
        .action(Box::new(FailingExecutor))
        .action(Box::new(MockExecutor::new("subsequent")))
        .build()
        .expect("engine build failed");

    let def = make_def(
        "fail-fast",
        vec![call_node("failing"), call_node("subsequent")],
    );

    let persistence = make_persistence();
    let mut state = make_state(
        "fail-fast",
        Arc::clone(&persistence),
        named_executors([
            Box::new(FailingExecutor) as Box<dyn ActionExecutor>,
            Box::new(MockExecutor::new("subsequent")) as Box<dyn ActionExecutor>,
        ]),
    );
    // fail_fast = true is the default
    state.exec_config = WorkflowExecConfig {
        fail_fast: true,
        ..WorkflowExecConfig::default()
    };

    let result = engine.run(&def, &mut state).expect("run returns Ok");

    assert!(!result.all_succeeded);

    let steps = persistence
        .get_steps(&result.workflow_run_id)
        .expect("get_steps failed");

    let subsequent_ran = steps.iter().any(|s| s.step_name == "subsequent");
    assert!(
        !subsequent_ran,
        "subsequent step should be skipped due to fail_fast; got steps: {:?}",
        steps
    );
}

// ---------------------------------------------------------------------------
// Event sink: VecSink captures StepStarted and StepCompleted
// ---------------------------------------------------------------------------

#[test]
fn event_sink_captures_step_events() {
    let sink = VecSink::new();
    let sink_ref = Arc::clone(&sink);

    let engine = FlowEngineBuilder::new()
        .action(Box::new(MockExecutor::new("step-a")))
        .event_sink(Box::new(ForwardSink(sink_ref)))
        .build()
        .expect("engine build failed");

    let def = make_def("step-events", vec![call_node("step-a")]);

    let persistence = make_persistence();
    let mut state = make_state(
        "step-events",
        Arc::clone(&persistence),
        named_executors([Box::new(MockExecutor::new("step-a")) as Box<dyn ActionExecutor>]),
    );

    engine.run(&def, &mut state).expect("run should succeed");

    let events = sink.collected();
    let kinds: Vec<&str> = events
        .iter()
        .map(|e| match &e.event {
            runkon_flow::EngineEvent::RunStarted { .. } => "RunStarted",
            runkon_flow::EngineEvent::RunCompleted { .. } => "RunCompleted",
            runkon_flow::EngineEvent::RunResumed { .. } => "RunResumed",
            runkon_flow::EngineEvent::RunCancelled { .. } => "RunCancelled",
            runkon_flow::EngineEvent::StepStarted { .. } => "StepStarted",
            runkon_flow::EngineEvent::StepCompleted { .. } => "StepCompleted",
            runkon_flow::EngineEvent::StepRetrying { .. } => "StepRetrying",
            runkon_flow::EngineEvent::GateWaiting { .. } => "GateWaiting",
            runkon_flow::EngineEvent::GateResolved { .. } => "GateResolved",
            runkon_flow::EngineEvent::FanOutItemsCollected { .. } => "FanOutItemsCollected",
            runkon_flow::EngineEvent::FanOutItemStarted { .. } => "FanOutItemStarted",
            runkon_flow::EngineEvent::FanOutItemCompleted { .. } => "FanOutItemCompleted",
            runkon_flow::EngineEvent::MetricsUpdated { .. } => "MetricsUpdated",
            _ => "Other",
        })
        .collect();

    assert!(
        kinds.contains(&"StepStarted"),
        "should have StepStarted event; got: {:?}",
        kinds
    );
    assert!(
        kinds.contains(&"StepCompleted"),
        "should have StepCompleted event; got: {:?}",
        kinds
    );
}
