mod common;

use std::sync::Arc;

use runkon_flow::status::WorkflowRunStatus;
use runkon_flow::traits::persistence::WorkflowPersistence;
use runkon_flow::CancellationReason;
use runkon_flow::EngineEvent;
use runkon_flow::FlowEngineBuilder;

use common::{
    call_node, make_def, make_def_with_always, make_persistence, make_state,
    make_state_with_resume_ctx, named_executors, ActionExecutor, FailingExecutor, ForwardSink,
    MockExecutor, VecSink,
};

// ---------------------------------------------------------------------------
// Resume path: RunResumed emitted, RunStarted absent
// ---------------------------------------------------------------------------

#[test]
fn run_resumed_event_emitted_when_resume_ctx_set() {
    let sink = VecSink::new();
    let sink_ref = Arc::clone(&sink);

    let engine = FlowEngineBuilder::new()
        .action(Box::new(MockExecutor::new("worker")))
        .event_sink(Box::new(ForwardSink(sink_ref)))
        .build()
        .expect("engine build failed");

    let def = make_def("resume-test", vec![call_node("worker")]);

    let persistence = make_persistence();
    let mut state = make_state_with_resume_ctx(
        "resume-test",
        Arc::clone(&persistence),
        named_executors([Box::new(MockExecutor::new("worker")) as Box<dyn ActionExecutor>]),
    );

    engine.run(&def, &mut state).expect("run should succeed");

    let events = sink.collected();
    let has_resumed = events
        .iter()
        .any(|e| matches!(&e.event, EngineEvent::RunResumed { .. }));
    let has_started = events
        .iter()
        .any(|e| matches!(&e.event, EngineEvent::RunStarted { .. }));

    assert!(
        has_resumed,
        "RunResumed event should be emitted when resume_ctx is set; events: {:?}",
        events
            .iter()
            .map(|e| format!("{:?}", e.event))
            .collect::<Vec<_>>()
    );
    assert!(
        !has_started,
        "RunStarted should not be emitted when resume_ctx is set; events: {:?}",
        events
            .iter()
            .map(|e| format!("{:?}", e.event))
            .collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------------
// workflow_status injection: "completed" when body succeeds
// ---------------------------------------------------------------------------

#[test]
fn always_block_receives_workflow_status_completed() {
    let engine = FlowEngineBuilder::new()
        .action(Box::new(MockExecutor::new("body-step")))
        .action(Box::new(MockExecutor::new("always-step")))
        .build()
        .expect("engine build failed");

    let def = make_def_with_always(
        "status-completed",
        vec![call_node("body-step")],
        vec![call_node("always-step")],
    );

    let persistence = make_persistence();
    let mut state = make_state(
        "status-completed",
        Arc::clone(&persistence),
        named_executors([
            Box::new(MockExecutor::new("body-step")) as Box<dyn ActionExecutor>,
            Box::new(MockExecutor::new("always-step")) as Box<dyn ActionExecutor>,
        ]),
    );

    let result = engine.run(&def, &mut state).expect("run should succeed");

    assert!(result.all_succeeded, "body success → all_succeeded = true");
    assert_eq!(
        state.inputs.get("workflow_status").map(|s| s.as_str()),
        Some("completed"),
        "workflow_status should be 'completed' when body succeeds"
    );
}

// ---------------------------------------------------------------------------
// workflow_status injection: "failed" when body fails
// ---------------------------------------------------------------------------

#[test]
fn always_block_receives_workflow_status_failed() {
    let engine = FlowEngineBuilder::new()
        .action(Box::new(FailingExecutor))
        .action(Box::new(MockExecutor::new("always-step")))
        .build()
        .expect("engine build failed");

    let def = make_def_with_always(
        "status-failed",
        vec![call_node("failing")],
        vec![call_node("always-step")],
    );

    let persistence = make_persistence();
    let mut state = make_state(
        "status-failed",
        Arc::clone(&persistence),
        named_executors([
            Box::new(FailingExecutor) as Box<dyn ActionExecutor>,
            Box::new(MockExecutor::new("always-step")) as Box<dyn ActionExecutor>,
        ]),
    );

    engine
        .run(&def, &mut state)
        .expect("run returns Ok even when body fails");

    assert_eq!(
        state.inputs.get("workflow_status").map(|s| s.as_str()),
        Some("failed"),
        "workflow_status should be 'failed' when body fails"
    );
}

// ---------------------------------------------------------------------------
// Cancellation: RunCancelled event + WorkflowRunStatus::Cancelled in persistence
// ---------------------------------------------------------------------------

#[test]
fn cancelled_run_emits_run_cancelled_event_and_status() {
    let sink = VecSink::new();
    let sink_ref = Arc::clone(&sink);

    let engine = FlowEngineBuilder::new()
        .action(Box::new(MockExecutor::new("should-not-run")))
        .event_sink(Box::new(ForwardSink(sink_ref)))
        .build()
        .expect("engine build failed");

    let def = make_def("cancel-event-test", vec![call_node("should-not-run")]);

    let persistence = make_persistence();
    let mut state = make_state(
        "cancel-event-test",
        Arc::clone(&persistence),
        named_executors([Box::new(MockExecutor::new("should-not-run")) as Box<dyn ActionExecutor>]),
    );

    let run_id = state.workflow_run_id.clone();

    // Pre-cancel so the engine halts at the first step boundary.
    state
        .cancellation
        .cancel(CancellationReason::UserRequested(None));

    engine
        .run(&def, &mut state)
        .expect("run returns Ok even when cancelled");

    let events = sink.collected();
    let has_cancelled = events
        .iter()
        .any(|e| matches!(&e.event, EngineEvent::RunCancelled { .. }));
    assert!(
        has_cancelled,
        "RunCancelled event should be emitted; events: {:?}",
        events
            .iter()
            .map(|e| format!("{:?}", e.event))
            .collect::<Vec<_>>()
    );

    let run = persistence
        .get_run(&run_id)
        .expect("get_run failed")
        .expect("run record not found");
    assert_eq!(
        run.status,
        WorkflowRunStatus::Cancelled,
        "persistence should show Cancelled status after cancellation"
    );
}

// ---------------------------------------------------------------------------
// Always block failure is non-fatal to terminal status
// ---------------------------------------------------------------------------

#[test]
fn always_block_failure_is_non_fatal() {
    let engine = FlowEngineBuilder::new()
        .action(Box::new(MockExecutor::new("body-step")))
        .action(Box::new(FailingExecutor))
        .build()
        .expect("engine build failed");

    let def = make_def_with_always(
        "always-failure",
        vec![call_node("body-step")],
        vec![call_node("failing")],
    );

    let persistence = make_persistence();
    let mut state = make_state(
        "always-failure",
        Arc::clone(&persistence),
        named_executors([
            Box::new(MockExecutor::new("body-step")) as Box<dyn ActionExecutor>,
            Box::new(FailingExecutor) as Box<dyn ActionExecutor>,
        ]),
    );

    let run_id = state.workflow_run_id.clone();

    let result = engine.run(&def, &mut state).expect("run returns Ok");

    assert!(
        result.all_succeeded,
        "always block failure should not change terminal status when body succeeded"
    );

    let run = persistence
        .get_run(&run_id)
        .expect("get_run failed")
        .expect("run record not found");
    assert_eq!(
        run.status,
        WorkflowRunStatus::Completed,
        "persistence should show Completed status when body succeeds despite always block failure"
    );
}
