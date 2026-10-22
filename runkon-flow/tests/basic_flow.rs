mod common;

use std::collections::HashMap;
use std::sync::Arc;

use runkon_flow::status::WorkflowStepStatus;
use runkon_flow::traits::persistence::WorkflowPersistence;
use runkon_flow::FlowEngineBuilder;

use common::{
    call_node, make_def, make_def_with_always, make_persistence, make_state, named_executors,
    ActionExecutor, ForwardSink, MockExecutor, VecSink,
};

// ---------------------------------------------------------------------------
// parse → validate → run
// ---------------------------------------------------------------------------

#[test]
fn parse_validate_run_single_step() {
    let dsl = r#"workflow single-step {
  meta {
    description = "one-step smoke test"
    trigger     = "manual"
  }
  call my-agent
}"#;

    let def = runkon_flow::dsl::parse_workflow_str(dsl, "test.wf")
        .expect("DSL should parse without errors");

    let engine = FlowEngineBuilder::new()
        .action(Box::new(MockExecutor::new("my-agent")))
        .build()
        .expect("engine build failed");

    engine.validate(&def).expect("workflow should be valid");

    let persistence = make_persistence();
    let mut state = make_state(
        "single-step",
        Arc::clone(&persistence),
        named_executors([Box::new(MockExecutor::new("my-agent")) as Box<dyn ActionExecutor>]),
    );

    let result = engine.run(&def, &mut state).expect("run should succeed");

    assert!(result.all_succeeded, "all steps should succeed");
    let steps = persistence
        .get_steps(&result.workflow_run_id)
        .expect("get_steps failed");
    assert_eq!(steps.len(), 1, "exactly one step should be recorded");
    assert_eq!(
        steps[0].status,
        WorkflowStepStatus::Completed,
        "step should be Completed"
    );
}

// ---------------------------------------------------------------------------
// Multi-step sequential execution
// ---------------------------------------------------------------------------

#[test]
fn multi_step_sequential_all_succeed() {
    let engine = FlowEngineBuilder::new()
        .action(Box::new(MockExecutor::new("alpha")))
        .action(Box::new(MockExecutor::new("beta")))
        .action(Box::new(MockExecutor::new("gamma")))
        .build()
        .expect("engine build failed");

    let def = make_def(
        "multi-step",
        vec![call_node("alpha"), call_node("beta"), call_node("gamma")],
    );

    let persistence = make_persistence();
    let mut state = make_state(
        "multi-step",
        Arc::clone(&persistence),
        named_executors([
            Box::new(MockExecutor::new("alpha")) as Box<dyn ActionExecutor>,
            Box::new(MockExecutor::new("beta")) as Box<dyn ActionExecutor>,
            Box::new(MockExecutor::new("gamma")) as Box<dyn ActionExecutor>,
        ]),
    );

    let result = engine.run(&def, &mut state).expect("run should succeed");

    assert!(result.all_succeeded);

    let steps = persistence
        .get_steps(&result.workflow_run_id)
        .expect("get_steps failed");
    assert_eq!(steps.len(), 3);
    // Steps are stored in insertion order; positions 0, 1, 2.
    let completed = steps
        .iter()
        .filter(|s| s.status == WorkflowStepStatus::Completed)
        .count();
    assert_eq!(completed, 3, "all three steps should be Completed");
}

// ---------------------------------------------------------------------------
// Empty workflow — no steps
// ---------------------------------------------------------------------------

#[test]
fn empty_workflow_succeeds() {
    let engine = FlowEngineBuilder::new()
        .build()
        .expect("engine build failed");

    let def = make_def("empty", vec![]);
    let persistence = make_persistence();
    let mut state = make_state("empty", Arc::clone(&persistence), HashMap::new());

    let result = engine
        .run(&def, &mut state)
        .expect("empty run should succeed");

    assert!(result.all_succeeded);
    let steps = persistence
        .get_steps(&result.workflow_run_id)
        .expect("get_steps failed");
    assert!(
        steps.is_empty(),
        "empty workflow should produce no step records"
    );
}

// ---------------------------------------------------------------------------
// Persistence state assertions
// ---------------------------------------------------------------------------

#[test]
fn persistence_step_statuses_after_run() {
    let engine = FlowEngineBuilder::new()
        .action(Box::new(MockExecutor::new("step-a")))
        .action(Box::new(MockExecutor::new("step-b")))
        .build()
        .expect("engine build failed");

    let def = make_def("two-step", vec![call_node("step-a"), call_node("step-b")]);

    let persistence = make_persistence();
    let mut state = make_state(
        "two-step",
        Arc::clone(&persistence),
        named_executors([
            Box::new(MockExecutor::new("step-a")) as Box<dyn ActionExecutor>,
            Box::new(MockExecutor::new("step-b")) as Box<dyn ActionExecutor>,
        ]),
    );

    let result = engine.run(&def, &mut state).expect("run should succeed");

    let steps = persistence
        .get_steps(&result.workflow_run_id)
        .expect("get_steps failed");

    assert_eq!(steps.len(), 2);
    for step in &steps {
        assert_eq!(
            step.status,
            WorkflowStepStatus::Completed,
            "step '{}' should be Completed",
            step.step_name
        );
    }
    // Verify step names were recorded
    let names: Vec<&str> = steps.iter().map(|s| s.step_name.as_str()).collect();
    assert!(names.contains(&"step-a"), "step-a should be in records");
    assert!(names.contains(&"step-b"), "step-b should be in records");
}

// ---------------------------------------------------------------------------
// Always block runs on success
// ---------------------------------------------------------------------------

#[test]
fn always_block_runs_on_success() {
    let engine = FlowEngineBuilder::new()
        .action(Box::new(MockExecutor::new("work")))
        .action(Box::new(MockExecutor::new("cleanup")))
        .build()
        .expect("engine build failed");

    let def = make_def_with_always(
        "always-success",
        vec![call_node("work")],
        vec![call_node("cleanup")],
    );

    let persistence = make_persistence();
    let mut state = make_state(
        "always-success",
        Arc::clone(&persistence),
        named_executors([
            Box::new(MockExecutor::new("work")) as Box<dyn ActionExecutor>,
            Box::new(MockExecutor::new("cleanup")) as Box<dyn ActionExecutor>,
        ]),
    );

    let result = engine.run(&def, &mut state).expect("run should succeed");

    assert!(result.all_succeeded);
    let steps = persistence
        .get_steps(&result.workflow_run_id)
        .expect("get_steps failed");

    let names: Vec<&str> = steps.iter().map(|s| s.step_name.as_str()).collect();
    assert!(names.contains(&"work"), "body step should run");
    assert!(
        names.contains(&"cleanup"),
        "always step should run after body success"
    );
}

// ---------------------------------------------------------------------------
// Event sink receives events during run
// ---------------------------------------------------------------------------

#[test]
fn event_sink_captures_run_lifecycle() {
    let sink = VecSink::new();
    let sink_ref = Arc::clone(&sink);

    let engine = FlowEngineBuilder::new()
        .action(Box::new(MockExecutor::new("worker")))
        .event_sink(Box::new(ForwardSink(sink_ref)))
        .build()
        .expect("engine build failed");

    let def = make_def("event-test", vec![call_node("worker")]);

    let persistence = make_persistence();
    let mut state = make_state(
        "event-test",
        Arc::clone(&persistence),
        named_executors([Box::new(MockExecutor::new("worker")) as Box<dyn ActionExecutor>]),
    );

    engine.run(&def, &mut state).expect("run should succeed");

    let events = sink.collected();
    assert!(!events.is_empty(), "event sink should receive events");

    let has_run_started = events
        .iter()
        .any(|e| matches!(e.event, runkon_flow::EngineEvent::RunStarted { .. }));
    let has_run_completed = events
        .iter()
        .any(|e| matches!(e.event, runkon_flow::EngineEvent::RunCompleted { .. }));
    assert!(has_run_started, "RunStarted event should be emitted");
    assert!(has_run_completed, "RunCompleted event should be emitted");
}

// ---------------------------------------------------------------------------
// Output warnings — a completed step that recorded no usable output
// ---------------------------------------------------------------------------

const OUTPUT_WARNING: &str = "agent output has no <<<FLOW_OUTPUT>>> block; \
                              recorded markers: [] and empty context";

/// Completes successfully but reports `metadata_keys::OUTPUT_WARNING`, the way
/// the agent executors do when the agent ends without a FLOW_OUTPUT block.
struct WarningExecutor(&'static str);

impl ActionExecutor for WarningExecutor {
    fn name(&self) -> &str {
        self.0
    }

    fn execute(
        &self,
        _ctx: &dyn runkon_flow::traits::run_context::RunContext,
        _info: &runkon_flow::traits::action_executor::StepInfo,
        _params: &runkon_flow::traits::action_executor::ActionParams,
    ) -> Result<
        runkon_flow::traits::action_executor::ActionOutput,
        runkon_flow::engine_error::EngineError,
    > {
        Ok(runkon_flow::traits::action_executor::ActionOutput {
            metadata: HashMap::from([(
                runkon_flow::constants::metadata_keys::OUTPUT_WARNING.to_string(),
                OUTPUT_WARNING.to_string(),
            )]),
            ..Default::default()
        })
    }
}

fn run_warning_workflow(dsl: &str) -> Vec<runkon_flow::types::WorkflowRunStep> {
    let def = runkon_flow::dsl::parse_workflow_str(dsl, "test.wf").expect("DSL should parse");
    let engine = FlowEngineBuilder::new()
        .action(Box::new(WarningExecutor("warned")))
        .action(Box::new(MockExecutor::new("clean")))
        .build()
        .expect("engine build failed");
    let persistence = make_persistence();
    let mut state = make_state(
        "output-warning",
        Arc::clone(&persistence),
        named_executors([
            Box::new(WarningExecutor("warned")) as Box<dyn ActionExecutor>,
            Box::new(MockExecutor::new("clean")) as Box<dyn ActionExecutor>,
        ]),
    );
    let result = engine.run(&def, &mut state).expect("run should succeed");
    assert!(
        result.all_succeeded,
        "an output warning must not fail the run"
    );
    persistence
        .get_steps(&result.workflow_run_id)
        .expect("get_steps failed")
}

fn assert_warning_recorded(steps: &[runkon_flow::types::WorkflowRunStep]) {
    let step = |name: &str| {
        steps
            .iter()
            .find(|s| s.step_name == name)
            .unwrap_or_else(|| panic!("no step named {name}: {steps:?}"))
    };
    let warned = step("warned");
    assert_eq!(warned.status, WorkflowStepStatus::Completed);
    assert_eq!(warned.step_error.as_deref(), Some(OUTPUT_WARNING));
    let clean = step("clean");
    assert_eq!(clean.status, WorkflowStepStatus::Completed);
    assert_eq!(clean.step_error, None);
}

#[test]
fn call_step_output_warning_is_stored_as_step_error() {
    let steps = run_warning_workflow(
        r#"workflow output-warning {
  call warned
  call clean
}"#,
    );
    assert_warning_recorded(&steps);
}

#[test]
fn parallel_step_output_warning_is_stored_as_step_error() {
    let steps = run_warning_workflow(
        r#"workflow output-warning {
  parallel {
    call warned
    call clean
  }
}"#,
    );
    assert_warning_recorded(&steps);
}

// ---------------------------------------------------------------------------
// Script input env prefix — an invalid provider prefix refuses the run
// ---------------------------------------------------------------------------

struct BadPrefixProvider;

impl runkon_flow::ScriptEnvProvider for BadPrefixProvider {
    fn env(
        &self,
        _ctx: &dyn runkon_flow::traits::run_context::RunContext,
        _as_identity: Option<&str>,
    ) -> HashMap<String, String> {
        HashMap::new()
    }

    fn input_env_prefix(&self) -> &str {
        "PATH="
    }
}

#[test]
fn invalid_input_env_prefix_refuses_the_run_before_any_step() {
    let engine = FlowEngineBuilder::new()
        .action(Box::new(MockExecutor::new("alpha")))
        .build()
        .expect("engine build failed");
    let def = make_def("bad-prefix", vec![call_node("alpha")]);
    let persistence = make_persistence();
    let mut state = make_state(
        "bad-prefix",
        Arc::clone(&persistence),
        named_executors([Box::new(MockExecutor::new("alpha")) as Box<dyn ActionExecutor>]),
    );
    state.script_env_provider = Arc::new(BadPrefixProvider);

    let err = engine
        .run(&def, &mut state)
        .expect_err("an invalid prefix must refuse the run");
    let msg = err.to_string();
    assert!(
        msg.contains("invalid script input env prefix"),
        "got: {msg}"
    );
    assert!(
        msg.contains("\"PATH=\""),
        "error should name the prefix; got: {msg}"
    );
    let steps = persistence
        .get_steps(&state.workflow_run_id)
        .expect("get_steps failed");
    assert!(steps.is_empty(), "no step may run: {steps:?}");
}
