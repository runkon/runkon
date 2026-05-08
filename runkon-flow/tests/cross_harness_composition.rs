//! Cross-harness composition integration test.
//!
//! harness_a runs the parent workflow; harness_b runs the child workflow.
//! CrossHarnessChildRunner bridges them: when the parent's call-workflow step
//! fires, the runner creates a run in persistence_b and dispatches it to
//! engine_b. The child run lives entirely within harness_b — it does not
//! appear in persistence_a, and sink_b (not sink_a) captures its lifecycle
//! events.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use runkon_flow::dsl::{CallWorkflowNode, WorkflowDef, WorkflowNode};
use runkon_flow::engine::{ChildWorkflowContext, ChildWorkflowInput, ChildWorkflowRunner};
use runkon_flow::engine_error::Result as EngineResult;
use runkon_flow::persistence_memory::InMemoryWorkflowPersistence;
use runkon_flow::status::WorkflowRunStatus;
use runkon_flow::traits::action_executor::ActionRegistry;
use runkon_flow::traits::persistence::{NewRun, WorkflowPersistence};
use runkon_flow::types::{WorkflowResult, WorkflowRun};
use runkon_flow::{FlowEngine, FlowEngineBuilder};

use common::{
    call_node, make_def, make_persistence, named_executors, ActionExecutor, ForwardSink,
    MockExecutor, VecSink,
};

/// Bridges a call-workflow step from harness_a into harness_b.
///
/// When the parent workflow (running in harness_a) reaches a `call workflow`
/// step, this runner creates a fresh run in `persistence_b` and dispatches
/// it to `engine_b`. The child run lives entirely within harness_b.
struct CrossHarnessChildRunner {
    engine_b: Arc<FlowEngine>,
    persistence_b: Arc<InMemoryWorkflowPersistence>,
    child_def: WorkflowDef,
    /// Shared with the test body so assertions can read the child run ID
    /// without needing to downcast `Arc<dyn ChildWorkflowRunner>`.
    last_child_run_id: Arc<Mutex<Option<String>>>,
}

impl ChildWorkflowRunner for CrossHarnessChildRunner {
    fn execute_child(
        &self,
        workflow_name: &str,
        parent_ctx: &ChildWorkflowContext,
        params: ChildWorkflowInput,
    ) -> EngineResult<WorkflowResult> {
        // 1. Create the child run in harness_b's persistence (not harness_a's).
        let child_run = self.persistence_b.create_run(NewRun {
            workflow_name: workflow_name.to_string(),
            parent_run_id: parent_ctx.workflow_run_id.clone(),
            parent_workflow_run_id: Some(parent_ctx.workflow_run_id.clone()),
            dry_run: false,
            trigger: "cross-harness".to_string(),
            definition_snapshot: None,
        })?;

        // 2. Build child ExecutionState wired to persistence_b.
        let mut child_state = runkon_flow::test_helpers::make_test_execution_state(
            Arc::clone(&self.persistence_b) as Arc<dyn WorkflowPersistence>,
            child_run.id.clone(),
        );

        // 3. Wire up executors, inputs, parent linkage, and cancellation.
        //    engine_b injects sink_b into child_state.event_sinks on run().
        child_state.action_registry = Arc::new(ActionRegistry::from_executors(
            named_executors(
                [Box::new(MockExecutor::new("child-agent")) as Box<dyn ActionExecutor>],
            ),
            None,
        ));
        child_state.workflow_name = workflow_name.to_string();
        child_state.inputs = params.inputs;
        child_state.parent_run_id = parent_ctx.workflow_run_id.clone();
        child_state.depth = params.depth;
        child_state.cancellation = params.cancellation;

        // 4. Execute the child workflow in harness_b.
        let result = self.engine_b.run(&self.child_def, &mut child_state)?;

        // 5. Record the child run ID for test assertions.
        *self.last_child_run_id.lock().unwrap() = Some(child_run.id);

        Ok(result)
    }

    fn resume_child(
        &self,
        _workflow_run_id: &str,
        _model: Option<&str>,
        _parent_ctx: &ChildWorkflowContext,
    ) -> EngineResult<WorkflowResult> {
        unimplemented!("CrossHarnessChildRunner does not support resume_child")
    }

    fn find_resumable_child(
        &self,
        _parent_run_id: &str,
        _workflow_name: &str,
    ) -> EngineResult<Option<WorkflowRun>> {
        Ok(None)
    }
}

#[test]
fn parent_in_harness_a_can_invoke_child_in_harness_b() {
    // -----------------------------------------------------------------------
    // harness_b — child workflow "child-wf" runs here
    // -----------------------------------------------------------------------
    let sink_b = VecSink::new();
    let persistence_b = make_persistence();
    let engine_b = Arc::new(
        FlowEngineBuilder::new()
            .action(Box::new(MockExecutor::new("child-agent")))
            .event_sink(Box::new(ForwardSink(Arc::clone(&sink_b))))
            .build()
            .expect("engine_b build failed"),
    );
    let child_def = make_def("child-wf", vec![call_node("child-agent")]);

    // -----------------------------------------------------------------------
    // harness_a — parent workflow "parent-wf" runs here
    //
    // No action executors needed: the single call-workflow step is dispatched
    // via CrossHarnessChildRunner, not by the engine's own ActionRegistry.
    // Validation skips unresolvable CallWorkflow nodes when no WorkflowResolver
    // is configured (flow_engine.rs:562 — `if let Some(resolver) = ctx.workflow_resolver`).
    // -----------------------------------------------------------------------
    let sink_a = VecSink::new();
    let persistence_a = make_persistence();
    let engine_a = FlowEngineBuilder::new()
        .event_sink(Box::new(ForwardSink(Arc::clone(&sink_a))))
        .build()
        .expect("engine_a build failed");

    let parent_def = make_def(
        "parent-wf",
        vec![WorkflowNode::CallWorkflow(CallWorkflowNode {
            workflow: "child-wf".to_string(),
            inputs: HashMap::new(),
            retries: 0,
            on_fail: None,
            bot_name: None,
        })],
    );

    // -----------------------------------------------------------------------
    // Wire the bridge: CrossHarnessChildRunner routes the call-workflow step
    // from harness_a to harness_b.
    // -----------------------------------------------------------------------
    let captured_child_run_id: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let runner = CrossHarnessChildRunner {
        engine_b: Arc::clone(&engine_b),
        persistence_b: Arc::clone(&persistence_b),
        child_def,
        last_child_run_id: Arc::clone(&captured_child_run_id),
    };

    // Create the parent run in persistence_a, then build the execution state.
    let parent_run = persistence_a
        .create_run(NewRun {
            workflow_name: "parent-wf".to_string(),
            parent_run_id: String::new(),
            parent_workflow_run_id: None,
            dry_run: false,
            trigger: "test".to_string(),
            definition_snapshot: None,
        })
        .expect("create parent run failed");

    let mut parent_state = runkon_flow::test_helpers::make_test_execution_state(
        Arc::clone(&persistence_a) as Arc<dyn WorkflowPersistence>,
        parent_run.id.clone(),
    );
    parent_state.workflow_name = "parent-wf".to_string();
    parent_state.child_runner = Some(Arc::new(runner));

    // -----------------------------------------------------------------------
    // Run the parent workflow in harness_a
    // -----------------------------------------------------------------------
    let parent_result = engine_a
        .run(&parent_def, &mut parent_state)
        .expect("parent run should succeed");

    // -----------------------------------------------------------------------
    // Assertions
    // -----------------------------------------------------------------------
    let child_run_id = captured_child_run_id
        .lock()
        .unwrap()
        .clone()
        .expect("execute_child should have been called; child_run_id is None");

    // 1. Parent completed successfully.
    assert!(parent_result.all_succeeded, "parent run should succeed");

    // 2. execute_child was called — child_run_id is non-empty.
    assert!(!child_run_id.is_empty(), "child_run_id must be non-empty");

    // 3. Child run lives in persistence_b with Completed status.
    let child_run = persistence_b
        .get_run(&child_run_id)
        .expect("get_run against persistence_b failed")
        .expect("child run not found in persistence_b");
    assert_eq!(
        child_run.status,
        WorkflowRunStatus::Completed,
        "child run in harness_b should be Completed"
    );

    // 4. Child run is NOT in persistence_a.
    let not_in_a = persistence_a
        .get_run(&child_run_id)
        .expect("get_run against persistence_a failed");
    assert!(
        not_in_a.is_none(),
        "child run must not exist in harness_a's persistence"
    );

    // 5. sink_b received lifecycle events for the child run.
    let sink_b_child_events: Vec<_> = sink_b
        .collected()
        .into_iter()
        .filter(|e| e.run_id == child_run_id)
        .collect();
    assert!(
        !sink_b_child_events.is_empty(),
        "harness_b's sink should have received child run events"
    );

    // 6. sink_a has no events belonging to the child run.
    let sink_a_child_events: Vec<_> = sink_a
        .collected()
        .into_iter()
        .filter(|e| e.run_id == child_run_id)
        .collect();
    assert!(
        sink_a_child_events.is_empty(),
        "harness_a's sink must not contain any child run events"
    );
}
