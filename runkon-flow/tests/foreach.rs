mod common;

use std::collections::HashMap;
use std::sync::Arc;

use runkon_flow::dsl::OnChildFail;
use runkon_flow::executors::foreach::execute_foreach;
use runkon_flow::traits::persistence::WorkflowPersistence;

use common::{
    foreach_node, make_foreach_state, make_foreach_state_cancellable, make_persistence,
    ordered_foreach_node, CancellingMockRunner, FailingOrderedItemProvider, MockChildRunner,
    MockItemProvider, MockOrderedItemProvider,
};

// ---------------------------------------------------------------------------
// Helper: retrieve fan-out items for the foreach step by step name prefix.
// ---------------------------------------------------------------------------

fn fan_out_items(
    persistence: &runkon_flow::persistence_memory::InMemoryWorkflowPersistence,
    workflow_run_id: &str,
    step_name_prefix: &str,
) -> Vec<runkon_flow::types::FanOutItemRow> {
    let steps = persistence.get_steps(workflow_run_id).unwrap();
    let step = steps
        .iter()
        .find(|s| s.step_name.contains(step_name_prefix))
        .unwrap_or_else(|| panic!("no step matching '{step_name_prefix}'"));
    persistence.get_fan_out_items(&step.id, None).unwrap()
}

fn count_status(items: &[runkon_flow::types::FanOutItemRow], status: &str) -> usize {
    items.iter().filter(|i| i.status == status).count()
}

// ---------------------------------------------------------------------------
// Test 1: sequential (max_parallel = 1), 3 items, all succeed
// ---------------------------------------------------------------------------

#[test]
fn test_foreach_sequential_all_succeed() {
    let items_data = vec![
        ("ticket", "t1", "T-1"),
        ("ticket", "t2", "T-2"),
        ("ticket", "t3", "T-3"),
    ];
    let outcomes: HashMap<String, bool> = items_data
        .iter()
        .map(|(_, id, _)| (id.to_string(), true))
        .collect();

    let persistence = make_persistence();
    let mut state = make_foreach_state(
        "sequential-test",
        Arc::clone(&persistence),
        MockChildRunner::new(outcomes),
        MockItemProvider::new("tickets", items_data),
    );

    let node = foreach_node("fan-out", "tickets", "child-wf", 1, OnChildFail::Halt);
    let result = execute_foreach(&mut state, &node, 0);

    assert!(result.is_ok(), "expected Ok, got: {:?}", result);
    assert!(state.all_succeeded, "all_succeeded should be true");

    let items = fan_out_items(&persistence, &state.workflow_run_id, "foreach:fan-out");
    assert_eq!(items.len(), 3, "should have 3 fan-out items");
    assert_eq!(
        count_status(&items, "completed"),
        3,
        "all 3 should be completed"
    );
    assert_eq!(count_status(&items, "failed"), 0);
    assert_eq!(count_status(&items, "pending"), 0);
}

// ---------------------------------------------------------------------------
// Test 2: parallel fan-out (max_parallel = 3), 5 items, all succeed
// ---------------------------------------------------------------------------

#[test]
fn test_foreach_parallel_fan_out_all_succeed() {
    let items_data: Vec<(&str, &str, &str)> = vec![
        ("ticket", "t1", "T-1"),
        ("ticket", "t2", "T-2"),
        ("ticket", "t3", "T-3"),
        ("ticket", "t4", "T-4"),
        ("ticket", "t5", "T-5"),
    ];
    let outcomes: HashMap<String, bool> = items_data
        .iter()
        .map(|(_, id, _)| (id.to_string(), true))
        .collect();

    let persistence = make_persistence();
    let mut state = make_foreach_state(
        "parallel-test",
        Arc::clone(&persistence),
        MockChildRunner::new(outcomes),
        MockItemProvider::new("tickets", items_data),
    );

    let node = foreach_node("fan-out", "tickets", "child-wf", 3, OnChildFail::Continue);
    let result = execute_foreach(&mut state, &node, 0);

    assert!(result.is_ok(), "expected Ok, got: {:?}", result);
    assert!(state.all_succeeded, "all_succeeded should be true");

    let items = fan_out_items(&persistence, &state.workflow_run_id, "foreach:fan-out");
    assert_eq!(items.len(), 5, "should have 5 fan-out items");
    assert_eq!(
        count_status(&items, "completed"),
        5,
        "all 5 should be completed"
    );
    assert_eq!(count_status(&items, "failed"), 0);
    assert_eq!(count_status(&items, "pending"), 0);
}

// ---------------------------------------------------------------------------
// Test 3: halt on failure (max_parallel = 1)
// Failing item is placed first so it is always dispatched first (position()
// finds index 0 when all items are eligible; swap_remove(0) moves the last
// element to 0, which is one of the still-pending items). After the failure
// halt=true prevents any further dispatch, leaving the remaining 2 pending.
// ---------------------------------------------------------------------------

#[test]
fn test_foreach_on_child_fail_halt() {
    let items_data = vec![
        ("ticket", "t_fail", "T-fail"), // dispatched first; fails → halt
        ("ticket", "t1", "T-1"),
        ("ticket", "t2", "T-2"),
    ];
    let mut outcomes = HashMap::new();
    outcomes.insert("t_fail".to_string(), false); // fails
    outcomes.insert("t1".to_string(), true);
    outcomes.insert("t2".to_string(), true);

    let persistence = make_persistence();
    let mut state = make_foreach_state(
        "halt-test",
        Arc::clone(&persistence),
        MockChildRunner::new(outcomes),
        MockItemProvider::new("tickets", items_data),
    );

    let node = foreach_node("fan-out", "tickets", "child-wf", 1, OnChildFail::Halt);
    let result = execute_foreach(&mut state, &node, 0);

    // Step fails (fail_fast=false so returns Ok)
    assert!(
        result.is_ok(),
        "expected Ok (fail_fast=false), got: {:?}",
        result
    );
    assert!(!state.all_succeeded, "all_succeeded should be false");

    let items = fan_out_items(&persistence, &state.workflow_run_id, "foreach:fan-out");
    assert_eq!(items.len(), 3, "should have 3 fan-out items");
    assert_eq!(
        count_status(&items, "completed"),
        0,
        "no items should complete"
    );
    assert_eq!(count_status(&items, "failed"), 1, "t_fail should fail");
    // t1 and t2 were never dispatched; halt fired before them
    assert_eq!(
        count_status(&items, "pending"),
        2,
        "t1 and t2 should remain pending (not dispatched)"
    );
}

// ---------------------------------------------------------------------------
// Test 4: continue past failure (max_parallel = 1)
// Item 2 fails but the step continues to dispatch item 3.
// ---------------------------------------------------------------------------

#[test]
fn test_foreach_on_child_fail_continue() {
    let items_data = vec![
        ("ticket", "t1", "T-1"),
        ("ticket", "t2", "T-2"),
        ("ticket", "t3", "T-3"),
    ];
    let mut outcomes = HashMap::new();
    outcomes.insert("t1".to_string(), true);
    outcomes.insert("t2".to_string(), false); // fails but we continue
    outcomes.insert("t3".to_string(), true);

    let persistence = make_persistence();
    let mut state = make_foreach_state(
        "continue-test",
        Arc::clone(&persistence),
        MockChildRunner::new(outcomes),
        MockItemProvider::new("tickets", items_data),
    );

    let node = foreach_node("fan-out", "tickets", "child-wf", 1, OnChildFail::Continue);
    let result = execute_foreach(&mut state, &node, 0);

    assert!(
        result.is_ok(),
        "expected Ok (fail_fast=false), got: {:?}",
        result
    );
    assert!(
        !state.all_succeeded,
        "all_succeeded should be false (one item failed)"
    );

    let items = fan_out_items(&persistence, &state.workflow_run_id, "foreach:fan-out");
    assert_eq!(items.len(), 3);
    assert_eq!(
        count_status(&items, "completed"),
        2,
        "t1 and t3 should complete"
    );
    assert_eq!(count_status(&items, "failed"), 1, "t2 should fail");
    assert_eq!(
        count_status(&items, "pending"),
        0,
        "no items should remain pending"
    );
}

// ---------------------------------------------------------------------------
// Test 5: SkipDependents — failing item causes its dependents to be skipped
// ---------------------------------------------------------------------------

#[test]
fn test_foreach_on_child_fail_skip_dependents() {
    // t2 depends on t1; t3 has no dependencies.
    // t1 fails → t2 must be skipped; t3 must still complete.
    let items_data = vec![
        ("ticket", "t1", "T-1"),
        ("ticket", "t2", "T-2"),
        ("ticket", "t3", "T-3"),
    ];
    let mut outcomes = HashMap::new();
    outcomes.insert("t1".to_string(), false); // fails
    outcomes.insert("t2".to_string(), true); // would succeed but must be skipped
    outcomes.insert("t3".to_string(), true); // independent — must complete

    let persistence = make_persistence();
    let mut state = make_foreach_state(
        "skip-deps-test",
        Arc::clone(&persistence),
        MockChildRunner::new(outcomes),
        MockOrderedItemProvider::new("tickets", items_data, vec![("t1", "t2")]),
    );

    let node = ordered_foreach_node(
        "fan-out",
        "tickets",
        "child-wf",
        1,
        OnChildFail::SkipDependents,
    );
    let result = execute_foreach(&mut state, &node, 0);

    assert!(
        result.is_ok(),
        "expected Ok (fail_fast=false), got: {:?}",
        result
    );
    assert!(!state.all_succeeded, "all_succeeded should be false");

    let items = fan_out_items(&persistence, &state.workflow_run_id, "foreach:fan-out");
    assert_eq!(items.len(), 3);
    assert_eq!(count_status(&items, "failed"), 1, "t1 should fail");
    assert_eq!(count_status(&items, "skipped"), 1, "t2 should be skipped");
    assert_eq!(count_status(&items, "completed"), 1, "t3 should complete");
    assert_eq!(
        count_status(&items, "pending"),
        0,
        "no items should remain pending"
    );
}

// ---------------------------------------------------------------------------
// Test 6: ordered dependency graph — dep constraint enforced even with high parallelism
// ---------------------------------------------------------------------------

#[test]
fn test_foreach_ordered_with_dependencies() {
    // Chain: t1 → t2 → t3. Even with max_parallel=3 each item must wait for its
    // predecessor to land in terminal_ids before it can be dispatched.
    let items_data = vec![
        ("ticket", "t1", "T-1"),
        ("ticket", "t2", "T-2"),
        ("ticket", "t3", "T-3"),
    ];
    let outcomes: HashMap<String, bool> = items_data
        .iter()
        .map(|(_, id, _)| (id.to_string(), true))
        .collect();

    let persistence = make_persistence();
    let mut state = make_foreach_state(
        "ordered-test",
        Arc::clone(&persistence),
        MockChildRunner::new(outcomes),
        MockOrderedItemProvider::new("tickets", items_data, vec![("t1", "t2"), ("t2", "t3")]),
    );

    let node = ordered_foreach_node("fan-out", "tickets", "child-wf", 3, OnChildFail::Continue);
    let result = execute_foreach(&mut state, &node, 0);

    assert!(result.is_ok(), "expected Ok, got: {:?}", result);
    assert!(state.all_succeeded, "all items should succeed");

    let items = fan_out_items(&persistence, &state.workflow_run_id, "foreach:fan-out");
    assert_eq!(items.len(), 3);
    assert_eq!(
        count_status(&items, "completed"),
        3,
        "all 3 should complete"
    );
    assert_eq!(count_status(&items, "pending"), 0);
    assert_eq!(count_status(&items, "failed"), 0);
}

// ---------------------------------------------------------------------------
// Test 7: cancellation during parallel dispatch stops further item dispatch
// ---------------------------------------------------------------------------

#[test]
fn test_foreach_cancellation() {
    // The runner cancels the parent token after the first item completes.
    // Items t2 and t3 must remain pending (never dispatched).
    let items_data = vec![
        ("ticket", "t1", "T-1"),
        ("ticket", "t2", "T-2"),
        ("ticket", "t3", "T-3"),
    ];
    let outcomes: HashMap<String, bool> = items_data
        .iter()
        .map(|(_, id, _)| (id.to_string(), true))
        .collect();

    let cancellation = runkon_flow::CancellationToken::new();

    let persistence = make_persistence();
    let mut state = make_foreach_state_cancellable(
        "cancel-test",
        Arc::clone(&persistence),
        CancellingMockRunner::new(outcomes, 1, cancellation.clone()),
        MockItemProvider::new("tickets", items_data),
        cancellation,
    );

    let node = foreach_node("fan-out", "tickets", "child-wf", 1, OnChildFail::Continue);
    let result = execute_foreach(&mut state, &node, 0);

    assert!(result.is_ok(), "expected Ok, got: {:?}", result);

    let items = fan_out_items(&persistence, &state.workflow_run_id, "foreach:fan-out");
    assert_eq!(items.len(), 3, "all 3 fan-out items should be created");
    assert_eq!(
        count_status(&items, "completed"),
        1,
        "only t1 should complete"
    );
    assert_eq!(
        count_status(&items, "pending"),
        2,
        "t2 and t3 should remain pending after cancellation"
    );
}

// ---------------------------------------------------------------------------
// Test 8: empty provider — step completes immediately with 0-item summary
// ---------------------------------------------------------------------------

#[test]
fn test_foreach_empty_items() {
    let persistence = make_persistence();
    let mut state = make_foreach_state(
        "empty-test",
        Arc::clone(&persistence),
        MockChildRunner::all_succeed(&[]),
        MockItemProvider::new("tickets", vec![]),
    );

    let node = foreach_node("fan-out", "tickets", "child-wf", 1, OnChildFail::Halt);
    let result = execute_foreach(&mut state, &node, 0);

    assert!(result.is_ok(), "expected Ok, got: {:?}", result);
    assert!(state.all_succeeded, "empty foreach should succeed");

    let steps = persistence.get_steps(&state.workflow_run_id).unwrap();
    let step = steps
        .iter()
        .find(|s| s.step_name == "foreach:fan-out")
        .unwrap();
    assert_eq!(
        step.status,
        runkon_flow::status::WorkflowStepStatus::Completed,
        "step should be Completed"
    );
    // No fan-out items were created
    let items = persistence.get_fan_out_items(&step.id, None).unwrap();
    assert_eq!(items.len(), 0, "no fan-out items for empty provider");
}

// ---------------------------------------------------------------------------
// Test 9: persistence error in Phase 1 (get_fan_out_items) propagates as Err
// ---------------------------------------------------------------------------

#[test]
fn test_foreach_persistence_error_propagates() {
    let persistence = make_persistence();
    let mut state = make_foreach_state(
        "persistence-fail-test",
        Arc::clone(&persistence),
        MockChildRunner::all_succeed(&["t1"]),
        MockItemProvider::new("tickets", vec![("ticket", "t1", "T-1")]),
    );

    // Inject a failure into get_fan_out_items before executing.
    persistence.set_fail_get_fan_out_items(true);

    let node = foreach_node("fan-out", "tickets", "child-wf", 1, OnChildFail::Halt);
    let result = execute_foreach(&mut state, &node, 0);

    assert!(result.is_err(), "expected Err from persistence failure");
    assert!(
        matches!(
            result.unwrap_err(),
            runkon_flow::engine_error::EngineError::Persistence(_)
        ),
        "error should be EngineError::Persistence"
    );
}

// ---------------------------------------------------------------------------
// Test 10: ordered execution with failing dependencies() propagates as Err
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Test 11: item context fields are injected as item.* keys into child inputs
// ---------------------------------------------------------------------------

/// Item provider that returns items with a pre-populated context HashMap.
/// Stores items as (item_type, item_id, item_ref, context) tuples so that
/// `FanOutItem` — which does not implement `Clone` — can be freshly created on each call.
struct ContextItemProvider {
    name: String,
    items: Vec<(String, String, String, HashMap<String, String>)>,
}

impl runkon_flow::traits::item_provider::ItemProvider for ContextItemProvider {
    fn name(&self) -> &str {
        &self.name
    }

    fn items(
        &self,
        _ctx: &dyn runkon_flow::traits::run_context::RunContext,
        _info: &runkon_flow::traits::item_provider::ProviderInfo,
        _scope: Option<&dyn std::any::Any>,
        _filter: &HashMap<String, String>,
    ) -> Result<
        Vec<runkon_flow::traits::item_provider::FanOutItem>,
        runkon_flow::engine_error::EngineError,
    > {
        use runkon_flow::traits::item_provider::FanOutItem;
        Ok(self
            .items
            .iter()
            .map(|(t, i, r, ctx)| FanOutItem {
                item_type: t.clone(),
                item_id: i.clone(),
                item_ref: r.clone(),
                context: ctx.clone(),
            })
            .collect())
    }
}

/// Child runner that records the full `inputs` map for each call.
struct InputCapturingRunner {
    captured: std::sync::Mutex<Vec<HashMap<String, String>>>,
}

impl InputCapturingRunner {
    fn new() -> Self {
        Self {
            captured: std::sync::Mutex::new(vec![]),
        }
    }

    fn captured_inputs(&self) -> Vec<HashMap<String, String>> {
        self.captured.lock().unwrap().clone()
    }
}

impl runkon_flow::engine::ChildWorkflowRunner for InputCapturingRunner {
    fn execute_child(
        &self,
        workflow_name: &str,
        _parent_ctx: &runkon_flow::engine::ChildWorkflowContext,
        params: runkon_flow::engine::ChildWorkflowInput,
    ) -> runkon_flow::engine_error::Result<runkon_flow::types::WorkflowResult> {
        self.captured.lock().unwrap().push(params.inputs.clone());
        let item_id = params.inputs.get("item.id").cloned().unwrap_or_default();
        Ok(runkon_flow::types::WorkflowResult {
            workflow_run_id: format!("mock-run-{item_id}"),
            workflow_name: workflow_name.to_string(),
            all_succeeded: true,
            total_cost: 0.0,
            total_turns: 0,
            total_duration_ms: 0,
            total_input_tokens: 0,
            total_output_tokens: 0,
            total_cache_read_input_tokens: 0,
            total_cache_creation_input_tokens: 0,
        })
    }

    fn resume_child(
        &self,
        _workflow_run_id: &str,
        _model: Option<&str>,
        _parent_ctx: &runkon_flow::engine::ChildWorkflowContext,
    ) -> runkon_flow::engine_error::Result<runkon_flow::types::WorkflowResult> {
        unimplemented!()
    }

    fn find_resumable_child(
        &self,
        _parent_run_id: &str,
        _workflow_name: &str,
    ) -> runkon_flow::engine_error::Result<Option<runkon_flow::types::WorkflowRun>> {
        Ok(None)
    }
}

#[test]
fn test_foreach_item_context_injected_into_child_inputs() {
    use runkon_flow::ItemProviderRegistry;

    let mut ctx = HashMap::new();
    ctx.insert("title".to_string(), "Fix the bug".to_string());
    ctx.insert("state".to_string(), "open".to_string());

    let provider = ContextItemProvider {
        name: "tickets".to_string(),
        items: vec![(
            "ticket".to_string(),
            "t1".to_string(),
            "T-1".to_string(),
            ctx,
        )],
    };

    let runner = Arc::new(InputCapturingRunner::new());
    let persistence = make_persistence();

    let mut state = common::make_state("ctx-inject-test", Arc::clone(&persistence), HashMap::new());
    state.child_runner =
        Some(Arc::clone(&runner) as Arc<dyn runkon_flow::engine::ChildWorkflowRunner>);
    state.exec_config.fail_fast = false;
    let mut registry = ItemProviderRegistry::new();
    registry.register(provider);
    state.registry = Arc::new(registry);

    let node = foreach_node("fan-out", "tickets", "child-wf", 1, OnChildFail::Halt);
    let result = execute_foreach(&mut state, &node, 0);
    assert!(result.is_ok(), "expected Ok, got: {:?}", result);

    let inputs_list = runner.captured_inputs();
    assert_eq!(inputs_list.len(), 1, "one child run expected");
    let inputs = &inputs_list[0];

    // Struct-level fields are always present.
    assert_eq!(inputs.get("item.id").map(String::as_str), Some("t1"));
    assert_eq!(inputs.get("item.ref").map(String::as_str), Some("T-1"));

    // Context keys are injected as item.* variables.
    assert_eq!(
        inputs.get("item.title").map(String::as_str),
        Some("Fix the bug"),
        "item.title should be injected from context"
    );
    assert_eq!(
        inputs.get("item.state").map(String::as_str),
        Some("open"),
        "item.state should be injected from context"
    );
}

#[test]
fn test_foreach_ordered_dependencies_error_propagates() {
    let persistence = make_persistence();
    let mut state = make_foreach_state(
        "deps-fail-test",
        Arc::clone(&persistence),
        MockChildRunner::all_succeed(&["t1", "t2"]),
        FailingOrderedItemProvider::new(
            "tickets",
            vec![("ticket", "t1", "T-1"), ("ticket", "t2", "T-2")],
        ),
    );

    let node = ordered_foreach_node("fan-out", "tickets", "child-wf", 1, OnChildFail::Halt);
    let result = execute_foreach(&mut state, &node, 0);

    assert!(
        result.is_err(),
        "expected Err from dependency fetch failure"
    );
    assert!(
        matches!(
            result.unwrap_err(),
            runkon_flow::engine_error::EngineError::Workflow(_)
        ),
        "error should be EngineError::Workflow"
    );
}
