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
// Item 1 succeeds, item 2 fails → halt; item 3 must not be dispatched.
// ---------------------------------------------------------------------------

#[test]
fn test_foreach_on_child_fail_halt() {
    let items_data = vec![
        ("ticket", "t1", "T-1"),
        ("ticket", "t2", "T-2"),
        ("ticket", "t3", "T-3"),
    ];
    let mut outcomes = HashMap::new();
    outcomes.insert("t1".to_string(), true);
    outcomes.insert("t2".to_string(), false); // fails
    outcomes.insert("t3".to_string(), true);

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
    assert_eq!(count_status(&items, "completed"), 1, "t1 should complete");
    assert_eq!(count_status(&items, "failed"), 1, "t2 should fail");
    // t3 was never dispatched; stays pending
    assert_eq!(
        count_status(&items, "pending"),
        1,
        "t3 should remain pending (not dispatched)"
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
