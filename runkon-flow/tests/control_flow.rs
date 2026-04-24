mod common;

use std::sync::Arc;

use runkon_flow::dsl::{Condition, IfNode, UnlessNode, WorkflowNode};
use runkon_flow::traits::persistence::WorkflowPersistence;
use runkon_flow::FlowEngineBuilder;

use common::{
    call_node, make_def, make_persistence, make_state, named_executors, ActionExecutor,
    MockExecutor,
};

// ---------------------------------------------------------------------------
// if — condition taken (BoolInput = "true")
// ---------------------------------------------------------------------------

#[test]
fn if_condition_taken_runs_body() {
    let engine = FlowEngineBuilder::new()
        .action(Box::new(MockExecutor::new("inner-step")))
        .build()
        .expect("engine build failed");

    let if_node = WorkflowNode::If(IfNode {
        condition: Condition::BoolInput {
            input: "run_it".to_string(),
        },
        body: vec![call_node("inner-step")],
    });

    let def = make_def("if-taken", vec![if_node]);

    let persistence = make_persistence();
    let mut state = make_state(
        "if-taken",
        Arc::clone(&persistence),
        named_executors([Box::new(MockExecutor::new("inner-step")) as Box<dyn ActionExecutor>]),
    );
    state
        .inputs
        .insert("run_it".to_string(), "true".to_string());

    let result = engine.run(&def, &mut state).expect("run should succeed");

    assert!(result.all_succeeded);
    let steps = persistence
        .get_steps(&result.workflow_run_id)
        .expect("get_steps failed");

    let inner_ran = steps.iter().any(|s| s.step_name == "inner-step");
    assert!(inner_ran, "inner-step should run when if condition is true");
}

// ---------------------------------------------------------------------------
// if — condition not taken (BoolInput = "false")
// ---------------------------------------------------------------------------

#[test]
fn if_condition_not_taken_skips_body() {
    let engine = FlowEngineBuilder::new()
        .action(Box::new(MockExecutor::new("inner-step")))
        .build()
        .expect("engine build failed");

    let if_node = WorkflowNode::If(IfNode {
        condition: Condition::BoolInput {
            input: "run_it".to_string(),
        },
        body: vec![call_node("inner-step")],
    });

    let def = make_def("if-not-taken", vec![if_node]);

    let persistence = make_persistence();
    let mut state = make_state(
        "if-not-taken",
        Arc::clone(&persistence),
        named_executors([Box::new(MockExecutor::new("inner-step")) as Box<dyn ActionExecutor>]),
    );
    state
        .inputs
        .insert("run_it".to_string(), "false".to_string());

    let result = engine.run(&def, &mut state).expect("run should succeed");

    assert!(result.all_succeeded);
    let steps = persistence
        .get_steps(&result.workflow_run_id)
        .expect("get_steps failed");

    let inner_ran = steps.iter().any(|s| s.step_name == "inner-step");
    assert!(
        !inner_ran,
        "inner-step should be skipped when if condition is false"
    );
}

// ---------------------------------------------------------------------------
// unless — body runs when condition is false
// ---------------------------------------------------------------------------

#[test]
fn unless_condition_false_runs_body() {
    let engine = FlowEngineBuilder::new()
        .action(Box::new(MockExecutor::new("fallback-step")))
        .build()
        .expect("engine build failed");

    let unless_node = WorkflowNode::Unless(UnlessNode {
        condition: Condition::BoolInput {
            input: "skip_me".to_string(),
        },
        body: vec![call_node("fallback-step")],
    });

    let def = make_def("unless-false", vec![unless_node]);

    let persistence = make_persistence();
    let mut state = make_state(
        "unless-false",
        Arc::clone(&persistence),
        named_executors([Box::new(MockExecutor::new("fallback-step")) as Box<dyn ActionExecutor>]),
    );
    state
        .inputs
        .insert("skip_me".to_string(), "false".to_string());

    let result = engine.run(&def, &mut state).expect("run should succeed");

    assert!(result.all_succeeded);
    let steps = persistence
        .get_steps(&result.workflow_run_id)
        .expect("get_steps failed");

    let fallback_ran = steps.iter().any(|s| s.step_name == "fallback-step");
    assert!(
        fallback_ran,
        "fallback-step should run when unless condition is false"
    );
}

// ---------------------------------------------------------------------------
// unless — body skipped when condition is true
// ---------------------------------------------------------------------------

#[test]
fn unless_condition_true_skips_body() {
    let engine = FlowEngineBuilder::new()
        .action(Box::new(MockExecutor::new("fallback-step")))
        .build()
        .expect("engine build failed");

    let unless_node = WorkflowNode::Unless(UnlessNode {
        condition: Condition::BoolInput {
            input: "skip_me".to_string(),
        },
        body: vec![call_node("fallback-step")],
    });

    let def = make_def("unless-true", vec![unless_node]);

    let persistence = make_persistence();
    let mut state = make_state(
        "unless-true",
        Arc::clone(&persistence),
        named_executors([Box::new(MockExecutor::new("fallback-step")) as Box<dyn ActionExecutor>]),
    );
    state
        .inputs
        .insert("skip_me".to_string(), "true".to_string());

    let result = engine.run(&def, &mut state).expect("run should succeed");

    assert!(result.all_succeeded);
    let steps = persistence
        .get_steps(&result.workflow_run_id)
        .expect("get_steps failed");

    let fallback_ran = steps.iter().any(|s| s.step_name == "fallback-step");
    assert!(
        !fallback_ran,
        "fallback-step should be skipped when unless condition is true"
    );
}

// ---------------------------------------------------------------------------
// if using StepMarker condition
// ---------------------------------------------------------------------------

#[test]
fn if_step_marker_condition_taken() {
    let engine = FlowEngineBuilder::new()
        .action(Box::new(MockExecutor::with_markers(
            "detector",
            &["has_issues"],
        )))
        .action(Box::new(MockExecutor::new("reporter")))
        .build()
        .expect("engine build failed");

    let if_node = WorkflowNode::If(IfNode {
        condition: Condition::StepMarker {
            step: "detector".to_string(),
            marker: "has_issues".to_string(),
        },
        body: vec![call_node("reporter")],
    });

    let def = make_def("if-marker", vec![call_node("detector"), if_node]);

    let persistence = make_persistence();
    let mut state = make_state(
        "if-marker",
        Arc::clone(&persistence),
        named_executors([
            Box::new(MockExecutor::with_markers("detector", &["has_issues"]))
                as Box<dyn ActionExecutor>,
            Box::new(MockExecutor::new("reporter")) as Box<dyn ActionExecutor>,
        ]),
    );

    let result = engine.run(&def, &mut state).expect("run should succeed");

    assert!(result.all_succeeded);
    let steps = persistence
        .get_steps(&result.workflow_run_id)
        .expect("get_steps failed");

    let reporter_ran = steps.iter().any(|s| s.step_name == "reporter");
    assert!(
        reporter_ran,
        "reporter should run when detector produces the marker"
    );
}

// ---------------------------------------------------------------------------
// if using StepMarker — condition not met
// ---------------------------------------------------------------------------

#[test]
fn if_step_marker_condition_not_met_skips_body() {
    // detector produces no markers → condition is false → reporter is skipped
    let engine = FlowEngineBuilder::new()
        .action(Box::new(MockExecutor::new("detector")))
        .action(Box::new(MockExecutor::new("reporter")))
        .build()
        .expect("engine build failed");

    let if_node = WorkflowNode::If(IfNode {
        condition: Condition::StepMarker {
            step: "detector".to_string(),
            marker: "has_issues".to_string(),
        },
        body: vec![call_node("reporter")],
    });

    let def = make_def("if-marker-false", vec![call_node("detector"), if_node]);

    let persistence = make_persistence();
    let mut state = make_state(
        "if-marker-false",
        Arc::clone(&persistence),
        named_executors([
            Box::new(MockExecutor::new("detector")) as Box<dyn ActionExecutor>,
            Box::new(MockExecutor::new("reporter")) as Box<dyn ActionExecutor>,
        ]),
    );

    let result = engine.run(&def, &mut state).expect("run should succeed");

    assert!(result.all_succeeded);
    let steps = persistence
        .get_steps(&result.workflow_run_id)
        .expect("get_steps failed");

    let reporter_ran = steps.iter().any(|s| s.step_name == "reporter");
    assert!(
        !reporter_ran,
        "reporter should be skipped when detector produces no marker"
    );
    let detector_ran = steps.iter().any(|s| s.step_name == "detector");
    assert!(detector_ran, "detector itself should still run");
}

// ---------------------------------------------------------------------------
// Nested if blocks
// ---------------------------------------------------------------------------

#[test]
fn nested_if_both_conditions_true() {
    let engine = FlowEngineBuilder::new()
        .action(Box::new(MockExecutor::new("deep-step")))
        .build()
        .expect("engine build failed");

    let inner_if = WorkflowNode::If(IfNode {
        condition: Condition::BoolInput {
            input: "inner".to_string(),
        },
        body: vec![call_node("deep-step")],
    });

    let outer_if = WorkflowNode::If(IfNode {
        condition: Condition::BoolInput {
            input: "outer".to_string(),
        },
        body: vec![inner_if],
    });

    let def = make_def("nested-if", vec![outer_if]);

    let persistence = make_persistence();
    let mut state = make_state(
        "nested-if",
        Arc::clone(&persistence),
        named_executors([Box::new(MockExecutor::new("deep-step")) as Box<dyn ActionExecutor>]),
    );
    state.inputs.insert("outer".to_string(), "true".to_string());
    state.inputs.insert("inner".to_string(), "true".to_string());

    let result = engine.run(&def, &mut state).expect("run should succeed");

    assert!(result.all_succeeded);
    let steps = persistence
        .get_steps(&result.workflow_run_id)
        .expect("get_steps failed");

    let deep_ran = steps.iter().any(|s| s.step_name == "deep-step");
    assert!(
        deep_ran,
        "deep-step should run when both outer and inner conditions are true"
    );
}

#[test]
fn nested_if_outer_false_skips_inner() {
    let engine = FlowEngineBuilder::new()
        .action(Box::new(MockExecutor::new("deep-step")))
        .build()
        .expect("engine build failed");

    let inner_if = WorkflowNode::If(IfNode {
        condition: Condition::BoolInput {
            input: "inner".to_string(),
        },
        body: vec![call_node("deep-step")],
    });

    let outer_if = WorkflowNode::If(IfNode {
        condition: Condition::BoolInput {
            input: "outer".to_string(),
        },
        body: vec![inner_if],
    });

    let def = make_def("nested-if-outer-false", vec![outer_if]);

    let persistence = make_persistence();
    let mut state = make_state(
        "nested-if-outer-false",
        Arc::clone(&persistence),
        named_executors([Box::new(MockExecutor::new("deep-step")) as Box<dyn ActionExecutor>]),
    );
    state
        .inputs
        .insert("outer".to_string(), "false".to_string());
    state.inputs.insert("inner".to_string(), "true".to_string());

    let result = engine.run(&def, &mut state).expect("run should succeed");

    assert!(result.all_succeeded);
    let steps = persistence
        .get_steps(&result.workflow_run_id)
        .expect("get_steps failed");

    let deep_ran = steps.iter().any(|s| s.step_name == "deep-step");
    assert!(
        !deep_ran,
        "deep-step should be skipped when outer if is false"
    );
}
