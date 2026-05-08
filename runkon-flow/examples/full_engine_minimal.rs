//! End-to-end example: build a `FlowEngine`, define a 2-step workflow, and run it.
//!
//! Requires: `cargo run --example full_engine_minimal -p runkon-flow --features test-utils`

use std::collections::HashMap;
use std::sync::Arc;

use runkon_flow::engine_error::EngineError;
use runkon_flow::persistence_memory::InMemoryWorkflowPersistence;
use runkon_flow::test_helpers::make_test_execution_state;
use runkon_flow::traits::action_executor::{ActionExecutor, ActionOutput, ActionParams, StepInfo};
use runkon_flow::traits::persistence::{NewRun, WorkflowPersistence};
use runkon_flow::traits::run_context::RunContext;
use runkon_flow::ActionRegistry;
use runkon_flow::FlowEngineBuilder;

// Inline EchoExecutor — each example has its own main(), so we can't reuse
// the definition from echo_executor.rs via #[path] (that would duplicate main).
struct EchoExecutor;

impl ActionExecutor for EchoExecutor {
    fn name(&self) -> &str {
        "echo"
    }

    fn execute(
        &self,
        _ctx: &dyn RunContext,
        _info: &StepInfo,
        params: &ActionParams,
    ) -> Result<ActionOutput, EngineError> {
        let text = params
            .inputs
            .get("text")
            .cloned()
            .unwrap_or_else(|| format!("echo:{}", params.name));
        Ok(ActionOutput {
            result_text: Some(text),
            ..Default::default()
        })
    }
}

fn main() {
    // 1. Build the engine with EchoExecutor registered under the name "echo".
    let engine = FlowEngineBuilder::new()
        .action(Box::new(EchoExecutor))
        .build()
        .expect("engine build failed");

    // 2. Parse a 2-step workflow that calls "echo" twice.
    let dsl = r#"workflow two-step {
  meta {
    description = "two-step echo demo"
    trigger     = "manual"
  }
  call echo
  call echo
}"#;
    let def = runkon_flow::dsl::parse_workflow_str(dsl, "full_engine_minimal.wf")
        .expect("DSL parse failed");

    // 3. Create in-memory persistence and register a run record.
    let persistence = Arc::new(InMemoryWorkflowPersistence::new());
    let run = (persistence.as_ref() as &dyn WorkflowPersistence)
        .create_run(NewRun {
            workflow_name: "two-step".into(),
            parent_run_id: String::new(),
            dry_run: false,
            trigger: "example".into(),
            definition_snapshot: None,
            parent_workflow_run_id: None,
        })
        .expect("create_run failed");

    // 4. Build ExecutionState via test helper, then wire in the matching executor registry.
    let mut state = make_test_execution_state(
        Arc::clone(&persistence) as Arc<dyn WorkflowPersistence>,
        run.id,
    );
    state.workflow_name = "two-step".into();
    state.action_registry = Arc::new(ActionRegistry::from_executors(
        [(
            "echo".to_string(),
            Box::new(EchoExecutor) as Box<dyn ActionExecutor>,
        )]
        .into_iter()
        .collect::<HashMap<_, _>>(),
        None,
    ));

    // 5. Run the workflow end-to-end.
    let result = engine.run(&def, &mut state).expect("run failed");
    println!("workflow:   {}", result.workflow_name);
    println!("run_id:     {}", result.workflow_run_id);
    println!("succeeded:  {}", result.all_succeeded);
}
