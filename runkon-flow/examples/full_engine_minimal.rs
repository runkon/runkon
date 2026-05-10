//! End-to-end example: build a `FlowEngine`, define a 2-step workflow, and run it.
//!
//! Demonstrates the production entry point `FlowEngine::run_workflow` so no
//! `test-utils` feature is needed for the entry point itself.
//!
//! `InMemoryWorkflowPersistence` still requires `--features test-utils` because
//! it is a test-only persistence backend. A real deployment would supply a
//! `SqliteWorkflowPersistence` or a custom `WorkflowPersistence` impl instead.
//!
//! Run with:
//!   cargo run --example full_engine_minimal -p runkon-flow --features test-utils

use std::collections::HashMap;
use std::sync::Arc;

use runkon_flow::engine_error::EngineError;
use runkon_flow::persistence_memory::InMemoryWorkflowPersistence;
use runkon_flow::traits::action_executor::{ActionExecutor, ActionOutput, ActionParams, StepInfo};
use runkon_flow::traits::persistence::{NewRun, WorkflowPersistence};
use runkon_flow::traits::run_context::{NoopRunContext, RunContext};
use runkon_flow::traits::script_env_provider::NoOpScriptEnvProvider;
use runkon_flow::types::WorkflowExecConfig;
use runkon_flow::ActionRegistry;
use runkon_flow::CancellationToken;
use runkon_flow::FlowEngineBuilder;
use runkon_flow::ItemProviderRegistry;
use runkon_flow::RunInput;

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

    // 4. Build a per-run ActionRegistry with the EchoExecutor.
    let action_registry = Arc::new(ActionRegistry::from_executors(
        [(
            "echo".to_string(),
            Box::new(EchoExecutor) as Box<dyn ActionExecutor>,
        )]
        .into_iter()
        .collect::<HashMap<_, _>>(),
        None,
    ));

    // 5. Run the workflow via the production entry point — no test_helpers needed.
    let result = engine
        .run_workflow(
            &def,
            RunInput {
                persistence: Arc::clone(&persistence) as Arc<dyn WorkflowPersistence>,
                workflow_run_id: run.id,
                workflow_name: "two-step".into(),
                action_registry,
                item_provider_registry: Arc::new(ItemProviderRegistry::new()),
                script_env_provider: Arc::new(NoOpScriptEnvProvider),
                run_ctx: Arc::new(NoopRunContext::default()),
                extra_plugin_dirs: vec![],
                model: None,
                exec_config: WorkflowExecConfig::default(),
                inputs: HashMap::new(),
                parent_run_id: String::new(),
                depth: 0,
                target_label: None,
                default_as_identity: None,
                triggered_by_hook: false,
                schema_resolver: None,
                child_runner: None,
                cancellation: CancellationToken::new(),
                event_sinks: vec![],
            },
        )
        .expect("run failed");

    println!("workflow:   {}", result.workflow_name);
    println!("run_id:     {}", result.workflow_run_id);
    println!("succeeded:  {}", result.all_succeeded);
}
