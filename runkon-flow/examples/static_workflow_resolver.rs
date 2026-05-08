use std::sync::Arc;

use runkon_flow::dsl::WorkflowDef;
use runkon_flow::engine_error::EngineError;
use runkon_flow::traits::workflow_resolver::WorkflowResolver;

/// `WorkflowResolver` that returns a single hard-coded `WorkflowDef` by name.
struct StaticWorkflowResolver {
    name: String,
    def: Arc<WorkflowDef>,
}

impl WorkflowResolver for StaticWorkflowResolver {
    fn resolve(&self, name: &str) -> Result<Arc<WorkflowDef>, EngineError> {
        if name == self.name {
            Ok(Arc::clone(&self.def))
        } else {
            Err(EngineError::WorkflowNotFound(name.to_string()))
        }
    }
}

fn main() {
    let dsl = r#"workflow hello {
  meta {
    description = "static resolver example"
    trigger     = "manual"
  }
  call my-agent
}"#;
    let def = runkon_flow::dsl::parse_workflow_str(dsl, "hello.wf").expect("DSL parse failed");

    let resolver = StaticWorkflowResolver {
        name: "hello".into(),
        def: Arc::new(def),
    };

    // Known name resolves successfully:
    match resolver.resolve("hello") {
        Ok(def) => println!("resolved '{}': {}", def.name, def.description),
        Err(e) => println!("error: {}", e),
    }

    // Unknown name returns WorkflowNotFound:
    match resolver.resolve("unknown") {
        Ok(_) => println!("unexpected success"),
        Err(EngineError::WorkflowNotFound(name)) => println!("not found: '{}'", name),
        Err(e) => println!("unexpected error: {}", e),
    }
}
