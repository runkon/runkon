//! Standalone doc example: `runkon-flow-executors` is self-contained.
//!
//! Constructs a `FlowEngine` wired entirely from `runkon_flow_executors` types
//! and in-memory helpers from `runkon_flow` — zero `conductor_*` imports.
//! Compilation alone proves the extraction is real; no workflow is executed.
//!
//! Run:
//!   cargo run --example standalone_flow --features test-utils

use std::sync::Arc;

use runkon_flow::engine_error::EngineError;
use runkon_flow::persistence_memory::InMemoryWorkflowPersistence;
use runkon_flow::traits::action_executor::{ActionExecutor, ActionOutput, ActionParams, StepInfo};
use runkon_flow::traits::run_context::RunContext;
use runkon_flow::{FlowEngineBuilder, InMemoryWorkflowResolver};
use runkon_flow_executors::anthropic_api::ApiCallExecutor;
use runkon_flow_executors::{ChannelEventSink, ClaudeAgentExecutor, PathPrependingEnvProvider};
use runkon_runtimes::{AgentRuntime, Result as RkResult, RuntimeError, RuntimeResolver};

// Stub resolver so ClaudeAgentExecutor can be instantiated without a real runtime.
// resolve() always returns Err — the executor is never dispatched in this example.
struct StubRuntimeResolver;

impl RuntimeResolver for StubRuntimeResolver {
    fn resolve(&self, name: &str) -> RkResult<Box<dyn AgentRuntime>> {
        Err(RuntimeError::Config(format!(
            "stub resolver: '{name}' is not wired for actual execution"
        )))
    }
}

// Wraps ClaudeAgentExecutor + ApiCallExecutor behind the ActionExecutor trait.
// Real conductor wiring lives in conductor-core::workflow::runkon_bridge.
struct StandaloneExecutor {
    _agent: ClaudeAgentExecutor,
    _api: ApiCallExecutor,
}

impl ActionExecutor for StandaloneExecutor {
    fn name(&self) -> &str {
        "standalone"
    }

    fn execute(
        &self,
        _ctx: &dyn RunContext,
        _info: &StepInfo,
        _params: &ActionParams,
    ) -> Result<ActionOutput, EngineError> {
        unimplemented!("doc example — not for actual execution")
    }
}

fn main() {
    // Instantiate all executor types — compilation proves zero conductor_* imports needed.
    let agent = ClaudeAgentExecutor::new(Arc::new(StubRuntimeResolver), None);
    let api = ApiCallExecutor::new("sk-placeholder".to_string());
    let env = PathPrependingEnvProvider::new(vec![]);
    let (tx, _rx) = std::sync::mpsc::channel();
    let sink = ChannelEventSink(tx);
    let _persistence = InMemoryWorkflowPersistence::new();
    let resolver =
        InMemoryWorkflowResolver::new(std::iter::empty::<(&str, runkon_flow::dsl::WorkflowDef)>());

    let executor = StandaloneExecutor {
        _agent: agent,
        _api: api,
    };

    let _engine = FlowEngineBuilder::new()
        .action_fallback(Box::new(executor))
        .expect("fallback executor registration failed")
        .script_env_provider(Box::new(env))
        .event_sink(Box::new(sink))
        .workflow_resolver(Box::new(resolver))
        .build()
        .expect("FlowEngine construction failed");

    println!(
        "runkon-flow-executors is self-contained — FlowEngine built with zero conductor_* imports"
    );
}
