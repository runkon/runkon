use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{atomic::AtomicBool, Arc};
use std::time::Duration;

use crate::engine_error::EngineError;
use crate::output_schema::OutputSchema;

/// Trait for pluggable action execution.
pub trait ActionExecutor: Send + Sync {
    #[allow(dead_code)]
    fn name(&self) -> &str;
    fn execute(
        &self,
        ectx: &ExecutionContext,
        params: &ActionParams,
    ) -> Result<ActionOutput, EngineError>;
    #[allow(dead_code)]
    fn cancel(&self, execution_id: &str) -> Result<(), EngineError> {
        let _ = execution_id;
        Ok(())
    }
}

/// Per-invocation inputs passed to an `ActionExecutor`.
pub struct ActionParams {
    pub name: String,
    pub inputs: HashMap<String, String>,
    #[allow(dead_code)]
    pub retries_remaining: u32,
    pub retry_error: Option<String>,
    pub snippets: Vec<String>,
    pub dry_run: bool,
    #[allow(dead_code)]
    pub gate_feedback: Option<String>,
    pub schema: Option<OutputSchema>,
}

/// Output produced by an `ActionExecutor` on success.
#[derive(Debug, Default)]
pub struct ActionOutput {
    pub markers: Vec<String>,
    pub context: Option<String>,
    pub result_text: Option<String>,
    pub structured_output: Option<String>,
    pub cost_usd: Option<f64>,
    pub num_turns: Option<i64>,
    pub duration_ms: Option<i64>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cache_read_input_tokens: Option<i64>,
    pub cache_creation_input_tokens: Option<i64>,
    pub child_run_id: Option<String>,
}

/// Conductor-specific execution context passed to every `ActionExecutor::execute` call.
pub struct ExecutionContext {
    /// Pre-created `agent_runs` row ID for this invocation.
    pub run_id: String,
    /// Absolute path to the worktree root.
    pub working_dir: PathBuf,
    /// Absolute path to the repository root.
    pub repo_path: String,
    /// Per-step timeout (from `WorkflowExecConfig`).
    pub step_timeout: Duration,
    /// Shutdown signal shared with the workflow engine.
    pub shutdown: Option<Arc<AtomicBool>>,
    /// Resolved model override for this step.
    pub model: Option<String>,
    /// Bot identity name for this step, if any.
    pub bot_name: Option<String>,
    /// Extra plugin directories to search for agent definitions.
    pub plugin_dirs: Vec<String>,
    /// Name of the parent workflow (used for workflow-local agent resolution).
    pub workflow_name: String,
    /// Worktree ID for this invocation, if any.
    pub worktree_id: Option<String>,
    /// Parent workflow run ID.
    pub parent_run_id: String,
    /// Step ID for this invocation.
    pub step_id: String,
}

/// Holds named and fallback `ActionExecutor` implementations.
pub struct ActionRegistry {
    named: HashMap<String, Box<dyn ActionExecutor>>,
    fallback: Option<Box<dyn ActionExecutor>>,
}

impl ActionRegistry {
    /// Construct a registry from pre-built maps (called only by `FlowEngineBuilder`).
    pub(crate) fn new(
        named: HashMap<String, Box<dyn ActionExecutor>>,
        fallback: Option<Box<dyn ActionExecutor>>,
    ) -> Self {
        Self { named, fallback }
    }

    /// Construct a registry for external consumers such as integration-test harnesses.
    ///
    /// Production code should use [`FlowEngineBuilder::action`] to register executors.
    pub fn from_executors(
        named: HashMap<String, Box<dyn ActionExecutor>>,
        fallback: Option<Box<dyn ActionExecutor>>,
    ) -> Self {
        Self { named, fallback }
    }

    /// Returns `true` if the named executor is registered OR a fallback is configured.
    ///
    /// Mirrors the fallback semantics of `dispatch()`: a harness that registers only
    /// a fallback executor passes all action name checks.
    pub fn has_action(&self, name: &str) -> bool {
        self.named.contains_key(name) || self.fallback.is_some()
    }

    fn find_executor(&self, name: &str) -> Option<&dyn ActionExecutor> {
        self.named
            .get(name)
            .map(|e| e.as_ref())
            .or(self.fallback.as_deref())
    }

    /// Find the executor for `name` and run it.
    pub fn dispatch(
        &self,
        name: &str,
        ectx: &ExecutionContext,
        params: &ActionParams,
    ) -> Result<ActionOutput, EngineError> {
        match self.find_executor(name) {
            Some(e) => e.execute(ectx, params),
            None => Err(EngineError::Workflow(format!(
                "no registered ActionExecutor for '{}' and no fallback configured",
                name
            ))),
        }
    }

    /// Call `cancel()` on the executor for `name`, if registered.
    /// Used by `FlowEngine::cancel_run()` to fire-and-forget executor-level cancellation.
    pub fn cancel(&self, name: &str, execution_id: &str) -> Result<(), EngineError> {
        match self.find_executor(name) {
            Some(e) => e.cancel(execution_id),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{make_ectx, make_params};

    struct NoopExecutor;

    impl ActionExecutor for NoopExecutor {
        fn name(&self) -> &str {
            "noop"
        }
        fn execute(
            &self,
            _ectx: &ExecutionContext,
            _params: &ActionParams,
        ) -> Result<ActionOutput, EngineError> {
            Ok(ActionOutput {
                markers: vec!["done".to_string()],
                context: Some("noop ran".to_string()),
                ..Default::default()
            })
        }
    }

    #[test]
    fn dispatch_named_executor() {
        let registry = ActionRegistry::new(
            [(
                "noop".to_string(),
                Box::new(NoopExecutor) as Box<dyn ActionExecutor>,
            )]
            .into_iter()
            .collect(),
            None,
        );
        let ectx = make_ectx();
        let params = make_params("noop");
        let output = registry.dispatch("noop", &ectx, &params).unwrap();
        assert_eq!(output.markers, vec!["done"]);
    }

    #[test]
    fn dispatch_fallback_when_no_named_match() {
        let registry = ActionRegistry::new(
            std::collections::HashMap::new(),
            Some(Box::new(NoopExecutor)),
        );
        let ectx = make_ectx();
        let params = make_params("anything");
        let output = registry.dispatch("anything", &ectx, &params).unwrap();
        assert_eq!(output.markers, vec!["done"]);
    }

    #[test]
    fn dispatch_error_when_no_executor_or_fallback() {
        let registry = ActionRegistry::new(std::collections::HashMap::new(), None);
        let ectx = make_ectx();
        let params = make_params("missing");
        let err = registry.dispatch("missing", &ectx, &params).unwrap_err();
        assert!(
            err.to_string()
                .contains("no registered ActionExecutor for 'missing'"),
            "got: {err}"
        );
    }

    #[test]
    fn cancel_default_impl_is_noop() {
        let executor = NoopExecutor;
        assert!(executor.cancel("any-id").is_ok());
    }

    #[test]
    fn has_action_named_executor_found() {
        let registry = ActionRegistry::new(
            [(
                "noop".to_string(),
                Box::new(NoopExecutor) as Box<dyn ActionExecutor>,
            )]
            .into_iter()
            .collect(),
            None,
        );
        assert!(registry.has_action("noop"));
        assert!(!registry.has_action("missing"));
    }

    #[test]
    fn has_action_true_with_fallback_regardless_of_name() {
        let registry = ActionRegistry::new(
            std::collections::HashMap::new(),
            Some(Box::new(NoopExecutor)),
        );
        assert!(registry.has_action("anything"));
        assert!(registry.has_action("also_this"));
    }

    #[test]
    fn has_action_false_when_empty() {
        let registry = ActionRegistry::new(std::collections::HashMap::new(), None);
        assert!(!registry.has_action("noop"));
    }
}
