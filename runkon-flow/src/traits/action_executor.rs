use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::engine_error::EngineError;
use crate::traits::run_context::RunContext;

/// Trait for pluggable action execution.
pub trait ActionExecutor: Send + Sync {
    #[allow(dead_code)]
    fn name(&self) -> &str;
    fn execute(
        &self,
        ctx: &dyn RunContext,
        info: &StepInfo,
        params: &ActionParams,
    ) -> Result<ActionOutput, EngineError>;
    #[allow(dead_code)]
    fn cancel(&self, execution_id: &str) -> Result<(), EngineError> {
        let _ = execution_id;
        Ok(())
    }
}

/// Engine-populated per-call info for a workflow step.
pub struct StepInfo {
    pub step_id: String,
    pub step_timeout: Duration,
}

/// Per-invocation inputs passed to an `ActionExecutor`.
pub struct ActionParams {
    pub name: String,
    pub inputs: Arc<HashMap<String, String>>,
    #[allow(dead_code)]
    pub retries_remaining: u32,
    pub retry_error: Option<String>,
    pub snippets: Vec<String>,
    pub dry_run: bool,
    #[allow(dead_code)]
    pub gate_feedback: Option<String>,
    pub extensions: crate::extensions::Extensions,
    /// Optional named variant of the executor's underlying tool/agent.
    ///
    /// Cross-runtime convention — every major terminal AI tool uses `--model`:
    /// Claude (`claude-opus-4-7`), OpenAI Codex (`gpt-5.4`, `gpt-5.3-codex`),
    /// Moonshot Kimi (`k2.5`), Google Gemini (`gemini-2.5-flash`), Simon
    /// Willison's `llm` CLI, `aichat`, image-gen tools, etc.
    ///
    /// Conductor's `CliRuntime` substitutes `{{model}}` into a configurable arg
    /// template so any of these tools can be driven by a runkon-flow workflow
    /// (`runkon-runtimes/src/runtime/cli.rs`).
    ///
    /// Executors that have no concept of named variants (e.g. `SendEmailExecutor`,
    /// `HttpRequestExecutor`) ignore the field. `None` means "use the executor's
    /// default."
    pub model: Option<String>,
    /// When `Some`, names the identity this step's action should act as.
    /// Executor implementations resolve it into harness-defined auth
    /// material — typically credentials threaded into the spawned agent.
    /// Examples:
    ///
    /// - GitHub App installation name → `GH_TOKEN`
    /// - AWS service account ID → `AWS_ACCESS_KEY_ID` / related vars
    /// - Slack bot user ID → `SLACK_BOT_TOKEN`
    /// - Agent persona key → API key scoped to that persona
    ///
    /// Executors that don't model named identities ignore the field.
    pub as_identity: Option<String>,
    pub plugin_dirs: Vec<String>,
}

/// Output produced by an `ActionExecutor` on success.
#[derive(Debug, Default)]
pub struct ActionOutput {
    pub markers: Vec<String>,
    pub context: Option<String>,
    pub result_text: Option<String>,
    pub structured_output: Option<String>,
    /// Executor-specific key/value metadata. Claude executors populate the seven
    /// metric keys defined in `runkon_flow::constants::metadata_keys`.
    pub metadata: HashMap<String, String>,
    pub child_run_id: Option<String>,
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

    /// Construct a registry for external consumers that build registries outside the
    /// `FlowEngineBuilder` pipeline — such as bridge adapters or integration-test harnesses
    /// — that cannot use the builder's fluent API.
    pub fn from_executors(
        named: HashMap<String, Box<dyn ActionExecutor>>,
        fallback: Option<Box<dyn ActionExecutor>>,
    ) -> Self {
        Self::new(named, fallback)
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
        ctx: &dyn RunContext,
        info: &StepInfo,
        params: &ActionParams,
    ) -> Result<ActionOutput, EngineError> {
        match self.find_executor(name) {
            Some(e) => e.execute(ctx, info, params),
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
    use crate::test_helpers::{make_params, make_run_ctx, make_step_info};

    struct NoopExecutor;

    impl ActionExecutor for NoopExecutor {
        fn name(&self) -> &str {
            "noop"
        }
        fn execute(
            &self,
            _ctx: &dyn RunContext,
            _info: &StepInfo,
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
        let ctx = make_run_ctx();
        let info = make_step_info();
        let params = make_params("noop");
        let output = registry
            .dispatch("noop", ctx.as_ref(), &info, &params)
            .unwrap();
        assert_eq!(output.markers, vec!["done"]);
    }

    #[test]
    fn dispatch_fallback_when_no_named_match() {
        let registry = ActionRegistry::new(
            std::collections::HashMap::new(),
            Some(Box::new(NoopExecutor)),
        );
        let ctx = make_run_ctx();
        let info = make_step_info();
        let params = make_params("anything");
        let output = registry
            .dispatch("anything", ctx.as_ref(), &info, &params)
            .unwrap();
        assert_eq!(output.markers, vec!["done"]);
    }

    #[test]
    fn dispatch_error_when_no_executor_or_fallback() {
        let registry = ActionRegistry::new(std::collections::HashMap::new(), None);
        let ctx = make_run_ctx();
        let info = make_step_info();
        let params = make_params("missing");
        let err = registry
            .dispatch("missing", ctx.as_ref(), &info, &params)
            .unwrap_err();
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
