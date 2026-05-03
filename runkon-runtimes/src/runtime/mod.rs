//! AgentRuntime trait and dispatch infrastructure (RFC 007).

pub mod claude;
pub mod cli;
pub mod script;

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{atomic::AtomicBool, Arc};

use crate::agent_def::AgentDef;
use crate::config::RuntimeConfig;
use crate::error::{Result, RuntimeError};
use crate::permission::PermissionMode;
use crate::run::RunHandle;
use crate::tracker::{RunEventSink, RunTracker};

/// Sealed capability token for `AgentRuntime::spawn_impl`.
pub mod private {
    pub struct Seal(());
    impl Seal {
        pub(super) fn new() -> Self {
            Self(())
        }
    }
}

/// Trait implemented by every agent runtime.
pub trait AgentRuntime {
    /// Launch the agent for `request`.
    fn spawn_impl(&self, request: &RuntimeRequest, _seal: private::Seal) -> Result<()>;

    /// Validates `request.run_id` then delegates to `spawn_impl`.
    fn spawn_validated(&self, request: &RuntimeRequest) -> Result<()> {
        crate::text_util::validate_run_id(&request.run_id)?;
        self.spawn_impl(request, private::Seal::new())
    }

    /// Block until the agent completes or is cancelled.
    fn poll(
        &self,
        run_id: &str,
        shutdown: Option<&Arc<AtomicBool>>,
        step_timeout: std::time::Duration,
    ) -> std::result::Result<RunHandle, PollError>;

    /// Returns true if the agent process / session represented by `run` is still live.
    fn is_alive(&self, run: &RunHandle) -> bool;

    /// Forcibly cancel the agent represented by `run`.
    fn cancel(&self, run: &RunHandle) -> Result<()>;
}

/// Per-invocation parameters passed to `AgentRuntime::spawn`.
///
/// The `model` field is a workflow-level *override*; the agent definition
/// also carries a `model` from its frontmatter. Runtimes should call
/// [`RuntimeRequest::resolved_model`] rather than reading `self.model`
/// directly so the override-then-frontmatter precedence is honored.
pub struct RuntimeRequest {
    pub run_id: String,
    pub agent_def: AgentDef,
    pub prompt: String,
    pub working_dir: PathBuf,
    pub model: Option<String>,
    pub extra_cli_args: Vec<(Cow<'static, str>, Cow<'static, str>)>,
    pub plugin_dirs: Vec<String>,
    pub resume_session_id: Option<String>,
    pub tracker: Arc<dyn RunTracker>,
    pub event_sink: Arc<dyn RunEventSink>,
}

impl RuntimeRequest {
    /// Final model to launch the agent with: workflow-level override
    /// (`self.model`) wins; otherwise fall back to the agent file's
    /// frontmatter `model:` field. `None` means no model flag is set,
    /// and the spawned subprocess inherits the host's default.
    pub fn resolved_model(&self) -> Option<&str> {
        self.model.as_deref().or(self.agent_def.model.as_deref())
    }
}

impl Default for RuntimeRequest {
    fn default() -> Self {
        Self {
            run_id: String::new(),
            agent_def: crate::agent_def::AgentDef::default(),
            prompt: String::new(),
            working_dir: PathBuf::new(),
            model: None,
            extra_cli_args: vec![],
            plugin_dirs: vec![],
            resume_session_id: None,
            tracker: Arc::new(crate::tracker::NoopTracker),
            event_sink: Arc::new(crate::tracker::NoopEventSink),
        }
    }
}

/// Error returned by `AgentRuntime::poll`.
#[derive(Debug)]
pub enum PollError {
    /// Agent subprocess exited without emitting a `result` event.
    NoResult,
    /// Workflow shutdown was requested while the agent was running.
    Cancelled,
    /// Poll failed with an explanatory message.
    Failed(String),
}

impl std::fmt::Display for PollError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoResult => write!(f, "agent exited without result"),
            Self::Cancelled => write!(f, "executor shutdown requested"),
            Self::Failed(msg) => write!(f, "{msg}"),
        }
    }
}

/// Options injected at runtime construction time.
pub struct RuntimeOptions {
    /// Binary to spawn for headless re-invocation.
    pub binary_path: PathBuf,
    /// Where to write the per-run JSONL log of the agent's stdout stream.
    pub log_path_for_run: Arc<dyn Fn(&str) -> PathBuf + Send + Sync>,
    /// Where `CliRuntime` writes `<run_id>/output.json`.
    pub workspace_root: PathBuf,
}

/// Resolve a runtime name to a boxed `AgentRuntime` implementation.
pub fn resolve_runtime(
    name: &str,
    permission_mode: PermissionMode,
    runtimes: &HashMap<String, RuntimeConfig>,
    options: &RuntimeOptions,
) -> Result<Box<dyn AgentRuntime>> {
    if name == "claude" {
        let claude_options = claude::ClaudeRuntimeOptions {
            permission_mode,
            binary_path: options.binary_path.clone(),
            log_path_for_run: options.log_path_for_run.clone(),
        };
        return Ok(Box::new(claude::ClaudeRuntime::new(claude_options)));
    }
    let rt_config = runtimes.get(name).ok_or_else(|| {
        RuntimeError::Config(format!(
            "unknown runtime '{name}' — only 'claude' is built-in; \
                 add a `[runtimes.{name}]` section to your host config for CLI agents"
        ))
    })?;
    match rt_config.runtime_type.as_deref().unwrap_or("cli") {
        "cli" => Ok(Box::new(cli::CliRuntime::new(
            rt_config.clone(),
            options.workspace_root.clone(),
        ))),
        "script" => Ok(Box::new(script::ScriptRuntime::new(rt_config.clone()))),
        t => Err(RuntimeError::Config(format!(
            "unsupported runtime type '{t}' for '{name}'"
        ))),
    }
}

/// Extract a value from a serde_json::Value using a dot-separated path.
pub fn extract_json_path<'a>(
    value: &'a serde_json::Value,
    path: &str,
) -> Option<Cow<'a, serde_json::Value>> {
    let parts: Vec<&str> = path.split('.').collect();
    extract_path_recursive(value, &parts)
}

fn extract_path_recursive<'a>(
    value: &'a serde_json::Value,
    parts: &[&str],
) -> Option<Cow<'a, serde_json::Value>> {
    if parts.is_empty() {
        return Some(Cow::Borrowed(value));
    }
    let head = parts[0];
    let tail = &parts[1..];
    if head == "*" {
        match value {
            serde_json::Value::Object(m) => {
                if tail.is_empty() {
                    return Some(Cow::Owned(serde_json::Value::Array(
                        m.values().cloned().collect(),
                    )));
                }
                let sum: f64 = m
                    .values()
                    .filter_map(|child| extract_path_recursive(child, tail))
                    .filter_map(|v| v.as_f64())
                    .sum();
                return Some(Cow::Owned(serde_json::json!(sum)));
            }
            serde_json::Value::Array(a) => {
                if tail.is_empty() {
                    return Some(Cow::Borrowed(value));
                }
                let sum: f64 = a
                    .iter()
                    .filter_map(|child| extract_path_recursive(child, tail))
                    .filter_map(|v| v.as_f64())
                    .sum();
                return Some(Cow::Owned(serde_json::json!(sum)));
            }
            _ => return None,
        }
    }
    match value {
        serde_json::Value::Object(m) => {
            let child = m.get(head)?;
            extract_path_recursive(child, tail)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_def::{AgentDef, AgentRole};
    use crate::tracker::NoopEventSink;
    use serde_json::json;
    use test_util::NoopTracker;

    fn make_request(req_model: Option<&str>, def_model: Option<&str>) -> RuntimeRequest {
        RuntimeRequest {
            run_id: "r".to_string(),
            agent_def: AgentDef {
                name: "test".to_string(),
                role: AgentRole::Reviewer,
                can_commit: false,
                model: def_model.map(String::from),
                runtime: "claude".to_string(),
                prompt: String::new(),
            },
            prompt: "p".to_string(),
            working_dir: PathBuf::from("/tmp"),
            model: req_model.map(String::from),
            extra_cli_args: vec![],
            plugin_dirs: vec![],
            resume_session_id: None,
            tracker: Arc::new(NoopTracker),
            event_sink: Arc::new(NoopEventSink),
        }
    }

    #[test]
    fn resolved_model_prefers_request_override_over_agent_def() {
        let req = make_request(Some("sonnet"), Some("haiku"));
        assert_eq!(req.resolved_model(), Some("sonnet"));
    }

    #[test]
    fn resolved_model_falls_back_to_agent_def() {
        let req = make_request(None, Some("claude-sonnet-4-6"));
        assert_eq!(req.resolved_model(), Some("claude-sonnet-4-6"));
    }

    #[test]
    fn resolved_model_returns_none_when_neither_set() {
        let req = make_request(None, None);
        assert_eq!(req.resolved_model(), None);
    }

    #[test]
    fn test_extract_simple_field() {
        let v = json!({"response": "hello", "status": "ok"});
        assert_eq!(
            extract_json_path(&v, "response").as_deref(),
            Some(&json!("hello"))
        );
    }

    #[test]
    fn test_extract_nested_field() {
        let v = json!({"stats": {"total": 42}});
        assert_eq!(
            extract_json_path(&v, "stats.total").as_deref(),
            Some(&json!(42))
        );
    }

    #[test]
    fn test_extract_wildcard_sum() {
        let v = json!({
            "models": {
                "a": {"tokens": {"total": 100}},
                "b": {"tokens": {"total": 200}}
            }
        });
        let result = extract_json_path(&v, "models.*.tokens.total");
        assert!(result.is_some());
        let n = result.unwrap().as_f64().unwrap();
        assert!((n - 300.0).abs() < 0.01);
    }

    #[test]
    fn test_extract_missing_returns_none() {
        let v = json!({"a": 1});
        assert!(extract_json_path(&v, "b").is_none());
    }
}

/// Helper to mark a run as cancelled via a tracker stored in a `Mutex<Option<Arc<dyn RunTracker>>>`.
pub(crate) fn mark_cancelled_via_tracker(
    tracker_mtx: &std::sync::Mutex<Option<std::sync::Arc<dyn RunTracker>>>,
    run_id: &str,
    context: &str,
) {
    if let Some(ref tracker) = tracker_mtx.lock().unwrap_or_else(|e| e.into_inner()).take() {
        if let Err(e) = tracker.mark_cancelled(run_id) {
            tracing::warn!("{context}: failed to mark run {run_id} cancelled: {e}");
        }
    }
}

/// Best-effort: mark a run as cancelled and log a uniform warning if the DB
/// write fails. Used by every runtime's poll loop in shutdown/timeout
/// branches; the runtime-specific subprocess kill is left to the caller
/// because it differs by runtime (PID-based vs `Child::kill`).
pub(crate) fn mark_cancelled_with_reason(
    tracker: &dyn RunTracker,
    run_id: &str,
    context: &str,
    reason: &str,
) {
    if let Err(e) = tracker.mark_cancelled(run_id) {
        tracing::warn!("{context}: failed to mark run {run_id} cancelled on {reason}: {e}");
    }
}

/// Best-effort: record the subprocess pid and runtime name on the tracker,
/// logging warnings on failure rather than aborting spawn.
pub(crate) fn record_pid_and_runtime(
    tracker: &dyn RunTracker,
    run_id: &str,
    pid: u32,
    runtime: &str,
    context: &str,
) {
    if let Err(e) = tracker.record_pid(run_id, pid) {
        tracing::warn!("{context}: failed to persist subprocess pid {pid} for run {run_id}: {e}");
    }
    if let Err(e) = tracker.record_runtime(run_id, runtime) {
        tracing::warn!("{context}: failed to persist runtime '{runtime}' for run {run_id}: {e}");
    }
}

#[cfg(test)]
pub mod test_util {
    use crate::error::Result;
    use crate::run::{RunHandle, RunStatus};
    use crate::tracker::RunTracker;

    pub struct NoopTracker;

    impl RunTracker for NoopTracker {
        fn record_pid(&self, _run_id: &str, _pid: u32) -> Result<()> {
            Ok(())
        }
        fn record_runtime(&self, _run_id: &str, _name: &str) -> Result<()> {
            Ok(())
        }
        fn mark_cancelled(&self, _run_id: &str) -> Result<()> {
            Ok(())
        }
        fn mark_failed_if_running(&self, _run_id: &str, _reason: &str) -> Result<()> {
            Ok(())
        }
        fn get_run(&self, _run_id: &str) -> Result<Option<RunHandle>> {
            Ok(None)
        }
    }

    pub fn make_test_run(runtime: &str, subprocess_pid: Option<i64>) -> RunHandle {
        RunHandle {
            id: "test-run".to_string(),
            status: RunStatus::Running,
            subprocess_pid,
            runtime: runtime.to_string(),
            session_id: None,
            result_text: None,
            started_at: "2024-01-01T00:00:00Z".to_string(),
            ended_at: None,
            log_file: None,
            model: None,
            cost_usd: None,
            num_turns: None,
            duration_ms: None,
            input_tokens: None,
            output_tokens: None,
            cache_read_input_tokens: None,
            cache_creation_input_tokens: None,
        }
    }
}
