use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use runkon_flow::constants::metadata_keys;
use runkon_flow::output_schema::OutputSchema;
use runkon_flow::traits::action_executor::ActionOutput;
use runkon_runtimes::{
    PollError, RunEventSink, RunStatus, RunTracker, RuntimeRequest, RuntimeResolver,
};

use crate::agent_loader::{self, BuildPromptParams};
use crate::anthropic_api::ApiCallExecutor;
use crate::output::interpret_agent_output;

/// Per-invocation context passed to [`ClaudeAgentExecutor::execute`].
pub struct ClaudeAgentContext {
    pub run_id: String,
    pub working_dir: std::path::PathBuf,
    pub repo_path: String,
    pub step_timeout: std::time::Duration,
    pub shutdown: Option<Arc<std::sync::atomic::AtomicBool>>,
    pub model: Option<String>,
    pub bot_name: Option<String>,
    pub plugin_dirs: Vec<String>,
    pub workflow_name: String,
    pub tracker: Arc<dyn RunTracker>,
    pub event_sink: Arc<dyn RunEventSink>,
    /// Named runtime configs (from `[runtimes.*]` in host config).
    /// Used to validate `supported_models` before spawning.
    pub runtimes: std::collections::HashMap<String, runkon_runtimes::config::RuntimeConfig>,
    /// Default runtime name applied when agent frontmatter omits `runtime:`.
    pub default_runtime: Option<String>,
    /// Per-invocation runtime override. When `Some`, the executor resolves and
    /// validates against this runtime instead of `agent_def.runtime`. Used by
    /// the host (e.g. when a workflow's model picker explicitly selects a
    /// non-default runtime, or when the host derives a runtime from the
    /// requested model name).
    pub runtime_override: Option<String>,
}

/// Agent step parameters passed to [`ClaudeAgentExecutor::execute`].
pub struct ClaudeAgentParams<'a> {
    pub name: &'a str,
    pub inputs: &'a HashMap<String, String>,
    pub snippet_refs: &'a [String],
    pub dry_run: bool,
    pub retry_error: Option<&'a str>,
    pub schema: Option<&'a OutputSchema>,
}

/// Executes a workflow step by loading a `.md` agent definition and either
/// calling the Anthropic API directly (when a schema and API key are present)
/// or spawning a subprocess via the injected [`RuntimeResolver`].
///
/// Zero `conductor_*` imports — host integration is provided via trait objects.
pub struct ClaudeAgentExecutor {
    runtime_resolver: Arc<dyn RuntimeResolver>,
    api_key: Option<String>,
}

impl ClaudeAgentExecutor {
    pub fn new(runtime_resolver: Arc<dyn RuntimeResolver>, api_key: Option<String>) -> Self {
        Self {
            runtime_resolver,
            api_key,
        }
    }

    pub fn execute(
        &self,
        ctx: &ClaudeAgentContext,
        params: &ClaudeAgentParams<'_>,
    ) -> Result<ActionOutput, String> {
        let working_dir = ctx.working_dir.to_str().ok_or_else(|| {
            format!(
                "working_dir '{}' contains invalid UTF-8",
                ctx.working_dir.display()
            )
        })?;
        let build_params = BuildPromptParams {
            inputs: params.inputs,
            snippet_refs: params.snippet_refs,
            retry_error: params.retry_error,
            dry_run: params.dry_run,
            schema: params.schema,
            default_runtime: ctx.default_runtime.as_deref(),
        };

        let (agent_def, prompt) = agent_loader::load_agent_and_build_prompt(
            working_dir,
            &ctx.repo_path,
            &ctx.plugin_dirs,
            &ctx.workflow_name,
            params.name,
            &build_params,
        )?;

        // The host can override the runtime resolved from `agent_def.runtime`
        // (e.g. when a workflow's model picker explicitly chose a non-default
        // runtime, or when the host derived a runtime from the requested model).
        let effective_runtime: &str = ctx
            .runtime_override
            .as_deref()
            .unwrap_or(&agent_def.runtime);

        // Validate supported_models before spawning — catches misconfiguration early.
        check_supported_models(
            effective_runtime,
            &agent_def.name,
            agent_def.model.as_deref(),
            ctx.model.as_deref(),
            &ctx.runtimes,
        )?;

        // API fast path: schema + key both present.
        if let (Some(schema), Some(api_key)) = (params.schema, self.api_key.as_deref()) {
            let model = ctx
                .model
                .as_deref()
                .unwrap_or(crate::anthropic_api::DEFAULT_API_MODEL);
            let executor = ApiCallExecutor::new(api_key.to_string());
            let out = executor
                .execute(&prompt, schema, model, ctx.step_timeout)
                .map_err(|e| format!("API call for '{}' failed: {e}", params.name))?;
            return Ok(ActionOutput {
                markers: out.markers,
                context: Some(out.context),
                result_text: Some(out.result_text),
                structured_output: Some(out.structured_output),
                metadata: out.metadata,
                child_run_id: None,
            });
        }

        // Subprocess path: resolve the runtime and spawn the agent.
        let runtime = self
            .runtime_resolver
            .resolve(effective_runtime)
            .map_err(|e| format!("failed to resolve runtime '{effective_runtime}': {e}"))?;

        let extra_cli_args: Vec<(Cow<'static, str>, Cow<'static, str>)> = match &ctx.bot_name {
            Some(name) => vec![(Cow::Borrowed("--bot-name"), Cow::Owned(name.clone()))],
            None => vec![],
        };

        let request = RuntimeRequest {
            run_id: ctx.run_id.clone(),
            agent_def,
            prompt,
            working_dir: ctx.working_dir.clone(),
            model: ctx.model.clone(),
            extra_cli_args,
            plugin_dirs: ctx.plugin_dirs.clone(),
            resume_session_id: None,
            tracker: ctx.tracker.clone(),
            event_sink: ctx.event_sink.clone(),
        };

        runtime
            .spawn_validated(&request)
            .map_err(|e| format!("failed to spawn agent: {e}"))?;

        let completed = match runtime.poll(&ctx.run_id, ctx.shutdown.as_ref(), ctx.step_timeout) {
            Ok(run) => run,
            Err(PollError::Cancelled) => {
                return Err("executor shutdown requested".to_string());
            }
            Err(e) => {
                return Err(e.to_string());
            }
        };

        let succeeded = completed.status == RunStatus::Completed;

        let (markers, context, structured_output) =
            interpret_agent_output(completed.result_text.as_deref(), params.schema, succeeded)?;

        if succeeded {
            let mut metadata = HashMap::new();
            if let Some(v) = completed.cost_usd {
                metadata.insert(metadata_keys::COST_USD.to_string(), v.to_string());
            }
            if let Some(v) = completed.num_turns {
                metadata.insert(metadata_keys::NUM_TURNS.to_string(), v.to_string());
            }
            if let Some(v) = completed.duration_ms {
                metadata.insert(metadata_keys::DURATION_MS.to_string(), v.to_string());
            }
            if let Some(v) = completed.input_tokens {
                metadata.insert(metadata_keys::INPUT_TOKENS.to_string(), v.to_string());
            }
            if let Some(v) = completed.output_tokens {
                metadata.insert(metadata_keys::OUTPUT_TOKENS.to_string(), v.to_string());
            }
            if let Some(v) = completed.cache_read_input_tokens {
                metadata.insert(
                    metadata_keys::CACHE_READ_INPUT_TOKENS.to_string(),
                    v.to_string(),
                );
            }
            if let Some(v) = completed.cache_creation_input_tokens {
                metadata.insert(
                    metadata_keys::CACHE_CREATION_INPUT_TOKENS.to_string(),
                    v.to_string(),
                );
            }
            Ok(ActionOutput {
                markers,
                context: Some(context),
                result_text: completed.result_text,
                structured_output,
                metadata,
                child_run_id: None,
            })
        } else {
            let detail = completed.result_text.unwrap_or_else(|| {
                format!(
                    "agent '{}' completed with status {:?} but no result text",
                    params.name, completed.status
                )
            });
            Err(detail)
        }
    }
}

/// Validate that the resolved model is allowed by the runtime's `supported_models`.
///
/// The built-in `claude` runtime always returns `Ok` — its `supported_models` are
/// additive picker entries (alongside the host's known Claude models), not a strict
/// allowlist. Non-claude runtimes enforce `supported_models` as a strict allowlist
/// when the field is non-empty.
fn check_supported_models(
    runtime: &str,
    agent_name: &str,
    agent_default_model: Option<&str>,
    request_model: Option<&str>,
    runtimes: &HashMap<String, runkon_runtimes::config::RuntimeConfig>,
) -> Result<(), String> {
    if runtime == "claude" {
        return Ok(());
    }
    let Some(rt_config) = runtimes.get(runtime) else {
        return Ok(());
    };
    if rt_config.supported_models.is_empty() {
        return Ok(());
    }
    let effective_model = request_model.or(agent_default_model);
    if let Some(m) = effective_model {
        if !rt_config.supported_models.iter().any(|s| s == m) {
            return Err(format!(
                "runtime '{}' only accepts models {:?}; \
                 agent '{}' resolved model is '{m}'",
                runtime, rt_config.supported_models, agent_name
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use runkon_flow::output_schema::{FieldDef, FieldType};
    use runkon_runtimes::{Result as RkResult, RuntimeError};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tempfile::TempDir;

    struct TrackingResolver {
        called: AtomicBool,
    }

    impl TrackingResolver {
        fn new() -> Self {
            Self {
                called: AtomicBool::new(false),
            }
        }

        fn was_called(&self) -> bool {
            self.called.load(Ordering::SeqCst)
        }
    }

    impl RuntimeResolver for TrackingResolver {
        fn resolve(&self, _name: &str) -> RkResult<Box<dyn runkon_runtimes::AgentRuntime>> {
            self.called.store(true, Ordering::SeqCst);
            Err(RuntimeError::Config(
                "mock resolver — subprocess not available in tests".to_string(),
            ))
        }
    }

    fn make_schema() -> OutputSchema {
        OutputSchema {
            name: "test".to_string(),
            fields: vec![FieldDef {
                name: "ok".to_string(),
                required: true,
                field_type: FieldType::Boolean,
                desc: None,
                examples: None,
            }],
            markers: None,
        }
    }

    fn write_agent(dir: &TempDir) {
        let path = dir.path().join(".conductor").join("agents");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("test-agent.md"), "Do the work.").unwrap();
    }

    fn make_ctx(dir: &TempDir) -> ClaudeAgentContext {
        let dir_str = dir.path().to_str().unwrap().to_string();
        ClaudeAgentContext {
            run_id: "test-run".to_string(),
            working_dir: dir.path().to_path_buf(),
            repo_path: dir_str,
            step_timeout: Duration::from_millis(200),
            shutdown: None,
            model: None,
            bot_name: None,
            plugin_dirs: vec![],
            workflow_name: "test-wf".to_string(),
            tracker: Arc::new(runkon_runtimes::tracker::NoopTracker),
            event_sink: Arc::new(runkon_runtimes::NoopEventSink),
            runtimes: std::collections::HashMap::new(),
            default_runtime: None,
            runtime_override: None,
        }
    }

    #[test]
    fn delegates_to_api_executor_when_schema_and_key_present() {
        let tmp = TempDir::new().unwrap();
        write_agent(&tmp);

        let resolver = Arc::new(TrackingResolver::new());
        let resolver_ref = resolver.clone();
        let ctx = make_ctx(&tmp);

        let schema = make_schema();
        let params = ClaudeAgentParams {
            name: "test-agent",
            inputs: &HashMap::new(),
            snippet_refs: &[],
            dry_run: false,
            retry_error: None,
            schema: Some(&schema),
        };

        let executor = ClaudeAgentExecutor::new(resolver, Some("dummy-api-key".to_string()));

        // Execute — will fail (no real Anthropic endpoint) but the resolver must NOT be called.
        let result = executor.execute(&ctx, &params);

        // API path was taken: TrackingResolver was never invoked regardless of HTTP outcome.
        assert!(
            !resolver_ref.was_called(),
            "runtime resolver must not be called when schema + api_key are both present"
        );
        let _ = result; // Err expected (no real endpoint), but we only care that resolver wasn't called.
    }

    #[test]
    fn skips_api_executor_when_schema_absent() {
        let tmp = TempDir::new().unwrap();
        write_agent(&tmp);

        let resolver = Arc::new(TrackingResolver::new());
        let resolver_ref = resolver.clone();
        let ctx = make_ctx(&tmp);

        // No schema → subprocess path is taken even with api_key present.
        let params = ClaudeAgentParams {
            name: "test-agent",
            inputs: &HashMap::new(),
            snippet_refs: &[],
            dry_run: false,
            retry_error: None,
            schema: None,
        };

        let executor = ClaudeAgentExecutor::new(resolver, Some("dummy-api-key".to_string()));
        let result = executor.execute(&ctx, &params);

        assert!(
            resolver_ref.was_called(),
            "runtime resolver must be called when schema is absent"
        );
        assert!(result.is_err(), "expected Err from mock resolver, got Ok");
    }

    #[cfg(unix)]
    #[test]
    fn returns_error_for_invalid_utf8_working_dir() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        // 0xFF is never valid UTF-8, so PathBuf::to_str() returns None.
        let invalid_path = std::path::PathBuf::from(OsString::from_vec(vec![b'/', 0xFF, 0xFE]));

        let resolver = Arc::new(TrackingResolver::new());
        let ctx = ClaudeAgentContext {
            run_id: "test-run".to_string(),
            working_dir: invalid_path,
            repo_path: "/tmp".to_string(),
            step_timeout: Duration::from_millis(200),
            shutdown: None,
            model: None,
            bot_name: None,
            plugin_dirs: vec![],
            workflow_name: "test-wf".to_string(),
            tracker: Arc::new(runkon_runtimes::tracker::NoopTracker),
            event_sink: Arc::new(runkon_runtimes::NoopEventSink),
            runtimes: std::collections::HashMap::new(),
            default_runtime: None,
            runtime_override: None,
        };
        let params = ClaudeAgentParams {
            name: "test-agent",
            inputs: &HashMap::new(),
            snippet_refs: &[],
            dry_run: false,
            retry_error: None,
            schema: None,
        };

        let executor = ClaudeAgentExecutor::new(resolver, None);
        let result = executor.execute(&ctx, &params);

        assert!(
            result.is_err(),
            "expected Err for invalid UTF-8 working_dir"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("contains invalid UTF-8"),
            "error should mention invalid UTF-8, got: {err}"
        );
    }

    #[test]
    fn skips_api_executor_when_api_key_absent() {
        let tmp = TempDir::new().unwrap();
        write_agent(&tmp);

        let resolver = Arc::new(TrackingResolver::new());
        let resolver_ref = resolver.clone();
        let ctx = make_ctx(&tmp);

        let schema = make_schema();
        // No api_key → subprocess path is taken even with schema present.
        let params = ClaudeAgentParams {
            name: "test-agent",
            inputs: &HashMap::new(),
            snippet_refs: &[],
            dry_run: false,
            retry_error: None,
            schema: Some(&schema),
        };

        let executor = ClaudeAgentExecutor::new(resolver, None);
        let result = executor.execute(&ctx, &params);

        assert!(
            resolver_ref.was_called(),
            "runtime resolver must be called when api_key is absent"
        );
        assert!(result.is_err(), "expected Err from mock resolver, got Ok");
    }

    // ---------- check_supported_models ----------

    fn rt_config_with_models(models: &[&str]) -> runkon_runtimes::config::RuntimeConfig {
        runkon_runtimes::config::RuntimeConfig {
            supported_models: models.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn check_supported_models_claude_runtime_ignores_supported_models() {
        // Regression: when `runtimes.claude.supported_models` is populated, the
        // built-in claude runtime must NOT enforce it as a strict allowlist —
        // KNOWN_MODELS aliases/IDs (sonnet, haiku, claude-sonnet-4-6, etc.) must
        // still be accepted. See workflow run 01KR1T8FXPEJ4KPWHEWWN6BY5M.
        let mut runtimes = HashMap::new();
        runtimes.insert(
            "claude".to_string(),
            rt_config_with_models(&["claude-opus-4-7", "QuantTrio/Qwen3.5-122B-A10B-AWQ"]),
        );
        assert!(check_supported_models(
            "claude",
            "review-architecture",
            None,
            Some("claude-sonnet-4-6"),
            &runtimes
        )
        .is_ok());
    }

    #[test]
    fn check_supported_models_non_claude_strict_allowlist_rejects_unlisted() {
        let mut runtimes = HashMap::new();
        runtimes.insert(
            "qwen-local".to_string(),
            rt_config_with_models(&["qwen-72b"]),
        );
        let err = check_supported_models(
            "qwen-local",
            "my-agent",
            None,
            Some("disallowed-model"),
            &runtimes,
        )
        .unwrap_err();
        assert!(err.contains("disallowed-model"), "{err}");
        assert!(err.contains("qwen-72b"), "{err}");
    }

    #[test]
    fn check_supported_models_non_claude_listed_model_ok() {
        let mut runtimes = HashMap::new();
        runtimes.insert(
            "qwen-local".to_string(),
            rt_config_with_models(&["qwen-72b"]),
        );
        assert!(check_supported_models(
            "qwen-local",
            "my-agent",
            None,
            Some("qwen-72b"),
            &runtimes
        )
        .is_ok());
    }

    #[test]
    fn check_supported_models_empty_supported_models_accepts_any() {
        let mut runtimes = HashMap::new();
        runtimes.insert(
            "qwen-local".to_string(),
            runkon_runtimes::config::RuntimeConfig::default(),
        );
        assert!(check_supported_models(
            "qwen-local",
            "my-agent",
            None,
            Some("any-model"),
            &runtimes
        )
        .is_ok());
    }

    #[test]
    fn check_supported_models_no_resolved_model_skips_check() {
        let mut runtimes = HashMap::new();
        runtimes.insert(
            "qwen-local".to_string(),
            rt_config_with_models(&["qwen-72b"]),
        );
        // No model on agent_def, no request model — defer to host default.
        assert!(check_supported_models("qwen-local", "my-agent", None, None, &runtimes).is_ok());
    }

    #[test]
    fn check_supported_models_unknown_runtime_skips_check() {
        // No matching runtime entry — skip; the spawn step will fail with a clearer error.
        let runtimes = HashMap::new();
        assert!(check_supported_models(
            "missing-rt",
            "my-agent",
            None,
            Some("any-model"),
            &runtimes
        )
        .is_ok());
    }

    // ── runtime_override on ClaudeAgentContext ────────────────────────────────

    use std::sync::Mutex;

    /// Records the name passed to `resolve()` so tests can assert which
    /// runtime the executor selected.
    struct RecordingResolver {
        captured_name: Mutex<Option<String>>,
    }

    impl RecordingResolver {
        fn new() -> Self {
            Self {
                captured_name: Mutex::new(None),
            }
        }

        fn captured(&self) -> Option<String> {
            self.captured_name.lock().unwrap().clone()
        }
    }

    impl RuntimeResolver for RecordingResolver {
        fn resolve(&self, name: &str) -> RkResult<Box<dyn runkon_runtimes::AgentRuntime>> {
            *self.captured_name.lock().unwrap() = Some(name.to_string());
            Err(RuntimeError::Config(
                "recording resolver — does not actually spawn".to_string(),
            ))
        }
    }

    #[test]
    fn execute_uses_runtime_override_when_set() {
        // Agent file with no `runtime:` frontmatter falls back to "claude".
        // When `runtime_override` is set, the executor should resolve against
        // that name, not "claude".
        let tmp = TempDir::new().unwrap();
        write_agent(&tmp);

        let resolver = Arc::new(RecordingResolver::new());
        let mut ctx = make_ctx(&tmp);
        ctx.runtime_override = Some("qwen-local".to_string());
        // Register the runtime so check_supported_models doesn't reject it.
        ctx.runtimes.insert(
            "qwen-local".to_string(),
            runkon_runtimes::config::RuntimeConfig::default(),
        );

        let params = ClaudeAgentParams {
            name: "test-agent",
            inputs: &HashMap::new(),
            snippet_refs: &[],
            dry_run: false,
            retry_error: None,
            schema: None,
        };

        let executor = ClaudeAgentExecutor::new(resolver.clone(), None);
        let _ = executor.execute(&ctx, &params); // resolver returns Err — fine

        assert_eq!(
            resolver.captured(),
            Some("qwen-local".to_string()),
            "executor should resolve against runtime_override, not agent_def.runtime"
        );
    }

    #[test]
    fn execute_falls_back_to_agent_def_runtime_when_override_none() {
        let tmp = TempDir::new().unwrap();
        write_agent(&tmp);

        let resolver = Arc::new(RecordingResolver::new());
        let ctx = make_ctx(&tmp); // runtime_override: None

        let params = ClaudeAgentParams {
            name: "test-agent",
            inputs: &HashMap::new(),
            snippet_refs: &[],
            dry_run: false,
            retry_error: None,
            schema: None,
        };

        let executor = ClaudeAgentExecutor::new(resolver.clone(), None);
        let _ = executor.execute(&ctx, &params);

        // The bare agent file produces `agent_def.runtime = "claude"`.
        assert_eq!(resolver.captured(), Some("claude".to_string()));
    }
}
