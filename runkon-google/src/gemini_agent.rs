use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use runkon_flow::constants::metadata_keys;
use runkon_flow::output_schema::OutputSchema;
use runkon_flow::traits::action_executor::ActionOutput;
use runkon_runtimes::{
    PollError, RunEventSink, RunStatus, RunTracker, RuntimeRequest, RuntimeResolver,
};

use runkon_flow_executors::agent_loader::{self, BuildPromptParams};
use runkon_flow_executors::output::interpret_agent_output;

use crate::gemini_api::GeminiApiCallExecutor;

/// Per-invocation context passed to [`GeminiAgentExecutor::execute`].
pub struct GeminiAgentContext {
    pub run_id: String,
    pub working_dir: std::path::PathBuf,
    pub repo_path: String,
    pub step_timeout: std::time::Duration,
    pub shutdown: Option<Arc<std::sync::atomic::AtomicBool>>,
    pub model: Option<String>,
    pub workflow_name: String,
    pub tracker: Arc<dyn RunTracker>,
    pub event_sink: Arc<dyn RunEventSink>,
    /// Named runtime configs (from `[runtimes.*]` in host config).
    /// Used to validate `supported_models` before spawning.
    pub runtimes: std::collections::HashMap<String, runkon_runtimes::config::RuntimeConfig>,
    /// Default runtime name applied when agent frontmatter omits `runtime:`.
    pub default_runtime: Option<String>,
    /// Per-invocation runtime override.
    pub runtime_override: Option<String>,
}

/// Agent step parameters passed to [`GeminiAgentExecutor::execute`].
pub struct GeminiAgentParams<'a> {
    pub name: &'a str,
    pub inputs: &'a HashMap<String, String>,
    pub snippet_refs: &'a [String],
    pub dry_run: bool,
    pub retry_error: Option<&'a str>,
    pub schema: Option<&'a OutputSchema>,
}

/// Executes a workflow step by loading a `.md` agent definition and either
/// calling the Gemini API directly (when a schema and API key are present)
/// or spawning a subprocess via the injected [`RuntimeResolver`].
///
/// Zero `conductor_*` imports — host integration is provided via trait objects.
pub struct GeminiAgentExecutor {
    runtime_resolver: Arc<dyn RuntimeResolver>,
    api_key: Option<String>,
}

impl GeminiAgentExecutor {
    pub fn new(runtime_resolver: Arc<dyn RuntimeResolver>, api_key: Option<String>) -> Self {
        Self {
            runtime_resolver,
            api_key,
        }
    }

    pub fn execute(
        &self,
        ctx: &GeminiAgentContext,
        params: &GeminiAgentParams<'_>,
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
            &[],
            &ctx.workflow_name,
            params.name,
            &build_params,
        )?;

        let effective_runtime: &str = ctx
            .runtime_override
            .as_deref()
            .unwrap_or(&agent_def.runtime);

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
                .or(agent_def.model.as_deref())
                .or_else(|| {
                    ctx.runtimes
                        .get(effective_runtime)
                        .and_then(|rc| rc.default_model.as_deref())
                })
                .ok_or_else(|| {
                    format!(
                        "no model resolved for agent '{}': step did not specify a model, \
                         agent frontmatter declares no model, and runtime '{}' has no \
                         default_model configured",
                        params.name, effective_runtime
                    )
                })?;
            let executor = GeminiApiCallExecutor::new(api_key.to_string());
            let out = executor
                .execute(&prompt, schema, model, ctx.step_timeout)
                .map_err(|e| format!("Gemini API call for '{}' failed: {e}", params.name))?;
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

        let extra_cli_args: Vec<(Cow<'static, str>, Cow<'static, str>)> = vec![];

        let request = RuntimeRequest {
            run_id: ctx.run_id.clone(),
            effective_runtime: effective_runtime.to_string(),
            agent_def,
            prompt,
            working_dir: ctx.working_dir.clone(),
            model: ctx.model.clone(),
            extra_cli_args,
            plugin_dirs: vec![],
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
/// All runtimes enforce `supported_models` as a strict allowlist when the field is non-empty.
fn check_supported_models(
    runtime: &str,
    agent_name: &str,
    agent_default_model: Option<&str>,
    request_model: Option<&str>,
    runtimes: &HashMap<String, runkon_runtimes::config::RuntimeConfig>,
) -> Result<(), String> {
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

    fn write_agent_with_model(dir: &TempDir, model: &str) {
        let path = dir.path().join(".conductor").join("agents");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(
            path.join("test-agent.md"),
            format!("---\nmodel: {model}\n---\nDo the work."),
        )
        .unwrap();
    }

    fn make_ctx(dir: &TempDir) -> GeminiAgentContext {
        let dir_str = dir.path().to_str().unwrap().to_string();
        GeminiAgentContext {
            run_id: "test-run".to_string(),
            working_dir: dir.path().to_path_buf(),
            repo_path: dir_str,
            step_timeout: Duration::from_millis(200),
            shutdown: None,
            model: None,
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
        let mut ctx = make_ctx(&tmp);
        ctx.model = Some("gemini-1.5-pro".to_string());

        let schema = make_schema();
        let params = GeminiAgentParams {
            name: "test-agent",
            inputs: &HashMap::new(),
            snippet_refs: &[],
            dry_run: false,
            retry_error: None,
            schema: Some(&schema),
        };

        let executor = GeminiAgentExecutor::new(resolver, Some("dummy-api-key".to_string()));

        // Execute — will fail (no real Gemini endpoint) but the resolver must NOT be called.
        let result = executor.execute(&ctx, &params);

        // API path was taken: TrackingResolver was never invoked regardless of HTTP outcome.
        assert!(
            !resolver_ref.was_called(),
            "runtime resolver must not be called when schema + api_key are both present"
        );
        let _ = result; // Err expected (no real endpoint), but we only care that resolver wasn't called.
    }

    #[test]
    fn api_path_uses_runtime_default_model_when_step_model_absent() {
        let tmp = TempDir::new().unwrap();
        write_agent(&tmp);

        let resolver = Arc::new(TrackingResolver::new());
        let mut ctx = make_ctx(&tmp);
        // No per-step model — resolution must fall through to runtime config.
        ctx.model = None;
        ctx.runtimes.insert(
            "claude".to_string(),
            runkon_runtimes::config::RuntimeConfig {
                default_model: Some("model-from-config".to_string()),
                ..Default::default()
            },
        );

        let schema = make_schema();
        let params = GeminiAgentParams {
            name: "test-agent",
            inputs: &HashMap::new(),
            snippet_refs: &[],
            dry_run: false,
            retry_error: None,
            schema: Some(&schema),
        };

        let executor = GeminiAgentExecutor::new(resolver, Some("dummy-api-key".to_string()));
        let result = executor.execute(&ctx, &params);

        // Model resolved successfully from runtime config; any Err must not be "no model resolved".
        if let Err(ref e) = result {
            assert!(
                !e.contains("no model resolved"),
                "model resolution should succeed via runtime default_model, got: {e}"
            );
        }
    }

    #[test]
    fn api_path_uses_agent_frontmatter_model_when_step_model_absent() {
        let tmp = TempDir::new().unwrap();
        write_agent_with_model(&tmp, "model-from-frontmatter");

        let resolver = Arc::new(TrackingResolver::new());
        let mut ctx = make_ctx(&tmp);
        ctx.model = None;
        // No runtime default — model must come from agent frontmatter.

        let schema = make_schema();
        let params = GeminiAgentParams {
            name: "test-agent",
            inputs: &HashMap::new(),
            snippet_refs: &[],
            dry_run: false,
            retry_error: None,
            schema: Some(&schema),
        };

        let executor = GeminiAgentExecutor::new(resolver, Some("dummy-api-key".to_string()));
        let result = executor.execute(&ctx, &params);

        // Model resolved from frontmatter; any Err must not be "no model resolved".
        if let Err(ref e) = result {
            assert!(
                !e.contains("no model resolved"),
                "model resolution should succeed via agent frontmatter model, got: {e}"
            );
        }
    }

    #[test]
    fn api_path_errors_when_no_model_and_no_runtime_default() {
        let tmp = TempDir::new().unwrap();
        write_agent(&tmp);

        let resolver = Arc::new(TrackingResolver::new());
        let ctx = make_ctx(&tmp); // model: None, runtimes: empty

        let schema = make_schema();
        let params = GeminiAgentParams {
            name: "test-agent",
            inputs: &HashMap::new(),
            snippet_refs: &[],
            dry_run: false,
            retry_error: None,
            schema: Some(&schema),
        };

        let executor = GeminiAgentExecutor::new(resolver, Some("dummy-api-key".to_string()));
        let result = executor.execute(&ctx, &params);

        let err = result.expect_err("expected Err when no model and no runtime default");
        assert!(
            err.contains("no model resolved"),
            "error should mention 'no model resolved', got: {err}"
        );
        assert!(
            err.contains("test-agent"),
            "error should name the agent, got: {err}"
        );
    }

    #[test]
    fn skips_api_executor_when_schema_absent() {
        let tmp = TempDir::new().unwrap();
        write_agent(&tmp);

        let resolver = Arc::new(TrackingResolver::new());
        let resolver_ref = resolver.clone();
        let ctx = make_ctx(&tmp);

        // No schema → subprocess path is taken even with api_key present.
        let params = GeminiAgentParams {
            name: "test-agent",
            inputs: &HashMap::new(),
            snippet_refs: &[],
            dry_run: false,
            retry_error: None,
            schema: None,
        };

        let executor = GeminiAgentExecutor::new(resolver, Some("dummy-api-key".to_string()));
        let result = executor.execute(&ctx, &params);

        assert!(
            resolver_ref.was_called(),
            "runtime resolver must be called when schema is absent"
        );
        assert!(result.is_err(), "expected Err from mock resolver, got Ok");
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
        let params = GeminiAgentParams {
            name: "test-agent",
            inputs: &HashMap::new(),
            snippet_refs: &[],
            dry_run: false,
            retry_error: None,
            schema: Some(&schema),
        };

        let executor = GeminiAgentExecutor::new(resolver, None);
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
    fn check_supported_models_rejects_unlisted_model() {
        let mut runtimes = HashMap::new();
        runtimes.insert(
            "gemini-local".to_string(),
            rt_config_with_models(&["gemini-1.5-pro"]),
        );
        let err = check_supported_models(
            "gemini-local",
            "my-agent",
            None,
            Some("disallowed-model"),
            &runtimes,
        )
        .unwrap_err();
        assert!(err.contains("disallowed-model"), "{err}");
        assert!(err.contains("gemini-1.5-pro"), "{err}");
    }

    #[test]
    fn check_supported_models_listed_model_ok() {
        let mut runtimes = HashMap::new();
        runtimes.insert(
            "gemini-local".to_string(),
            rt_config_with_models(&["gemini-1.5-pro"]),
        );
        assert!(check_supported_models(
            "gemini-local",
            "my-agent",
            None,
            Some("gemini-1.5-pro"),
            &runtimes
        )
        .is_ok());
    }

    #[test]
    fn check_supported_models_passes_when_list_empty() {
        let mut runtimes = HashMap::new();
        runtimes.insert(
            "gemini-local".to_string(),
            runkon_runtimes::config::RuntimeConfig::default(),
        );
        assert!(check_supported_models(
            "gemini-local",
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
            "gemini-local".to_string(),
            rt_config_with_models(&["gemini-1.5-pro"]),
        );
        assert!(check_supported_models("gemini-local", "my-agent", None, None, &runtimes).is_ok());
    }

    #[test]
    fn check_supported_models_unknown_runtime_skips_check() {
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

    // ── runtime_override on GeminiAgentContext ────────────────────────────────

    use std::sync::Mutex;

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
        let tmp = TempDir::new().unwrap();
        write_agent(&tmp);

        let resolver = Arc::new(RecordingResolver::new());
        let mut ctx = make_ctx(&tmp);
        ctx.runtime_override = Some("gemini-local".to_string());
        ctx.runtimes.insert(
            "gemini-local".to_string(),
            runkon_runtimes::config::RuntimeConfig::default(),
        );

        let params = GeminiAgentParams {
            name: "test-agent",
            inputs: &HashMap::new(),
            snippet_refs: &[],
            dry_run: false,
            retry_error: None,
            schema: None,
        };

        let executor = GeminiAgentExecutor::new(resolver.clone(), None);
        let _ = executor.execute(&ctx, &params);

        assert_eq!(
            resolver.captured(),
            Some("gemini-local".to_string()),
            "executor should resolve against runtime_override, not agent_def.runtime"
        );
    }

    #[test]
    fn execute_falls_back_to_agent_def_runtime_when_override_none() {
        let tmp = TempDir::new().unwrap();
        write_agent(&tmp);

        let resolver = Arc::new(RecordingResolver::new());
        let ctx = make_ctx(&tmp); // runtime_override: None

        let params = GeminiAgentParams {
            name: "test-agent",
            inputs: &HashMap::new(),
            snippet_refs: &[],
            dry_run: false,
            retry_error: None,
            schema: None,
        };

        let executor = GeminiAgentExecutor::new(resolver.clone(), None);
        let _ = executor.execute(&ctx, &params);

        // The bare agent file produces `agent_def.runtime = "claude"`.
        assert_eq!(resolver.captured(), Some("claude".to_string()));
    }

    #[cfg(unix)]
    #[test]
    fn returns_error_for_invalid_utf8_working_dir() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let invalid_path = std::path::PathBuf::from(OsString::from_vec(vec![b'/', 0xFF, 0xFE]));

        let resolver = Arc::new(TrackingResolver::new());
        let ctx = GeminiAgentContext {
            run_id: "test-run".to_string(),
            working_dir: invalid_path,
            repo_path: "/tmp".to_string(),
            step_timeout: Duration::from_millis(200),
            shutdown: None,
            model: None,
            workflow_name: "test-wf".to_string(),
            tracker: Arc::new(runkon_runtimes::tracker::NoopTracker),
            event_sink: Arc::new(runkon_runtimes::NoopEventSink),
            runtimes: std::collections::HashMap::new(),
            default_runtime: None,
            runtime_override: None,
        };
        let params = GeminiAgentParams {
            name: "test-agent",
            inputs: &HashMap::new(),
            snippet_refs: &[],
            dry_run: false,
            retry_error: None,
            schema: None,
        };

        let executor = GeminiAgentExecutor::new(resolver, None);
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
}
