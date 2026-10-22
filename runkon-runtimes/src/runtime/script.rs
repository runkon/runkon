use std::io::Read;
use std::process::{Child, Stdio};
use std::sync::{atomic::AtomicBool, Arc, Mutex};
use std::time::Duration;

use crate::config::{RuntimeConfig, DEFAULT_SCRIPT_PROMPT_ENV};
use crate::error::{Result, RuntimeError};
use crate::run::{RunHandle, RunStatus};
use crate::tracker::{RunEventSink, RunTracker, RuntimeEvent};

use super::{AgentRuntime, PollError, RuntimeRequest};

struct ScriptState {
    child: Child,
    start: std::time::Instant,
}

/// Whether `name` is a variable name `sh` can read back as `$name`: an ASCII
/// letter or `_`, then ASCII letters, digits or `_`. (`$2PROMPT` would expand
/// `$2` followed by `PROMPT`.)
fn is_shell_var_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// ScriptRuntime runs any shell command via `sh -c <command>`,
/// passing the prompt through the env var named by `prompt_env`
/// (`FLOW_PROMPT` by default) and capturing stdout as `result_text`. No tmux
/// dependency.
pub struct ScriptRuntime {
    config: RuntimeConfig,
    state: Mutex<Option<ScriptState>>,
    tracker: Mutex<Option<Arc<dyn RunTracker>>>,
    event_sink: Mutex<Option<Arc<dyn RunEventSink>>>,
}

impl ScriptRuntime {
    pub fn new(config: RuntimeConfig) -> Self {
        Self {
            config,
            state: Mutex::new(None),
            tracker: Mutex::new(None),
            event_sink: Mutex::new(None),
        }
    }
}

impl AgentRuntime for ScriptRuntime {
    fn spawn_impl(&self, request: &RuntimeRequest, _seal: super::private::Seal) -> Result<()> {
        let command = self.config.command.as_deref().ok_or_else(|| {
            RuntimeError::Config(
                "ScriptRuntime: `command` is required in the runtime config".to_string(),
            )
        })?;

        let prompt_env = self
            .config
            .prompt_env
            .as_deref()
            .unwrap_or(DEFAULT_SCRIPT_PROMPT_ENV);
        if !is_shell_var_name(prompt_env) {
            return Err(RuntimeError::Config(format!(
                "ScriptRuntime: invalid `prompt_env` {prompt_env:?}: it must be a shell \
                 variable name (an ASCII letter or '_', then letters, digits or '_')"
            )));
        }

        super::validate_config_env(&self.config.env, "ScriptRuntime")?;
        if self.config.env.contains_key(prompt_env) {
            return Err(RuntimeError::Config(format!(
                "ScriptRuntime: `env` sets {prompt_env:?}, which is the runtime's `prompt_env`; \
                 the prompt would overwrite it — remove the entry or choose another `prompt_env`"
            )));
        }

        if request.agent_def.can_commit {
            tracing::warn!(
                "ScriptRuntime: agent '{}' has can_commit=true but ScriptRuntime produces no \
                 output schema — commit validation is not enforced",
                request.agent_def.name
            );
        }

        let child = std::process::Command::new("sh")
            .args(["-c", command])
            .envs(&self.config.env)
            .env(prompt_env, &request.prompt)
            .current_dir(&request.working_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                RuntimeError::Agent(format!("ScriptRuntime: failed to spawn command: {e}"))
            })?;

        let pid = child.id();
        super::record_pid_and_runtime(
            request.tracker.as_ref(),
            &request.run_id,
            pid,
            &request.effective_runtime,
            "ScriptRuntime",
        );

        *self.state.lock().unwrap_or_else(|e| e.into_inner()) = Some(ScriptState {
            child,
            start: std::time::Instant::now(),
        });
        *self.tracker.lock().unwrap_or_else(|e| e.into_inner()) = Some(request.tracker.clone());
        *self.event_sink.lock().unwrap_or_else(|e| e.into_inner()) =
            Some(request.event_sink.clone());
        Ok(())
    }

    fn poll(
        &self,
        run_id: &str,
        shutdown: Option<&Arc<AtomicBool>>,
        step_timeout: Duration,
    ) -> std::result::Result<RunHandle, PollError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(|| PollError::Failed("ScriptRuntime::poll called before spawn".into()))?;

        let tracker = self
            .tracker
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| {
                PollError::Failed(
                    "ScriptRuntime::poll called before spawn (tracker missing)".into(),
                )
            })?;

        let event_sink = self
            .event_sink
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| {
                PollError::Failed(
                    "ScriptRuntime::poll called before spawn (event_sink missing)".into(),
                )
            })?;

        let poll_start = std::time::Instant::now();
        loop {
            if let Some(flag) = shutdown {
                if flag.load(std::sync::atomic::Ordering::Relaxed) {
                    let _ = state.child.kill();
                    super::mark_cancelled_with_reason(
                        tracker.as_ref(),
                        run_id,
                        "ScriptRuntime",
                        "shutdown",
                    );
                    return Err(PollError::Cancelled);
                }
            }

            if poll_start.elapsed() > step_timeout {
                let _ = state.child.kill();
                super::mark_cancelled_with_reason(
                    tracker.as_ref(),
                    run_id,
                    "ScriptRuntime",
                    "timeout",
                );
                return Err(PollError::NoResult);
            }

            match state.child.try_wait() {
                Ok(Some(exit_status)) => {
                    let exit_code = exit_status.code().unwrap_or(1);
                    let is_error = exit_code != 0;
                    let duration_ms = state.start.elapsed().as_millis() as i64;

                    let mut stdout = String::new();
                    let mut stderr = String::new();
                    if let Some(mut out) = state.child.stdout.take() {
                        if let Err(e) = out.read_to_string(&mut stdout) {
                            tracing::warn!(
                                "ScriptRuntime: stdout read failed for run {run_id}: {e}"
                            );
                        }
                    }
                    if let Some(mut err) = state.child.stderr.take() {
                        if let Err(e) = err.read_to_string(&mut stderr) {
                            tracing::warn!(
                                "ScriptRuntime: stderr read failed for run {run_id}: {e}"
                            );
                        }
                    }

                    if is_error {
                        let err_msg = {
                            let s = stderr.trim();
                            if s.is_empty() {
                                format!("process exited with code {exit_code}")
                            } else {
                                format!("process exited with code {exit_code}: {s}")
                            }
                        };
                        event_sink.on_event(
                            run_id,
                            RuntimeEvent::Failed {
                                error: err_msg,
                                session_id: None,
                            },
                        );
                    } else {
                        event_sink.on_event(
                            run_id,
                            RuntimeEvent::Completed {
                                result_text: Some(stdout.trim().to_string()),
                                session_id: None,
                                cost_usd: None,
                                num_turns: None,
                                duration_ms: Some(duration_ms),
                                input_tokens: None,
                                output_tokens: None,
                                cache_read_input_tokens: None,
                                cache_creation_input_tokens: None,
                            },
                        );
                    }

                    let run = tracker
                        .get_run(run_id)
                        .map_err(|e| PollError::Failed(format!("DB error: {e}")))?
                        .ok_or_else(|| {
                            PollError::Failed(format!("run {run_id} not found in DB"))
                        })?;

                    return match run.status {
                        RunStatus::Failed => Err(PollError::Failed(
                            run.result_text
                                .clone()
                                .unwrap_or_else(|| "script failed".to_string()),
                        )),
                        RunStatus::Completed => Ok(run),
                        _ => Err(PollError::NoResult),
                    };
                }
                Ok(None) => {
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(e) => {
                    let reason = e.to_string();
                    if let Err(db_err) = tracker.mark_failed_if_running(run_id, &reason) {
                        tracing::warn!("ScriptRuntime: failed to mark run {run_id} failed after wait error: {db_err}");
                    }
                    return Err(PollError::Failed(format!("wait error: {e}")));
                }
            }
        }
    }

    fn is_alive(&self, _run: &RunHandle) -> bool {
        false
    }

    fn cancel(&self, run: &RunHandle) -> Result<()> {
        if let Some(mut state) = self.state.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = state.child.kill();
        }
        super::mark_cancelled_via_tracker(&self.tracker, &run.id, "ScriptRuntime");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use super::*;
    use crate::agent_def::AgentDef;
    use crate::config::RuntimeConfig;
    use crate::runtime::test_util::make_test_run;
    use crate::tracker::{NoopEventSink, NoopTracker};

    fn make_runtime(command: Option<&str>) -> ScriptRuntime {
        ScriptRuntime::new(RuntimeConfig {
            command: command.map(|s| s.to_string()),
            ..RuntimeConfig::default()
        })
    }

    fn make_request(run_id: &str) -> RuntimeRequest {
        RuntimeRequest {
            run_id: run_id.to_string(),
            agent_def: AgentDef::default(),
            effective_runtime: "script".to_string(),
            prompt: String::new(),
            working_dir: std::env::temp_dir(),
            model: None,
            extra_cli_args: vec![],
            plugin_dirs: vec![],
            resume_session_id: None,
            tracker: Arc::new(NoopTracker),
            event_sink: Arc::new(NoopEventSink),
        }
    }

    #[test]
    fn is_alive_always_false() {
        let runtime = make_runtime(Some("echo hi"));
        assert!(!runtime.is_alive(&make_test_run("script", None)));
    }

    #[test]
    fn cancel_is_noop() {
        let runtime = make_runtime(Some("echo hi"));
        assert!(runtime.cancel(&make_test_run("script", None)).is_ok());
    }

    #[test]
    fn spawn_validated_succeeds_with_valid_command() {
        let runtime = make_runtime(Some("echo hello"));
        let request = make_request("test-spawn-ok");
        let result = runtime.spawn_validated(&request);
        assert!(result.is_ok(), "spawn_validated should succeed: {result:?}");
        // Clean up — cancel to terminate any lingering child
        let _ = runtime.cancel(&make_test_run("script", None));
    }

    #[test]
    fn spawn_validated_fails_without_command_config() {
        let runtime = make_runtime(None);
        let request = make_request("test-spawn-no-cmd");
        let result = runtime.spawn_validated(&request);
        assert!(result.is_err(), "spawn without command should fail");
    }

    #[test]
    #[ignore = "spawns a real subprocess; run explicitly with --ignored"]
    fn poll_shutdown_flag_causes_cancellation() {
        let runtime = make_runtime(Some("sleep 100"));
        let request = make_request("test-shutdown");
        runtime
            .spawn_validated(&request)
            .expect("spawn should succeed");

        let shutdown = Arc::new(AtomicBool::new(true));
        let result = runtime.poll("test-shutdown", Some(&shutdown), Duration::from_secs(30));
        assert!(
            matches!(result, Err(PollError::Cancelled)),
            "poll with shutdown flag should return Cancelled; got: {result:?}"
        );
    }

    /// Records the `result_text` of a `Completed` event.
    #[derive(Default)]
    struct CompletedSink(Mutex<Option<String>>);

    impl crate::tracker::EventSink for CompletedSink {
        fn on_event(&self, _run_id: &str, event: RuntimeEvent) {
            if let RuntimeEvent::Completed { result_text, .. } = event {
                *self.0.lock().unwrap() = result_text;
            }
        }
    }

    /// Spawn `command` with `prompt_env` and prompt "hello"; return its stdout.
    fn run_with_prompt_env(prompt_env: Option<&str>, command: &str, run_id: &str) -> String {
        run_with_config(
            RuntimeConfig {
                prompt_env: prompt_env.map(str::to_string),
                ..RuntimeConfig::default()
            },
            command,
            run_id,
        )
    }

    /// Spawn `command` under `config` with prompt "hello"; return its stdout.
    fn run_with_config(config: RuntimeConfig, command: &str, run_id: &str) -> String {
        let runtime = ScriptRuntime::new(RuntimeConfig {
            command: Some(command.to_string()),
            ..config
        });
        let sink = Arc::new(CompletedSink::default());
        let request = RuntimeRequest {
            prompt: "hello".to_string(),
            event_sink: sink.clone(),
            ..make_request(run_id)
        };
        runtime
            .spawn_validated(&request)
            .expect("spawn should succeed");
        // NoopTracker keeps no runs, so poll's final lookup fails; the output
        // has already been reported through the event sink by then.
        let _ = runtime.poll(run_id, None, Duration::from_secs(30));
        let out = sink.0.lock().unwrap().clone();
        out.expect("script should complete")
    }

    const PRINT_BOTH_PROMPT_VARS: &str =
        "printf '%s|%s' \"${FLOW_PROMPT:-unset}\" \"${CONDUCTOR_PROMPT:-unset}\"";

    #[test]
    fn prompt_is_passed_as_flow_prompt_by_default() {
        let out = run_with_prompt_env(None, PRINT_BOTH_PROMPT_VARS, "test-prompt-default");
        assert_eq!(out.trim(), "hello|unset");
    }

    #[test]
    fn prompt_env_overrides_the_variable_name() {
        let out = run_with_prompt_env(
            Some("CONDUCTOR_PROMPT"),
            PRINT_BOTH_PROMPT_VARS,
            "test-prompt-override",
        );
        assert_eq!(out.trim(), "unset|hello");
    }

    #[test]
    fn shell_variable_names_are_accepted_as_prompt_env() {
        for (i, name) in [
            "FLOW_PROMPT",
            "CONDUCTOR_PROMPT_2",
            "_PROMPT",
            "p",
            "_1",
            "_PROMPT_2",
        ]
        .into_iter()
        .enumerate()
        {
            let out = run_with_prompt_env(
                Some(name),
                &format!("printf '%s' \"${name}\""),
                &format!("test-prompt-accepted-{i}"),
            );
            assert_eq!(out.trim(), "hello", "{name:?}");
        }
    }

    #[test]
    fn invalid_prompt_env_fails_the_spawn() {
        for bad in ["", "BAD=NAME", "BAD\0NAME", "MY-PROMPT", "2PROMPT"] {
            let runtime = ScriptRuntime::new(RuntimeConfig {
                command: Some("echo should-not-run".to_string()),
                prompt_env: Some(bad.to_string()),
                ..RuntimeConfig::default()
            });
            let err = runtime
                .spawn_validated(&make_request("test-prompt-invalid"))
                .expect_err("an invalid prompt_env must fail the spawn");
            assert!(
                matches!(&err, RuntimeError::Config(m) if m.contains("invalid `prompt_env`")),
                "{bad:?}: {err:?}"
            );
        }
    }

    #[test]
    fn config_env_is_passed_to_the_script() {
        let out = run_with_config(
            RuntimeConfig {
                env: HashMap::from([(
                    "MY_API_BASE".to_string(),
                    "http://localhost:8000".to_string(),
                )]),
                ..RuntimeConfig::default()
            },
            "printf '%s|%s' \"$MY_API_BASE\" \"$FLOW_PROMPT\"",
            "test-config-env",
        );
        assert_eq!(out.trim(), "http://localhost:8000|hello");
    }

    #[test]
    fn config_env_colliding_with_prompt_env_fails_the_spawn() {
        for prompt_env in [None, Some("CONDUCTOR_PROMPT")] {
            let name = prompt_env.unwrap_or(DEFAULT_SCRIPT_PROMPT_ENV);
            let runtime = ScriptRuntime::new(RuntimeConfig {
                command: Some("echo should-not-run".to_string()),
                prompt_env: prompt_env.map(str::to_string),
                env: HashMap::from([(name.to_string(), "x".to_string())]),
                ..RuntimeConfig::default()
            });
            let err = runtime
                .spawn_validated(&make_request("test-env-collision"))
                .expect_err("an env key equal to prompt_env must fail the spawn");
            assert!(
                matches!(&err, RuntimeError::Config(m) if m.contains("`prompt_env`")),
                "{name:?}: {err:?}"
            );
        }
    }

    #[test]
    fn invalid_config_env_key_fails_the_spawn() {
        for bad in ["", "BAD=KEY", "BAD\0KEY"] {
            let runtime = ScriptRuntime::new(RuntimeConfig {
                command: Some("echo should-not-run".to_string()),
                env: HashMap::from([(bad.to_string(), "v".to_string())]),
                ..RuntimeConfig::default()
            });
            let err = runtime
                .spawn_validated(&make_request("test-env-invalid"))
                .expect_err("an invalid env key must fail the spawn");
            assert!(
                matches!(&err, RuntimeError::Config(m) if m.contains("invalid `env` key")),
                "{bad:?}: {err:?}"
            );
        }
    }

    #[test]
    fn poll_before_spawn_returns_failed() {
        let runtime = make_runtime(Some("echo hi"));
        let result = runtime.poll("no-such-run", None, Duration::from_millis(10));
        assert!(matches!(result, Err(PollError::Failed(_))));
    }
}
