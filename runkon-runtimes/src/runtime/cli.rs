use std::path::PathBuf;
use std::sync::{atomic::AtomicBool, Arc, Mutex};
use std::time::Duration;

use crate::config::RuntimeConfig;
use crate::error::{Result, RuntimeError};
use crate::process_utils;
use crate::run::RunHandle;
use crate::tracker::{RunEventSink, RunTracker, RuntimeEvent};

use super::{AgentRuntime, PollError, RuntimeRequest};

/// CliRuntime spawns any CLI agent as a headless subprocess with stdout redirected to a
/// JSON output file. Supports prompt injection via arg substitution or stdin.
pub struct CliRuntime {
    config: RuntimeConfig,
    workspace_root: PathBuf,
    state: Mutex<Option<CliState>>,
    tracker: Mutex<Option<Arc<dyn RunTracker>>>,
    event_sink: Mutex<Option<Arc<dyn RunEventSink>>>,
}

struct CliState {
    child: std::process::Child,
    pid: u32,
    output_path: PathBuf,
    start: std::time::Instant,
}

impl CliRuntime {
    pub fn new(config: RuntimeConfig, workspace_root: PathBuf) -> Self {
        Self {
            config,
            workspace_root,
            state: Mutex::new(None),
            tracker: Mutex::new(None),
            event_sink: Mutex::new(None),
        }
    }
}

impl AgentRuntime for CliRuntime {
    fn spawn_impl(&self, request: &RuntimeRequest, _seal: super::private::Seal) -> Result<()> {
        // Defense-in-depth: spawn_validated already validates, but spawn_impl is
        // technically reachable via the sealed trait — re-validate so a future
        // refactor that adds a new entrypoint can't bypass the path-safety check.
        crate::text_util::validate_run_id(&request.run_id)?;

        let binary =
            self.config.binary.as_deref().ok_or_else(|| {
                RuntimeError::Config("CliRuntime: `binary` is required".to_string())
            })?;

        let resolved_model = request
            .resolved_model()
            .or(self.config.default_model.as_deref())
            .unwrap_or("");

        let run_dir = self.workspace_root.join(&request.run_id);
        std::fs::create_dir_all(&run_dir).map_err(|e| {
            RuntimeError::Agent(format!("CliRuntime: failed to create run dir: {e}"))
        })?;
        let output_path = run_dir.join("output.json");

        let prompt_via = self.config.prompt_via.as_deref().unwrap_or("arg");

        let args: Vec<String> = self
            .config
            .args
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|arg| {
                let a = arg.replace("{{model}}", resolved_model);
                if prompt_via == "arg" {
                    a.replace("{{prompt}}", &request.prompt)
                } else {
                    a.replace("{{prompt}}", "")
                }
            })
            .filter(|a| !a.is_empty())
            .collect();

        let output_file = std::fs::File::create(&output_path).map_err(|e| {
            RuntimeError::Agent(format!("CliRuntime: failed to create output file: {e}"))
        })?;

        let stdin_cfg = if prompt_via == "stdin" {
            std::process::Stdio::piped()
        } else {
            std::process::Stdio::null()
        };

        let mut cmd = std::process::Command::new(binary);
        cmd.args(&args)
            .stdout(std::process::Stdio::from(output_file))
            .stderr(std::process::Stdio::null())
            .stdin(stdin_cfg);

        let mut child = cmd
            .spawn()
            .map_err(|e| RuntimeError::Agent(format!("CliRuntime: spawn failed: {e}")))?;

        if prompt_via == "stdin" {
            if let Some(mut stdin) = child.stdin.take() {
                use std::io::Write;
                if let Err(e) = stdin.write_all(request.prompt.as_bytes()) {
                    tracing::warn!(
                        "CliRuntime: failed to write prompt to stdin for run {}: {e}",
                        request.run_id
                    );
                }
            }
        }

        let pid = child.id();

        super::record_pid_and_runtime(
            request.tracker.as_ref(),
            &request.run_id,
            pid,
            &request.agent_def.runtime,
            "CliRuntime",
        );

        *self.state.lock().unwrap_or_else(|e| e.into_inner()) = Some(CliState {
            child,
            pid,
            output_path,
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
            .ok_or_else(|| PollError::Failed("CliRuntime::poll called before spawn".into()))?;

        let tracker = self
            .tracker
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| {
                PollError::Failed("CliRuntime::poll called before spawn (tracker missing)".into())
            })?;

        let event_sink = self
            .event_sink
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| {
                PollError::Failed(
                    "CliRuntime::poll called before spawn (event_sink missing)".into(),
                )
            })?;

        let poll_start = std::time::Instant::now();
        loop {
            if let Some(flag) = shutdown {
                if flag.load(std::sync::atomic::Ordering::Relaxed) {
                    process_utils::cancel_subprocess(state.pid);
                    super::mark_cancelled_with_reason(
                        tracker.as_ref(),
                        run_id,
                        "CliRuntime",
                        "shutdown",
                    );
                    return Err(PollError::Cancelled);
                }
            }

            if poll_start.elapsed() > step_timeout {
                process_utils::cancel_subprocess(state.pid);
                super::mark_cancelled_with_reason(
                    tracker.as_ref(),
                    run_id,
                    "CliRuntime",
                    "timeout",
                );
                return Err(PollError::NoResult);
            }

            match state.child.try_wait() {
                Ok(Some(exit_status)) => {
                    let exit_code = exit_status.code().unwrap_or(1);
                    let duration_ms = state.start.elapsed().as_millis() as i64;
                    let is_error = exit_code != 0;

                    let (result_text, input_tokens, output_tokens) =
                        match std::fs::read_to_string(&state.output_path) {
                            Ok(content) => parse_output(&content, &self.config),
                            Err(e) => {
                                tracing::warn!(
                                    "CliRuntime: failed to read output file {}: {e}",
                                    state.output_path.display()
                                );
                                (
                                    if is_error {
                                        Some(format!("process exited with code {exit_code}"))
                                    } else {
                                        None
                                    },
                                    None,
                                    None,
                                )
                            }
                        };

                    if is_error {
                        let err_msg = result_text
                            .clone()
                            .unwrap_or_else(|| format!("process exited with code {exit_code}"));
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
                                result_text,
                                session_id: None,
                                cost_usd: None,
                                num_turns: Some(1),
                                duration_ms: Some(duration_ms),
                                input_tokens,
                                output_tokens,
                                cache_read_input_tokens: None,
                                cache_creation_input_tokens: None,
                            },
                        );
                    }

                    return tracker
                        .get_run(run_id)
                        .map_err(|e| PollError::Failed(format!("DB error: {e}")))?
                        .ok_or_else(|| {
                            PollError::Failed(format!(
                                "run {run_id} not found in DB after completion"
                            ))
                        });
                }
                Ok(None) => {
                    std::thread::sleep(Duration::from_millis(500));
                }
                Err(e) => {
                    let reason = e.to_string();
                    if let Err(db_err) = tracker.mark_failed_if_running(run_id, &reason) {
                        tracing::warn!("CliRuntime: failed to mark run {run_id} failed after wait error: {db_err}");
                    }
                    return Err(PollError::Failed(format!("wait error: {e}")));
                }
            }
        }
    }

    fn is_alive(&self, run: &RunHandle) -> bool {
        #[cfg(unix)]
        if let Some(pid) = run.subprocess_pid {
            return process_utils::pid_is_alive(pid as u32);
        }
        let _ = run;
        false
    }

    fn cancel(&self, run: &RunHandle) -> Result<()> {
        let child = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .map(|s| s.child);

        if let Some(mut c) = child {
            let _ = c.kill();
            let _ = c.wait();
        } else {
            #[cfg(unix)]
            if let Some(pid) = run.subprocess_pid {
                process_utils::cancel_subprocess(pid as u32);
            }
        }

        super::mark_cancelled_via_tracker(&self.tracker, &run.id, "CliRuntime");
        Ok(())
    }
}

fn parse_output(
    content: &str,
    config: &RuntimeConfig,
) -> (Option<String>, Option<i64>, Option<i64>) {
    let Ok(json) = serde_json::from_str::<serde_json::Value>(content) else {
        return (Some(content.trim().to_string()), None, None);
    };

    let result_text = config
        .result_field
        .as_deref()
        .and_then(|path| super::extract_json_path(&json, path))
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .or_else(|| Some(content.trim().to_string()));

    let tokens = config
        .token_fields
        .as_deref()
        .and_then(|path| super::extract_json_path(&json, path))
        .and_then(|v| v.as_f64())
        .map(|n| n as i64);

    (result_text, tokens, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RuntimeConfig;
    use crate::runtime::test_util::make_test_run;

    fn make_runtime(binary: &str) -> CliRuntime {
        CliRuntime::new(
            RuntimeConfig {
                binary: Some(binary.to_string()),
                ..RuntimeConfig::default()
            },
            std::env::temp_dir().join("conductor-workspaces"),
        )
    }

    #[test]
    fn parse_output_plain_text() {
        let config = RuntimeConfig::default();
        let (result, _, _) = parse_output("hello world", &config);
        assert_eq!(result.as_deref(), Some("hello world"));
    }

    #[test]
    fn parse_output_json_result_field() {
        let config = RuntimeConfig {
            result_field: Some("response".to_string()),
            ..RuntimeConfig::default()
        };
        let json = r#"{"response": "the answer", "status": "ok"}"#;
        let (result, _, _) = parse_output(json, &config);
        assert_eq!(result.as_deref(), Some("the answer"));
    }

    #[test]
    fn is_alive_returns_false_when_no_pid() {
        let runtime = make_runtime("echo");
        assert!(!runtime.is_alive(&make_test_run("cli", None)));
    }

    #[cfg(unix)]
    #[test]
    fn is_alive_returns_true_for_self() {
        let runtime = make_runtime("echo");
        let run = make_test_run("cli", Some(std::process::id() as i64));
        assert!(runtime.is_alive(&run));
    }

    #[test]
    fn poll_before_spawn_returns_failed() {
        let runtime = make_runtime("echo");
        let result = runtime.poll("no-such-run", None, Duration::from_millis(10));
        assert!(matches!(result, Err(PollError::Failed(_))));
    }

    #[test]
    fn poisoned_mutex_does_not_panic_on_poll() {
        let runtime = make_runtime("echo");
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = runtime.state.lock().unwrap();
            panic!("intentional poison");
        }));
        let result = runtime.poll("no-such-run", None, Duration::from_millis(10));
        assert!(matches!(result, Err(PollError::Failed(_))));
    }
}
