use std::io::Read;
use std::process::{Child, Stdio};
use std::sync::{atomic::AtomicBool, Arc, Mutex};
use std::time::Duration;

use crate::config::RuntimeConfig;
use crate::error::{Result, RuntimeError};
use crate::run::{RunHandle, RunStatus};
use crate::tracker::{RunEventSink, RunTracker, RuntimeEvent};

use super::{AgentRuntime, PollError, RuntimeRequest};

struct ScriptState {
    child: Child,
    start: std::time::Instant,
}

/// ScriptRuntime runs any shell command via `sh -c <command>`,
/// passing the prompt through the `CONDUCTOR_PROMPT` environment variable and
/// capturing stdout as `result_text`. No tmux dependency.
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

        if request.agent_def.can_commit {
            tracing::warn!(
                "ScriptRuntime: agent '{}' has can_commit=true but ScriptRuntime produces no \
                 output schema — commit validation is not enforced",
                request.agent_def.name
            );
        }

        let child = std::process::Command::new("sh")
            .args(["-c", command])
            .env("CONDUCTOR_PROMPT", &request.prompt)
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
            "script",
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
    use super::*;
    use crate::config::RuntimeConfig;
    use crate::runtime::test_util::make_test_run;

    fn make_runtime(command: Option<&str>) -> ScriptRuntime {
        ScriptRuntime::new(RuntimeConfig {
            command: command.map(|s| s.to_string()),
            ..RuntimeConfig::default()
        })
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
}
