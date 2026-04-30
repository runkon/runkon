//! ClaudeRuntime — wraps the existing headless subprocess spawn/poll logic.

use std::path::PathBuf;
use std::sync::{atomic::AtomicBool, Arc, Mutex};
use std::time::Duration;

use crate::error::{Result, RuntimeError};
use crate::headless::{DrainOutcome, SpawnHeadlessParams};
use crate::permission::PermissionMode;
use crate::process_utils;
use crate::run::RunHandle;
use crate::tracker::{RunEventSink, RunTracker};

use super::{AgentRuntime, PollError, RuntimeRequest};

/// Claude-specific configuration captured at construction time.
#[derive(Clone)]
pub struct ClaudeRuntimeOptions {
    pub permission_mode: PermissionMode,
    pub binary_path: PathBuf,
    pub log_path_for_run: Arc<dyn Fn(&str) -> PathBuf + Send + Sync>,
}

impl Default for ClaudeRuntimeOptions {
    fn default() -> Self {
        Self {
            permission_mode: PermissionMode::default(),
            binary_path: PathBuf::from(crate::headless::resolve_conductor_bin()),
            log_path_for_run: Arc::new(|run_id| std::env::temp_dir().join(format!("{run_id}.log"))),
        }
    }
}

/// Runtime that spawns a `conductor agent run` subprocess (headless mode).
pub struct ClaudeRuntime {
    options: ClaudeRuntimeOptions,
    #[cfg(unix)]
    handle: Arc<Mutex<Option<crate::headless::HeadlessHandle>>>,
    prompt_file: Arc<Mutex<Option<PathBuf>>>,
    tracker: Arc<Mutex<Option<Arc<dyn RunTracker>>>>,
    event_sink: Arc<Mutex<Option<Arc<dyn RunEventSink>>>>,
}

impl ClaudeRuntime {
    pub fn new(options: ClaudeRuntimeOptions) -> Self {
        Self {
            options,
            #[cfg(unix)]
            handle: Arc::new(Mutex::new(None)),
            prompt_file: Arc::new(Mutex::new(None)),
            tracker: Arc::new(Mutex::new(None)),
            event_sink: Arc::new(Mutex::new(None)),
        }
    }
}

impl Default for ClaudeRuntime {
    fn default() -> Self {
        Self::new(ClaudeRuntimeOptions::default())
    }
}

impl AgentRuntime for ClaudeRuntime {
    fn spawn_impl(&self, request: &RuntimeRequest, _seal: super::private::Seal) -> Result<()> {
        #[cfg(unix)]
        {
            let params = SpawnHeadlessParams {
                run_id: &request.run_id,
                working_dir: request.working_dir.to_str().unwrap_or("."),
                prompt: &request.prompt,
                resume_session_id: None,
                model: request.resolved_model(),
                bot_name: request.bot_name.as_deref(),
                permission_mode: Some(&self.options.permission_mode),
                plugin_dirs: &request.plugin_dirs,
            };
            let (h, pf) = crate::headless::try_spawn_headless_run(
                &params,
                &self.options.binary_path.to_string_lossy(),
            )
            .map_err(RuntimeError::Workflow)?;
            *self.handle.lock().unwrap_or_else(|e| e.into_inner()) = Some(h);
            *self.prompt_file.lock().unwrap_or_else(|e| e.into_inner()) = Some(pf);
            *self.tracker.lock().unwrap_or_else(|e| e.into_inner()) = Some(request.tracker.clone());
            *self.event_sink.lock().unwrap_or_else(|e| e.into_inner()) =
                Some(request.event_sink.clone());
            Ok(())
        }
        #[cfg(not(unix))]
        {
            let _ = request;
            Err(RuntimeError::Workflow(
                "ClaudeRuntime headless spawn is not supported on non-Unix platforms".into(),
            ))
        }
    }

    fn poll(
        &self,
        run_id: &str,
        shutdown: Option<&Arc<AtomicBool>>,
        step_timeout: Duration,
    ) -> std::result::Result<RunHandle, PollError> {
        #[cfg(unix)]
        {
            poll_unix(self, run_id, shutdown, step_timeout)
        }
        #[cfg(not(unix))]
        {
            let _ = (run_id, shutdown, step_timeout);
            Err(PollError::Failed(
                "ClaudeRuntime poll is not supported on non-Unix platforms".into(),
            ))
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
        #[cfg(unix)]
        {
            if let Some(h) = self.handle.lock().unwrap_or_else(|e| e.into_inner()).take() {
                h.abort();
            }
            if let Some(pid) = run.subprocess_pid {
                process_utils::cancel_subprocess(pid as u32);
            }
            super::mark_cancelled_via_tracker(&self.tracker, &run.id, "ClaudeRuntime");
        }
        let _ = run;
        Ok(())
    }
}

#[cfg(unix)]
fn poll_unix(
    rt: &ClaudeRuntime,
    run_id: &str,
    shutdown: Option<&Arc<AtomicBool>>,
    step_timeout: Duration,
) -> std::result::Result<RunHandle, PollError> {
    let handle = rt
        .handle
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take()
        .ok_or_else(|| PollError::Failed("ClaudeRuntime::poll called before spawn".into()))?;

    let prompt_file = rt
        .prompt_file
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take();

    let tracker = rt
        .tracker
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take()
        .ok_or_else(|| {
            PollError::Failed("ClaudeRuntime::poll called before spawn (tracker missing)".into())
        })?;

    let event_sink = rt
        .event_sink
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take()
        .ok_or_else(|| {
            PollError::Failed("ClaudeRuntime::poll called before spawn (event_sink missing)".into())
        })?;

    let pid = handle.pid();
    let log_path = (rt.options.log_path_for_run)(run_id);

    if let Err(e) = tracker.record_pid(run_id, pid) {
        tracing::warn!("ClaudeRuntime: failed to persist subprocess pid {pid}: {e}");
    }

    let (stderr_pipe, stdout_pipe, finish) = handle.into_stderr_drain_parts();

    let run_id_owned = run_id.to_string();
    let event_sink_for_drain = event_sink.clone();
    let (tx, rx) = std::sync::mpsc::channel::<DrainOutcome>();

    let stderr_run_id = run_id.to_string();
    std::thread::spawn(move || {
        use std::io::{BufRead, BufReader};
        let reader = BufReader::new(stderr_pipe);
        for line in reader.lines() {
            match line {
                Ok(l) => tracing::trace!(target: "conductor::agent::stderr", "{l}"),
                Err(e) => {
                    tracing::warn!(
                        "ClaudeRuntime: stderr read failed for run {stderr_run_id}, ending stderr drain: {e}"
                    );
                    break;
                }
            }
        }
    });

    std::thread::spawn(move || {
        let outcome = crate::headless::drain_stream_json(
            stdout_pipe,
            &run_id_owned,
            &log_path,
            &*event_sink_for_drain,
        );
        if let Some(pf) = prompt_file {
            let _ = std::fs::remove_file(pf);
        }
        finish();
        let _ = tx.send(outcome);
    });

    // Helper: tear down the running agent (warn → mark cancelled → kill process
    // → drain remaining output up to 6s). Used by the shutdown and timeout
    // branches below to avoid duplicating the same 4-step sequence.
    let abort_poll = |reason: &str| {
        tracing::warn!("ClaudeRuntime: {reason} for run {run_id}, cancelling");
        super::mark_cancelled_with_reason(tracker.as_ref(), run_id, "ClaudeRuntime", reason);
        process_utils::cancel_subprocess(pid);
        let _ = rx.recv_timeout(Duration::from_secs(6));
    };

    let start = std::time::Instant::now();
    let drain_outcome = loop {
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(outcome) => break outcome,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if let Some(flag) = shutdown {
                    if flag.load(std::sync::atomic::Ordering::Relaxed) {
                        abort_poll("shutdown requested");
                        return Err(PollError::Cancelled);
                    }
                }
                if start.elapsed() > step_timeout {
                    abort_poll("step timeout reached");
                    break DrainOutcome::NoResult;
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                tracing::warn!("ClaudeRuntime: drain thread disconnected for run {run_id}");
                break DrainOutcome::NoResult;
            }
        }
    };

    match drain_outcome {
        DrainOutcome::Completed => tracker
            .get_run(run_id)
            .map_err(|e| PollError::Failed(format!("DB error after drain: {e}")))?
            .ok_or_else(|| PollError::Failed(format!("run {run_id} not found in DB after drain"))),
        DrainOutcome::NoResult => {
            if let Err(e) = tracker.mark_failed_if_running(run_id, "agent exited without result") {
                tracing::warn!(
                    "ClaudeRuntime: failed to mark run {run_id} failed after no-result: {e}"
                );
            }
            Err(PollError::NoResult)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_def::{AgentDef, AgentRole};
    use crate::runtime::test_util::make_test_run;
    use crate::tracker::NoopEventSink;

    fn make_request(run_id: &str) -> RuntimeRequest {
        RuntimeRequest {
            run_id: run_id.to_string(),
            agent_def: AgentDef {
                name: "test".to_string(),
                role: AgentRole::Reviewer,
                can_commit: false,
                model: None,
                runtime: "claude".to_string(),
                prompt: String::new(),
            },
            prompt: "p".to_string(),
            working_dir: std::path::PathBuf::from("/tmp"),
            model: None,
            bot_name: None,
            plugin_dirs: vec![],
            tracker: Arc::new(NoopTracker),
            event_sink: Arc::new(NoopEventSink),
        }
    }

    struct NoopTracker;

    impl RunTracker for NoopTracker {
        fn record_pid(&self, _run_id: &str, _pid: u32) -> Result<()> {
            Ok(())
        }
        fn record_runtime(&self, _run_id: &str, _runtime_name: &str) -> Result<()> {
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

    #[test]
    fn spawn_rejects_path_traversal_run_id() {
        let runtime = ClaudeRuntime::default();
        let request = make_request("../../etc/cron.d/payload");
        let err = runtime
            .spawn_validated(&request)
            .expect_err("expected Err for path-traversal run_id");
        assert!(
            matches!(err, RuntimeError::InvalidInput(_)),
            "expected InvalidInput, got: {err:?}"
        );
    }

    #[test]
    fn spawn_rejects_slash_in_run_id() {
        let runtime = ClaudeRuntime::default();
        let request = make_request("run/id");
        assert!(runtime.spawn_validated(&request).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn poll_before_spawn_returns_failed() {
        let runtime = ClaudeRuntime::default();
        let result = runtime.poll("some-run-id", None, Duration::from_millis(10));
        assert!(
            matches!(result, Err(PollError::Failed(_))),
            "expected Failed, got: {result:?}"
        );
    }

    #[cfg(not(unix))]
    #[test]
    fn poll_fails_on_non_unix() {
        let runtime = ClaudeRuntime::default();
        let result = runtime.poll("some-run-id", None, Duration::from_millis(10));
        assert!(
            matches!(result, Err(PollError::Failed(_))),
            "expected Failed on non-Unix, got: {result:?}"
        );
    }

    #[test]
    fn is_alive_returns_false_when_no_pid() {
        let runtime = ClaudeRuntime::default();
        let run = make_test_run("claude", None);
        assert!(!runtime.is_alive(&run));
    }

    #[cfg(unix)]
    #[test]
    fn is_alive_returns_true_for_self() {
        let runtime = ClaudeRuntime::default();
        let run = make_test_run("claude", Some(std::process::id() as i64));
        assert!(runtime.is_alive(&run));
    }

    #[cfg(unix)]
    #[test]
    fn is_alive_returns_false_for_dead_pid() {
        let mut child = std::process::Command::new("true").spawn().unwrap();
        child.wait().unwrap();
        let dead_pid = child.id() as i64;
        let runtime = ClaudeRuntime::default();
        let run = make_test_run("claude", Some(dead_pid));
        assert!(!runtime.is_alive(&run));
    }

    #[test]
    fn cancel_with_no_handle_and_no_pid() {
        let runtime = ClaudeRuntime::default();
        let run = make_test_run("claude", None);
        assert!(runtime.cancel(&run).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn cancel_with_dead_pid_returns_ok() {
        let mut child = std::process::Command::new("true").spawn().unwrap();
        child.wait().unwrap();
        let dead_pid = child.id() as i64;
        let runtime = ClaudeRuntime::default();
        let run = make_test_run("claude", Some(dead_pid));
        assert!(runtime.cancel(&run).is_ok());
    }

    /// Inject a long-running child so we can test poll without needing the real conductor binary.
    #[cfg(unix)]
    fn inject_sleep_child(runtime: &ClaudeRuntime, secs: u64) -> u32 {
        use std::os::unix::process::CommandExt;
        use std::process::Stdio;
        let child = std::process::Command::new("sleep")
            .arg(secs.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0)
            .spawn()
            .expect("sleep must be available");
        let handle = crate::headless::HeadlessHandle::from_child(child)
            .expect("HeadlessHandle from_child failed");
        let pid = handle.pid();
        *runtime.handle.lock().unwrap() = Some(handle);
        *runtime.tracker.lock().unwrap() = Some(Arc::new(NoopTracker));
        *runtime.event_sink.lock().unwrap() = Some(Arc::new(NoopEventSink));
        pid
    }

    #[cfg(unix)]
    #[test]
    fn poll_timeout_returns_no_result() {
        let runtime = ClaudeRuntime::default();
        let _pid = inject_sleep_child(&runtime, 60);
        let result = runtime.poll("timeout-run", None, Duration::from_millis(100));
        assert!(
            matches!(result, Err(PollError::NoResult)),
            "expected NoResult after timeout, got: {result:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn poll_shutdown_flag_returns_cancelled() {
        let runtime = ClaudeRuntime::default();
        let _pid = inject_sleep_child(&runtime, 60);
        let flag = Arc::new(AtomicBool::new(true));
        let result = runtime.poll("shutdown-run", Some(&flag), Duration::from_secs(300));
        assert!(
            matches!(result, Err(PollError::Cancelled)),
            "expected Cancelled when shutdown flag is set, got: {result:?}"
        );
    }
}
