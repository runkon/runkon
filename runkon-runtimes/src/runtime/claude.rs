//! ClaudeRuntime — wraps the existing headless subprocess spawn/poll logic.

use std::path::PathBuf;
use std::sync::{atomic::AtomicBool, Arc, Mutex};
use std::time::Duration;

use crate::error::{Result, RuntimeError};
use crate::headless::DrainOutcome;
use crate::permission::PermissionMode;
use crate::process_utils;
use crate::run::RunHandle;
use crate::tracker::{RunEventSink, RunTracker};

use super::{AgentRuntime, PollError, RuntimeRequest};

/// Per-spawn data passed to the injected argv builder.
pub struct ClaudeArgvRequest<'a> {
    pub run_id: &'a str,
    pub working_dir: &'a str,
    pub prompt: &'a str,
    pub resume_session_id: Option<&'a str>,
    pub model: Option<&'a str>,
    pub extra_cli_args: &'a [(
        std::borrow::Cow<'static, str>,
        std::borrow::Cow<'static, str>,
    )],
    pub permission_mode: Option<&'a PermissionMode>,
    pub plugin_dirs: &'a [String],
}

/// Injectable argv builder for [`ClaudeRuntime`].
pub type ArgvBuilder = Arc<
    dyn for<'a> Fn(
            &'a ClaudeArgvRequest<'a>,
        ) -> std::result::Result<
            (
                Vec<std::borrow::Cow<'static, str>>,
                Option<std::path::PathBuf>,
            ),
            String,
        > + Send
        + Sync,
>;

/// Claude-specific configuration captured at construction time.
#[derive(Clone)]
pub struct ClaudeRuntimeOptions {
    pub permission_mode: PermissionMode,
    pub binary_path: PathBuf,
    /// Per-runtime environment variable overrides injected into the spawned
    /// subprocess via `Command::envs()` (overlay — parent env is preserved).
    /// Use this for endpoint/auth vars like `ANTHROPIC_BASE_URL`.
    pub env: std::collections::HashMap<String, String>,
    pub log_path_for_run: Arc<dyn Fn(&str) -> PathBuf + Send + Sync>,
    pub argv_builder: ArgvBuilder,
    /// If `Some(t)`, `drain_stream_json` returns `StalledOut` when no output
    /// is received for longer than `t`. `None` disables stall detection.
    pub stall_threshold: Option<Duration>,
    /// If `Some(n)`, `drain_stream_json` returns `TurnCapReached(n)` after
    /// counting `n` `"assistant"` events. `None` disables the turn cap.
    pub max_turns: Option<u32>,
}

/// Runtime that spawns a headless agent subprocess via the injected argv builder.
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

impl AgentRuntime for ClaudeRuntime {
    fn spawn_impl(&self, request: &RuntimeRequest, _seal: super::private::Seal) -> Result<()> {
        #[cfg(unix)]
        {
            let wd = request.working_dir.to_str().unwrap_or(".");
            let argv_req = ClaudeArgvRequest {
                run_id: &request.run_id,
                working_dir: wd,
                prompt: &request.prompt,
                resume_session_id: request.resume_session_id.as_deref(),
                model: request.resolved_model(),
                extra_cli_args: &request.extra_cli_args,
                permission_mode: Some(&self.options.permission_mode),
                plugin_dirs: &request.plugin_dirs,
            };
            let (args, prompt_file) =
                (self.options.argv_builder)(&argv_req).map_err(RuntimeError::Workflow)?;
            let h = crate::headless::spawn_headless(
                &args,
                std::path::Path::new(wd),
                &self.options.binary_path.to_string_lossy(),
                &self.options.env,
            )
            .map_err(|e| {
                if let Some(ref pf) = prompt_file {
                    let _ = std::fs::remove_file(pf);
                }
                RuntimeError::Workflow(format!(
                    "spawn failed for run {} (working_dir={}): {e}",
                    &request.run_id, wd
                ))
            })?;
            *self.handle.lock().unwrap_or_else(|e| e.into_inner()) = Some(h);
            *self.prompt_file.lock().unwrap_or_else(|e| e.into_inner()) = prompt_file;
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

    let stall_threshold = rt.options.stall_threshold;
    let max_turns = rt.options.max_turns;
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
                Ok(l) => tracing::trace!(target: "runkon::agent::stderr", "{l}"),
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
            stall_threshold,
            max_turns,
        );
        if let Some(pf) = prompt_file {
            let _ = std::fs::remove_file(pf);
        }
        // Unblock poll_unix immediately — don't let cleanup gate the result.
        let _ = tx.send(outcome);
        // Kill the whole process group (pgid == pid because spawn_headless uses
        // .process_group(0)). Terminates claude + all descendants.
        process_utils::cancel_subprocess(pid);
        // Reap the direct child; returns promptly since the group is now dead.
        finish();
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
        DrainOutcome::StalledOut(elapsed) => {
            let msg = format!("stall_timeout: no events for {}s", elapsed.as_secs());
            tracing::warn!("ClaudeRuntime: {msg} for run {run_id}");
            if let Err(e) = tracker.mark_failed_if_running(run_id, &msg) {
                tracing::warn!("ClaudeRuntime: failed to persist stall failure for {run_id}: {e}");
            }
            Err(PollError::Failed(msg))
        }
        DrainOutcome::TurnCapReached(count) => {
            let msg = format!("turn_cap_reached: {} turns", count);
            tracing::warn!("ClaudeRuntime: {msg} for run {run_id}");
            if let Err(e) = tracker.mark_failed_if_running(run_id, &msg) {
                tracing::warn!(
                    "ClaudeRuntime: failed to persist turn-cap failure for {run_id}: {e}"
                );
            }
            Err(PollError::Failed(msg))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_def::{AgentDef, AgentRole};
    use crate::runtime::test_util::{make_test_run, NoopTracker};
    use crate::tracker::NoopEventSink;

    fn make_test_runtime(
        stall_threshold: Option<Duration>,
        max_turns: Option<u32>,
    ) -> ClaudeRuntime {
        ClaudeRuntime::new(ClaudeRuntimeOptions {
            permission_mode: PermissionMode::default(),
            binary_path: std::path::PathBuf::from("/nonexistent/agent-bin"),
            env: std::collections::HashMap::new(),
            log_path_for_run: Arc::new(|run_id| std::env::temp_dir().join(format!("{run_id}.log"))),
            argv_builder: Arc::new(|_| Err("test stub: no argv_builder configured".to_string())),
            stall_threshold,
            max_turns,
        })
    }

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
            extra_cli_args: vec![],
            plugin_dirs: vec![],
            resume_session_id: None,
            tracker: Arc::new(NoopTracker),
            event_sink: Arc::new(NoopEventSink),
        }
    }

    #[test]
    fn claude_runtime_options_env_field_round_trips() {
        let mut env = std::collections::HashMap::new();
        env.insert(
            "ANTHROPIC_BASE_URL".to_string(),
            "https://proxy.example.com".to_string(),
        );
        env.insert("ANTHROPIC_AUTH_TOKEN".to_string(), "test-token".to_string());

        let options = ClaudeRuntimeOptions {
            permission_mode: PermissionMode::default(),
            binary_path: std::path::PathBuf::from("/usr/local/bin/claude"),
            env: env.clone(),
            log_path_for_run: Arc::new(|run_id| std::env::temp_dir().join(format!("{run_id}.log"))),
            argv_builder: Arc::new(|_| Err("stub".to_string())),
            stall_threshold: None,
            max_turns: None,
        };

        assert_eq!(
            options.env.get("ANTHROPIC_BASE_URL").map(String::as_str),
            Some("https://proxy.example.com")
        );
        assert_eq!(
            options.env.get("ANTHROPIC_AUTH_TOKEN").map(String::as_str),
            Some("test-token")
        );
        assert_eq!(options.env.len(), 2);
    }

    #[test]
    fn spawn_rejects_path_traversal_run_id() {
        let runtime = make_test_runtime(None, None);
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
        let runtime = make_test_runtime(None, None);
        let request = make_request("run/id");
        assert!(runtime.spawn_validated(&request).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn poll_before_spawn_returns_failed() {
        let runtime = make_test_runtime(None, None);
        let result = runtime.poll("some-run-id", None, Duration::from_millis(10));
        assert!(
            matches!(result, Err(PollError::Failed(_))),
            "expected Failed, got: {result:?}"
        );
    }

    #[cfg(not(unix))]
    #[test]
    fn poll_fails_on_non_unix() {
        let runtime = make_test_runtime(None, None);
        let result = runtime.poll("some-run-id", None, Duration::from_millis(10));
        assert!(
            matches!(result, Err(PollError::Failed(_))),
            "expected Failed on non-Unix, got: {result:?}"
        );
    }

    #[test]
    fn is_alive_returns_false_when_no_pid() {
        let runtime = make_test_runtime(None, None);
        let run = make_test_run("claude", None);
        assert!(!runtime.is_alive(&run));
    }

    #[cfg(unix)]
    #[test]
    fn is_alive_returns_true_for_self() {
        let runtime = make_test_runtime(None, None);
        let run = make_test_run("claude", Some(std::process::id() as i64));
        assert!(runtime.is_alive(&run));
    }

    #[cfg(unix)]
    #[test]
    fn is_alive_returns_false_for_dead_pid() {
        let mut child = std::process::Command::new("true").spawn().unwrap();
        child.wait().unwrap();
        let dead_pid = child.id() as i64;
        let runtime = make_test_runtime(None, None);
        let run = make_test_run("claude", Some(dead_pid));
        assert!(!runtime.is_alive(&run));
    }

    #[test]
    fn cancel_with_no_handle_and_no_pid() {
        let runtime = make_test_runtime(None, None);
        let run = make_test_run("claude", None);
        assert!(runtime.cancel(&run).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn cancel_with_dead_pid_returns_ok() {
        let mut child = std::process::Command::new("true").spawn().unwrap();
        child.wait().unwrap();
        let dead_pid = child.id() as i64;
        let runtime = make_test_runtime(None, None);
        let run = make_test_run("claude", Some(dead_pid));
        assert!(runtime.cancel(&run).is_ok());
    }

    /// Inject a script child that forks a long-running grandchild, emits a result
    /// event, then blocks in `wait` — simulating claude waiting for cargo nextest.
    #[cfg(unix)]
    fn inject_script_child(runtime: &ClaudeRuntime) -> (u32, tempfile::NamedTempFile) {
        use std::io::Write as _;
        use std::os::unix::process::CommandExt;
        use std::process::Stdio;

        let mut script = tempfile::NamedTempFile::new().expect("tempfile");
        writeln!(script, "sleep 300 &").unwrap();
        writeln!(script, r#"echo '{{"type":"result","result":"done"}}'"#).unwrap();
        writeln!(script, "wait").unwrap();

        let child = std::process::Command::new("sh")
            .arg(script.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0)
            .spawn()
            .expect("sh must be available");
        let handle = crate::headless::HeadlessHandle::from_child(child)
            .expect("HeadlessHandle from_child failed");
        let pid = handle.pid();
        *runtime.handle.lock().unwrap() = Some(handle);
        *runtime.tracker.lock().unwrap() = Some(Arc::new(NoopTracker));
        *runtime.event_sink.lock().unwrap() = Some(Arc::new(NoopEventSink));
        (pid, script)
    }

    /// After poll returns, the drain thread must kill the whole process group.
    /// A leaked grandchild (sleep 300, simulating cargo nextest) must be dead
    /// within 10 s — well within the 5-s SIGTERM grace + SIGKILL cycle.
    #[cfg(unix)]
    #[test]
    fn poll_kills_leaked_grandchildren_after_result() {
        let runtime = make_test_runtime(None, None);
        let (pgid, _script) = inject_script_child(&runtime);

        // poll returns Err::Failed because NoopTracker.get_run returns None;
        // that is expected — we are testing process-group cleanup, not DB.
        let _ = runtime.poll("pgkill-test", None, Duration::from_secs(30));

        // Assert the process group is dead within 10 s.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            if !crate::process_utils::pid_is_alive(pgid) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "process group {pgid} still alive 10 s after poll returned"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
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
        let runtime = make_test_runtime(None, None);
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
        let runtime = make_test_runtime(None, None);
        let _pid = inject_sleep_child(&runtime, 60);
        let flag = Arc::new(AtomicBool::new(true));
        let result = runtime.poll("shutdown-run", Some(&flag), Duration::from_secs(300));
        assert!(
            matches!(result, Err(PollError::Cancelled)),
            "expected Cancelled when shutdown flag is set, got: {result:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn poll_returns_failed_on_stall() {
        let runtime = make_test_runtime(Some(Duration::from_millis(200)), None);
        let _pid = inject_sleep_child(&runtime, 60);
        let result = runtime.poll("stall-run", None, Duration::from_secs(30));
        assert!(
            matches!(result, Err(PollError::Failed(ref msg)) if msg.contains("stall_timeout")),
            "expected Failed(stall_timeout), got: {result:?}"
        );
    }

    /// Inject a child that emits 4 assistant JSONL events then exits (no result event).
    #[cfg(unix)]
    fn inject_turn_cap_child(runtime: &ClaudeRuntime) -> (u32, tempfile::NamedTempFile) {
        use std::io::Write as _;
        use std::os::unix::process::CommandExt;
        use std::process::Stdio;

        let assistant_line = r#"{"type":"assistant","usage":{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}"#;
        let mut script = tempfile::NamedTempFile::new().expect("tempfile");
        for _ in 0..4 {
            writeln!(script, "echo '{assistant_line}'").unwrap();
        }

        let child = std::process::Command::new("sh")
            .arg(script.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0)
            .spawn()
            .expect("sh must be available");
        let handle = crate::headless::HeadlessHandle::from_child(child)
            .expect("HeadlessHandle from_child failed");
        let pid = handle.pid();
        *runtime.handle.lock().unwrap() = Some(handle);
        *runtime.tracker.lock().unwrap() = Some(Arc::new(NoopTracker));
        *runtime.event_sink.lock().unwrap() = Some(Arc::new(NoopEventSink));
        (pid, script)
    }

    #[cfg(unix)]
    #[test]
    fn poll_returns_failed_on_turn_cap() {
        let runtime = make_test_runtime(None, Some(3));
        let (_pid, _script) = inject_turn_cap_child(&runtime);
        let result = runtime.poll("turn-cap-run", None, Duration::from_secs(30));
        assert!(
            matches!(result, Err(PollError::Failed(ref msg)) if msg.contains("turn_cap_reached")),
            "expected Failed(turn_cap_reached), got: {result:?}"
        );
    }
}
