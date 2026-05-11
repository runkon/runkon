//! ClaudeRuntime — wraps the existing headless subprocess spawn/poll logic.

use std::path::PathBuf;
use std::sync::{atomic::AtomicBool, Arc, Mutex};
use std::time::Duration;

use runkon_runtimes::error::{Result, RuntimeError};
use runkon_runtimes::headless::{DrainOutcome, LineEventParser, ParseSignal};
use runkon_runtimes::permission::PermissionMode;
use runkon_runtimes::process_utils;
use runkon_runtimes::run::RunHandle;
use runkon_runtimes::runtime::{AgentRuntime, PollError, RuntimeRequest};
use runkon_runtimes::tracker::{RunEventSink, RunTracker, RuntimeEvent};

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
    /// counting `n` turn-tick events. `None` disables the turn cap.
    pub max_turns: Option<u32>,
}

/// Runtime that spawns a headless agent subprocess via the injected argv builder.
pub struct ClaudeRuntime {
    options: ClaudeRuntimeOptions,
    #[cfg(unix)]
    handle: Arc<Mutex<Option<runkon_runtimes::headless::HeadlessHandle>>>,
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
    fn spawn_impl(
        &self,
        request: &RuntimeRequest,
        _seal: runkon_runtimes::runtime::private::Seal,
    ) -> Result<()> {
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
            let h = runkon_runtimes::headless::spawn_headless(
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
            mark_cancelled_via_tracker(&self.tracker, &run.id, "ClaudeRuntime");
        }
        let _ = run;
        Ok(())
    }
}

/// Classifies Claude CLI JSON stream events into vendor-neutral [`ParseSignal`]s.
pub struct ClaudeLineEventParser;

impl LineEventParser for ClaudeLineEventParser {
    fn classify(&mut self, value: &serde_json::Value) -> ParseSignal {
        match value.get("type").and_then(|v| v.as_str()).unwrap_or("") {
            "system" => {
                if value.get("subtype").and_then(|v| v.as_str()) == Some("init") {
                    ParseSignal::Emit(RuntimeEvent::Init {
                        model: value
                            .get("model")
                            .and_then(|v| v.as_str())
                            .map(String::from),
                        session_id: value
                            .get("session_id")
                            .and_then(|v| v.as_str())
                            .map(String::from),
                    })
                } else {
                    ParseSignal::Ignore
                }
            }
            "assistant" => {
                let usage = value
                    .get("message")
                    .and_then(|m| m.get("usage"))
                    .or_else(|| value.get("usage"));
                if let Some(u) = usage {
                    ParseSignal::TurnWithEvent(RuntimeEvent::Tokens {
                        input: u.get("input_tokens").and_then(|v| v.as_i64()).unwrap_or(0),
                        output: u.get("output_tokens").and_then(|v| v.as_i64()).unwrap_or(0),
                        cache_read: u
                            .get("cache_read_input_tokens")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0),
                        cache_create: u
                            .get("cache_creation_input_tokens")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0),
                    })
                } else {
                    ParseSignal::TurnTick
                }
            }
            "result" => {
                if value
                    .get("is_error")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    ParseSignal::Terminal {
                        final_event: RuntimeEvent::Failed {
                            error: value
                                .get("result")
                                .and_then(|v| v.as_str())
                                .unwrap_or("agent reported an error")
                                .to_string(),
                            session_id: value
                                .get("session_id")
                                .and_then(|v| v.as_str())
                                .map(String::from),
                        },
                    }
                } else {
                    let usage = value.get("usage");
                    ParseSignal::Terminal {
                        final_event: RuntimeEvent::Completed {
                            result_text: value
                                .get("result")
                                .and_then(|v| v.as_str())
                                .map(String::from),
                            session_id: value
                                .get("session_id")
                                .and_then(|v| v.as_str())
                                .map(String::from),
                            cost_usd: value.get("total_cost_usd").and_then(|v| v.as_f64()),
                            num_turns: value.get("num_turns").and_then(|v| v.as_i64()),
                            duration_ms: value.get("duration_ms").and_then(|v| v.as_i64()),
                            input_tokens: usage
                                .and_then(|u| u.get("input_tokens"))
                                .and_then(|v| v.as_i64()),
                            output_tokens: usage
                                .and_then(|u| u.get("output_tokens"))
                                .and_then(|v| v.as_i64()),
                            cache_read_input_tokens: usage
                                .and_then(|u| u.get("cache_read_input_tokens"))
                                .and_then(|v| v.as_i64()),
                            cache_creation_input_tokens: usage
                                .and_then(|u| u.get("cache_creation_input_tokens"))
                                .and_then(|v| v.as_i64()),
                        },
                    }
                }
            }
            _ => ParseSignal::Ignore,
        }
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
        let outcome = runkon_runtimes::headless::drain_stream_json(
            stdout_pipe,
            &run_id_owned,
            &log_path,
            &*event_sink_for_drain,
            stall_threshold,
            max_turns,
            ClaudeLineEventParser,
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
        mark_cancelled_with_reason(tracker.as_ref(), run_id, "ClaudeRuntime", reason);
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

fn mark_cancelled_via_tracker(
    tracker_mtx: &Mutex<Option<Arc<dyn RunTracker>>>,
    run_id: &str,
    context: &str,
) {
    if let Some(ref tracker) = tracker_mtx.lock().unwrap_or_else(|e| e.into_inner()).take() {
        if let Err(e) = tracker.mark_cancelled(run_id) {
            tracing::warn!("{context}: failed to mark run {run_id} cancelled: {e}");
        }
    }
}

fn mark_cancelled_with_reason(tracker: &dyn RunTracker, run_id: &str, context: &str, reason: &str) {
    if let Err(e) = tracker.mark_cancelled(run_id) {
        tracing::warn!("{context}: failed to mark run {run_id} cancelled on {reason}: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runkon_runtimes::agent_def::{AgentDef, AgentRole};
    use runkon_runtimes::headless::drain_stream_json;
    use runkon_runtimes::run::{RunHandle, RunStatus};
    use runkon_runtimes::tracker::{NoopEventSink, NoopTracker};

    fn make_test_run(runtime: &str, subprocess_pid: Option<i64>) -> RunHandle {
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

    // ── ClaudeLineEventParser event-shape tests (ported from headless.rs) ──

    #[derive(Default, Clone)]
    struct RecordingSink {
        events: std::sync::Arc<std::sync::Mutex<Vec<RuntimeEvent>>>,
    }

    impl runkon_runtimes::tracker::EventSink for RecordingSink {
        fn on_event(&self, _run_id: &str, event: RuntimeEvent) {
            self.events.lock().unwrap().push(event);
        }
    }

    fn run_drain(lines: &[&str]) -> (runkon_runtimes::headless::DrainOutcome, RecordingSink) {
        let input = lines.join("\n");
        let log_file =
            std::env::temp_dir().join(format!("test-drain-{:?}.log", std::thread::current().id()));
        let sink = RecordingSink::default();
        let outcome = drain_stream_json(
            std::io::Cursor::new(input.into_bytes()),
            "run-1",
            &log_file,
            &sink,
            None,
            None,
            ClaudeLineEventParser,
        );
        let _ = std::fs::remove_file(&log_file);
        (outcome, sink)
    }

    #[test]
    fn result_event_returns_completed() {
        let (outcome, sink) = run_drain(&[r#"{"type":"result","result":"hello"}"#]);
        assert_eq!(outcome, runkon_runtimes::headless::DrainOutcome::Completed);
        let events = sink.events.lock().unwrap();
        assert!(matches!(events[0], RuntimeEvent::Completed { .. }));
    }

    #[test]
    fn error_result_returns_completed() {
        let (outcome, sink) = run_drain(&[r#"{"type":"result","is_error":true,"result":"oops"}"#]);
        assert_eq!(outcome, runkon_runtimes::headless::DrainOutcome::Completed);
        let events = sink.events.lock().unwrap();
        assert!(matches!(events[0], RuntimeEvent::Failed { .. }));
    }

    #[test]
    fn no_result_returns_no_result() {
        let (outcome, sink) = run_drain(&[r#"{"type":"system","subtype":"init"}"#]);
        assert_eq!(outcome, runkon_runtimes::headless::DrainOutcome::NoResult);
        let events = sink.events.lock().unwrap();
        // system/init lines emit an Init event even though there's no final result
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], RuntimeEvent::Init { .. }));
    }

    /// Reader that yields `prefix`, then returns an `io::Error` on the next read.
    struct ErrorAfterReader {
        prefix: std::io::Cursor<Vec<u8>>,
        errored: bool,
    }

    impl std::io::Read for ErrorAfterReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = self.prefix.read(buf)?;
            if n == 0 && !self.errored {
                self.errored = true;
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "test broken pipe",
                ));
            }
            Ok(n)
        }
    }

    #[test]
    fn returns_no_result_on_stdout_read_error() {
        let prefix = b"{\"type\":\"system\",\"subtype\":\"init\"}\n".to_vec();
        let reader = ErrorAfterReader {
            prefix: std::io::Cursor::new(prefix),
            errored: false,
        };
        let log_file = std::env::temp_dir().join(format!(
            "test-drain-read-err-{:?}.log",
            std::thread::current().id()
        ));
        let sink = RecordingSink::default();
        let outcome = drain_stream_json(
            reader,
            "run-err",
            &log_file,
            &sink,
            None,
            None,
            ClaudeLineEventParser,
        );
        let _ = std::fs::remove_file(&log_file);
        assert_eq!(outcome, runkon_runtimes::headless::DrainOutcome::NoResult);
        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], RuntimeEvent::Init { .. }));
    }

    #[test]
    fn token_update_emitted() {
        let (outcome, sink) = run_drain(&[
            r#"{"type":"assistant","usage":{"input_tokens":10,"output_tokens":20,"cache_read_input_tokens":5,"cache_creation_input_tokens":3}}"#,
            r#"{"type":"result","result":"done"}"#,
        ]);
        assert_eq!(outcome, runkon_runtimes::headless::DrainOutcome::Completed);
        let events = sink.events.lock().unwrap();
        assert!(matches!(
            events[0],
            RuntimeEvent::Tokens {
                input: 10,
                output: 20,
                cache_read: 5,
                cache_create: 3,
            }
        ));
    }

    #[test]
    fn cost_turns_duration_parsed() {
        let (outcome, sink) = run_drain(&[
            r#"{"type":"result","result":"ok","total_cost_usd":0.42,"num_turns":7,"duration_ms":12345,"usage":{"input_tokens":100,"output_tokens":50}}"#,
        ]);
        assert_eq!(outcome, runkon_runtimes::headless::DrainOutcome::Completed);
        let events = sink.events.lock().unwrap();
        match &events[0] {
            RuntimeEvent::Completed {
                cost_usd,
                num_turns,
                duration_ms,
                input_tokens,
                output_tokens,
                ..
            } => {
                assert_eq!(*cost_usd, Some(0.42));
                assert_eq!(*num_turns, Some(7));
                assert_eq!(*duration_ms, Some(12345));
                assert_eq!(*input_tokens, Some(100));
                assert_eq!(*output_tokens, Some(50));
            }
            other => panic!("expected Completed event, got: {other:?}"),
        }
    }

    // ── ClaudeRuntime unit tests ──

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
        let handle = runkon_runtimes::headless::HeadlessHandle::from_child(child)
            .expect("HeadlessHandle from_child failed");
        let pid = handle.pid();
        *runtime.handle.lock().unwrap() = Some(handle);
        *runtime.tracker.lock().unwrap() = Some(Arc::new(NoopTracker));
        *runtime.event_sink.lock().unwrap() = Some(Arc::new(NoopEventSink));
        (pid, script)
    }

    /// After poll returns, the drain thread must kill the whole process group.
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
            if !runkon_runtimes::process_utils::pid_is_alive(pgid) {
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
        let handle = runkon_runtimes::headless::HeadlessHandle::from_child(child)
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
        let handle = runkon_runtimes::headless::HeadlessHandle::from_child(child)
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
