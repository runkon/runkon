//! GeminiRuntime — bespoke headless subprocess runtime for Gemini CLI.
//!
//! Targets `--output-format stream-json`: a JSONL stream with six event types.
//! Use this when you need per-turn telemetry, stall detection, or turn-cap
//! enforcement. For a zero-Rust-code setup, see the `cli`-type recipe in
//! docs/recipes/gemini-cli.md instead.

use std::borrow::Cow;
use std::collections::HashMap;
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

/// Construction-time options for [`GeminiRuntime`].
#[derive(Clone)]
pub struct GeminiRuntimeOptions {
    /// Path to the `gemini` binary (or a stub for tests).
    pub binary_path: PathBuf,
    /// Environment variable overlays (e.g. `GEMINI_API_KEY`).
    pub env: HashMap<String, String>,
    /// Mapped to `--approval-mode <value>` via `PermissionMode::Other`.
    /// `PermissionMode::Default` → no flag appended.
    pub permission_mode: PermissionMode,
    /// Returns the JSONL log path for a given run id.
    pub log_path_for_run: Arc<dyn Fn(&str) -> PathBuf + Send + Sync>,
    /// If `Some(t)`, `drain_stream_json` returns `StalledOut` after no output
    /// for longer than `t`. `None` disables stall detection.
    pub stall_threshold: Option<Duration>,
    /// If `Some(n)`, `drain_stream_json` returns `TurnCapReached(n)` after
    /// counting `n` `tool_use` events. `None` disables the cap.
    ///
    /// Note: Gemini emits internal strategic tools (`update_topic`,
    /// `invoke_agent`, etc.) that count toward this limit. Set a higher
    /// bound than the number of expected external tool calls per session.
    pub max_turns: Option<u32>,
}

/// Runtime that spawns a headless `gemini` subprocess with `--output-format stream-json`.
pub struct GeminiRuntime {
    options: GeminiRuntimeOptions,
    #[cfg(unix)]
    handle: Arc<Mutex<Option<runkon_runtimes::headless::HeadlessHandle>>>,
    tracker: Arc<Mutex<Option<Arc<dyn RunTracker>>>>,
    event_sink: Arc<Mutex<Option<Arc<dyn RunEventSink>>>>,
}

impl GeminiRuntime {
    pub fn new(options: GeminiRuntimeOptions) -> Self {
        Self {
            options,
            #[cfg(unix)]
            handle: Arc::new(Mutex::new(None)),
            tracker: Arc::new(Mutex::new(None)),
            event_sink: Arc::new(Mutex::new(None)),
        }
    }
}

impl AgentRuntime for GeminiRuntime {
    fn spawn_impl(
        &self,
        request: &RuntimeRequest,
        _seal: runkon_runtimes::runtime::private::Seal,
    ) -> Result<()> {
        #[cfg(unix)]
        {
            let wd = request.working_dir.to_str().unwrap_or(".");

            let mut args: Vec<Cow<'static, str>> = vec![
                Cow::Borrowed("--output-format"),
                Cow::Borrowed("stream-json"),
                Cow::Borrowed("--prompt"),
                Cow::Owned(request.prompt.clone()),
            ];

            if let Some(model) = request.resolved_model() {
                args.push(Cow::Borrowed("--model"));
                args.push(Cow::Owned(model.to_string()));
            }

            if let Some(mode_value) = self.options.permission_mode.cli_flag_value() {
                args.push(Cow::Borrowed("--approval-mode"));
                args.push(Cow::Owned(mode_value.to_string()));
            }

            for (k, v) in &request.extra_cli_args {
                args.push(Cow::Owned(format!("--{k}")));
                args.push(Cow::Owned(v.to_string()));
            }

            if let Some(session_id) = &request.resume_session_id {
                args.push(Cow::Borrowed("--resume"));
                args.push(Cow::Owned(session_id.clone()));
            }

            let h = runkon_runtimes::headless::spawn_headless(
                &args,
                std::path::Path::new(wd),
                &self.options.binary_path.to_string_lossy(),
                &self.options.env,
            )
            .map_err(|e| {
                RuntimeError::Workflow(format!(
                    "GeminiRuntime: spawn failed for run {} (working_dir={wd}): {e}",
                    &request.run_id
                ))
            })?;

            *self.handle.lock().unwrap_or_else(|e| e.into_inner()) = Some(h);
            *self.tracker.lock().unwrap_or_else(|e| e.into_inner()) =
                Some(request.tracker.clone());
            *self.event_sink.lock().unwrap_or_else(|e| e.into_inner()) =
                Some(request.event_sink.clone());
            Ok(())
        }
        #[cfg(not(unix))]
        {
            let _ = request;
            Err(RuntimeError::Workflow(
                "GeminiRuntime headless spawn is not supported on non-Unix platforms".into(),
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
                "GeminiRuntime poll is not supported on non-Unix platforms".into(),
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
            mark_cancelled_via_tracker(&self.tracker, &run.id, "GeminiRuntime");
        }
        let _ = run;
        Ok(())
    }
}

/// Classifies Gemini CLI stream-json events into vendor-neutral [`ParseSignal`]s.
///
/// Event types (from `--output-format stream-json`):
/// - `init`        → `Emit(Init)`
/// - `message`     → `Ignore` (trace-only; no token usage on these events)
/// - `tool_use`    → `TurnTick` (one invocation = one turn for cap purposes)
/// - `tool_result` → `Ignore` (debug-only)
/// - `error`       → `Ignore` (non-fatal; warn-logged)
/// - `result`      → `Terminal { Completed | Failed }`
pub struct GeminiLineEventParser;

impl LineEventParser for GeminiLineEventParser {
    fn classify(&mut self, value: &serde_json::Value) -> ParseSignal {
        match value["type"].as_str().unwrap_or("") {
            "init" => ParseSignal::Emit(RuntimeEvent::Init {
                model: value["model"].as_str().map(String::from),
                session_id: value["session_id"].as_str().map(String::from),
            }),
            "message" => {
                tracing::trace!(
                    target: "runkon::agent::gemini",
                    role = value["role"].as_str().unwrap_or(""),
                    "gemini message event"
                );
                ParseSignal::Ignore
            }
            "tool_use" => {
                tracing::debug!(
                    target: "runkon::agent::gemini",
                    tool_name = value["tool_name"].as_str().unwrap_or(""),
                    "gemini tool_use event"
                );
                ParseSignal::TurnTick
            }
            "tool_result" => {
                tracing::debug!(
                    target: "runkon::agent::gemini",
                    status = value["status"].as_str().unwrap_or(""),
                    "gemini tool_result event"
                );
                ParseSignal::Ignore
            }
            "error" => {
                tracing::warn!(
                    target: "runkon::agent::gemini",
                    severity = value["severity"].as_str().unwrap_or(""),
                    message = value["message"].as_str().unwrap_or(""),
                    "gemini error event (non-fatal)"
                );
                ParseSignal::Ignore
            }
            "result" => parse_result_event(value),
            _ => ParseSignal::Ignore,
        }
    }
}

fn parse_result_event(value: &serde_json::Value) -> ParseSignal {
    let session_id = value["session_id"].as_str().map(String::from);
    let status = value["status"].as_str().unwrap_or("error");
    let stats = &value["stats"];

    if status == "success" {
        let input_tokens = stats["input_tokens"].as_i64();
        let output_tokens = stats["output_tokens"].as_i64();
        let cache_read = stats["cached"].as_i64();
        let duration_ms = stats["duration_ms"].as_i64();
        let num_turns = stats["tool_calls"].as_i64();

        ParseSignal::Terminal {
            final_event: RuntimeEvent::Completed {
                result_text: None,
                session_id,
                cost_usd: None,
                num_turns,
                duration_ms,
                input_tokens,
                output_tokens,
                cache_read_input_tokens: cache_read,
                cache_creation_input_tokens: None,
            },
        }
    } else {
        let error_msg = value["error"]
            .as_object()
            .and_then(|o| o.get("message"))
            .and_then(|v| v.as_str())
            .unwrap_or("gemini reported an error")
            .to_string();

        ParseSignal::Terminal {
            final_event: RuntimeEvent::Failed {
                error: error_msg,
                session_id,
            },
        }
    }
}

#[cfg(unix)]
fn poll_unix(
    rt: &GeminiRuntime,
    run_id: &str,
    shutdown: Option<&Arc<AtomicBool>>,
    step_timeout: Duration,
) -> std::result::Result<RunHandle, PollError> {
    let handle = rt
        .handle
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take()
        .ok_or_else(|| PollError::Failed("GeminiRuntime::poll called before spawn".into()))?;

    let tracker = rt
        .tracker
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take()
        .ok_or_else(|| {
            PollError::Failed(
                "GeminiRuntime::poll called before spawn (tracker missing)".into(),
            )
        })?;

    let event_sink = rt
        .event_sink
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take()
        .ok_or_else(|| {
            PollError::Failed(
                "GeminiRuntime::poll called before spawn (event_sink missing)".into(),
            )
        })?;

    let pid = handle.pid();
    let log_path = (rt.options.log_path_for_run)(run_id);

    if let Err(e) = tracker.record_pid(run_id, pid) {
        tracing::warn!("GeminiRuntime: failed to persist subprocess pid {pid}: {e}");
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
                        "GeminiRuntime: stderr read failed for run {stderr_run_id}, ending drain: {e}"
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
            GeminiLineEventParser,
        );
        let _ = tx.send(outcome);
        process_utils::cancel_subprocess(pid);
        finish();
    });

    let abort_poll = |reason: &str| {
        tracing::warn!("GeminiRuntime: {reason} for run {run_id}, cancelling");
        mark_cancelled_with_reason(tracker.as_ref(), run_id, "GeminiRuntime", reason);
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
                tracing::warn!("GeminiRuntime: drain thread disconnected for run {run_id}");
                break DrainOutcome::NoResult;
            }
        }
    };

    match drain_outcome {
        DrainOutcome::Completed => tracker
            .get_run(run_id)
            .map_err(|e| PollError::Failed(format!("DB error after drain: {e}")))?
            .ok_or_else(|| {
                PollError::Failed(format!("run {run_id} not found in DB after drain"))
            }),
        DrainOutcome::NoResult => {
            if let Err(e) = tracker.mark_failed_if_running(run_id, "agent exited without result") {
                tracing::warn!(
                    "GeminiRuntime: failed to mark run {run_id} failed after no-result: {e}"
                );
            }
            Err(PollError::NoResult)
        }
        DrainOutcome::StalledOut(elapsed) => {
            let msg = format!("stall_timeout: no events for {}s", elapsed.as_secs());
            tracing::warn!("GeminiRuntime: {msg} for run {run_id}");
            if let Err(e) = tracker.mark_failed_if_running(run_id, &msg) {
                tracing::warn!(
                    "GeminiRuntime: failed to persist stall failure for {run_id}: {e}"
                );
            }
            Err(PollError::Failed(msg))
        }
        DrainOutcome::TurnCapReached(count) => {
            let msg = format!("turn_cap_reached: {} turns", count);
            tracing::warn!("GeminiRuntime: {msg} for run {run_id}");
            if let Err(e) = tracker.mark_failed_if_running(run_id, &msg) {
                tracing::warn!(
                    "GeminiRuntime: failed to persist turn-cap failure for {run_id}: {e}"
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

fn mark_cancelled_with_reason(
    tracker: &dyn RunTracker,
    run_id: &str,
    context: &str,
    reason: &str,
) {
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
    use runkon_runtimes::tracker::{EventSink, NoopEventSink, NoopTracker};

    // ── event sink helper ─────────────────────────────────────────────────────

    #[derive(Default, Clone)]
    struct RecordingSink {
        events: std::sync::Arc<Mutex<Vec<RuntimeEvent>>>,
    }

    impl EventSink for RecordingSink {
        fn on_event(&self, _run_id: &str, event: RuntimeEvent) {
            self.events.lock().unwrap().push(event);
        }
    }

    fn run_drain(lines: &[&str]) -> (runkon_runtimes::headless::DrainOutcome, RecordingSink) {
        let input = lines.join("\n");
        let log_file = std::env::temp_dir().join(format!(
            "test-gemini-drain-{:?}.log",
            std::thread::current().id()
        ));
        let sink = RecordingSink::default();
        let outcome = drain_stream_json(
            std::io::Cursor::new(input.into_bytes()),
            "run-1",
            &log_file,
            &sink,
            None,
            None,
            GeminiLineEventParser,
        );
        let _ = std::fs::remove_file(&log_file);
        (outcome, sink)
    }

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

    fn make_runtime(
        stall_threshold: Option<Duration>,
        max_turns: Option<u32>,
    ) -> GeminiRuntime {
        GeminiRuntime::new(GeminiRuntimeOptions {
            binary_path: PathBuf::from("/nonexistent/gemini"),
            env: HashMap::new(),
            permission_mode: PermissionMode::Default,
            log_path_for_run: Arc::new(|run_id| {
                std::env::temp_dir().join(format!("{run_id}.log"))
            }),
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
                runtime: "gemini".to_string(),
                prompt: String::new(),
            },
            prompt: "p".to_string(),
            working_dir: PathBuf::from("/tmp"),
            model: None,
            extra_cli_args: vec![],
            plugin_dirs: vec![],
            resume_session_id: None,
            tracker: Arc::new(NoopTracker),
            event_sink: Arc::new(NoopEventSink),
        }
    }

    // ── GeminiLineEventParser unit tests ─────────────────────────────────────

    #[test]
    fn parse_init_event() {
        let (outcome, sink) = run_drain(&[
            r#"{"type":"init","session_id":"abc123","model":"gemini-2.5-flash","timestamp":"2024-01-01T00:00:00Z"}"#,
        ]);
        assert_eq!(outcome, runkon_runtimes::headless::DrainOutcome::NoResult);
        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            RuntimeEvent::Init { model, session_id } => {
                assert_eq!(model.as_deref(), Some("gemini-2.5-flash"));
                assert_eq!(session_id.as_deref(), Some("abc123"));
            }
            other => panic!("expected Init, got: {other:?}"),
        }
    }

    #[test]
    fn parse_message_user_ignored() {
        let (_, sink) = run_drain(&[
            r#"{"type":"message","role":"user","content":"hello","timestamp":"2024-01-01T00:00:00Z"}"#,
        ]);
        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 0, "user message must be ignored");
    }

    #[test]
    fn parse_message_assistant_ignored() {
        let (_, sink) = run_drain(&[
            r#"{"type":"message","role":"assistant","content":"hi there","delta":true,"timestamp":"2024-01-01T00:00:00Z"}"#,
        ]);
        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 0, "assistant message must be ignored regardless of delta flag");
    }

    #[test]
    fn parse_tool_use_returns_turn_tick() {
        // tool_use increments the drain's turn counter but emits no event.
        let (_, sink) = run_drain(&[
            r#"{"type":"tool_use","tool_name":"read_file","tool_id":"read_file_1234_0","parameters":{},"timestamp":"2024-01-01T00:00:00Z"}"#,
        ]);
        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 0, "tool_use must emit no RuntimeEvent");
    }

    #[test]
    fn parse_tool_result_ignored() {
        let (_, sink) = run_drain(&[
            r#"{"type":"tool_result","tool_id":"read_file_1234_0","status":"success","output":"file contents","timestamp":"2024-01-01T00:00:00Z"}"#,
        ]);
        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 0, "tool_result must be ignored");
    }

    #[test]
    fn parse_error_event_ignored() {
        let (_, sink) = run_drain(&[
            r#"{"type":"error","severity":"warning","message":"Ripgrep is not available","timestamp":"2024-01-01T00:00:00Z"}"#,
        ]);
        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 0, "error event must be ignored (non-fatal)");
    }

    #[test]
    fn parse_result_success_token_mapping() {
        let (outcome, sink) = run_drain(&[
            r#"{"type":"result","status":"success","session_id":"sess1","stats":{"input_tokens":100,"output_tokens":50,"cached":20,"input":80,"total_tokens":150,"duration_ms":1234,"tool_calls":2},"timestamp":"2024-01-01T00:00:00Z"}"#,
        ]);
        assert_eq!(outcome, runkon_runtimes::headless::DrainOutcome::Completed);
        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            RuntimeEvent::Completed {
                session_id,
                input_tokens,
                output_tokens,
                cache_read_input_tokens,
                cache_creation_input_tokens,
                num_turns,
                duration_ms,
                cost_usd,
                ..
            } => {
                assert_eq!(session_id.as_deref(), Some("sess1"));
                assert_eq!(*input_tokens, Some(100));
                assert_eq!(*output_tokens, Some(50));
                assert_eq!(*cache_read_input_tokens, Some(20));
                assert_eq!(*cache_creation_input_tokens, None, "Gemini doesn't report cache creation");
                assert_eq!(*num_turns, Some(2));
                assert_eq!(*duration_ms, Some(1234));
                assert_eq!(*cost_usd, None, "Gemini reports tokens only, no cost");
            }
            other => panic!("expected Completed, got: {other:?}"),
        }
    }

    #[test]
    fn parse_result_error() {
        let (outcome, sink) = run_drain(&[
            r#"{"type":"result","status":"error","session_id":"sess2","error":{"type":"FatalCancellationError","message":"Operation cancelled.","code":130},"timestamp":"2024-01-01T00:00:00Z"}"#,
        ]);
        assert_eq!(outcome, runkon_runtimes::headless::DrainOutcome::Completed);
        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            RuntimeEvent::Failed { error, session_id } => {
                assert_eq!(session_id.as_deref(), Some("sess2"));
                assert!(
                    error.contains("Operation cancelled"),
                    "error must contain Gemini's message, got: {error}"
                );
            }
            other => panic!("expected Failed, got: {other:?}"),
        }
    }

    #[test]
    fn drain_counts_turns_per_tool_use() {
        // Two tool_use events followed by result with tool_calls=2.
        let (outcome, sink) = run_drain(&[
            r#"{"type":"init","session_id":"s","model":"gemini-2.5-flash","timestamp":"2024-01-01T00:00:00Z"}"#,
            r#"{"type":"tool_use","tool_name":"read_file","tool_id":"read_file_1_0","parameters":{},"timestamp":"2024-01-01T00:00:00Z"}"#,
            r#"{"type":"tool_result","tool_id":"read_file_1_0","status":"success","output":"data","timestamp":"2024-01-01T00:00:00Z"}"#,
            r#"{"type":"tool_use","tool_name":"write_file","tool_id":"write_file_2_0","parameters":{},"timestamp":"2024-01-01T00:00:00Z"}"#,
            r#"{"type":"tool_result","tool_id":"write_file_2_0","status":"success","timestamp":"2024-01-01T00:00:00Z"}"#,
            r#"{"type":"message","role":"assistant","content":"done","delta":true,"timestamp":"2024-01-01T00:00:00Z"}"#,
            r#"{"type":"result","status":"success","session_id":"s","stats":{"input_tokens":200,"output_tokens":80,"cached":0,"input":200,"total_tokens":280,"duration_ms":5000,"tool_calls":2},"timestamp":"2024-01-01T00:00:00Z"}"#,
        ]);
        assert_eq!(outcome, runkon_runtimes::headless::DrainOutcome::Completed);
        let events = sink.events.lock().unwrap();
        let completed = events.iter().find_map(|e| {
            if let RuntimeEvent::Completed { num_turns, .. } = e {
                Some(*num_turns)
            } else {
                None
            }
        });
        assert_eq!(
            completed,
            Some(Some(2)),
            "num_turns must reflect tool_calls from result.stats"
        );
    }

    // ── GeminiRuntime spawn wiring tests ─────────────────────────────────────

    #[test]
    fn spawn_rejects_path_traversal_run_id() {
        let runtime = make_runtime(None, None);
        let request = make_request("../../etc/cron.d/payload");
        let err = runtime
            .spawn_validated(&request)
            .expect_err("expected Err for path-traversal run_id");
        assert!(matches!(
            err,
            runkon_runtimes::error::RuntimeError::InvalidInput(_)
        ));
    }

    #[test]
    fn is_alive_returns_false_when_no_pid() {
        let runtime = make_runtime(None, None);
        let run = make_test_run("gemini", None);
        assert!(!runtime.is_alive(&run));
    }

    #[cfg(unix)]
    #[test]
    fn is_alive_returns_true_for_self() {
        let runtime = make_runtime(None, None);
        let run = make_test_run("gemini", Some(std::process::id() as i64));
        assert!(runtime.is_alive(&run));
    }

    #[test]
    fn cancel_with_no_handle_and_no_pid() {
        let runtime = make_runtime(None, None);
        let run = make_test_run("gemini", None);
        assert!(runtime.cancel(&run).is_ok());
    }

    /// Helper: inject a Unix child that outputs Gemini stream-json events.
    #[cfg(unix)]
    fn inject_script_child(runtime: &GeminiRuntime) -> (u32, tempfile::NamedTempFile) {
        use std::io::Write as _;
        use std::os::unix::process::CommandExt;
        use std::process::Stdio;

        let mut script = tempfile::NamedTempFile::new().expect("tempfile");
        writeln!(script, r#"echo '{{"type":"init","session_id":"s","model":"gemini-2.5-flash","timestamp":"2024-01-01T00:00:00Z"}}'"#).unwrap();
        writeln!(script, r#"echo '{{"type":"result","status":"success","session_id":"s","stats":{{"input_tokens":10,"output_tokens":5,"cached":0,"input":10,"total_tokens":15,"duration_ms":100,"tool_calls":0}},"timestamp":"2024-01-01T00:00:00Z"}}'"#).unwrap();

        let child = std::process::Command::new("sh")
            .arg(script.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0)
            .spawn()
            .expect("sh must be available");
        let handle =
            runkon_runtimes::headless::HeadlessHandle::from_child(child).expect("HeadlessHandle");
        let pid = handle.pid();
        *runtime.handle.lock().unwrap() = Some(handle);
        *runtime.tracker.lock().unwrap() = Some(Arc::new(NoopTracker));
        *runtime.event_sink.lock().unwrap() = Some(Arc::new(NoopEventSink));
        (pid, script)
    }

    #[cfg(unix)]
    #[test]
    fn poll_before_spawn_returns_failed() {
        let runtime = make_runtime(None, None);
        let result = runtime.poll("no-such-run", None, Duration::from_millis(10));
        assert!(matches!(result, Err(PollError::Failed(_))));
    }

    #[cfg(unix)]
    #[test]
    fn poll_returns_no_result_on_timeout() {
        use std::os::unix::process::CommandExt;
        use std::process::Stdio;
        let runtime = make_runtime(None, None);
        let child = std::process::Command::new("sleep")
            .arg("60")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0)
            .spawn()
            .expect("sleep must be available");
        let handle =
            runkon_runtimes::headless::HeadlessHandle::from_child(child).expect("HeadlessHandle");
        *runtime.handle.lock().unwrap() = Some(handle);
        *runtime.tracker.lock().unwrap() = Some(Arc::new(NoopTracker));
        *runtime.event_sink.lock().unwrap() = Some(Arc::new(NoopEventSink));

        let result = runtime.poll("timeout-run", None, Duration::from_millis(200));
        assert!(
            matches!(result, Err(PollError::NoResult)),
            "expected NoResult on timeout, got: {result:?}"
        );
    }

    /// Verify --resume <id> appears in spawned argv.
    #[cfg(unix)]
    #[test]
    fn resume_flag_wired_in_spawn() {
        use std::io::Write as _;
        use std::os::unix::fs::PermissionsExt as _;

        let tmp = tempfile::tempdir().unwrap();
        let args_file = tmp.path().join("args.txt");
        let args_file_str = args_file.to_str().unwrap();

        let script_path = tmp.path().join("stub-resume.sh");
        {
            let mut f = std::fs::File::create(&script_path).unwrap();
            writeln!(f, "#!/bin/sh").unwrap();
            writeln!(f, "printf '%s\\n' \"$@\" > {args_file_str}").unwrap();
            writeln!(f, r#"echo '{{"type":"result","status":"success","session_id":"s","stats":{{"input_tokens":1,"output_tokens":1,"cached":0,"input":1,"total_tokens":2,"duration_ms":10,"tool_calls":0}}}}'"#).unwrap();
        }
        std::fs::set_permissions(&script_path, std::fs::Permissions::from_mode(0o755)).unwrap();

        let runtime = GeminiRuntime::new(GeminiRuntimeOptions {
            binary_path: script_path.clone(),
            env: HashMap::new(),
            permission_mode: PermissionMode::Default,
            log_path_for_run: Arc::new(|run_id| {
                std::env::temp_dir().join(format!("{run_id}.log"))
            }),
            stall_threshold: None,
            max_turns: None,
        });

        let mut request = make_request("resume-spawn-test");
        request.resume_session_id = Some("my-session-42".to_string());

        runtime.spawn_validated(&request).unwrap();
        let _ = runtime.poll("resume-spawn-test", None, Duration::from_secs(5));

        std::thread::sleep(Duration::from_millis(100));

        let captured = std::fs::read_to_string(&args_file).unwrap_or_default();
        let args: Vec<&str> = captured.lines().collect();

        assert!(
            args.contains(&"--resume"),
            "--resume must be in spawned argv; got: {args:?}"
        );
        assert!(
            args.contains(&"my-session-42"),
            "'my-session-42' must be in spawned argv; got: {args:?}"
        );

        let r_pos = args.iter().position(|a| *a == "--resume");
        let id_pos = args.iter().position(|a| *a == "my-session-42");
        if let (Some(r), Some(i)) = (r_pos, id_pos) {
            assert_eq!(i, r + 1, "--resume must immediately precede the session id");
        }
    }

    /// Verify --approval-mode <value> is appended for PermissionMode::Other.
    #[cfg(unix)]
    #[test]
    fn approval_mode_other_wired() {
        use std::io::Write as _;
        use std::os::unix::fs::PermissionsExt as _;

        let tmp = tempfile::tempdir().unwrap();
        let args_file = tmp.path().join("args-approval.txt");
        let args_file_str = args_file.to_str().unwrap();

        let script_path = tmp.path().join("stub-approval.sh");
        {
            let mut f = std::fs::File::create(&script_path).unwrap();
            writeln!(f, "#!/bin/sh").unwrap();
            writeln!(f, "printf '%s\\n' \"$@\" > {args_file_str}").unwrap();
            writeln!(f, r#"echo '{{"type":"result","status":"success","session_id":"s","stats":{{"input_tokens":1,"output_tokens":1,"cached":0,"input":1,"total_tokens":2,"duration_ms":10,"tool_calls":0}}}}'"#).unwrap();
        }
        std::fs::set_permissions(&script_path, std::fs::Permissions::from_mode(0o755)).unwrap();

        let runtime = GeminiRuntime::new(GeminiRuntimeOptions {
            binary_path: script_path.clone(),
            env: HashMap::new(),
            permission_mode: PermissionMode::Other(Cow::Borrowed("yolo")),
            log_path_for_run: Arc::new(|run_id| {
                std::env::temp_dir().join(format!("{run_id}.log"))
            }),
            stall_threshold: None,
            max_turns: None,
        });

        let request = make_request("approval-other-test");
        runtime.spawn_validated(&request).unwrap();
        let _ = runtime.poll("approval-other-test", None, Duration::from_secs(5));

        std::thread::sleep(Duration::from_millis(100));

        let captured = std::fs::read_to_string(&args_file).unwrap_or_default();
        let args: Vec<&str> = captured.lines().collect();

        assert!(
            args.contains(&"--approval-mode"),
            "--approval-mode must be in spawned argv; got: {args:?}"
        );
        assert!(
            args.contains(&"yolo"),
            "'yolo' must be in spawned argv; got: {args:?}"
        );
    }

    /// Verify no --approval-mode flag for PermissionMode::Default.
    #[cfg(unix)]
    #[test]
    fn approval_mode_non_other_no_flag() {
        use std::io::Write as _;
        use std::os::unix::fs::PermissionsExt as _;

        let tmp = tempfile::tempdir().unwrap();
        let args_file = tmp.path().join("args-no-approval.txt");
        let args_file_str = args_file.to_str().unwrap();

        let script_path = tmp.path().join("stub-no-approval.sh");
        {
            let mut f = std::fs::File::create(&script_path).unwrap();
            writeln!(f, "#!/bin/sh").unwrap();
            writeln!(f, "printf '%s\\n' \"$@\" > {args_file_str}").unwrap();
            writeln!(f, r#"echo '{{"type":"result","status":"success","session_id":"s","stats":{{"input_tokens":1,"output_tokens":1,"cached":0,"input":1,"total_tokens":2,"duration_ms":10,"tool_calls":0}}}}'"#).unwrap();
        }
        std::fs::set_permissions(&script_path, std::fs::Permissions::from_mode(0o755)).unwrap();

        let runtime = GeminiRuntime::new(GeminiRuntimeOptions {
            binary_path: script_path.clone(),
            env: HashMap::new(),
            permission_mode: PermissionMode::Default,
            log_path_for_run: Arc::new(|run_id| {
                std::env::temp_dir().join(format!("{run_id}.log"))
            }),
            stall_threshold: None,
            max_turns: None,
        });

        let request = make_request("approval-default-test");
        runtime.spawn_validated(&request).unwrap();
        let _ = runtime.poll("approval-default-test", None, Duration::from_secs(5));

        std::thread::sleep(Duration::from_millis(100));

        let captured = std::fs::read_to_string(&args_file).unwrap_or_default();
        let args: Vec<&str> = captured.lines().collect();

        assert!(
            !args.contains(&"--approval-mode"),
            "--approval-mode must NOT be present for PermissionMode::Default; got: {args:?}"
        );
    }
}
