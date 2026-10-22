//! Shared runtime helpers for spawning and polling agent runs.

use std::borrow::Cow;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use crate::run::{RunHandle, RunStatus};
use crate::runtime::PollError;
use crate::tracker::{EventSink, RunEventSink, RunTracker, RuntimeEvent};

/// Handle to a headless agent subprocess.
#[cfg(unix)]
pub struct HeadlessHandle {
    pid: u32,
    stdout: Option<std::process::ChildStdout>,
    stderr: Option<std::process::ChildStderr>,
    child: Option<std::process::Child>,
}

#[cfg(unix)]
impl HeadlessHandle {
    pub fn from_child(mut child: std::process::Child) -> std::result::Result<Self, String> {
        let pid = child.id();
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "HeadlessHandle: child has no stdout pipe".to_string())?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| "HeadlessHandle: child has no stderr pipe".to_string())?;
        Ok(Self {
            pid,
            stdout: Some(stdout),
            stderr: Some(stderr),
            child: Some(child),
        })
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn into_stderr_drain_parts(
        mut self,
    ) -> (
        std::process::ChildStderr,
        std::process::ChildStdout,
        impl FnOnce(),
    ) {
        let stderr = self.stderr.take().expect("stderr already taken");
        let stdout = self.stdout.take().expect("stdout already taken");
        let mut child = self.child.take().expect("child already taken");
        let finish = move || {
            let _ = child.wait();
        };
        (stderr, stdout, finish)
    }

    pub fn into_drain_parts(mut self) -> (std::process::ChildStdout, impl FnOnce()) {
        let stdout = self.stdout.take().expect("stdout already taken");
        let stderr = self.stderr.take().expect("stderr already taken");
        let mut child = self.child.take().expect("child already taken");
        let finish = move || {
            drop(stderr);
            let _ = child.wait();
        };
        (stdout, finish)
    }

    pub fn abort(mut self) {
        self.cleanup();
    }

    fn cleanup(&mut self) {
        drop(self.stdout.take());
        drop(self.stderr.take());
        if let Some(mut child) = self.child.take() {
            // SIGKILL the whole process group so no descendants survive.
            unsafe { libc::kill(-(self.pid as libc::pid_t), libc::SIGKILL) };
            let _ = child.wait();
        }
    }
}

#[cfg(unix)]
impl Drop for HeadlessHandle {
    fn drop(&mut self) {
        self.cleanup();
    }
}

/// Spawn a headless agent subprocess.
///
/// `env` is applied via `Command::envs()` (overlay — parent env is preserved).
/// Keys in `env` override inherited values; all other env vars pass through unchanged.
///
/// The child's stdin is `/dev/null`: the prompt travels in `args`, and the host's stdin
/// may carry data meant for the host (such as a protocol stream) that the child must not read.
#[cfg(unix)]
pub fn spawn_headless(
    args: &[Cow<'static, str>],
    working_dir: &std::path::Path,
    binary_path: &str,
    env: &std::collections::HashMap<String, String>,
) -> std::result::Result<HeadlessHandle, String> {
    use std::process::Stdio;
    let child = Command::new(binary_path)
        .args(args.iter().map(|a| a.as_ref()))
        .current_dir(working_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .envs(env)
        .spawn()
        .map_err(|e| format!("Failed to spawn headless agent: {e}"))?;

    HeadlessHandle::from_child(child)
}

/// Result of draining a headless subprocess stdout stream.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum DrainOutcome {
    /// A `result` event was seen; the run was finalized in the DB.
    Completed,
    /// EOF before any `result` event (SIGTERM path or unexpected crash).
    /// Caller must mark the run as cancelled/failed in the DB.
    NoResult,
    /// No output received for longer than `stall_threshold`.
    /// The subprocess was NOT killed here; the caller is responsible.
    StalledOut(std::time::Duration),
    /// The host-enforced turn cap was reached. `u32` is the number of turns counted.
    /// The subprocess was NOT killed here; the caller is responsible.
    TurnCapReached(u32),
}

/// Classifies a parsed JSON line from a headless agent's stdout stream into a
/// vendor-neutral signal that `drain_stream_json` can act on without knowing
/// the vendor's event schema.
pub trait LineEventParser {
    fn classify(&mut self, value: &serde_json::Value) -> ParseSignal;
}

/// Signal returned by [`LineEventParser::classify`].
pub enum ParseSignal {
    /// Emit an event without counting a turn.
    Emit(RuntimeEvent),
    /// Count a turn tick without emitting an event.
    TurnTick,
    /// Count a turn AND emit an event (e.g. "assistant" with usage).
    TurnWithEvent(RuntimeEvent),
    /// Agent signalled final status; the drain loop returns Completed.
    Terminal { final_event: RuntimeEvent },
    /// Ignore this line entirely.
    Ignore,
}

/// Drain the stdout of a headless subprocess, persisting events to the DB.
///
/// When `stall_threshold` is `Some(t)`, returns `DrainOutcome::StalledOut` if
/// no output is received for longer than `t`. The caller must then kill the
/// subprocess; passing `None` disables stall detection and behaves as before.
///
/// When `max_turns` is `Some(n)`, returns `DrainOutcome::TurnCapReached(n)` after
/// counting `n` turn-tick events. The caller must then kill the subprocess;
/// passing `None` disables turn-cap enforcement.
///
/// `stdout` must be `Send + 'static` because it is moved into an inner reader
/// thread so that the blocking `BufReader::lines()` loop can be interrupted by
/// a `recv_timeout` in the outer loop.
pub fn drain_stream_json<S: EventSink + ?Sized>(
    stdout: impl std::io::Read + Send + 'static,
    run_id: &str,
    log_file: &std::path::Path,
    sink: &S,
    stall_threshold: Option<std::time::Duration>,
    max_turns: Option<u32>,
    mut parser: impl LineEventParser,
) -> DrainOutcome {
    use std::io::{BufRead, BufReader, Write};
    use std::sync::mpsc;
    use std::sync::mpsc::RecvTimeoutError;
    use std::time::Instant;

    let mut log_writer = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_file)
        .map_err(|e| {
            tracing::warn!(
                "[drain_stream_json] failed to open log file {}: {e}",
                log_file.display()
            );
        })
        .ok();

    // Spawn an inner thread that drives the blocking BufReader::lines() iterator.
    // The outer loop uses recv_timeout so it can detect stalls without being
    // stuck in a blocking read() that cannot be interrupted from outside.
    let (line_tx, line_rx) = mpsc::channel::<std::io::Result<String>>();
    std::thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            if line_tx.send(line).is_err() {
                break;
            }
        }
    });

    let mut last_event_at = Instant::now();
    let mut turn_count = 0u32;
    loop {
        let recv_result = match stall_threshold {
            Some(t) => line_rx.recv_timeout(t),
            None => line_rx.recv().map_err(|_| RecvTimeoutError::Disconnected),
        };
        let line = match recv_result {
            Ok(Ok(l)) => {
                last_event_at = Instant::now();
                l
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    "[drain_stream_json] stdout read failed for run {run_id}, ending drain: {e}"
                );
                break;
            }
            Err(RecvTimeoutError::Disconnected) => break,
            Err(RecvTimeoutError::Timeout) => {
                return DrainOutcome::StalledOut(last_event_at.elapsed());
            }
        };

        if let Some(ref mut w) = log_writer {
            if let Err(e) = writeln!(w, "{line}") {
                tracing::warn!("[drain_stream_json] failed to write log line: {e}");
            }
        }

        let value = match serde_json::from_str::<serde_json::Value>(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };

        sink.on_raw_value(run_id, &value);

        match parser.classify(&value) {
            ParseSignal::Ignore => {}
            ParseSignal::Emit(event) => sink.on_event(run_id, event),
            ParseSignal::TurnTick => {
                turn_count += 1;
                if let Some(cap) = max_turns {
                    if turn_count >= cap {
                        return DrainOutcome::TurnCapReached(turn_count);
                    }
                }
            }
            ParseSignal::TurnWithEvent(event) => {
                sink.on_event(run_id, event);
                turn_count += 1;
                if let Some(cap) = max_turns {
                    if turn_count >= cap {
                        return DrainOutcome::TurnCapReached(turn_count);
                    }
                }
            }
            ParseSignal::Terminal { final_event } => {
                sink.on_event(run_id, final_event);
                return DrainOutcome::Completed;
            }
        }
    }

    DrainOutcome::NoResult
}

/// Event sink wrapper that keeps the terminal event (`Completed` or `Failed`)
/// while forwarding every event to the host's sink.
///
/// `EventSink::on_event` returns `()`, so a host whose write of the terminal event
/// fails cannot report it. Keeping the event here lets `poll` fall back to it
/// (see [`resolve_completed_run`]).
pub struct TerminalEventCapture {
    inner: Arc<dyn RunEventSink>,
    terminal: Mutex<Option<RuntimeEvent>>,
}

impl TerminalEventCapture {
    pub fn new(inner: Arc<dyn RunEventSink>) -> Self {
        Self {
            inner,
            terminal: Mutex::new(None),
        }
    }

    /// Take the terminal event captured so far, if any.
    pub fn take_terminal(&self) -> Option<RuntimeEvent> {
        self.terminal
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }
}

impl EventSink for TerminalEventCapture {
    fn on_event(&self, run_id: &str, event: RuntimeEvent) {
        if matches!(
            event,
            RuntimeEvent::Completed { .. } | RuntimeEvent::Failed { .. }
        ) {
            *self.terminal.lock().unwrap_or_else(|e| e.into_inner()) = Some(event.clone());
        }
        self.inner.on_event(run_id, event);
    }

    fn on_raw_value(&self, run_id: &str, value: &serde_json::Value) {
        self.inner.on_raw_value(run_id, value);
    }
}

/// Lay a terminal event over a run handle. A captured field that is `None`
/// keeps the handle's existing value.
fn overlay_terminal(row: &mut RunHandle, event: RuntimeEvent) {
    match event {
        RuntimeEvent::Completed {
            result_text,
            session_id,
            cost_usd,
            num_turns,
            duration_ms,
            input_tokens,
            output_tokens,
            cache_read_input_tokens,
            cache_creation_input_tokens,
        } => {
            row.status = RunStatus::Completed;
            row.result_text = result_text.or(row.result_text.take());
            row.session_id = session_id.or(row.session_id.take());
            row.cost_usd = cost_usd.or(row.cost_usd);
            row.num_turns = num_turns.or(row.num_turns);
            row.duration_ms = duration_ms.or(row.duration_ms);
            row.input_tokens = input_tokens.or(row.input_tokens);
            row.output_tokens = output_tokens.or(row.output_tokens);
            row.cache_read_input_tokens = cache_read_input_tokens.or(row.cache_read_input_tokens);
            row.cache_creation_input_tokens =
                cache_creation_input_tokens.or(row.cache_creation_input_tokens);
        }
        RuntimeEvent::Failed { error, session_id } => {
            row.status = RunStatus::Failed;
            row.result_text = Some(error);
            row.session_id = session_id.or(row.session_id.take());
        }
        _ => {}
    }
}

/// Reconcile the host's row with the terminal event the drain saw, after
/// [`DrainOutcome::Completed`].
///
/// A row the host already finished is returned unchanged. A row still `Running`,
/// missing or unreadable means the host failed to record the terminal event, so
/// the run is built from the event and a warning is logged. With no captured
/// event the row, or an error, is returned as before.
pub fn resolve_completed_run(
    tracker: &dyn RunTracker,
    run_id: &str,
    terminal: Option<RuntimeEvent>,
    pid: u32,
    runtime_name: &str,
    log_path: &Path,
    context: &str,
) -> Result<RunHandle, PollError> {
    let row = tracker.get_run(run_id);
    let Some(event) = terminal else {
        return row
            .map_err(|e| PollError::Failed(format!("DB error after drain: {e}")))?
            .ok_or_else(|| PollError::Failed(format!("run {run_id} not found in DB after drain")));
    };
    let mut handle = match row {
        Ok(Some(row)) if row.status != RunStatus::Running => return Ok(row),
        Ok(Some(row)) => {
            tracing::warn!(
                "{context}: host row for run {run_id} is still running after the agent's result; using the drained result"
            );
            row
        }
        Ok(None) => {
            tracing::warn!(
                "{context}: host has no row for run {run_id} after the agent's result; using the drained result"
            );
            blank_handle(run_id, pid, runtime_name, log_path)
        }
        Err(e) => {
            tracing::warn!(
                "{context}: failed to read run {run_id} after the agent's result ({e}); using the drained result"
            );
            blank_handle(run_id, pid, runtime_name, log_path)
        }
    };
    overlay_terminal(&mut handle, event);
    Ok(handle)
}

fn blank_handle(run_id: &str, pid: u32, runtime_name: &str, log_path: &Path) -> RunHandle {
    RunHandle {
        id: run_id.to_string(),
        status: RunStatus::Running,
        subprocess_pid: Some(pid as i64),
        runtime: runtime_name.to_string(),
        session_id: None,
        result_text: None,
        started_at: String::new(),
        ended_at: None,
        log_file: Some(log_path.display().to_string()),
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

#[cfg(test)]
mod capture_tests {
    use super::*;
    use crate::error::RuntimeError;

    use crate::test_support::FixedRowTracker as RowTracker;

    #[derive(Default)]
    struct CountingSink(Mutex<usize>);

    impl EventSink for CountingSink {
        fn on_event(&self, _: &str, _: RuntimeEvent) {
            *self.0.lock().unwrap() += 1;
        }
    }

    fn row(status: RunStatus) -> RunHandle {
        let mut h = blank_handle("r1", 1, "claude", Path::new("/tmp/row.log"));
        h.status = status;
        h.started_at = "2024-01-01T00:00:00Z".to_string();
        h.model = Some("m".to_string());
        h
    }

    fn completed(text: &str) -> RuntimeEvent {
        RuntimeEvent::Completed {
            result_text: Some(text.to_string()),
            session_id: Some("s1".to_string()),
            cost_usd: Some(0.5),
            num_turns: Some(2),
            duration_ms: Some(10),
            input_tokens: Some(3),
            output_tokens: Some(4),
            cache_read_input_tokens: None,
            cache_creation_input_tokens: None,
        }
    }

    fn resolve(
        tracker: &RowTracker,
        terminal: Option<RuntimeEvent>,
    ) -> Result<RunHandle, PollError> {
        resolve_completed_run(
            tracker,
            "r1",
            terminal,
            42,
            "claude",
            Path::new("/tmp/x.log"),
            "Test",
        )
    }

    #[test]
    fn capture_forwards_all_events_and_keeps_terminal() {
        let inner = Arc::new(CountingSink::default());
        let capture = TerminalEventCapture::new(inner.clone());
        capture.on_event(
            "r1",
            RuntimeEvent::Init {
                model: None,
                session_id: None,
            },
        );
        assert!(capture.take_terminal().is_none());
        capture.on_event("r1", completed("done"));
        assert_eq!(*inner.0.lock().unwrap(), 2);
        assert!(matches!(
            capture.take_terminal(),
            Some(RuntimeEvent::Completed { .. })
        ));
    }

    #[test]
    fn running_row_gets_captured_completed() {
        let t = RowTracker::from_fn(|| Ok(Some(row(RunStatus::Running))));
        let h = resolve(&t, Some(completed("done"))).unwrap();
        assert_eq!(h.status, RunStatus::Completed);
        assert_eq!(h.result_text.as_deref(), Some("done"));
        assert_eq!(h.num_turns, Some(2));
        assert_eq!(h.started_at, "2024-01-01T00:00:00Z");
        assert_eq!(h.model.as_deref(), Some("m"));
    }

    #[test]
    fn terminal_host_row_wins() {
        let t = RowTracker::from_fn(|| {
            let mut r = row(RunStatus::Completed);
            r.result_text = Some("from host".to_string());
            Ok(Some(r))
        });
        let h = resolve(&t, Some(completed("drained"))).unwrap();
        assert_eq!(h.result_text.as_deref(), Some("from host"));
    }

    #[test]
    fn get_run_error_falls_back_to_event() {
        let t = RowTracker::from_fn(|| Err(RuntimeError::Config("db locked".into())));
        let h = resolve(&t, Some(completed("done"))).unwrap();
        assert_eq!(h.status, RunStatus::Completed);
        assert_eq!(h.id, "r1");
        assert_eq!(h.runtime, "claude");
        assert_eq!(h.subprocess_pid, Some(42));
    }

    #[test]
    fn missing_row_falls_back_to_event() {
        let t = RowTracker::from_fn(|| Ok(None));
        let h = resolve(&t, Some(completed("done"))).unwrap();
        assert_eq!(h.status, RunStatus::Completed);
        assert_eq!(h.result_text.as_deref(), Some("done"));
        assert_eq!(h.log_file.as_deref(), Some("/tmp/x.log"));
    }

    #[test]
    fn running_row_gets_captured_failed() {
        let t = RowTracker::from_fn(|| Ok(Some(row(RunStatus::Running))));
        let ev = RuntimeEvent::Failed {
            error: "boom".to_string(),
            session_id: None,
        };
        let h = resolve(&t, Some(ev)).unwrap();
        assert_eq!(h.status, RunStatus::Failed);
        assert_eq!(h.result_text.as_deref(), Some("boom"));
    }

    #[test]
    fn no_capture_keeps_existing_behaviour() {
        let t = RowTracker::from_fn(|| Ok(None));
        assert!(matches!(resolve(&t, None), Err(PollError::Failed(m)) if m.contains("not found")));
        let t = RowTracker::from_fn(|| Ok(Some(row(RunStatus::Running))));
        assert_eq!(resolve(&t, None).unwrap().status, RunStatus::Running);
    }
}

#[cfg(test)]
mod tests {
    // ------------------------------------------------------------------
    // spawn_headless env injection tests
    // ------------------------------------------------------------------

    /// The child reads nothing from the host's stdin, which keeps its pending data.
    #[cfg(unix)]
    #[test]
    fn spawn_headless_does_not_read_host_stdin() {
        use runkon_test_support::*;
        use std::io::Read;

        if !is_stdin_probe_child() {
            run_in_child_with_piped_stdin(
                "headless::tests::spawn_headless_does_not_read_host_stdin",
            );
            return;
        }
        let args: Vec<std::borrow::Cow<'static, str>> = vec![
            std::borrow::Cow::Borrowed("-c"),
            std::borrow::Cow::Borrowed("printf '[%s]' \"$(cat)\""),
        ];
        let handle = super::spawn_headless(
            &args,
            std::path::Path::new("/tmp"),
            "/bin/sh",
            &std::collections::HashMap::new(),
        )
        .unwrap();
        let (mut stdout, finish) = handle.into_drain_parts();
        let mut output = String::new();
        stdout.read_to_string(&mut output).unwrap();
        finish();
        assert_eq!(output, "[]");
        assert_stdin_untouched_and_report();
    }

    /// Env var injected via the `env` map must be visible in the child process.
    #[cfg(unix)]
    #[test]
    fn spawn_headless_injects_env_var() {
        use std::io::Read;

        let env = std::collections::HashMap::from([(
            "CONDUCTOR_TEST_ENV_2929".to_string(),
            "injected-value".to_string(),
        )]);

        let args: Vec<std::borrow::Cow<'static, str>> = vec![
            std::borrow::Cow::Borrowed("-c"),
            std::borrow::Cow::Borrowed("printf '%s' \"$CONDUCTOR_TEST_ENV_2929\""),
        ];

        let handle =
            super::spawn_headless(&args, std::path::Path::new("/tmp"), "/bin/sh", &env).unwrap();
        let (mut stdout, finish) = handle.into_drain_parts();
        let mut output = String::new();
        stdout.read_to_string(&mut output).unwrap();
        finish();

        assert!(
            output.contains("injected-value"),
            "expected 'injected-value' in child stdout, got: {output:?}"
        );
    }

    /// An env var set in the `env` map must override a same-named var inherited
    /// from the parent process (overlay semantics, not replacement).
    ///
    /// Uses HOME, which is reliably set on Unix, as the "pre-existing parent var"
    /// so no unsafe set_var mutation is needed.
    #[cfg(unix)]
    #[test]
    fn spawn_headless_env_overlay_wins_over_parent() {
        use std::io::Read;

        // HOME is always set in the parent on Unix. Override it with a synthetic
        // value to confirm that the env map entry beats the inherited parent var.
        let env = std::collections::HashMap::from([(
            "HOME".to_string(),
            "/conductor-overlay-test".to_string(),
        )]);

        let args: Vec<std::borrow::Cow<'static, str>> = vec![
            std::borrow::Cow::Borrowed("-c"),
            std::borrow::Cow::Borrowed("printf '%s' \"$HOME\""),
        ];

        let handle =
            super::spawn_headless(&args, std::path::Path::new("/tmp"), "/bin/sh", &env).unwrap();
        let (mut stdout, finish) = handle.into_drain_parts();
        let mut output = String::new();
        stdout.read_to_string(&mut output).unwrap();
        finish();

        assert!(
            output.contains("/conductor-overlay-test"),
            "expected '/conductor-overlay-test' (overlay wins), got: {output:?}"
        );
    }

    // ------------------------------------------------------------------
    // drain_stream_json tests
    // ------------------------------------------------------------------

    use std::sync::{Arc, Mutex};

    #[derive(Default, Clone)]
    struct RecordingSink {
        events: Arc<Mutex<Vec<crate::tracker::RuntimeEvent>>>,
    }

    impl crate::tracker::EventSink for RecordingSink {
        fn on_event(&self, _run_id: &str, event: crate::tracker::RuntimeEvent) {
            self.events.lock().unwrap().push(event);
        }
    }

    /// Minimal parser for drain-behavior tests: counts assistant turns and
    /// terminates on result events. Does not interpret event fields.
    struct TestParser;

    impl super::LineEventParser for TestParser {
        fn classify(&mut self, value: &serde_json::Value) -> super::ParseSignal {
            match value.get("type").and_then(|v| v.as_str()).unwrap_or("") {
                "assistant" => super::ParseSignal::TurnTick,
                "result" => super::ParseSignal::Terminal {
                    final_event: crate::tracker::RuntimeEvent::Completed {
                        result_text: None,
                        session_id: None,
                        cost_usd: None,
                        num_turns: None,
                        duration_ms: None,
                        input_tokens: None,
                        output_tokens: None,
                        cache_read_input_tokens: None,
                        cache_creation_input_tokens: None,
                    },
                },
                _ => super::ParseSignal::Ignore,
            }
        }
    }

    /// A `Read` impl that blocks forever: simulates a stalled subprocess stdout.
    struct BlockingReader {
        _rx: std::sync::mpsc::Receiver<()>,
    }

    impl std::io::Read for BlockingReader {
        fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
            let _ = self._rx.recv(); // blocks until sender dropped (never in this test)
            Ok(0)
        }
    }

    #[test]
    fn drain_stream_json_stalls_when_threshold_exceeded() {
        let (_tx, rx) = std::sync::mpsc::channel::<()>();
        let reader = BlockingReader { _rx: rx };
        let log_file = std::env::temp_dir().join(format!(
            "test-drain-stall-{:?}.log",
            std::thread::current().id()
        ));
        let sink = RecordingSink::default();
        let start = std::time::Instant::now();
        let outcome = super::drain_stream_json(
            reader,
            "stall-run",
            &log_file,
            &sink,
            Some(std::time::Duration::from_millis(100)),
            None,
            TestParser,
        );
        let elapsed = start.elapsed();
        let _ = std::fs::remove_file(&log_file);
        assert!(
            matches!(outcome, super::DrainOutcome::StalledOut(_)),
            "expected StalledOut, got: {outcome:?}"
        );
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "stall detection took too long: {elapsed:?}"
        );
    }

    /// A `Read` impl that returns chunks from a channel with controlled timing.
    /// Each `recv()` blocks until the sender pushes data, simulating a live stream.
    struct ChunkedReader {
        rx: std::sync::mpsc::Receiver<Vec<u8>>,
        current: Vec<u8>,
        pos: usize,
    }

    impl std::io::Read for ChunkedReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.pos >= self.current.len() {
                match self.rx.recv() {
                    Ok(chunk) => {
                        self.current = chunk;
                        self.pos = 0;
                    }
                    Err(_) => return Ok(0), // sender dropped → EOF
                }
            }
            let n = buf.len().min(self.current.len() - self.pos);
            buf[..n].copy_from_slice(&self.current[self.pos..self.pos + n]);
            self.pos += n;
            Ok(n)
        }
    }

    #[test]
    fn drain_stream_json_does_not_stall_with_steady_events() {
        let (tx, rx) = std::sync::mpsc::channel::<Vec<u8>>();
        let reader = ChunkedReader {
            rx,
            current: vec![],
            pos: 0,
        };
        let log_file = std::env::temp_dir().join(format!(
            "test-drain-steady-{:?}.log",
            std::thread::current().id()
        ));
        let sink = RecordingSink::default();

        // Writer thread: send lines every ~20ms, then a result event to EOF.
        std::thread::spawn(move || {
            for _ in 0..5 {
                std::thread::sleep(std::time::Duration::from_millis(20));
                let _ = tx.send(b"{\"type\":\"system\",\"subtype\":\"init\"}\n".to_vec());
            }
            let _ = tx.send(b"{\"type\":\"result\",\"result\":\"steady done\"}\n".to_vec());
            // tx dropped here → ChunkedReader::read returns 0 (EOF)
        });

        let outcome = super::drain_stream_json(
            reader,
            "steady-run",
            &log_file,
            &sink,
            Some(std::time::Duration::from_millis(500)),
            None,
            TestParser,
        );
        let _ = std::fs::remove_file(&log_file);
        assert_eq!(
            outcome,
            super::DrainOutcome::Completed,
            "steady stream must not stall"
        );
    }

    #[test]
    fn drain_stream_json_turn_cap_reached() {
        // Feed 4 assistant events with a cap of 3: drain must stop at turn 3.
        let lines = [
            r#"{"type":"assistant","usage":{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}"#,
            r#"{"type":"assistant","usage":{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}"#,
            r#"{"type":"assistant","usage":{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}"#,
            r#"{"type":"assistant","usage":{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}"#,
        ];
        let input = lines.join("\n");
        let log_file = std::env::temp_dir().join(format!(
            "test-drain-turncap-{:?}.log",
            std::thread::current().id()
        ));
        let sink = RecordingSink::default();
        let outcome = super::drain_stream_json(
            std::io::Cursor::new(input.into_bytes()),
            "cap-run",
            &log_file,
            &sink,
            None,
            Some(3),
            TestParser,
        );
        let _ = std::fs::remove_file(&log_file);
        assert_eq!(
            outcome,
            super::DrainOutcome::TurnCapReached(3),
            "expected TurnCapReached(3), got: {outcome:?}"
        );
    }
}
