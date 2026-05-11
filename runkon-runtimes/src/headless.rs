//! Shared runtime helpers for spawning and polling agent runs.

use std::borrow::Cow;
use std::process::Command;

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use crate::tracker::{EventSink, RuntimeEvent};

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

#[cfg(test)]
mod tests {
    // ------------------------------------------------------------------
    // spawn_headless env injection tests
    // ------------------------------------------------------------------

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
