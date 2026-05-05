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
#[cfg(unix)]
pub fn spawn_headless(
    args: &[Cow<'static, str>],
    working_dir: &std::path::Path,
    binary_path: &str,
) -> std::result::Result<HeadlessHandle, String> {
    use std::process::Stdio;
    let child = Command::new(binary_path)
        .args(args.iter().map(|a| a.as_ref()))
        .current_dir(working_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
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

/// Drain the stdout of a headless subprocess, persisting events to the DB.
///
/// When `stall_threshold` is `Some(t)`, returns `DrainOutcome::StalledOut` if
/// no output is received for longer than `t`. The caller must then kill the
/// subprocess; passing `None` disables stall detection and behaves as before.
///
/// When `max_turns` is `Some(n)`, returns `DrainOutcome::TurnCapReached(n)` after
/// counting `n` `"assistant"` events. The caller must then kill the subprocess;
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

        let event_type = value.get("type").and_then(|v| v.as_str()).unwrap_or("");

        match event_type {
            "system" => {
                let subtype = value.get("subtype").and_then(|v| v.as_str()).unwrap_or("");
                if subtype == "init" {
                    let model = value
                        .get("model")
                        .and_then(|v| v.as_str())
                        .map(String::from);
                    let session_id = value
                        .get("session_id")
                        .and_then(|v| v.as_str())
                        .map(String::from);
                    sink.on_event(run_id, RuntimeEvent::Init { model, session_id });
                }
            }
            "assistant" => {
                let usage = value
                    .get("message")
                    .and_then(|m| m.get("usage"))
                    .or_else(|| value.get("usage"));
                if let Some(usage) = usage {
                    let input = usage
                        .get("input_tokens")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    let output = usage
                        .get("output_tokens")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    let cache_read = usage
                        .get("cache_read_input_tokens")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    let cache_create = usage
                        .get("cache_creation_input_tokens")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    sink.on_event(
                        run_id,
                        RuntimeEvent::Tokens {
                            input,
                            output,
                            cache_read,
                            cache_create,
                        },
                    );
                }
                turn_count += 1;
                if let Some(cap) = max_turns {
                    if turn_count >= cap {
                        return DrainOutcome::TurnCapReached(turn_count);
                    }
                }
            }
            "result" => {
                let is_error = value
                    .get("is_error")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if is_error {
                    let error = value
                        .get("result")
                        .and_then(|v| v.as_str())
                        .unwrap_or("agent reported an error")
                        .to_string();
                    let session_id = value
                        .get("session_id")
                        .and_then(|v| v.as_str())
                        .map(String::from);
                    sink.on_event(run_id, RuntimeEvent::Failed { error, session_id });
                } else {
                    let result_text = value
                        .get("result")
                        .and_then(|v| v.as_str())
                        .map(String::from);
                    let session_id = value
                        .get("session_id")
                        .and_then(|v| v.as_str())
                        .map(String::from);
                    let cost_usd = value.get("total_cost_usd").and_then(|v| v.as_f64());
                    let num_turns = value.get("num_turns").and_then(|v| v.as_i64());
                    let duration_ms = value.get("duration_ms").and_then(|v| v.as_i64());
                    let usage = value.get("usage");
                    let input_tokens = usage
                        .and_then(|u| u.get("input_tokens"))
                        .and_then(|v| v.as_i64());
                    let output_tokens = usage
                        .and_then(|u| u.get("output_tokens"))
                        .and_then(|v| v.as_i64());
                    let cache_read_input_tokens = usage
                        .and_then(|u| u.get("cache_read_input_tokens"))
                        .and_then(|v| v.as_i64());
                    let cache_creation_input_tokens = usage
                        .and_then(|u| u.get("cache_creation_input_tokens"))
                        .and_then(|v| v.as_i64());
                    sink.on_event(
                        run_id,
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
                        },
                    );
                }
                return DrainOutcome::Completed;
            }
            _ => {}
        }
    }

    DrainOutcome::NoResult
}

#[cfg(test)]
mod tests {
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

    fn run_drain(lines: &[&str]) -> (super::DrainOutcome, RecordingSink) {
        let input = lines.join("\n");
        let log_file =
            std::env::temp_dir().join(format!("test-drain-{:?}.log", std::thread::current().id()));
        let sink = RecordingSink::default();
        let outcome = super::drain_stream_json(
            std::io::Cursor::new(input.into_bytes()),
            "run-1",
            &log_file,
            &sink,
            None,
            None,
        );
        let _ = std::fs::remove_file(&log_file);
        (outcome, sink)
    }

    #[test]
    fn drain_stream_json_result_event_returns_completed() {
        let (outcome, sink) = run_drain(&[r#"{"type":"result","result":"hello"}"#]);
        assert_eq!(outcome, super::DrainOutcome::Completed);
        let events = sink.events.lock().unwrap();
        assert!(matches!(
            events[0],
            crate::tracker::RuntimeEvent::Completed { .. }
        ));
    }

    #[test]
    fn drain_stream_json_error_result_returns_completed() {
        let (outcome, sink) = run_drain(&[r#"{"type":"result","is_error":true,"result":"oops"}"#]);
        assert_eq!(outcome, super::DrainOutcome::Completed);
        let events = sink.events.lock().unwrap();
        assert!(matches!(
            events[0],
            crate::tracker::RuntimeEvent::Failed { .. }
        ));
    }

    #[test]
    fn drain_stream_json_no_result_returns_no_result() {
        let (outcome, sink) = run_drain(&[r#"{"type":"system","subtype":"init"}"#]);
        assert_eq!(outcome, super::DrainOutcome::NoResult);
        let events = sink.events.lock().unwrap();
        // system/init lines emit an Init event even though there's no final result
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            crate::tracker::RuntimeEvent::Init { .. }
        ));
    }

    /// Reader that yields `prefix`, then returns an `io::Error` on the next read.
    /// Used to exercise the stdout read-error branch of `drain_stream_json`.
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
    fn drain_stream_json_returns_no_result_on_stdout_read_error() {
        // Stream yields one valid JSON line (which emits an Init event), then
        // a hard read error before any `result` event. drain_stream_json must
        // return NoResult cleanly without panicking.
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
        let outcome = super::drain_stream_json(reader, "run-err", &log_file, &sink, None, None);
        let _ = std::fs::remove_file(&log_file);
        assert_eq!(outcome, super::DrainOutcome::NoResult);
        // The init event from before the error should still have been emitted.
        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            crate::tracker::RuntimeEvent::Init { .. }
        ));
    }

    #[test]
    fn drain_stream_json_token_update_emitted() {
        let (outcome, sink) = run_drain(&[
            r#"{"type":"assistant","usage":{"input_tokens":10,"output_tokens":20,"cache_read_input_tokens":5,"cache_creation_input_tokens":3}}"#,
            r#"{"type":"result","result":"done"}"#,
        ]);
        assert_eq!(outcome, super::DrainOutcome::Completed);
        let events = sink.events.lock().unwrap();
        assert!(matches!(
            events[0],
            crate::tracker::RuntimeEvent::Tokens {
                input: 10,
                output: 20,
                cache_read: 5,
                cache_create: 3,
            }
        ));
    }

    #[test]
    fn drain_stream_json_cost_turns_duration_parsed() {
        let (outcome, sink) = run_drain(&[
            r#"{"type":"result","result":"ok","total_cost_usd":0.42,"num_turns":7,"duration_ms":12345,"usage":{"input_tokens":100,"output_tokens":50}}"#,
        ]);
        assert_eq!(outcome, super::DrainOutcome::Completed);
        let events = sink.events.lock().unwrap();
        match &events[0] {
            crate::tracker::RuntimeEvent::Completed {
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
        );
        let _ = std::fs::remove_file(&log_file);
        assert_eq!(
            outcome,
            super::DrainOutcome::TurnCapReached(3),
            "expected TurnCapReached(3), got: {outcome:?}"
        );
    }
}
