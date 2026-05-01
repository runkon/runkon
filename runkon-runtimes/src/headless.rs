//! Shared runtime helpers for spawning and polling agent runs.

use std::borrow::Cow;
use std::process::Command;

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use crate::permission::PermissionMode;
use crate::tracker::{EventSink, RuntimeEvent};

const DEFAULT_AGENT_ERROR_MSG: &str = "Claude reported an error";

/// Resolve the path to the `conductor` binary.
///
/// Looks for a sibling `conductor` next to the current executable first,
/// then falls back to the bare name (relying on `$PATH`).
pub fn resolve_conductor_bin() -> String {
    let resolved = std::env::current_exe()
        .ok()
        .and_then(|p| {
            let sibling = p.parent()?.join("conductor");
            sibling
                .exists()
                .then(|| sibling.to_string_lossy().into_owned())
        })
        .unwrap_or_else(|| "conductor".to_string());
    tracing::debug!("[conductor] resolved binary: {resolved}");
    resolved
}

/// Maximum number of CLI arguments produced by `build_headless_agent_args`.
const AGENT_ARGS_CAPACITY: usize = 20;

fn push_optional_agent_flags(
    args: &mut Vec<Cow<'static, str>>,
    resume_session_id: Option<&str>,
    model: Option<&str>,
    permission_mode: Option<&PermissionMode>,
    extra_cli_args: &[(Cow<'static, str>, Cow<'static, str>)],
    extra_plugin_dirs: &[String],
) {
    if let Some(id) = resume_session_id {
        args.push(Cow::Borrowed("--resume"));
        args.push(Cow::Owned(id.to_string()));
    }
    if let Some(m) = model {
        args.push(Cow::Borrowed("--model"));
        args.push(Cow::Owned(m.to_string()));
    }
    if let Some(mode) = permission_mode {
        if let Some(val) = mode.cli_flag_value() {
            args.push(Cow::Borrowed("--permission-mode"));
            args.push(Cow::Owned(val.to_string()));
        }
    }
    for (flag, val) in extra_cli_args {
        args.push(flag.clone());
        args.push(val.clone());
    }
    for dir in extra_plugin_dirs {
        args.push(Cow::Borrowed("--plugin-dir"));
        args.push(Cow::Owned(dir.clone()));
    }
}

/// Write `prompt` to a temp file with mode 0o600 (Unix) and return the path.
fn write_prompt_file(
    run_id: &str,
    prompt: &str,
) -> std::result::Result<std::path::PathBuf, String> {
    let prompt_file_path = std::env::temp_dir().join(format!("conductor-prompt-{run_id}.txt"));

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&prompt_file_path)
            .and_then(|mut f| {
                use std::io::Write;
                f.write_all(prompt.as_bytes())
            })
            .map_err(|e| {
                format!(
                    "Failed to write prompt file '{}': {e}",
                    prompt_file_path.display()
                )
            })?;
    }

    #[cfg(not(unix))]
    {
        std::fs::write(&prompt_file_path, prompt).map_err(|e| {
            format!(
                "Failed to write prompt file '{}': {e}",
                prompt_file_path.display()
            )
        })?;
    }

    Ok(prompt_file_path)
}

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
            let _ = child.kill();
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

/// Spawn a headless `conductor agent run` subprocess.
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
        .map_err(|e| format!("Failed to spawn conductor headless: {e}"))?;

    HeadlessHandle::from_child(child)
}

/// Parameters for spawning a headless agent subprocess.
pub struct SpawnHeadlessParams<'a> {
    pub run_id: &'a str,
    pub working_dir: &'a str,
    pub prompt: &'a str,
    pub resume_session_id: Option<&'a str>,
    pub model: Option<&'a str>,
    pub extra_cli_args: &'a [(Cow<'static, str>, Cow<'static, str>)],
    pub permission_mode: Option<&'a PermissionMode>,
    pub plugin_dirs: &'a [String],
}

/// Build headless args and spawn the conductor subprocess in one step.
#[cfg(unix)]
pub fn try_spawn_headless_run(
    params: &SpawnHeadlessParams<'_>,
    binary_path: &str,
) -> std::result::Result<(HeadlessHandle, std::path::PathBuf), String> {
    let (args, pf) = build_headless_agent_args(params).map_err(|e| {
        format!(
            "failed to prepare agent args for run {} (working_dir={}): {e}",
            params.run_id, params.working_dir
        )
    })?;
    let h = spawn_headless(&args, std::path::Path::new(params.working_dir), binary_path).map_err(
        |e| {
            let _ = std::fs::remove_file(&pf);
            format!(
                "spawn failed for run {} (working_dir={}): {e}",
                params.run_id, params.working_dir
            )
        },
    )?;
    Ok((h, pf))
}

/// Result of draining a headless subprocess stdout stream.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum DrainOutcome {
    /// A `result` event was seen; the run was finalized in the DB.
    Completed,
    /// EOF before any `result` event (SIGTERM path or unexpected crash).
    /// Caller must mark the run as cancelled/failed in the DB.
    NoResult,
}

/// Drain the stdout of a headless subprocess, persisting events to the DB.
pub fn drain_stream_json<S: EventSink + ?Sized>(
    stdout: impl std::io::Read,
    run_id: &str,
    log_file: &std::path::Path,
    sink: &S,
) -> DrainOutcome {
    use std::io::{BufRead, BufReader, Write};

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

    let reader = BufReader::new(stdout);
    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(
                    "[drain_stream_json] stdout read failed for run {run_id}, ending drain: {e}"
                );
                break;
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
                        .unwrap_or(DEFAULT_AGENT_ERROR_MSG)
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

/// Build `conductor agent run` args for a headless launch.
pub fn build_headless_agent_args(
    params: &SpawnHeadlessParams<'_>,
) -> std::result::Result<(Vec<Cow<'static, str>>, std::path::PathBuf), String> {
    crate::text_util::validate_run_id(params.run_id).map_err(|e| e.to_string())?;

    let run_id = params.run_id;
    let working_dir = params.working_dir;
    let prompt = params.prompt;
    let resume_session_id = params.resume_session_id;
    let model = params.model;
    let extra_cli_args = params.extra_cli_args;
    let permission_mode = params.permission_mode;
    let extra_plugin_dirs = params.plugin_dirs;

    let prompt_file_path = write_prompt_file(run_id, prompt)?;

    let mut args: Vec<Cow<'static, str>> = Vec::with_capacity(AGENT_ARGS_CAPACITY + 2);
    args.push(Cow::Borrowed("agent"));
    args.push(Cow::Borrowed("run"));
    args.push(Cow::Borrowed("--run-id"));
    args.push(Cow::Owned(run_id.to_string()));
    args.push(Cow::Borrowed("--worktree-path"));
    args.push(Cow::Owned(working_dir.to_string()));
    args.push(Cow::Borrowed("--prompt-file"));
    args.push(Cow::Owned(prompt_file_path.to_string_lossy().into_owned()));

    push_optional_agent_flags(
        &mut args,
        resume_session_id,
        model,
        permission_mode,
        extra_cli_args,
        extra_plugin_dirs,
    );

    Ok((args, prompt_file_path))
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    fn assert_file_prompt(args: &[Cow<'static, str>], expected_content: &str, expected_path: &str) {
        let file_idx = args
            .iter()
            .position(|a| a == "--prompt-file")
            .expect("--prompt-file flag missing");
        let file_path: &str = args[file_idx + 1].as_ref();
        assert_eq!(file_path, expected_path, "prompt file path mismatch");
        assert!(
            std::path::Path::new(file_path).exists(),
            "prompt file should have been written"
        );
        assert_eq!(
            std::fs::read_to_string(file_path).unwrap(),
            expected_content
        );
        assert!(
            !args.iter().any(|a| a == "--prompt"),
            "--prompt should not appear"
        );
    }

    fn make_params<'a>(
        run_id: &'a str,
        prompt: &'a str,
        resume_session_id: Option<&'a str>,
        model: Option<&'a str>,
        extra_cli_args: &'a [(Cow<'static, str>, Cow<'static, str>)],
    ) -> super::SpawnHeadlessParams<'a> {
        super::SpawnHeadlessParams {
            run_id,
            working_dir: "/tmp/wt",
            prompt,
            resume_session_id,
            model,
            extra_cli_args,
            permission_mode: None,
            plugin_dirs: &[],
        }
    }

    #[test]
    fn build_agent_args_short_prompt_uses_file() {
        let run_id = "run-short-1";
        let prompt = "short prompt";
        let (args, _) =
            super::build_headless_agent_args(&make_params(run_id, prompt, None, None, &[]))
                .unwrap();
        let expected_path = std::env::temp_dir().join(format!("conductor-prompt-{run_id}.txt"));
        assert_file_prompt(&args, prompt, expected_path.to_str().unwrap());
        let _ = std::fs::remove_file(&expected_path);
    }

    #[test]
    fn build_agent_args_long_prompt_uses_file() {
        let run_id = "run-long-99";
        let prompt = "x".repeat(513);
        let (args, _) =
            super::build_headless_agent_args(&make_params(run_id, &prompt, None, None, &[]))
                .unwrap();
        let expected_path = std::env::temp_dir().join(format!("conductor-prompt-{run_id}.txt"));
        assert_file_prompt(&args, &prompt, expected_path.to_str().unwrap());
        let _ = std::fs::remove_file(&expected_path);
    }

    #[test]
    fn build_agent_args_with_resume_sets_flag() {
        let run_id = "run-resume-sets-flag";
        let (args, _) = super::build_headless_agent_args(&make_params(
            run_id,
            "short prompt",
            Some("sess-abc"),
            None,
            &[],
        ))
        .unwrap();
        let resume_idx = args
            .iter()
            .position(|a| a == "--resume")
            .expect("--resume flag missing");
        assert_eq!(args[resume_idx + 1], "sess-abc");
        let _ = std::fs::remove_file(
            std::env::temp_dir().join(format!("conductor-prompt-{run_id}.txt")),
        );
    }

    #[test]
    fn build_agent_args_with_model_override() {
        let run_id = "run-model-override";
        let (args, _) = super::build_headless_agent_args(&make_params(
            run_id,
            "prompt",
            None,
            Some("claude-sonnet-4-6"),
            &[],
        ))
        .unwrap();
        let idx = args
            .iter()
            .position(|a| a == "--model")
            .expect("expected --model flag");
        assert_eq!(args[idx + 1], "claude-sonnet-4-6");
        let _ = std::fs::remove_file(
            std::env::temp_dir().join(format!("conductor-prompt-{run_id}.txt")),
        );
    }

    #[test]
    fn build_agent_args_with_extra_cli_args() {
        let run_id = "run-extra-cli-args-01";
        let extra = [(
            Cow::Borrowed("--custom-flag"),
            Cow::Owned("custom-value".to_string()),
        )];
        let (args, _) =
            super::build_headless_agent_args(&make_params(run_id, "prompt", None, None, &extra))
                .unwrap();
        let idx = args
            .iter()
            .position(|a| a == "--custom-flag")
            .expect("expected --custom-flag flag");
        assert_eq!(args[idx + 1], "custom-value");
        let _ = std::fs::remove_file(
            std::env::temp_dir().join(format!("conductor-prompt-{run_id}.txt")),
        );
    }

    #[cfg(unix)]
    #[test]
    fn build_agent_args_prompt_file_mode_0o600() {
        use std::os::unix::fs::MetadataExt;
        let run_id = "run-perm-600-01";
        let (args, _) = super::build_headless_agent_args(&make_params(
            run_id,
            "secret prompt",
            None,
            None,
            &[],
        ))
        .unwrap();
        let file_idx = args
            .iter()
            .position(|a| a == "--prompt-file")
            .expect("--prompt-file flag missing");
        let file_path = std::path::Path::new(args[file_idx + 1].as_ref());
        let mode = std::fs::metadata(file_path)
            .expect("prompt file must exist")
            .mode();
        assert_eq!(
            mode & 0o777,
            0o600,
            "prompt file must have mode 0o600, got {:#o}",
            mode & 0o777
        );
        let _ = std::fs::remove_file(file_path);
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

    fn run_drain(lines: &[&str]) -> (super::DrainOutcome, RecordingSink) {
        let input = lines.join("\n");
        let log_file =
            std::env::temp_dir().join(format!("test-drain-{:?}.log", std::thread::current().id()));
        let sink = RecordingSink::default();
        let outcome = super::drain_stream_json(input.as_bytes(), "run-1", &log_file, &sink);
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
        let outcome = super::drain_stream_json(reader, "run-err", &log_file, &sink);
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
}
