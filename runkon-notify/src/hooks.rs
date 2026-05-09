//! Notification hook execution.
//!
//! # Security model
//!
//! Shell hooks run user-configured commands via `sh -c <hook.run>`. The
//! command string is read directly from the hook configuration and is
//! **not** validated, sandboxed, or restricted to an allow-list. Event data
//! is passed via environment variables (safe), but the hook body has full
//! shell privileges.
//!
//! Treat hook configuration files as trusted input — do not let untrusted
//! sources modify them.
//!
//! HTTP hooks have a smaller blast radius: only the configured URL is
//! reachable; headers beginning with `$` resolve from the host process
//! environment.

use std::collections::HashMap;
use std::process::Stdio;
use std::time::Duration;

use serde::Deserialize;

use crate::event::Event;

/// Result of matching an `on` pattern against an event kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OnMatch {
    /// No sub-pattern matched.
    None,
    /// Matched; fires for any event.
    Any,
    /// Matched via a `:root` suffix. In this crate the `:root` modifier is
    /// recognized but not enforced — generic events have no parent concept.
    RootOnly,
}

/// Match a comma-separated `on` pattern against `event_kind`.
///
/// Each sub-pattern may have a `:root` suffix (e.g. `"stage.*:root"`).
/// Returns [`OnMatch::RootOnly`] if the matching sub-pattern had `:root`,
/// [`OnMatch::Any`] if it matched without `:root`, or [`OnMatch::None`] if
/// no sub-pattern matched.
pub(crate) fn on_pattern_match(on: &str, event_kind: &str) -> OnMatch {
    for part in on.split(',') {
        let part = part.trim();
        if let Some(pat) = part.strip_suffix(":root") {
            if glob_matches(pat, event_kind) {
                return OnMatch::RootOnly;
            }
        } else if glob_matches(part, event_kind) {
            return OnMatch::Any;
        }
    }
    OnMatch::None
}

/// Convenience wrapper: returns `true` if `on` matches `event_kind` (ignoring `:root`).
pub fn on_pattern_matches(on: &str, event_kind: &str) -> bool {
    on_pattern_match(on, event_kind) != OnMatch::None
}

/// Returns `true` if `pattern` matches `event_kind`.
///
/// Supported cases:
/// - `"*"` — matches everything.
/// - `"prefix.*"` — matches any string that starts with `"prefix."`.
/// - `"prefix/*"` — matches any string that starts with `"prefix/"` (branch globs).
/// - exact string — matches only when the strings are equal.
pub(crate) fn glob_matches(pattern: &str, event_kind: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix(".*") {
        return event_kind.starts_with(&format!("{prefix}."));
    }
    if let Some(prefix) = pattern.strip_suffix("/*") {
        return event_kind.starts_with(&format!("{prefix}/"));
    }
    pattern == event_kind
}

/// Resolve a header value: if it starts with `$`, look it up in the environment.
///
/// Returns the resolved value, or the original `$VAR_NAME` string if the
/// variable is not set, and logs a warning so silent auth failures are visible.
fn resolve_env_var(s: &str) -> String {
    if let Some(var_name) = s.strip_prefix('$') {
        match std::env::var(var_name) {
            Ok(val) => val,
            Err(_) => {
                tracing::warn!(
                    var = %var_name,
                    "HTTP hook header references unset env var ${var_name}; \
                     passing literal string — authentication may fail"
                );
                s.to_string()
            }
        }
    } else {
        s.to_string()
    }
}

/// Configuration for a single notification hook entry.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct HookConfig {
    /// Comma-separated event-kind pattern, e.g. `"stage.*"` or `"permit.approved,permit.denied"`.
    pub on: String,
    /// Shell command to execute via `sh -c`. Mutually inclusive with `url`.
    pub run: Option<String>,
    /// HTTP endpoint to POST the event JSON to. Mutually inclusive with `run`.
    pub url: Option<String>,
    /// Optional HTTP headers. Values starting with `$` are resolved from the environment.
    pub headers: Option<HashMap<String, String>>,
    /// Timeout in milliseconds for shell/HTTP hooks. Defaults to 10 000.
    pub timeout_ms: Option<u64>,
}

/// Build a `sh -c <cmd>` base command with event env vars and stdin closed.
///
/// Callers append their desired stdout/stderr disposition before spawning.
fn build_shell_command(cmd: &str, env_vars: &HashMap<String, String>) -> std::process::Command {
    let mut command = std::process::Command::new("sh");
    command
        .arg("-c")
        .arg(cmd)
        .envs(env_vars)
        .stdin(Stdio::null());
    command
}

/// Execute a shell hook for `event`, enforcing the configured timeout.
///
/// The command is run via `sh -c` with all `RUNKON_NOTIFY_*` env vars injected.
/// If the process does not finish within `timeout_ms`, it is killed via
/// `Child::kill()` and a warning is logged. All failures are non-fatal.
fn run_shell_hook(hook: &HookConfig, event: &Event) {
    let Some(ref cmd) = hook.run else { return };

    let timeout_ms = hook.timeout_ms.unwrap_or(10_000);
    let env_vars = event.to_env_vars();

    let mut child = match build_shell_command(cmd, &env_vars)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(cmd = %cmd, "shell hook spawn failed: {e}");
            return;
        }
    };

    let timeout = Duration::from_millis(timeout_ms);
    let start = std::time::Instant::now();

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    tracing::warn!(cmd = %cmd, "shell hook exited with non-zero status: {status}");
                }
                return;
            }
            Ok(None) => {
                if start.elapsed() >= timeout {
                    tracing::warn!(cmd = %cmd, timeout_ms, "shell hook timed out");
                    let _ = child.kill();
                    let _ = child.wait();
                    return;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => {
                tracing::warn!(cmd = %cmd, "shell hook wait error: {e}");
                let _ = child.kill();
                let _ = child.wait();
                return;
            }
        }
    }
}

/// Execute a shell hook synchronously and capture its output, for test/diagnostic use.
///
/// Unlike `run_shell_hook`, this function:
/// - Pipes stdout and stderr so they do not leak into the caller's terminal.
/// - Blocks until the child exits (no timeout polling; uses `wait_with_output()`).
/// - Returns `Err(stderr_text)` on non-zero exit, falling back to `"exited with status N"`
///   if stderr is empty.
fn run_shell_hook_capture(hook: &HookConfig, event: &Event) -> Result<(), String> {
    let Some(ref cmd) = hook.run else {
        return Ok(());
    };

    let env_vars = event.to_env_vars();

    let child = match build_shell_command(cmd, &env_vars)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => return Err(format!("spawn failed: {e}")),
    };

    match child.wait_with_output() {
        Ok(output) if output.status.success() => Ok(()),
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            if stderr.is_empty() {
                Err(format!("exited with status {}", output.status))
            } else {
                Err(stderr)
            }
        }
        Err(e) => Err(format!("wait error: {e}")),
    }
}

/// POST `event.to_json()` to `hook.url`, resolving `$VAR` header values from env.
///
/// Uses `ureq` (synchronous). All failures are non-fatal.
fn run_http_hook(hook: &HookConfig, event: &Event) {
    let Some(ref url) = hook.url else { return };

    let timeout_ms = hook.timeout_ms.unwrap_or(10_000);
    let payload = event.to_json();

    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_millis(timeout_ms))
        .build();

    let mut request = agent.post(url);

    if let Some(ref headers) = hook.headers {
        for (key, val) in headers {
            let resolved = resolve_env_var(val);
            request = request.set(key, &resolved);
        }
    }

    if let Err(e) = request.send_json(&payload) {
        tracing::warn!(url = %url, "HTTP hook POST failed: {e}");
    }
}

/// Fires user-configured notification hooks for a given event.
pub struct HookRunner {
    hooks: Vec<HookConfig>,
}

impl HookRunner {
    /// Create a runner from a slice of hook configs.
    pub fn new(hooks: &[HookConfig]) -> Self {
        Self {
            hooks: hooks.to_vec(),
        }
    }

    /// Run the first matching hook synchronously and return the real exit result.
    ///
    /// Intended for test/diagnostic use (e.g. a "Test Hook" UI feature). Unlike
    /// `fire()`, this method:
    /// - Blocks until the hook finishes.
    /// - Captures stdout/stderr so nothing leaks into the terminal.
    /// - Returns `Ok(())` on success or `Err(message)` on failure.
    ///
    /// Only shell (`run`) hooks return a meaningful result. HTTP hooks return
    /// `Ok(())` — they already swallow errors internally.
    pub fn run_test(&self, event: &Event) -> Result<(), String> {
        for hook in &self.hooks {
            if on_pattern_match(&hook.on, &event.kind) == OnMatch::None {
                continue;
            }
            if hook.run.is_some() {
                return run_shell_hook_capture(hook, event);
            }
            if hook.url.is_some() {
                run_http_hook(hook, event);
                return Ok(());
            }
        }
        Ok(())
    }

    /// Fire all hooks whose `on` pattern matches `event.kind`.
    ///
    /// Each matching hook is executed in a separate OS thread (fire-and-forget).
    /// Both `run` (shell) and `url` (HTTP) hooks can coexist in the same config
    /// entry; both are attempted when present. Failures are logged as warnings
    /// and never propagated.
    pub fn fire(&self, event: &Event) {
        for hook in &self.hooks {
            if on_pattern_match(&hook.on, &event.kind) == OnMatch::None {
                continue;
            }
            let hook_clone = hook.clone();
            let event_clone = event.clone();
            std::thread::spawn(move || {
                if hook_clone.run.is_some() {
                    run_shell_hook(&hook_clone, &event_clone);
                }
                if hook_clone.url.is_some() {
                    run_http_hook(&hook_clone, &event_clone);
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::event::{Event, Severity};

    fn demo_event() -> Event {
        Event {
            kind: "workflow_run.completed".into(),
            title: "Workflow finished".into(),
            body: "All steps passed.".into(),
            severity: Severity::Info,
            fields: HashMap::new(),
        }
    }

    // ── glob_matches ─────────────────────────────────────────────────────

    #[test]
    fn glob_star_matches_any() {
        assert!(glob_matches("*", "workflow_run.completed"));
        assert!(glob_matches("*", "gate.waiting"));
        assert!(glob_matches("*", "feedback.requested"));
    }

    #[test]
    fn glob_prefix_matches_same_category() {
        assert!(glob_matches("workflow_run.*", "workflow_run.completed"));
        assert!(glob_matches("workflow_run.*", "workflow_run.failed"));
        assert!(glob_matches("workflow_run.*", "workflow_run.cost_spike"));
    }

    #[test]
    fn glob_prefix_does_not_match_other_category() {
        assert!(!glob_matches("workflow_run.*", "gate.waiting"));
        assert!(!glob_matches("workflow_run.*", "agent_run.completed"));
        assert!(!glob_matches("workflow_run.*", "feedback.requested"));
    }

    #[test]
    fn glob_exact_matches_only_exact() {
        assert!(glob_matches("gate.waiting", "gate.waiting"));
        assert!(!glob_matches("gate.waiting", "gate.pending_too_long"));
        assert!(!glob_matches("gate.waiting", "workflow_run.completed"));
    }

    #[test]
    fn glob_prefix_does_not_partially_match_name() {
        assert!(!glob_matches("workflow.*", "workflow_run.completed"));
    }

    #[test]
    fn glob_slash_star_matches_branch_with_prefix() {
        assert!(glob_matches("feature/*", "feature/my-branch"));
        assert!(glob_matches("feature/*", "feature/foo"));
    }

    #[test]
    fn glob_slash_star_does_not_match_other_prefix() {
        assert!(!glob_matches("feature/*", "main"));
        assert!(!glob_matches("feature/*", "fix/my-fix"));
        assert!(!glob_matches("feature/*", "feature"));
    }

    // ── on_pattern_matches (comma-separated) ─────────────────────────────

    #[test]
    fn on_pattern_single_event_matches() {
        assert!(on_pattern_matches("gate.waiting", "gate.waiting"));
        assert!(!on_pattern_matches("gate.waiting", "gate.pending_too_long"));
    }

    #[test]
    fn on_pattern_comma_separated_matches_any() {
        assert!(on_pattern_matches(
            "workflow_run.completed,gate.waiting",
            "gate.waiting"
        ));
        assert!(on_pattern_matches(
            "workflow_run.completed,gate.waiting",
            "workflow_run.completed"
        ));
        assert!(!on_pattern_matches(
            "workflow_run.completed,gate.waiting",
            "agent_run.failed"
        ));
    }

    #[test]
    fn on_pattern_comma_with_spaces_trimmed() {
        assert!(on_pattern_matches(
            "workflow_run.completed , gate.waiting",
            "gate.waiting"
        ));
    }

    #[test]
    fn on_pattern_comma_with_wildcard() {
        assert!(on_pattern_matches(
            "workflow_run.*,gate.waiting",
            "workflow_run.failed"
        ));
        assert!(on_pattern_matches(
            "workflow_run.*,gate.waiting",
            "gate.waiting"
        ));
        assert!(!on_pattern_matches(
            "workflow_run.*,gate.waiting",
            "agent_run.completed"
        ));
    }

    #[test]
    fn on_pattern_empty_string_matches_nothing() {
        assert!(!on_pattern_matches("", "gate.waiting"));
    }

    // ── on_pattern_match with :root suffix ───────────────────────────────

    #[test]
    fn on_pattern_root_suffix_returns_root_only() {
        assert_eq!(
            on_pattern_match("workflow_run.completed:root", "workflow_run.completed"),
            OnMatch::RootOnly
        );
    }

    #[test]
    fn on_pattern_no_root_suffix_returns_any() {
        assert_eq!(
            on_pattern_match("workflow_run.completed", "workflow_run.completed"),
            OnMatch::Any
        );
    }

    #[test]
    fn on_pattern_root_suffix_no_match_returns_none() {
        assert_eq!(
            on_pattern_match("workflow_run.completed:root", "gate.waiting"),
            OnMatch::None
        );
    }

    #[test]
    fn on_pattern_comma_mixed_root_and_any() {
        let pat = "workflow_run.completed:root,gate.waiting";
        assert_eq!(
            on_pattern_match(pat, "workflow_run.completed"),
            OnMatch::RootOnly
        );
        assert_eq!(on_pattern_match(pat, "gate.waiting"), OnMatch::Any);
        assert_eq!(on_pattern_match(pat, "agent_run.failed"), OnMatch::None);
    }

    #[test]
    fn on_pattern_wildcard_with_root() {
        assert_eq!(
            on_pattern_match("workflow_run.*:root", "workflow_run.completed"),
            OnMatch::RootOnly
        );
        assert_eq!(
            on_pattern_match("workflow_run.*:root", "gate.waiting"),
            OnMatch::None
        );
    }

    // ── resolve_env_var ───────────────────────────────────────────────────

    #[test]
    fn resolve_env_var_non_dollar_passthrough() {
        assert_eq!(
            resolve_env_var("Bearer static-token"),
            "Bearer static-token"
        );
    }

    #[test]
    fn resolve_env_var_missing_returns_original() {
        let result = resolve_env_var("$__RUNKON_NOTIFY_TEST_UNSET_VAR_XYZ__");
        assert_eq!(result, "$__RUNKON_NOTIFY_TEST_UNSET_VAR_XYZ__");
    }

    #[test]
    fn resolve_env_var_set_var_is_resolved() {
        std::env::set_var("__RUNKON_NOTIFY_TEST_HOOK_VAR__", "resolved-value");
        let result = resolve_env_var("$__RUNKON_NOTIFY_TEST_HOOK_VAR__");
        std::env::remove_var("__RUNKON_NOTIFY_TEST_HOOK_VAR__");
        assert_eq!(result, "resolved-value");
    }

    // ── run_shell_hook_capture ────────────────────────────────────────────

    #[test]
    fn capture_success_returns_ok() {
        let hook = HookConfig {
            on: "*".into(),
            run: Some("exit 0".into()),
            timeout_ms: Some(5_000),
            ..Default::default()
        };
        assert!(run_shell_hook_capture(&hook, &demo_event()).is_ok());
    }

    #[test]
    fn capture_nonzero_with_stderr_returns_err_with_stderr_text() {
        let hook = HookConfig {
            on: "*".into(),
            run: Some("echo 'something went wrong' >&2; exit 1".into()),
            timeout_ms: Some(5_000),
            ..Default::default()
        };
        let result = run_shell_hook_capture(&hook, &demo_event());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("something went wrong"));
    }

    #[test]
    fn capture_nonzero_no_stderr_returns_err_with_status() {
        let hook = HookConfig {
            on: "*".into(),
            run: Some("exit 2".into()),
            timeout_ms: Some(5_000),
            ..Default::default()
        };
        let result = run_shell_hook_capture(&hook, &demo_event());
        assert!(result.is_err());
        let msg = result.unwrap_err();
        assert!(
            msg.contains("exit") || msg.contains("status") || msg.contains('2'),
            "got: {msg}"
        );
    }

    // ── run_shell_hook (env var injection) ────────────────────────────────

    #[test]
    fn run_shell_hook_writes_env_var_to_tempfile() {
        use std::io::Read;

        let dir = tempfile::tempdir().unwrap();
        let out_file = dir.path().join("out.txt");
        let out_path = out_file.to_str().unwrap().to_string();

        let hook = HookConfig {
            on: "*".into(),
            run: Some(format!("echo $RUNKON_NOTIFY_KIND > '{out_path}'")),
            timeout_ms: Some(5_000),
            ..Default::default()
        };

        run_shell_hook(&hook, &demo_event());

        // Allow the child process time to flush.
        std::thread::sleep(Duration::from_millis(200));

        let mut contents = String::new();
        std::fs::File::open(&out_file)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();
        assert_eq!(contents.trim(), "workflow_run.completed");
    }

    // ── HookRunner ────────────────────────────────────────────────────────

    #[test]
    fn hook_runner_no_hooks_fires_nothing() {
        let runner = HookRunner::new(&[]);
        runner.fire(&demo_event()); // must not panic or hang
    }

    #[test]
    fn hook_runner_non_matching_hook_not_spawned() {
        let hook = HookConfig {
            on: "agent_run.*".into(),
            run: None,
            url: None,
            ..Default::default()
        };
        let runner = HookRunner::new(&[hook]);
        runner.fire(&demo_event()); // workflow_run.completed does not match agent_run.*
    }

    #[test]
    fn hook_runner_fires_matching_shell_hook() {
        use std::io::Read;

        let dir = tempfile::tempdir().unwrap();
        let out_file = dir.path().join("fired.txt");
        let out_path = out_file.to_str().unwrap().to_string();

        let hook = HookConfig {
            on: "workflow_run.*".into(),
            run: Some(format!("echo fired > '{out_path}'")),
            timeout_ms: Some(5_000),
            ..Default::default()
        };
        let runner = HookRunner::new(&[hook]);
        runner.fire(&demo_event());

        std::thread::sleep(Duration::from_millis(500));

        let mut contents = String::new();
        std::fs::File::open(&out_file)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();
        assert_eq!(contents.trim(), "fired");
    }

    // ── HTTP hook tests ──────────────────────────────────────────────────
    //
    // A minimal in-process TCP mock server: bind to 127.0.0.1:0, accept one
    // connection, read the request bytes until "\r\n\r\n" + Content-Length,
    // reply 200 OK, and surface the captured request via a channel.

    fn spawn_http_mock() -> (String, std::sync::mpsc::Receiver<String>) {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = std::sync::mpsc::channel();

        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                let mut buf = Vec::new();
                let mut tmp = [0u8; 1024];
                let mut content_length: usize = 0;
                let mut headers_done = false;
                let mut header_end = 0usize;

                while let Ok(n) = stream.read(&mut tmp) {
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..n]);

                    if !headers_done {
                        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            headers_done = true;
                            header_end = pos + 4;
                            let header_str = String::from_utf8_lossy(&buf[..pos]);
                            for line in header_str.split("\r\n") {
                                if let Some(rest) =
                                    line.to_ascii_lowercase().strip_prefix("content-length:")
                                {
                                    content_length = rest.trim().parse().unwrap_or(0);
                                }
                            }
                        }
                    }

                    if headers_done && buf.len() >= header_end + content_length {
                        break;
                    }
                }

                let _ = tx.send(String::from_utf8_lossy(&buf).to_string());
                let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
                let _ = stream.flush();
            }
        });

        (format!("http://127.0.0.1:{port}/"), rx)
    }

    #[test]
    fn run_http_hook_posts_event_json() {
        let (url, rx) = spawn_http_mock();
        let hook = HookConfig {
            on: "*".into(),
            url: Some(url),
            timeout_ms: Some(5_000),
            ..Default::default()
        };
        run_http_hook(&hook, &demo_event());

        let received = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(received.starts_with("POST "), "request: {received}");
        assert!(received.contains("workflow_run.completed"));
        assert!(received.contains("Workflow finished"));
    }

    #[test]
    fn run_http_hook_resolves_env_var_header() {
        let (url, rx) = spawn_http_mock();
        std::env::set_var("__RUNKON_NOTIFY_TEST_HTTP_AUTH__", "Bearer abc123");
        let mut headers = HashMap::new();
        headers.insert(
            "Authorization".to_string(),
            "$__RUNKON_NOTIFY_TEST_HTTP_AUTH__".to_string(),
        );
        let hook = HookConfig {
            on: "*".into(),
            url: Some(url),
            headers: Some(headers),
            timeout_ms: Some(5_000),
            ..Default::default()
        };
        run_http_hook(&hook, &demo_event());
        std::env::remove_var("__RUNKON_NOTIFY_TEST_HTTP_AUTH__");

        let received = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(
            received
                .to_ascii_lowercase()
                .contains("authorization: bearer abc123"),
            "request: {received}"
        );
    }

    #[test]
    fn run_http_hook_swallows_unreachable_url() {
        // Port 1 is reserved/unused on every platform — connect should fail
        // immediately. The hook must swallow the error without panicking.
        let hook = HookConfig {
            on: "*".into(),
            url: Some("http://127.0.0.1:1/".into()),
            timeout_ms: Some(500),
            ..Default::default()
        };
        run_http_hook(&hook, &demo_event()); // must not panic
    }

    #[test]
    fn hook_runner_fires_matching_http_hook() {
        let (url, rx) = spawn_http_mock();
        let hook = HookConfig {
            on: "workflow_run.*".into(),
            url: Some(url),
            timeout_ms: Some(5_000),
            ..Default::default()
        };
        let runner = HookRunner::new(&[hook]);
        runner.fire(&demo_event());

        let received = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(received.starts_with("POST "), "request: {received}");
        assert!(received.contains("workflow_run.completed"));
    }

    #[test]
    fn run_test_dispatches_http_hook_and_returns_ok() {
        let (url, rx) = spawn_http_mock();
        let hook = HookConfig {
            on: "*".into(),
            url: Some(url),
            timeout_ms: Some(5_000),
            ..Default::default()
        };
        let runner = HookRunner::new(&[hook]);
        let result = runner.run_test(&demo_event());
        assert!(result.is_ok());

        let received = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(received.starts_with("POST "), "request: {received}");
    }
}
