//! Integration tests for the Gemini CLI recipe via CliRuntime.
//!
//! Each test writes a stub shell binary, wires it into a CliRuntime configured
//! with the recipe from docs/recipes/gemini-cli.md, and asserts on the emitted
//! RuntimeEvents. No real `gemini` binary is required.

#![cfg(unix)]

use std::io::Write as _;
use std::os::unix::fs::PermissionsExt as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use runkon_runtimes::agent_def::{AgentDef, AgentRole};
use runkon_runtimes::config::RuntimeConfig;
use runkon_runtimes::runtime::cli::CliRuntime;
use runkon_runtimes::runtime::{AgentRuntime, RuntimeRequest};
use runkon_runtimes::tracker::{EventSink, NoopTracker, RuntimeEvent};

// ── helpers ──────────────────────────────────────────────────────────────────

#[derive(Default, Clone)]
struct RecordingSink {
    events: Arc<Mutex<Vec<RuntimeEvent>>>,
}

impl EventSink for RecordingSink {
    fn on_event(&self, _run_id: &str, event: RuntimeEvent) {
        self.events.lock().unwrap().push(event);
    }
}

fn gemini_recipe_config(binary: &str) -> RuntimeConfig {
    RuntimeConfig {
        runtime_type: Some("cli".to_string()),
        binary: Some(binary.to_string()),
        args: Some(vec![
            "--prompt".to_string(),
            "{{prompt}}".to_string(),
            "--model".to_string(),
            "{{model}}".to_string(),
            "--output-format".to_string(),
            "json".to_string(),
        ]),
        prompt_via: Some("arg".to_string()),
        default_model: Some("gemini-2.5-flash".to_string()),
        result_field: Some("response".to_string()),
        token_fields: Some("stats.models.*.tokens.total".to_string()),
        ..RuntimeConfig::default()
    }
}

fn make_request(run_id: &str, sink: Arc<RecordingSink>) -> RuntimeRequest {
    RuntimeRequest {
        run_id: run_id.to_string(),
        agent_def: AgentDef {
            name: "test-agent".to_string(),
            role: AgentRole::Reviewer,
            can_commit: false,
            model: None,
            runtime: "gemini".to_string(),
            prompt: String::new(),
        },
        effective_runtime: "gemini".to_string(),
        prompt: "Say hello".to_string(),
        working_dir: std::path::PathBuf::from("/tmp"),
        model: None,
        extra_cli_args: vec![],
        plugin_dirs: vec![],
        resume_session_id: None,
        tracker: Arc::new(NoopTracker),
        event_sink: sink,
    }
}

fn write_stub(dir: &std::path::Path, name: &str, body: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).unwrap();
    writeln!(f, "#!/bin/sh").unwrap();
    write!(f, "{body}").unwrap();
    drop(f);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// Gemini success shape: response + wildcard token sum parsed correctly.
#[test]
fn gemini_success_shape_parses() {
    let tmp = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();

    let stub = write_stub(
        tmp.path(),
        "gemini-success.sh",
        r#"cat <<'EOF'
{"session_id":"test-session","response":"hello from gemini","stats":{"models":{"gemini-2.5-flash":{"tokens":{"total":500}}}}}
EOF
"#,
    );

    let config = gemini_recipe_config(stub.to_str().unwrap());
    let runtime = CliRuntime::new(config, workspace.path().to_path_buf());

    let sink = Arc::new(RecordingSink::default());
    let request = make_request("gemini-success", sink.clone());

    runtime.spawn_validated(&request).unwrap();
    let _ = runtime.poll("gemini-success", None, Duration::from_secs(10));

    let events = sink.events.lock().unwrap();
    let completed = events.iter().find_map(|e| {
        if let RuntimeEvent::Completed {
            result_text,
            input_tokens,
            ..
        } = e
        {
            Some((result_text.clone(), *input_tokens))
        } else {
            None
        }
    });

    let (result_text, input_tokens) =
        completed.expect("expected RuntimeEvent::Completed from Gemini success shape");

    assert_eq!(
        result_text.as_deref(),
        Some("hello from gemini"),
        "result_text must be the `response` field value"
    );
    assert_eq!(
        input_tokens,
        Some(500),
        "input_tokens must be the wildcard-summed tokens.total"
    );
}

/// Gemini fatal error shape: non-zero exit emits RuntimeEvent::Failed.
#[test]
fn gemini_error_shape_emits_failed() {
    let tmp = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();

    let stub = write_stub(
        tmp.path(),
        "gemini-error.sh",
        r#"cat <<'EOF'
{"session_id":"test-session","error":{"type":"FatalCancellationError","message":"Operation cancelled.","code":130}}
EOF
exit 130
"#,
    );

    let config = gemini_recipe_config(stub.to_str().unwrap());
    let runtime = CliRuntime::new(config, workspace.path().to_path_buf());

    let sink = Arc::new(RecordingSink::default());
    let request = make_request("gemini-error", sink.clone());

    runtime.spawn_validated(&request).unwrap();
    let _ = runtime.poll("gemini-error", None, Duration::from_secs(10));

    let events = sink.events.lock().unwrap();
    let failed = events.iter().find_map(|e| {
        if let RuntimeEvent::Failed { error, .. } = e {
            Some(error.clone())
        } else {
            None
        }
    });

    let error_text = failed.expect("expected RuntimeEvent::Failed from Gemini error shape");
    assert!(
        error_text.contains("FatalCancellationError") || error_text.contains("130"),
        "error must reference the Gemini error payload, got: {error_text}"
    );
}

/// --resume flag must appear in the spawned argv when resume_session_id is set.
#[test]
fn gemini_resume_flag_forwarded() {
    let tmp = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let args_file = tmp.path().join("captured-args.txt");

    let args_file_str = args_file.to_str().unwrap();
    let stub_body = format!(
        r#"printf '%s\n' "$@" > {args_file_str}
printf '{{"response": "ok"}}\n'
"#
    );
    let stub = write_stub(tmp.path(), "gemini-resume.sh", &stub_body);

    let config = gemini_recipe_config(stub.to_str().unwrap());
    let runtime = CliRuntime::new(config, workspace.path().to_path_buf());

    let sink = Arc::new(RecordingSink::default());
    let mut request = make_request("gemini-resume", sink.clone());
    request.resume_session_id = Some("test-session-id".to_string());

    runtime.spawn_validated(&request).unwrap();
    let _ = runtime.poll("gemini-resume", None, Duration::from_secs(10));

    // Give the stub a moment to flush the file before reading.
    std::thread::sleep(Duration::from_millis(50));

    let captured = std::fs::read_to_string(&args_file).unwrap_or_default();

    let args: Vec<&str> = captured.lines().collect();
    let has_resume_flag = args.contains(&"--resume");
    let has_session_id = args.contains(&"test-session-id");

    assert!(
        has_resume_flag,
        "--resume must appear in spawned argv; captured args: {args:?}"
    );
    assert!(
        has_session_id,
        "'test-session-id' must appear in spawned argv; captured args: {args:?}"
    );

    // Verify --resume immediately precedes the session id.
    let resume_pos = args.iter().position(|a| *a == "--resume");
    let id_pos = args.iter().position(|a| *a == "test-session-id");
    if let (Some(r), Some(i)) = (resume_pos, id_pos) {
        assert_eq!(
            i,
            r + 1,
            "--resume must immediately precede the session id in argv"
        );
    }
}
