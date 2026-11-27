use std::collections::{HashMap, VecDeque};
use std::io::Read;
use std::path::Path;

use crate::dsl::{InputDecl, InputType, ScriptNode, WorkflowDef};
use crate::engine::{handle_on_fail, record_step_success, ExecutionState};
use crate::engine_error::Result;
use crate::prompt_builder::build_variable_map;
use crate::traits::persistence::StepUpdate;

use wait_timeout::ChildExt;

/// Variables that neither workflow inputs nor the `env = { ... }` block may
/// set: overriding them would change what the script loads or who it acts as.
const SENSITIVE_ENV_VARS: &[&str] = &[
    "LD_PRELOAD",
    "LD_LIBRARY_PATH",
    "DYLD_LIBRARY_PATH",
    "PATH",
    "DYLD_INSERT_LIBRARIES",
    "PYTHONPATH",
    "RUBYLIB",
    "NODE_PATH",
    // Protect the bot-identity token resolved by `as = "..."` from being
    // silently overwritten by a workflow-authored env block.
    "GH_TOKEN",
];

/// A script step `env` value that references a declared input with no value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnresolvedEnvVar {
    pub step: String,
    pub env_key: String,
    pub variable: String,
}

impl std::fmt::Display for UnresolvedEnvVar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "unresolved variable {{{{{}}}}} in env {} of step {}",
            self.variable, self.env_key, self.step
        )
    }
}

/// Check one script step's `env` without executing it.
///
/// Reports each `{{name}}` in an env value where `name` is a declared input that
/// should have a value (required, has a `default`, or boolean) but is missing from
/// `inputs`. Optional inputs with no default and undeclared names are not reported.
/// The engine runs this at run start; `execute_script` called directly does not.
pub fn check_script_env(
    node: &ScriptNode,
    input_decls: &[InputDecl],
    inputs: &HashMap<String, String>,
) -> Vec<UnresolvedEnvVar> {
    let mut keys: Vec<&String> = node.env.keys().collect();
    keys.sort();
    let mut out = Vec::new();
    for key in keys {
        if key.contains('=') || key.contains('\0') || SENSITIVE_ENV_VARS.contains(&key.as_str()) {
            continue;
        }
        for name in crate::prompt_builder::placeholder_names(&node.env[key]) {
            let expected = input_decls.iter().any(|d| {
                d.name == name
                    && (d.required || d.default.is_some() || d.input_type == InputType::Boolean)
            });
            if expected && !inputs.contains_key(name) {
                let found = UnresolvedEnvVar {
                    step: node.name.clone(),
                    env_key: key.clone(),
                    variable: name.to_string(),
                };
                if !out.contains(&found) {
                    out.push(found);
                }
            }
        }
    }
    out
}

/// Run [`check_script_env`] on every script step of a workflow (body and `always`,
/// including steps nested in control-flow bodies).
pub fn check_workflow_script_env(
    def: &WorkflowDef,
    inputs: &HashMap<String, String>,
) -> Vec<UnresolvedEnvVar> {
    use crate::dsl::validation::collect_script_nodes;
    collect_script_nodes(&def.body)
        .into_iter()
        .chain(collect_script_nodes(&def.always))
        .flat_map(|n| check_script_env(n, &def.inputs, inputs))
        .collect()
}

/// How much of a script's stderr is kept in memory: the newest bytes.
const STDERR_CAPTURE_CAP: usize = 64 * 1024;
/// How much of a script's stdout is kept in memory: the newest bytes. Larger
/// than stderr's because it carries the FLOW_OUTPUT block, whose `context` and
/// extras can legitimately pass 64 KiB. A block cut by this cap fails to parse.
const STDOUT_CAPTURE_CAP: usize = 1024 * 1024;
/// Cap on a script log file (both sections together), in bytes.
const LOG_CAP: usize = 64 * 1024;
/// Share of [`LOG_CAP`] the stderr section may take when stdout needs room too.
const LOG_STDERR_BUDGET: usize = 48 * 1024;
/// How many characters of stderr a failed step's `step_error` keeps (the last).
const STEP_ERROR_STDERR_CHARS: usize = 2000;

/// What a script printed, as kept for its log.
struct CapturedOutput {
    stderr: String,
    /// Bytes of stderr dropped by the capture cap.
    stderr_dropped: u64,
    /// Stdout outside the FLOW_OUTPUT block.
    stray_stdout: String,
    /// Bytes of stdout dropped by the capture cap.
    stdout_dropped: u64,
}

/// How one run of the script ended.
enum ScriptAttempt {
    Succeeded {
        markers: Vec<String>,
        context: String,
        structured_output: Option<String>,
        duration_ms: i64,
        output: CapturedOutput,
    },
    /// A spawn error, timeout, non-zero exit or unparseable output block.
    /// `output` is `None` when the script never ran to produce any.
    Failed {
        error: String,
        output: Option<CapturedOutput>,
    },
}

impl ScriptAttempt {
    fn failed(error: String) -> Self {
        Self::Failed {
            error,
            output: None,
        }
    }
}

/// A byte buffer that keeps only the newest `cap` bytes pushed into it and
/// counts the bytes it dropped.
struct TailBuffer {
    buf: VecDeque<u8>,
    cap: usize,
    dropped: u64,
}

impl TailBuffer {
    fn new(cap: usize) -> Self {
        Self {
            buf: VecDeque::new(),
            cap,
            dropped: 0,
        }
    }

    fn push(&mut self, chunk: &[u8]) {
        if chunk.len() >= self.cap {
            self.dropped += (self.buf.len() + chunk.len() - self.cap) as u64;
            self.buf.clear();
            self.buf.extend(&chunk[chunk.len() - self.cap..]);
            return;
        }
        let over = (self.buf.len() + chunk.len()).saturating_sub(self.cap);
        self.buf.drain(..over);
        self.dropped += over as u64;
        self.buf.extend(chunk);
    }
}

/// The kept tail of a pipe and how many bytes were dropped before it.
struct Captured {
    bytes: Vec<u8>,
    dropped: u64,
}

/// Drop the continuation bytes a tail cut may have left at the front, so a cut
/// code point doesn't make the whole tail invalid UTF-8.
fn skip_partial_char(bytes: &[u8], dropped: u64) -> &[u8] {
    if dropped == 0 {
        return bytes;
    }
    let skip = bytes
        .iter()
        .take(3)
        .take_while(|b| (**b & 0b1100_0000) == 0b1000_0000)
        .count();
    &bytes[skip..]
}

/// Join a pipe reader thread and decode its bytes. A panicked reader yields an
/// empty string. Invalid UTF-8 is replaced when `lossy`, otherwise logged and
/// yields an empty string.
fn join_pipe(
    handle: std::thread::JoinHandle<Captured>,
    stream_name: &str,
    lossy: bool,
) -> (String, u64) {
    match handle.join() {
        Ok(captured) => {
            let bytes = skip_partial_char(&captured.bytes, captured.dropped);
            let text = if lossy {
                String::from_utf8_lossy(bytes).into_owned()
            } else {
                String::from_utf8(bytes.to_vec()).unwrap_or_else(|e| {
                    tracing::warn!("script: {stream_name} is not valid UTF-8: {e}");
                    String::new()
                })
            };
            (text, captured.dropped)
        }
        Err(_) => {
            tracing::warn!("script: {stream_name} reader thread panicked");
            (String::new(), 0)
        }
    }
}

/// Spawn a reader thread BEFORE waiting for the child to avoid pipe deadlock.
/// If the child writes more than the OS pipe buffer size to either stream
/// while the parent is blocked in wait(), the child would hang forever. The
/// thread reads to EOF but keeps only the newest `cap` bytes.
fn spawn_reader_thread<R: Read + Send + 'static>(
    mut pipe: R,
    stream_name: &'static str,
    cap: usize,
) -> std::thread::JoinHandle<Captured> {
    std::thread::spawn(move || {
        let mut tail = TailBuffer::new(cap);
        let mut chunk = [0u8; 8192];
        loop {
            match pipe.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => tail.push(&chunk[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => {
                    tracing::warn!("script: failed to read {stream_name} pipe: {e}");
                    break;
                }
            }
        }
        let dropped = tail.dropped;
        Captured {
            bytes: Vec::from(tail.buf),
            dropped,
        }
    })
}

/// Join both readers into a [`CapturedOutput`], returning the full decoded
/// stdout alongside it for FLOW_OUTPUT parsing.
fn join_output(
    stdout_handle: std::thread::JoinHandle<Captured>,
    stderr_handle: std::thread::JoinHandle<Captured>,
) -> (String, CapturedOutput) {
    let (stdout, stdout_dropped) = join_pipe(stdout_handle, "stdout", false);
    let (stderr, stderr_dropped) = join_pipe(stderr_handle, "stderr", true);
    let output = CapturedOutput {
        stderr,
        stderr_dropped,
        stray_stdout: crate::helpers::strip_flow_output_block(&stdout),
        stdout_dropped,
    };
    (stdout, output)
}

/// The last `max_chars` characters of `s`.
fn last_chars(s: &str, max_chars: usize) -> &str {
    let count = s.chars().count();
    match s.char_indices().nth(count.saturating_sub(max_chars)) {
        Some((i, _)) => &s[i..],
        None => "",
    }
}

/// The last `budget` bytes of `s`, cut at a character boundary, and how many
/// bytes that left out.
fn tail_str(s: &str, budget: usize) -> (&str, usize) {
    if s.len() <= budget {
        return (s, 0);
    }
    let mut start = s.len() - budget;
    while !s.is_char_boundary(start) {
        start += 1;
    }
    (&s[start..], start)
}

/// One log section: its header, a truncation marker when bytes were dropped
/// (by the capture cap or by `budget`), and the newest bytes that fit.
fn log_section(header: &str, text: &str, reader_dropped: u64, budget: usize) -> String {
    let (tail, cut) = tail_str(text, budget);
    let dropped = reader_dropped + cut as u64;
    let mut section = format!("--- {header} ---\n");
    if dropped > 0 {
        section.push_str(&format!("… [truncated {dropped} bytes]\n"));
    }
    section.push_str(tail);
    if !section.ends_with('\n') {
        section.push('\n');
    }
    section
}

/// Write the script log to `path`. Returns `Ok(false)` without creating a file
/// when there is nothing to log. The log is about [`LOG_CAP`] bytes: each
/// section keeps its newest bytes, and a stream that needs less than its share
/// leaves the rest to the other.
fn write_script_log(path: &Path, out: &CapturedOutput) -> std::io::Result<bool> {
    let has_stderr = !out.stderr.trim().is_empty() || out.stderr_dropped > 0;
    let has_stdout = !out.stray_stdout.trim().is_empty() || out.stdout_dropped > 0;
    if !has_stderr && !has_stdout {
        return Ok(false);
    }
    let stderr_len = out.stderr.len();
    let stdout_len = out.stray_stdout.len();
    let stderr_budget = if stdout_len <= LOG_CAP - LOG_STDERR_BUDGET {
        LOG_CAP - stdout_len
    } else {
        stderr_len.min(LOG_STDERR_BUDGET)
    };
    let stdout_budget = LOG_CAP - stderr_len.min(stderr_budget);

    let mut log = String::new();
    if has_stderr {
        log.push_str(&log_section(
            "stderr",
            &out.stderr,
            out.stderr_dropped,
            stderr_budget,
        ));
    }
    if has_stdout {
        log.push_str(&log_section(
            "stdout (outside FLOW_OUTPUT)",
            &out.stray_stdout,
            out.stdout_dropped,
            stdout_budget,
        ));
    }
    // The log holds raw script output, which can contain tokens (`set -x`
    // traces, error text), so create it owner-only on unix.
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            create_private_dir_all(parent)?;
        }
    }
    write_private_file(path, log.as_bytes())?;
    Ok(true)
}

/// `create_dir_all` with mode 0o700 for newly created directories on unix.
fn create_private_dir_all(dir: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(dir)
}

/// Write `data` to `path` (truncating), creating it with mode 0o600 on unix.
/// A pre-existing file is tightened to 0o600 as well.
fn write_private_file(path: &Path, data: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    file.write_all(data)
}

/// Write the log for one attempt when the host asked for one. Returns the path
/// to record on the step row: only a file that was written. A write failure is
/// logged and never fails the step.
fn write_attempt_log(path: Option<&Path>, output: Option<&CapturedOutput>) -> Option<String> {
    let (path, output) = (path?, output?);
    match write_script_log(path, output) {
        Ok(true) => Some(path.to_string_lossy().into_owned()),
        Ok(false) => None,
        Err(e) => {
            tracing::warn!(
                "script: failed to write log {}: {e}",
                path.to_string_lossy()
            );
            None
        }
    }
}

/// Run the script once. Persists nothing and leaves `step_results` alone.
fn run_script_once(
    node: &ScriptNode,
    script_cmd: &str,
    env_vars: &std::collections::HashMap<String, String>,
    working_dir: &str,
) -> ScriptAttempt {
    let mut cmd = std::process::Command::new("sh");
    cmd.arg("-c").arg(script_cmd);
    cmd.current_dir(working_dir);
    for (k, v) in env_vars {
        cmd.env(k, v);
    }
    // The host's stdin may carry a protocol stream; a step must never read it.
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return ScriptAttempt::failed(format!("Script '{}' failed to execute: {e}", node.name));
        }
    };

    let stdout_handle = match child.stdout.take() {
        Some(p) => spawn_reader_thread(p, "stdout", STDOUT_CAPTURE_CAP),
        None => {
            let _ = child.kill();
            let _ = child.wait();
            return ScriptAttempt::failed(format!(
                "script '{}': stdout pipe unavailable after spawn",
                node.name
            ));
        }
    };
    let stderr_handle = match child.stderr.take() {
        Some(p) => spawn_reader_thread(p, "stderr", STDERR_CAPTURE_CAP),
        None => {
            let _ = child.kill();
            let _ = child.wait();
            let _ = stdout_handle.join();
            return ScriptAttempt::failed(format!(
                "script '{}': stderr pipe unavailable after spawn",
                node.name
            ));
        }
    };

    let start = std::time::Instant::now();

    let status = if let Some(timeout_secs) = node.timeout {
        let timeout = std::time::Duration::from_secs(timeout_secs);
        match child.wait_timeout(timeout) {
            Ok(Some(s)) => s,
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                let (_, output) = join_output(stdout_handle, stderr_handle);
                return ScriptAttempt::Failed {
                    error: format!("Script '{}' timed out after {}s", node.name, timeout_secs),
                    output: Some(output),
                };
            }
            Err(e) => {
                let (_, output) = join_output(stdout_handle, stderr_handle);
                return ScriptAttempt::Failed {
                    error: format!("Script '{}' wait failed: {e}", node.name),
                    output: Some(output),
                };
            }
        }
    } else {
        match child.wait() {
            Ok(s) => s,
            Err(e) => {
                let (_, output) = join_output(stdout_handle, stderr_handle);
                return ScriptAttempt::Failed {
                    error: format!("Script '{}' wait failed: {e}", node.name),
                    output: Some(output),
                };
            }
        }
    };
    let duration_ms = start.elapsed().as_millis() as i64;

    let (stdout, output) = join_output(stdout_handle, stderr_handle);

    if !status.success() {
        let exit_code = status.code().unwrap_or(-1);

        // The reason a script fails is usually at the end of its stderr.
        let captured_stderr = last_chars(output.stderr.trim(), STEP_ERROR_STDERR_CHARS);

        let error = if captured_stderr.is_empty() {
            format!("Script '{}' exited with code {}", node.name, exit_code)
        } else {
            format!(
                "Script '{}' exited with code {}\n{}",
                node.name, exit_code, captured_stderr
            )
        };
        return ScriptAttempt::Failed {
            error,
            output: Some(output),
        };
    }

    tracing::info!(
        "script '{}': completed successfully in {}ms",
        node.name,
        duration_ms
    );

    // Parse FLOW_OUTPUT once; keep the full struct so we can persist its
    // JSON shape (including any `extras` fields) as structured_output for
    // downstream variable injection — see prompt_builder::build_variable_map.
    let (markers, context, structured_output) = match crate::helpers::parse_flow_output(&stdout) {
        Some(out) => {
            let json = serde_json::to_string(&out)
                .map_err(|e| {
                    tracing::warn!(
                        step = %node.name,
                        error = %e,
                        "script: failed to re-serialize FlowOutput as structured_output \
                         — downstream `{{name}}` variable injection from this step's \
                         extras will be unavailable",
                    );
                })
                .ok();
            (out.markers, out.context, json)
        }
        None => {
            // An output block that didn't parse must fail the step: recording
            // `markers: []` would silently turn every downstream `if` false.
            if let Some(marker) = crate::helpers::find_output_block_marker(&stdout) {
                let detail = match marker {
                    "<<<FLOW_OUTPUT>>>" => "a <<<FLOW_OUTPUT>>> block that could not be \
                         parsed (invalid JSON or missing <<<END_FLOW_OUTPUT>>>)"
                        .to_string(),
                    other => format!(
                        "an unrecognised {other} block; expected <<<FLOW_OUTPUT>>> … \
                         <<<END_FLOW_OUTPUT>>>"
                    ),
                };
                // Log the whole stdout: the bad block is what to look at.
                let output = CapturedOutput {
                    stray_stdout: stdout,
                    ..output
                };
                return ScriptAttempt::Failed {
                    error: format!("Script '{}' ({}) printed {detail}", node.name, node.run),
                    output: Some(output),
                };
            }
            let ctx: String = stdout.chars().take(2000).collect();
            (vec![], ctx, None)
        }
    };

    ScriptAttempt::Succeeded {
        markers,
        context,
        structured_output,
        duration_ms,
        output,
    }
}

pub fn execute_script(state: &mut ExecutionState, node: &ScriptNode, iteration: u32) -> Result<()> {
    let pos = state.position;
    state.position += 1;

    // Skip completed script steps on resume
    if super::skip_if_already_completed(state, &node.name, iteration, &node.name) {
        return Ok(());
    }

    if state.exec_config.dry_run {
        let step_id = super::insert_step_record(
            state,
            &node.name,
            crate::constants::STEP_ROLE_SCRIPT,
            pos,
            iteration,
            Some(0),
        )?;
        tracing::info!("script '{}': dry-run, skipping execution", node.name);
        super::persist_completed_step(
            state,
            &step_id,
            None,
            Some("dry-run: script not executed".to_string()),
            None,
            None,
            0,
            None,
            None,
        )?;
        record_step_success(
            state,
            node.name.clone(),
            crate::types::StepSuccess {
                step_name: node.name.clone(),
                result_text: Some("dry-run: script not executed".to_string()),
                iteration,
                ..crate::types::StepSuccess::default()
            },
        );
        return Ok(());
    }

    // `run_inner` rejects an invalid prefix before the run starts; this covers
    // callers that execute a script step directly. The error is about the
    // configuration, so it is never retried.
    let input_prefix = state.script_env_provider.input_env_prefix().to_string();
    if let Err(e) = crate::traits::script_env_provider::validate_input_env_prefix(&input_prefix) {
        let step_id = super::insert_step_record(
            state,
            &node.name,
            crate::constants::STEP_ROLE_SCRIPT,
            pos,
            iteration,
            Some(0),
        )?;
        let err_msg = format!("Script '{}': {e}", node.name);
        tracing::warn!("{}", err_msg);
        mark_attempt_failed(state, &step_id, &err_msg, 0, None)?;
        return fail_step(state, node, err_msg, iteration, 1);
    }

    // Build variable map for substitution, shell-quoting all values to prevent
    // injection when they are interpolated into the sh -c command string.
    // `{{#if}}` sees the raw values; only rendered placeholders are quoted.
    let vars = build_variable_map(state);
    let script_cmd = crate::prompt_builder::substitute_with(
        &node.run,
        |k| vars.get(k).map(String::as_str),
        |v, out| out.push_str(&crate::prompt_builder::shell_quote(v)),
        crate::prompt_builder::Unresolved::Strip,
    );

    tracing::info!("script '{}': executing command", node.name);

    // Build environment variables
    let mut env_vars: std::collections::HashMap<String, String> = std::collections::HashMap::new();

    // Inject PATH, GH_TOKEN (when `as = "..."` resolves to a host-known bot),
    // and other env from the script env provider. Falls back to the
    // workflow-level default bot when the step doesn't specify one.
    {
        let effective_bot = node
            .as_identity
            .as_deref()
            .or(state.default_as_identity.as_deref());
        let provider_env = state
            .script_env_provider
            .env(state.run_ctx.as_ref(), effective_bot);
        env_vars.extend(provider_env);
    }

    // Inject all current workflow inputs as env vars, prefixed with the
    // provider's `input_env_prefix()` (`FLOW_` by default).
    // Validate that keys consist only of alphanumeric characters and underscores to
    // prevent malformed env var names if a key contains `=` or a null byte.
    for (k, v) in &state.inputs {
        if !k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            tracing::warn!(
                "script '{}': input key {:?} contains characters invalid in an env var name, skipping",
                node.name,
                k
            );
            continue;
        }
        let name = format!("{input_prefix}{}", k.to_uppercase());
        if SENSITIVE_ENV_VARS.contains(&name.as_str()) {
            tracing::warn!(
                "script '{}': input {:?} would set security-sensitive variable {:?} — skipping",
                node.name,
                k,
                name
            );
            continue;
        }
        env_vars.insert(name, v.clone());
    }

    // Inject explicit env vars from the workflow `env = { ... }` block.
    // Template variables (e.g. `{{prior_output}}`) are substituted using the raw
    // (non-shell-quoted) variable map because values are passed as discrete env
    // var values, not interpolated into a shell command string.
    for (k, v) in &node.env {
        if k.contains('=') || k.contains('\0') {
            tracing::warn!(
                "script '{}': env key {:?} contains '=' or null byte, skipping",
                node.name,
                k
            );
            continue;
        }
        if SENSITIVE_ENV_VARS.contains(&k.as_str()) {
            tracing::warn!(
                "script '{}': env block overrides security-sensitive variable {:?} — skipping",
                node.name,
                k
            );
            continue;
        }
        let resolved = crate::prompt_builder::substitute_variables(v, &vars);
        env_vars.insert(k.clone(), resolved);
    }

    let working_dir = state.run_ctx.working_dir_str();

    // One step row per attempt, as `call` records its retries. Retries are
    // immediate: `call` has no backoff either.
    let max_attempts = 1 + node.retries;
    let mut last_error = String::new();
    for attempt in 0..max_attempts {
        let step_id = super::begin_retry_attempt(
            state,
            &node.name,
            crate::constants::STEP_ROLE_SCRIPT,
            pos,
            iteration,
            attempt,
        )?;

        let log_path = state.script_env_provider.script_log_path(
            state.run_ctx.as_ref(),
            &step_id,
            &node.name,
            iteration,
        );

        match run_script_once(node, &script_cmd, &env_vars, &working_dir) {
            ScriptAttempt::Succeeded {
                markers,
                context,
                structured_output,
                duration_ms,
                output,
            } => {
                let log_file = write_attempt_log(log_path.as_deref(), Some(&output));
                let markers_json = crate::helpers::serialize_or_empty_array(
                    &markers,
                    &format!("script '{}'", node.name),
                );

                // The log path goes on the step row only: `StepSuccess.output_file`
                // stays `None` so `{{prior_output_file}}` is unchanged.
                super::persist_completed_step(
                    state,
                    &step_id,
                    None,
                    Some(format!("Script '{}' completed", node.name)),
                    Some(context.clone()),
                    Some(markers_json),
                    attempt,
                    structured_output.clone(),
                    log_file,
                )?;

                record_step_success(
                    state,
                    node.name.clone(),
                    crate::types::StepSuccess {
                        step_name: node.name.clone(),
                        result_text: Some(format!("Script '{}' completed", node.name)),
                        metadata: std::collections::HashMap::from([(
                            crate::constants::metadata_keys::DURATION_MS.to_string(),
                            duration_ms.to_string(),
                        )]),
                        markers,
                        context,
                        iteration,
                        structured_output,
                        ..crate::types::StepSuccess::default()
                    },
                );

                return Ok(());
            }
            ScriptAttempt::Failed {
                error: err_msg,
                output,
            } => {
                let log_file = write_attempt_log(log_path.as_deref(), output.as_ref());
                tracing::warn!(
                    "script '{}' (attempt {}/{}): {}",
                    node.name,
                    attempt + 1,
                    max_attempts,
                    err_msg
                );
                mark_attempt_failed(state, &step_id, &err_msg, attempt, log_file)?;
                last_error = err_msg;
            }
        }
    }

    fail_step(state, node, last_error, iteration, max_attempts)
}

/// Record a failed attempt on its step row.
fn mark_attempt_failed(
    state: &ExecutionState,
    step_id: &str,
    err_msg: &str,
    retry_count: u32,
    output_file: Option<String>,
) -> Result<()> {
    let generation = state.expect_lease_generation();
    state.persistence.update_step(
        step_id,
        StepUpdate::failed(generation, err_msg.to_string(), retry_count)
            .with_output_file(output_file),
    )?;
    Ok(())
}

/// Route a step that has failed for good through its `on_fail` policy.
fn fail_step(
    state: &mut ExecutionState,
    node: &ScriptNode,
    err_msg: String,
    iteration: u32,
    attempts: u32,
) -> Result<()> {
    handle_on_fail(
        state,
        node.name.clone(),
        &node.name,
        &node.on_fail,
        err_msg,
        node.retries,
        iteration,
        attempts,
    )
}

#[cfg(test)]
mod tests {

    #[cfg(unix)]
    #[test]
    fn script_log_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a/b/step.log");
        let out = CapturedOutput {
            stderr: "boom".to_string(),
            stderr_dropped: 0,
            stray_stdout: String::new(),
            stdout_dropped: 0,
        };
        assert!(write_script_log(&path, &out).unwrap());
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&path), 0o600);
        assert_eq!(mode(path.parent().unwrap()), 0o700);
    }

    #[cfg(unix)]
    #[test]
    fn script_log_tightens_and_replaces_existing_file() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("step.log");
        std::fs::write(&path, "old content that is much longer than the new log").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let out = CapturedOutput {
            stderr: "boom".to_string(),
            stderr_dropped: 0,
            stray_stdout: String::new(),
            stdout_dropped: 0,
        };
        assert!(write_script_log(&path, &out).unwrap());
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("boom"));
        assert!(!content.contains("old content"));
    }
    use super::*;
    use crate::dsl::{ScriptNode, WorkflowNode};
    use crate::engine::ExecutionState;
    use crate::persistence_memory::InMemoryWorkflowPersistence;
    use crate::status::WorkflowStepStatus;

    use crate::traits::persistence::{NewRun, WorkflowPersistence};
    use crate::traits::run_context::NoopRunContext;

    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_persistence() -> (Arc<InMemoryWorkflowPersistence>, String) {
        let p = Arc::new(InMemoryWorkflowPersistence::new());
        let run = p
            .create_run(NewRun {
                workflow_name: "wf".to_string(),
                parent_run_id: String::new(),
                dry_run: false,
                trigger: "manual".to_string(),
                definition_snapshot: None,
                parent_workflow_run_id: None,
            })
            .unwrap();
        (p, run.id)
    }

    fn make_state(persistence: Arc<InMemoryWorkflowPersistence>, run_id: String) -> ExecutionState {
        let mut state = crate::test_helpers::make_test_execution_state(persistence, run_id);
        state.run_ctx = Arc::new(NoopRunContext::default().with_working_dir(std::env::temp_dir()))
            as Arc<dyn crate::traits::run_context::RunContext>;
        state
    }

    fn make_node(name: &str, run: &str) -> ScriptNode {
        ScriptNode {
            name: name.to_string(),
            run: run.to_string(),
            env: Default::default(),
            timeout: None,
            retries: 0,
            on_fail: None,
            as_identity: None,
        }
    }

    fn decl(name: &str, required: bool, default: Option<&str>, ty: InputType) -> InputDecl {
        InputDecl {
            name: name.to_string(),
            required,
            default: default.map(str::to_string),
            description: None,
            input_type: ty,
        }
    }

    fn env_node(env: &[(&str, &str)]) -> ScriptNode {
        let mut node = make_node("check", "true");
        node.env = env
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        node
    }

    #[test]
    fn env_check_flags_defaulted_input_missing_from_inputs() {
        let decls = [decl(
            "auto_rereview",
            false,
            Some("true"),
            InputType::String,
        )];
        let node = env_node(&[("AUTO_REREVIEW", "{{auto_rereview}}")]);
        let found = check_script_env(&node, &decls, &HashMap::new());
        assert_eq!(found.len(), 1);
        assert_eq!(
            found[0].to_string(),
            "unresolved variable {{auto_rereview}} in env AUTO_REREVIEW of step check"
        );
    }

    #[test]
    fn env_check_allows_optional_undeclared_present_and_engine_vars() {
        let decls = [
            decl("opt", false, None, InputType::String),
            decl("d", false, Some("x"), InputType::String),
        ];
        let node = env_node(&[
            ("A", "{{opt}}"),
            ("B", "{{whatever}}"),
            ("C", "{{prior_output}}"),
            ("D", "{{d}}"),
        ]);
        let inputs = HashMap::from([("d".to_string(), String::new())]);
        assert!(check_script_env(&node, &decls, &inputs).is_empty());
    }

    #[test]
    fn env_check_flags_required_and_boolean_inputs() {
        let decls = [
            decl("req", true, None, InputType::String),
            decl("flag", false, None, InputType::Boolean),
        ];
        let node = env_node(&[("A", "{{req}}"), ("B", "{{ flag }}")]);
        let found = check_script_env(&node, &decls, &HashMap::new());
        let vars: Vec<&str> = found.iter().map(|f| f.variable.as_str()).collect();
        assert_eq!(vars, vec!["req", "flag"]);
    }

    #[test]
    fn workflow_env_check_finds_body_and_always_steps() {
        let mut def = crate::test_helpers::make_def(
            "w",
            vec![WorkflowNode::Script(env_node(&[("X", "{{x}}")]))],
        );
        def.inputs = vec![decl("x", false, Some("1"), InputType::String)];
        let mut always = env_node(&[("X", "{{x}}")]);
        always.name = "cleanup".to_string();
        def.always = vec![WorkflowNode::Script(always)];
        let found = check_workflow_script_env(&def, &HashMap::new());
        let steps: Vec<&str> = found.iter().map(|f| f.step.as_str()).collect();
        assert_eq!(steps, vec!["check", "cleanup"]);
        let supplied = HashMap::from([("x".to_string(), "1".to_string())]);
        assert!(check_workflow_script_env(&def, &supplied).is_empty());
    }

    /// When the script emits a valid FLOW_OUTPUT block, markers and context
    /// must be extracted and stored on the step record.
    #[test]
    fn flow_output_markers_propagate_to_step_record() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        // Use printf to avoid shell newline differences across platforms.
        let script = concat!(
            "printf '<<<FLOW_OUTPUT>>>\\n",
            r#"{"markers":["test_passed"],"context":"step ctx"}"#,
            "\\n<<<END_FLOW_OUTPUT>>>\\n'"
        );
        let node = make_node("check", script);
        execute_script(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Completed);
        let markers: Vec<String> = step
            .markers_out
            .as_deref()
            .and_then(|m| serde_json::from_str(m).ok())
            .unwrap_or_default();
        assert_eq!(markers, vec!["test_passed"]);
        assert_eq!(step.context_out.as_deref(), Some("step ctx"));
    }

    /// `{{#if}}` sees the raw value while a rendered `{{var}}` is shell-quoted.
    #[test]
    fn script_if_branch_and_quoted_value() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        state
            .inputs
            .insert("greeting".to_string(), "it's a $HOME `x`".to_string());
        state.inputs.insert("empty".to_string(), String::new());
        let node = make_node(
            "quote",
            "printf '%s|' {{#if greeting}}{{greeting}}{{else}}none{{/if}} {{#if empty}}E{{else}}empty-branch{{/if}}",
        );
        execute_script(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps[0].status, WorkflowStepStatus::Completed);
        let ctx = steps[0].context_out.as_deref().unwrap_or("");
        assert!(
            ctx.contains("it's a $HOME `x`|empty-branch|"),
            "value must arrive as one literal argument, correct branches taken: {ctx:?}"
        );
    }

    /// When the script produces no FLOW_OUTPUT block, context falls back to
    /// raw stdout truncated to 2000 characters.
    #[test]
    fn falls_back_to_raw_stdout_when_no_flow_output_block() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let node = make_node("info", "echo 'plain output'");
        execute_script(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Completed);
        let ctx = step.context_out.as_deref().unwrap_or("");
        assert!(
            ctx.contains("plain output"),
            "context should contain stdout: {ctx:?}"
        );
        let markers: Vec<String> = step
            .markers_out
            .as_deref()
            .and_then(|m| serde_json::from_str(m).ok())
            .unwrap_or_default();
        assert!(markers.is_empty(), "no markers expected for plain stdout");
    }

    /// A step reads nothing from the host's stdin, which keeps its pending data.
    #[test]
    fn script_step_does_not_read_host_stdin() {
        use runkon_test_support::*;

        if !is_stdin_probe_child() {
            run_in_child_with_piped_stdin(
                "executors::script::tests::script_step_does_not_read_host_stdin",
            );
            return;
        }

        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let node = make_node("probe", "printf '[%s]' \"$(cat)\"");
        execute_script(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps[0].status, WorkflowStepStatus::Completed);
        assert_eq!(steps[0].context_out.as_deref().unwrap_or("").trim(), "[]");

        assert_stdin_untouched_and_report();
    }

    /// A script that prints an unrecognised output block (e.g. from an older
    /// protocol) must fail the step instead of silently recording `markers: []`.
    #[test]
    fn unrecognised_output_block_fails_step() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let script = concat!(
            "printf '<<<LEGACY_OUTPUT>>>\\n",
            r#"{"markers":["has_code_changes"],"context":"Found 6 code files"}"#,
            "\\n<<<END_LEGACY_OUTPUT>>>\\n'"
        );
        let node = make_node("detect-file-types", script);
        let result = execute_script(&mut state, &node, 0);
        assert!(
            result.is_err(),
            "expected failure for unrecognised output block"
        );

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Failed);
        let err_text = step.result_text.as_deref().unwrap_or("");
        assert!(
            err_text.contains("detect-file-types") && err_text.contains("LEGACY_OUTPUT"),
            "error should name the step and the marker found: {err_text}"
        );
    }

    /// A `<<<FLOW_OUTPUT>>>` block with invalid JSON must fail the step.
    #[test]
    fn malformed_flow_output_block_fails_step() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let script =
            "printf '<<<FLOW_OUTPUT>>>\\n{\"markers\": [oops]}\\n<<<END_FLOW_OUTPUT>>>\\n'";
        let node = make_node("broken", script);
        let result = execute_script(&mut state, &node, 0);
        assert!(result.is_err(), "expected failure for unparseable block");

        let steps = persistence.get_steps(&run_id).unwrap();
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Failed);
        let err_text = step.result_text.as_deref().unwrap_or("");
        assert!(
            err_text.contains("could not be parsed"),
            "error should say the block could not be parsed: {err_text}"
        );
    }

    /// A one-line `<<<FLOW_OUTPUT>>> {…}` with no end marker must fail the step
    /// instead of completing with `markers: []` (#89).
    #[test]
    fn one_line_flow_output_without_end_marker_fails_step() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let script = "echo '<<<FLOW_OUTPUT>>> {\"markers\": [\"pushed_new_commits\"]}'";
        let node = make_node("push", script);
        let result = execute_script(&mut state, &node, 0);
        assert!(result.is_err(), "expected failure for one-line block");

        let steps = persistence.get_steps(&run_id).unwrap();
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Failed);
        let err_text = step.result_text.as_deref().unwrap_or("");
        assert!(
            err_text.contains("could not be parsed")
                && err_text.contains("missing <<<END_FLOW_OUTPUT>>>"),
            "error should point at the missing end marker: {err_text}"
        );
    }

    /// `on_fail = continue` still applies to an unparseable output block.
    #[test]
    fn unrecognised_output_block_respects_on_fail_continue() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let mut node = make_node(
            "lenient",
            "printf '<<<LEGACY_OUTPUT>>>\\n{}\\n<<<END_LEGACY_OUTPUT>>>\\n'",
        );
        node.on_fail = Some(crate::dsl::OnFail::Continue);
        execute_script(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps[0].status, WorkflowStepStatus::Failed);
        assert_eq!(
            state.step_results["lenient"].status,
            WorkflowStepStatus::Skipped
        );
    }

    /// When state.inputs contains a key with invalid env-var characters (e.g. `=`),
    /// that key must be silently dropped while a valid key is still injected.
    #[test]
    fn invalid_env_var_key_is_dropped_valid_key_is_injected() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());

        // One valid key and one invalid key (contains `=`).
        state
            .inputs
            .insert("VALID_KEY".to_string(), "hello".to_string());
        state
            .inputs
            .insert("INVALID=KEY".to_string(), "world".to_string());

        // The script prints the value of the env var for the valid key and exits 0.
        let node = make_node("env_test", "echo $FLOW_VALID_KEY");
        execute_script(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Completed);
        let ctx = step.context_out.as_deref().unwrap_or("");
        assert!(
            ctx.contains("hello"),
            "valid key should be injected as FLOW_VALID_KEY; context: {ctx:?}"
        );
    }

    /// A provider that only overrides the input env-var prefix.
    struct PrefixProvider(&'static str);

    impl crate::traits::script_env_provider::ScriptEnvProvider for PrefixProvider {
        fn env(
            &self,
            _ctx: &dyn crate::traits::run_context::RunContext,
            _as_identity: Option<&str>,
        ) -> HashMap<String, String> {
            HashMap::new()
        }

        fn input_env_prefix(&self) -> &str {
            self.0
        }
    }

    /// Run `script` with one input, `rk_env_probe = "T-1"`, and return the step's context.
    ///
    /// The key is one no host sets: script steps inherit the parent environment,
    /// and a test run inside a conductor workflow already has `CONDUCTOR_RK_ENV_PROBE`.
    fn run_with_ticket_input(state: &mut ExecutionState, run_id: &str, script: &str) -> String {
        state
            .inputs
            .insert("rk_env_probe".to_string(), "T-1".to_string());
        execute_script(state, &make_node("env_prefix", script), 0).unwrap();
        let steps = state.persistence.get_steps(run_id).unwrap();
        assert_eq!(steps[0].status, WorkflowStepStatus::Completed);
        steps[0].context_out.clone().unwrap_or_default()
    }

    const PRINT_BOTH_PREFIXES: &str =
        "echo \"flow=${FLOW_RK_ENV_PROBE:-unset} conductor=${CONDUCTOR_RK_ENV_PROBE:-unset}\"";

    /// With the default provider, inputs use the neutral `FLOW_` prefix only.
    #[test]
    fn inputs_use_flow_prefix_by_default() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(persistence, run_id.clone());
        let ctx = run_with_ticket_input(&mut state, &run_id, PRINT_BOTH_PREFIXES);
        assert!(ctx.contains("flow=T-1 conductor=unset"), "context: {ctx:?}");
    }

    /// A provider can override the prefix, e.g. conductor keeping `CONDUCTOR_`.
    #[test]
    fn provider_overrides_input_env_prefix() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(persistence, run_id.clone());
        state.script_env_provider = Arc::new(PrefixProvider("CONDUCTOR_"));
        let ctx = run_with_ticket_input(&mut state, &run_id, PRINT_BOTH_PREFIXES);
        assert!(ctx.contains("flow=unset conductor=T-1"), "context: {ctx:?}");
    }

    /// An invalid prefix fails the step without running the script, rather than
    /// injecting a variable the host didn't intend (`PATH=` + `RK_ENV_PROBE`).
    #[test]
    fn invalid_prefix_fails_the_step_without_running_it() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(persistence, run_id.clone());
        state.script_env_provider = Arc::new(PrefixProvider("PATH="));
        state
            .inputs
            .insert("rk_env_probe".to_string(), "T-1".to_string());
        // With fail_fast (the default) the failed step also fails the call.
        let res = execute_script(&mut state, &make_node("env_prefix", "echo ran"), 0);
        assert!(res.is_err(), "fail_fast should surface the step failure");
        let steps = state.persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps[0].status, WorkflowStepStatus::Failed);
        let result = steps[0].result_text.as_deref().unwrap_or("");
        assert!(
            result.contains("invalid script input env prefix"),
            "result: {result:?}"
        );
        assert!(
            !result.contains("ran"),
            "script must not run; result: {result:?}"
        );
    }

    /// With an empty prefix, an input must not be able to replace a
    /// security-sensitive variable such as `PATH`.
    #[test]
    fn empty_prefix_cannot_override_sensitive_vars() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(persistence, run_id.clone());
        state.script_env_provider = Arc::new(PrefixProvider(""));
        state
            .inputs
            .insert("path".to_string(), "/nonexistent".to_string());
        let ctx = run_with_ticket_input(
            &mut state,
            &run_id,
            "echo \"probe=$RK_ENV_PROBE path=$PATH\"",
        );
        assert!(ctx.contains("probe=T-1"), "context: {ctx:?}");
        assert!(!ctx.contains("path=/nonexistent"), "context: {ctx:?}");
    }

    /// env block vars from node.env are passed to the subprocess.
    #[test]
    fn node_env_vars_are_injected_into_subprocess() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());

        let mut env = HashMap::new();
        env.insert("MY_TEST_VAR".to_string(), "expected_value".to_string());
        let node = ScriptNode {
            name: "env-inject".to_string(),
            run: "echo $MY_TEST_VAR".to_string(),
            env,
            timeout: None,
            retries: 0,
            on_fail: None,
            as_identity: None,
        };
        execute_script(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Completed);
        let ctx = step.context_out.as_deref().unwrap_or("");
        assert!(
            ctx.contains("expected_value"),
            "MY_TEST_VAR should be injected from node.env; context: {ctx:?}"
        );
    }

    /// Template variables in node.env values are substituted from workflow state.
    #[test]
    fn node_env_vars_support_template_substitution() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        // prior_context comes from state.contexts.last().context in build_variable_map
        state.contexts.push(crate::types::ContextEntry {
            step: "prev-step".to_string(),
            iteration: 0,
            context: "substituted".to_string(),
            markers: vec![],
            structured_output: None,
            output_file: None,
        });

        let mut env = HashMap::new();
        env.insert("TEMPLATED_VAR".to_string(), "{{prior_context}}".to_string());
        let node = ScriptNode {
            name: "env-template".to_string(),
            run: "echo $TEMPLATED_VAR".to_string(),
            env,
            timeout: None,
            retries: 0,
            on_fail: None,
            as_identity: None,
        };
        execute_script(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Completed);
        let ctx = step.context_out.as_deref().unwrap_or("");
        assert!(
            ctx.contains("substituted"),
            "template in env value should be substituted; context: {ctx:?}"
        );
    }

    /// Security-sensitive env vars in node.env are skipped, not injected.
    #[test]
    fn sensitive_env_vars_are_blocked() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());

        let mut env = HashMap::new();
        env.insert("LD_PRELOAD".to_string(), "/malicious/lib.so".to_string());
        env.insert(
            "DYLD_LIBRARY_PATH".to_string(),
            "/malicious/lib".to_string(),
        );
        env.insert("SAFE_VAR".to_string(), "allowed_value".to_string());
        let node = ScriptNode {
            name: "sensitive-test".to_string(),
            run: "echo SAFE_VAR=[$SAFE_VAR] LD_PRELOAD=[$LD_PRELOAD] DYLD_LIBRARY_PATH=[$DYLD_LIBRARY_PATH]".to_string(),
            env,
            timeout: None,
            retries: 0,
            on_fail: None,
            as_identity: None,
        };
        execute_script(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Completed);
        let ctx = step.context_out.as_deref().unwrap_or("");
        assert!(
            ctx.contains("SAFE_VAR=[allowed_value]"),
            "SAFE_VAR should be injected; context: {ctx:?}"
        );
        assert!(
            !ctx.contains("/malicious/lib.so"),
            "LD_PRELOAD should be blocked; context: {ctx:?}"
        );
        assert!(
            !ctx.contains("/malicious/lib"),
            "DYLD_LIBRARY_PATH should be blocked; context: {ctx:?}"
        );
    }

    /// A script that exceeds its timeout is killed and the step is marked Failed.
    #[test]
    fn script_timeout_kills_long_running_process() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let node = ScriptNode {
            name: "sleepy".to_string(),
            run: "sleep 5".to_string(),
            env: Default::default(),
            timeout: Some(1), // 1 second timeout
            retries: 0,
            on_fail: None,
            as_identity: None,
        };
        let result = execute_script(&mut state, &node, 0);
        // on_fail is None, so failure should return Err.
        assert!(result.is_err(), "expected timeout error");

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Failed);
        let err_text = step.result_text.as_deref().unwrap_or("");
        assert!(
            err_text.contains("timed out"),
            "error should mention timeout: {err_text}"
        );
    }

    /// A script that writes more than the OS pipe buffer to stdout must not deadlock.
    /// This is a regression test for the pipe-deadlock fix (reader threads spawned
    /// before wait()).
    #[test]
    fn pipe_deadlock_regression_large_stdout() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        // Output 128KB of 'x' to stdout — well above the typical 64KB pipe buffer.
        let node = ScriptNode {
            name: "bigout".to_string(),
            run: "python3 -c \"import sys; sys.stdout.write('x' * 131072)\"".to_string(),
            env: Default::default(),
            timeout: None,
            retries: 0,
            on_fail: None,
            as_identity: None,
        };
        execute_script(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Completed);
    }

    /// Non-UTF-8 stdout is handled gracefully: the step still completes and the
    /// invalid bytes are dropped with a logged warning.
    #[test]
    fn non_utf8_stdout_is_handled_gracefully() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let node = ScriptNode {
            name: "binary".to_string(),
            run: "python3 -c \"import sys; sys.stdout.buffer.write(bytes([0x80,0x81,0x82]))\""
                .to_string(),
            env: Default::default(),
            timeout: None,
            retries: 0,
            on_fail: None,
            as_identity: None,
        };
        execute_script(&mut state, &node, 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Completed);
    }

    /// A non-success exit code with stderr output must capture stderr in the step error.
    #[test]
    fn non_success_exit_with_stderr_capture() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let node = ScriptNode {
            name: "fails-with-stderr".to_string(),
            run: "echo 'error details' >&2 && exit 1".to_string(),
            env: Default::default(),
            timeout: None,
            retries: 0,
            on_fail: None,
            as_identity: None,
        };
        let result = execute_script(&mut state, &node, 0);
        assert!(result.is_err(), "expected failure for non-zero exit");

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Failed);
        let err_text = step.result_text.as_deref().unwrap_or("");
        assert!(
            err_text.contains("error details"),
            "stderr should be captured in error message: {err_text}"
        );
    }

    /// Non-UTF-8 stderr on a failed script must not panic; the step is marked Failed.
    #[test]
    fn non_utf8_stderr_is_handled_gracefully() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let node = ScriptNode {
            name: "bad-stderr".to_string(),
            run: "python3 -c \"import sys; sys.stderr.buffer.write(bytes([0x80,0x81,0x82])); sys.exit(1)\"".to_string(),
            env: Default::default(),
            timeout: None,
            retries: 0,
            on_fail: None,
            as_identity: None,
        };
        let result = execute_script(&mut state, &node, 0);
        assert!(result.is_err(), "expected failure for non-zero exit");

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        let step = &steps[0];
        assert_eq!(step.status, WorkflowStepStatus::Failed);
    }

    /// A unique marker path so retry tests can tell the first run from the next.
    fn marker_path(tag: &str) -> String {
        std::env::temp_dir()
            .join(format!(
                "runkon-script-retry-{tag}-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ))
            .to_string_lossy()
            .replace(['(', ')'], "")
    }

    /// Steps ordered by attempt; the in-memory store only sorts by position, which
    /// is shared by every attempt of a step.
    fn attempts(
        persistence: &Arc<InMemoryWorkflowPersistence>,
        run_id: &str,
    ) -> Vec<crate::types::WorkflowRunStep> {
        let mut steps = persistence.get_steps(run_id).unwrap();
        steps.sort_by_key(|s| s.retry_count);
        steps
    }

    /// Fails on the first run (creating the marker) and succeeds on the second.
    fn fail_once_script(marker: &str, fail_cmd: &str) -> String {
        format!("if [ -f '{marker}' ]; then echo ok; else touch '{marker}'; {fail_cmd}; fi")
    }

    #[test]
    fn retries_then_succeeds() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let marker = marker_path("succeeds");
        let _ = std::fs::remove_file(&marker);
        let mut node = make_node("flaky", &fail_once_script(&marker, "exit 1"));
        node.retries = 1;

        execute_script(&mut state, &node, 0).unwrap();
        let _ = std::fs::remove_file(&marker);

        let steps = attempts(&persistence, &run_id);
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].status, WorkflowStepStatus::Failed);
        assert_eq!(steps[0].retry_count, 0);
        assert_eq!(steps[1].status, WorkflowStepStatus::Completed);
        assert_eq!(steps[1].retry_count, 1);
        assert_eq!(
            state.step_results["flaky"].status,
            WorkflowStepStatus::Completed
        );
    }

    #[test]
    fn unparseable_output_is_retried() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let marker = marker_path("unparseable");
        let _ = std::fs::remove_file(&marker);
        let bad = "echo '<<<FLOW_OUTPUT>>> not json <<<END_FLOW_OUTPUT>>>'";
        let mut node = make_node("bad_output_once", &fail_once_script(&marker, bad));
        node.retries = 1;

        execute_script(&mut state, &node, 0).unwrap();
        let _ = std::fs::remove_file(&marker);

        let steps = attempts(&persistence, &run_id);
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].status, WorkflowStepStatus::Failed);
        assert!(steps[0]
            .result_text
            .as_deref()
            .unwrap_or("")
            .contains("could not be parsed"));
        assert_eq!(steps[1].status, WorkflowStepStatus::Completed);
    }

    #[test]
    fn timeout_is_retried() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let marker = marker_path("timeout");
        let _ = std::fs::remove_file(&marker);
        let mut node = make_node("slow_once", &fail_once_script(&marker, "sleep 5"));
        node.timeout = Some(1);
        node.retries = 1;

        execute_script(&mut state, &node, 0).unwrap();
        let _ = std::fs::remove_file(&marker);

        let steps = attempts(&persistence, &run_id);
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].status, WorkflowStepStatus::Failed);
        assert!(steps[0]
            .result_text
            .as_deref()
            .unwrap_or("")
            .contains("timed out"));
        assert_eq!(steps[1].status, WorkflowStepStatus::Completed);
    }

    #[test]
    fn on_fail_continue_after_retries_skips() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        let mut node = make_node("always_bad", "exit 1");
        node.retries = 1;
        node.on_fail = Some(crate::dsl::OnFail::Continue);

        execute_script(&mut state, &node, 0).unwrap();

        let steps = attempts(&persistence, &run_id);
        assert_eq!(steps.len(), 2);
        assert!(steps.iter().all(|s| s.status == WorkflowStepStatus::Failed));
        assert_eq!(
            state.step_results["always_bad"].status,
            WorkflowStepStatus::Skipped
        );
    }

    #[test]
    fn exhausted_retries_run_the_on_fail_agent_once() {
        use crate::traits::action_executor::{
            ActionExecutor, ActionOutput, ActionParams, ActionRegistry, StepInfo,
        };
        use std::sync::Mutex;

        struct Recorder(Arc<Mutex<Vec<HashMap<String, String>>>>);
        impl ActionExecutor for Recorder {
            fn name(&self) -> &str {
                "fixer"
            }
            fn execute(
                &self,
                _ctx: &dyn crate::traits::run_context::RunContext,
                _info: &StepInfo,
                params: &ActionParams,
            ) -> std::result::Result<ActionOutput, crate::engine_error::EngineError> {
                self.0.lock().unwrap().push((*params.inputs).clone());
                Ok(ActionOutput::default())
            }
        }

        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut named: HashMap<String, Box<dyn ActionExecutor>> = HashMap::new();
        named.insert(
            "fixer".to_string(),
            Box::new(Recorder(Arc::clone(&calls))) as Box<dyn ActionExecutor>,
        );

        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        state.action_registry = Arc::new(ActionRegistry::new(named, None));
        let mut node = make_node("doomed", "exit 3");
        node.retries = 2;
        node.on_fail = Some(crate::dsl::OnFail::Agent(crate::dsl::AgentRef::Name(
            "fixer".to_string(),
        )));

        // fail_fast is on, so the exhausted step still fails the call.
        assert!(execute_script(&mut state, &node, 0).is_err());

        let steps = attempts(&persistence, &run_id);
        let script_rows: Vec<_> = steps.iter().filter(|s| s.role == "script").collect();
        assert_eq!(script_rows.len(), 3);
        assert!(script_rows
            .iter()
            .all(|s| s.status == WorkflowStepStatus::Failed));
        let counts: Vec<i64> = script_rows.iter().map(|s| s.retry_count).collect();
        assert_eq!(counts, vec![0, 1, 2]);

        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "on_fail agent should run exactly once");
        assert_eq!(calls[0]["failed_step"], "doomed");
        assert!(calls[0]["failure_reason"].contains("exited with code 3"));
        assert_eq!(calls[0]["retry_count"], "2");
    }

    /// Returns `<dir>/<step_name>-<iteration>-<step_id>.log` for every attempt.
    struct LogPathProvider(std::path::PathBuf);

    impl crate::traits::script_env_provider::ScriptEnvProvider for LogPathProvider {
        fn env(
            &self,
            _ctx: &dyn crate::traits::run_context::RunContext,
            _as_identity: Option<&str>,
        ) -> HashMap<String, String> {
            HashMap::new()
        }

        fn script_log_path(
            &self,
            _ctx: &dyn crate::traits::run_context::RunContext,
            step_id: &str,
            step_name: &str,
            iteration: u32,
        ) -> Option<std::path::PathBuf> {
            Some(
                self.0
                    .join(format!("{step_name}-{iteration}-{step_id}.log")),
            )
        }
    }

    fn state_with_log_dir(
        dir: &tempfile::TempDir,
    ) -> (Arc<InMemoryWorkflowPersistence>, String, ExecutionState) {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        state.script_env_provider = Arc::new(LogPathProvider(dir.path().to_path_buf()));
        (persistence, run_id, state)
    }

    #[test]
    fn tail_buffer_keeps_the_newest_bytes_and_counts_the_rest() {
        let mut t = TailBuffer::new(4);
        t.push(b"ab");
        assert_eq!(Vec::from(t.buf.clone()), b"ab");
        assert_eq!(t.dropped, 0);
        t.push(b"cd");
        assert_eq!(Vec::from(t.buf.clone()), b"abcd");
        assert_eq!(t.dropped, 0);
        t.push(b"ef");
        assert_eq!(Vec::from(t.buf.clone()), b"cdef");
        assert_eq!(t.dropped, 2);
        t.push(b"0123456789");
        assert_eq!(Vec::from(t.buf.clone()), b"6789");
        assert_eq!(t.dropped, 2 + 4 + 6);
    }

    #[test]
    fn last_chars_and_tail_str_respect_char_boundaries() {
        assert_eq!(last_chars("héllo", 3), "llo");
        assert_eq!(last_chars("abc", 10), "abc");
        let (tail, cut) = tail_str("aéb", 2);
        assert_eq!((tail, cut), ("b", 3));
        assert_eq!(tail_str("abc", 5), ("abc", 0));
    }

    #[test]
    fn success_writes_log_and_records_path_on_the_row_only() {
        let dir = tempfile::tempdir().unwrap();
        let (persistence, run_id, mut state) = state_with_log_dir(&dir);
        let script = concat!(
            "echo 'warn: fallback' >&2; echo 'stray line'; ",
            "printf '<<<FLOW_OUTPUT>>>\\n{\"markers\":[\"m\"],\"context\":\"c\"}\\n<<<END_FLOW_OUTPUT>>>\\n'"
        );
        execute_script(&mut state, &make_node("logs", script), 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        let path = steps[0].output_file.clone().expect("log path recorded");
        let log = std::fs::read_to_string(&path).unwrap();
        assert_eq!(
            log,
            "--- stderr ---\nwarn: fallback\n--- stdout (outside FLOW_OUTPUT) ---\nstray line\n"
        );
        assert!(path.contains(&steps[0].id));
        assert_eq!(state.contexts.last().unwrap().output_file, None);
        assert!(!build_variable_map(&state).contains_key("prior_output_file"));
    }

    #[test]
    fn failure_writes_log_and_step_error_ends_with_the_tail_of_stderr() {
        let dir = tempfile::tempdir().unwrap();
        let (persistence, run_id, mut state) = state_with_log_dir(&dir);
        let script = "i=0; while [ $i -lt 500 ]; do printf 'xxxxxxxxx'; i=$((i+1)); done >&2; \
                      echo ' REASON_AT_END' >&2; exit 2";
        assert!(execute_script(&mut state, &make_node("bad", script), 0).is_err());

        let steps = persistence.get_steps(&run_id).unwrap();
        let err = steps[0].step_error.clone().unwrap();
        assert!(err.ends_with("REASON_AT_END"), "{err}");
        let header = "Script 'bad' exited with code 2\n";
        assert_eq!(err.chars().count(), header.chars().count() + 2000);
        let log = std::fs::read_to_string(steps[0].output_file.as_ref().unwrap()).unwrap();
        assert!(log.starts_with("--- stderr ---\n"));
        assert!(log.ends_with("REASON_AT_END\n"));
    }

    #[test]
    fn timeout_writes_log() {
        let dir = tempfile::tempdir().unwrap();
        let (persistence, run_id, mut state) = state_with_log_dir(&dir);
        let mut node = make_node("slow", "echo before >&2; sleep 5");
        node.timeout = Some(1);
        assert!(execute_script(&mut state, &node, 0).is_err());

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps[0].status, WorkflowStepStatus::Failed);
        let log = std::fs::read_to_string(steps[0].output_file.as_ref().unwrap()).unwrap();
        assert!(log.contains("before"), "{log}");
    }

    #[test]
    fn malformed_flow_output_writes_log_with_the_bad_block() {
        let dir = tempfile::tempdir().unwrap();
        let (persistence, run_id, mut state) = state_with_log_dir(&dir);
        let script =
            "echo 'why' >&2; printf '<<<FLOW_OUTPUT>>>\\n{not json\\n<<<END_FLOW_OUTPUT>>>\\n'";
        assert!(execute_script(&mut state, &make_node("bad", script), 0).is_err());

        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps[0].status, WorkflowStepStatus::Failed);
        let log = std::fs::read_to_string(steps[0].output_file.as_ref().unwrap()).unwrap();
        assert!(log.contains("--- stderr ---\nwhy\n"), "{log}");
        assert!(log.contains("{not json"), "{log}");
    }

    #[test]
    fn large_stderr_is_capped_with_a_truncation_marker() {
        let dir = tempfile::tempdir().unwrap();
        let (persistence, run_id, mut state) = state_with_log_dir(&dir);
        let script = "i=0; while [ $i -lt 30000 ]; do echo 0123456789 >&2; i=$((i+1)); done; \
                      echo LAST_LINE >&2";
        execute_script(&mut state, &make_node("noisy", script), 0).unwrap();

        let steps = persistence.get_steps(&run_id).unwrap();
        let log = std::fs::read_to_string(steps[0].output_file.as_ref().unwrap()).unwrap();
        assert!(log.len() <= LOG_CAP + 100, "log is {} bytes", log.len());
        assert!(
            log.starts_with("--- stderr ---\n… [truncated "),
            "{}",
            &log[..60]
        );
        assert!(log.ends_with("LAST_LINE\n"));
    }

    #[test]
    fn default_provider_writes_no_file_and_records_no_path() {
        let (persistence, run_id) = make_persistence();
        let mut state = make_state(Arc::clone(&persistence), run_id.clone());
        execute_script(&mut state, &make_node("quiet", "echo hi >&2"), 0).unwrap();
        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps[0].output_file, None);
    }

    #[test]
    fn silent_script_writes_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let (persistence, run_id, mut state) = state_with_log_dir(&dir);
        execute_script(&mut state, &make_node("silent", "true"), 0).unwrap();
        let steps = persistence.get_steps(&run_id).unwrap();
        assert_eq!(steps[0].output_file, None);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn restored_script_row_does_not_feed_prior_output_file() {
        let dir = tempfile::tempdir().unwrap();
        let (persistence, run_id, mut state) = state_with_log_dir(&dir);
        execute_script(&mut state, &make_node("logs", "echo w >&2"), 0).unwrap();
        let steps = persistence.get_steps(&run_id).unwrap();
        assert!(steps[0].output_file.is_some());
        let restored = crate::types::StepSuccess::from_workflow_run_step(
            "logs".to_string(),
            &steps[0],
            vec![],
            String::new(),
            0,
        );
        assert_eq!(restored.output_file, None);
    }

    #[test]
    fn each_retry_attempt_records_its_own_log() {
        let dir = tempfile::tempdir().unwrap();
        let (persistence, run_id, mut state) = state_with_log_dir(&dir);
        let mut node = make_node("flaky", "echo nope >&2; exit 1");
        node.retries = 1;
        assert!(execute_script(&mut state, &node, 0).is_err());

        let steps = persistence.get_steps(&run_id).unwrap();
        let paths: Vec<_> = steps
            .iter()
            .filter(|s| s.role == "script")
            .map(|s| s.output_file.clone().expect("each attempt has a log"))
            .collect();
        assert_eq!(paths.len(), 2);
        assert_ne!(paths[0], paths[1]);
    }
}
