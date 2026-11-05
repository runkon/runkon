//! In-session correction of agent output that fails schema validation.
//!
//! When a subprocess agent run completes but its `<<<FLOW_OUTPUT>>>` block does
//! not validate against the step's schema, [`run_agent_with_output_correction`]
//! resumes the same agent session with a short prompt quoting the validation
//! error, instead of failing the step and discarding the finished work.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use runkon_flow::constants::metadata_keys;
use runkon_flow::output_schema::OutputSchema;
use runkon_runtimes::{AgentRuntime, PollError, RunHandle, RunStatus, RuntimeRequest};

use crate::agent_loader::generate_prompt_instructions;
use crate::error::AgentError;
use crate::output::interpret_agent_output;

/// Maximum number of correction runs after the initial attempt.
pub const MAX_OUTPUT_CORRECTION_ATTEMPTS: u32 = 2;

/// Result of [`run_agent_with_output_correction`].
#[derive(Debug, Clone)]
pub struct AgentRunOutcome {
    /// The final attempt's handle: status, `result_text` and `session_id` are the
    /// final attempt's, while the metric fields are summed across all attempts.
    pub run: RunHandle,
    pub markers: Vec<String>,
    pub context: String,
    pub structured_output: Option<String>,
    /// Number of correction runs made (0 when the first output was accepted).
    pub correction_attempts: u32,
    /// The id of the earlier agent run this outcome was recovered from, when
    /// [`recover_prior_output`] produced it; `None` for a fresh run.
    pub recovered_from: Option<String>,
}

/// Prompt sent into the resumed session after a validation failure.
pub fn output_correction_prompt(error: &str, schema: &OutputSchema) -> String {
    format!(
        "Your <<<FLOW_OUTPUT>>> block failed schema validation:\n\n    {error}\n\n\
         Do not redo the work. Reply with only a corrected <<<FLOW_OUTPUT>>> ... \
         <<<END_FLOW_OUTPUT>>> block that fixes this error and conforms to the schema below.\n\n{}",
        generate_prompt_instructions(schema)
    )
}

/// Record the correction count in step metadata when any correction ran.
pub fn record_correction_attempts(metadata: &mut HashMap<String, String>, attempts: u32) {
    if attempts > 0 {
        metadata.insert(
            metadata_keys::OUTPUT_CORRECTION_ATTEMPTS.to_string(),
            attempts.to_string(),
        );
    }
}

/// Record the prior run whose output was reused in step metadata, when one was.
pub fn record_recovery(metadata: &mut HashMap<String, String>, recovered_from: Option<String>) {
    if let Some(prior) = recovered_from {
        metadata.insert(metadata_keys::OUTPUT_RECOVERED_FROM_RUN.to_string(), prior);
    }
}

fn spawn_and_poll(
    runtime: &dyn AgentRuntime,
    request: &RuntimeRequest,
    shutdown: Option<&Arc<AtomicBool>>,
    step_timeout: Duration,
) -> Result<RunHandle, String> {
    runtime
        .spawn_validated(request)
        .map_err(|e| format!("failed to spawn agent: {e}"))?;
    match runtime.poll(&request.run_id, shutdown, step_timeout) {
        Ok(run) => Ok(run),
        Err(PollError::Cancelled) => Err("executor shutdown requested".to_string()),
        Err(e) => Err(e.to_string()),
    }
}

fn add_opt<T: std::ops::Add<Output = T>>(a: Option<T>, b: Option<T>) -> Option<T> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x + y),
        (x, None) => x,
        (None, y) => y,
    }
}

fn add_metrics(total: &mut RunHandle, next: &RunHandle) {
    total.cost_usd = add_opt(total.cost_usd, next.cost_usd);
    total.num_turns = add_opt(total.num_turns, next.num_turns);
    total.duration_ms = add_opt(total.duration_ms, next.duration_ms);
    total.input_tokens = add_opt(total.input_tokens, next.input_tokens);
    total.output_tokens = add_opt(total.output_tokens, next.output_tokens);
    total.cache_read_input_tokens =
        add_opt(total.cache_read_input_tokens, next.cache_read_input_tokens);
    total.cache_creation_input_tokens = add_opt(
        total.cache_creation_input_tokens,
        next.cache_creation_input_tokens,
    );
}

/// Spawn `request`, poll it to completion and interpret its output.
///
/// If the run completed but its output fails schema validation and the run has a
/// session id, the session is resumed (same `run_id`) with a correction prompt,
/// up to [`MAX_OUTPUT_CORRECTION_ATTEMPTS`] times. Runs without a session id
/// (script and cli runtimes) fail immediately, as does any run when `schema` is
/// `None`.
pub fn run_agent_with_output_correction(
    runtime: &dyn AgentRuntime,
    mut request: RuntimeRequest,
    shutdown: Option<&Arc<AtomicBool>>,
    step_timeout: Duration,
    schema: Option<&OutputSchema>,
) -> Result<AgentRunOutcome, AgentError> {
    let run = spawn_and_poll(runtime, &request, shutdown, step_timeout)?;
    correct_from(runtime, &mut request, run, shutdown, step_timeout, schema)
}

/// Reuse the output of an earlier, completed agent run of a step that is being resumed.
///
/// Returns `None` when the caller should start a fresh run instead: the prior run did
/// not complete, has no `result_text`, or its output is still invalid and it has no
/// session to resume. Otherwise:
///
/// - if the captured output now passes `schema`, it is returned with no spawn;
/// - if it still fails and the run has a session id, that session is resumed with the
///   correction prompt (on `request`'s `run_id`), up to
///   [`MAX_OUTPUT_CORRECTION_ATTEMPTS`] times.
///
/// The prior run's metrics are cleared first: the failed attempt already recorded them,
/// so the outcome only carries new spend.
pub fn recover_prior_output(
    runtime: &dyn AgentRuntime,
    request: &mut RuntimeRequest,
    mut prior: RunHandle,
    shutdown: Option<&Arc<AtomicBool>>,
    step_timeout: Duration,
    schema: &OutputSchema,
) -> Option<Result<AgentRunOutcome, AgentError>> {
    if prior.status != RunStatus::Completed || prior.result_text.is_none() {
        return None;
    }
    let output_ok =
        interpret_agent_output(prior.result_text.as_deref(), Some(schema), true).is_ok();
    if !output_ok && prior.session_id.is_none() {
        return None;
    }
    let prior_id = prior.id.clone();
    prior.cost_usd = None;
    prior.num_turns = None;
    prior.duration_ms = None;
    prior.input_tokens = None;
    prior.output_tokens = None;
    prior.cache_read_input_tokens = None;
    prior.cache_creation_input_tokens = None;
    Some(
        correct_from(
            runtime,
            request,
            prior,
            shutdown,
            step_timeout,
            Some(schema),
        )
        .map(|mut outcome| {
            outcome.recovered_from = Some(prior_id);
            outcome
        }),
    )
}

/// Run an agent step, first trying to reuse the output of `prior_run_id`, the earlier
/// failed attempt of a step being resumed.
///
/// With a `schema` and a `prior_run_id`, the prior run is loaded through
/// `request.tracker` and handed to [`recover_prior_output`]. Every other case, including
/// a tracker error, a missing row or a prior run with nothing to reuse, falls back to
/// [`run_agent_with_output_correction`] (a fresh run), as before.
pub fn run_agent_resuming_prior(
    runtime: &dyn AgentRuntime,
    mut request: RuntimeRequest,
    prior_run_id: Option<&str>,
    shutdown: Option<&Arc<AtomicBool>>,
    step_timeout: Duration,
    schema: Option<&OutputSchema>,
) -> Result<AgentRunOutcome, AgentError> {
    if let (Some(prior_id), Some(schema)) = (prior_run_id, schema) {
        match request.tracker.get_run(prior_id) {
            Ok(Some(prior)) => {
                if let Some(outcome) = recover_prior_output(
                    runtime,
                    &mut request,
                    prior,
                    shutdown,
                    step_timeout,
                    schema,
                ) {
                    return outcome;
                }
                tracing::info!(
                    "prior agent run {prior_id} has no reusable output; starting a fresh run"
                );
            }
            Ok(None) => {
                tracing::warn!("prior agent run {prior_id} not found; starting a fresh run");
            }
            Err(e) => {
                tracing::warn!(
                    "failed to load prior agent run {prior_id}: {e}; starting a fresh run"
                );
            }
        }
    }
    run_agent_with_output_correction(runtime, request, shutdown, step_timeout, schema)
}

/// Interpret `run`'s output and, while it fails validation and the run has a session,
/// resume that session with a correction prompt.
fn correct_from(
    runtime: &dyn AgentRuntime,
    request: &mut RuntimeRequest,
    mut run: RunHandle,
    shutdown: Option<&Arc<AtomicBool>>,
    step_timeout: Duration,
    schema: Option<&OutputSchema>,
) -> Result<AgentRunOutcome, AgentError> {
    let mut attempts: u32 = 0;

    loop {
        let succeeded = run.status == RunStatus::Completed;
        match interpret_agent_output(run.result_text.as_deref(), schema, succeeded) {
            Ok((markers, context, structured_output)) => {
                return Ok(AgentRunOutcome {
                    run,
                    markers,
                    context,
                    structured_output,
                    correction_attempts: attempts,
                    recovered_from: None,
                });
            }
            Err(error) => {
                let (Some(schema), Some(session_id)) = (schema, run.session_id.clone()) else {
                    return Err(AgentError::Output {
                        error,
                        correction_attempts: 0,
                    });
                };
                if attempts >= MAX_OUTPUT_CORRECTION_ATTEMPTS {
                    return Err(AgentError::Output {
                        error,
                        correction_attempts: attempts,
                    });
                }
                attempts += 1;
                // The text a failed attempt reports, and the prompt quotes, is the
                // output failure as a step would show it.
                let original = AgentError::Output {
                    error,
                    correction_attempts: 0,
                };
                request.prompt = output_correction_prompt(&original.to_string(), schema);
                request.resume_session_id = Some(session_id);
                let next =
                    spawn_and_poll(runtime, request, shutdown, step_timeout).map_err(|ce| {
                        AgentError::Other(format!(
                            "output correction failed: {ce}; original error: {original}"
                        ))
                    })?;
                if next.status != RunStatus::Completed {
                    return Err(AgentError::Other(format!(
                        "output correction run ended with status {:?}; original error: {original}",
                        next.status
                    )));
                }
                // The resumed run overwrites the host's row, so carry the earlier
                // attempts' metrics forward into the new handle.
                let mut merged = next;
                add_metrics(&mut merged, &run);
                run = merged;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::scripted_handle;
    use runkon_flow::output_schema::{ArrayItems, FieldDef, FieldType};
    use runkon_runtimes::runtime::private::Seal;
    use std::sync::Mutex;

    #[test]
    fn record_recovery_inserts_prior_run_when_some() {
        let mut metadata = HashMap::new();
        record_recovery(&mut metadata, Some("run-1".to_string()));
        assert_eq!(
            metadata.get(metadata_keys::OUTPUT_RECOVERED_FROM_RUN),
            Some(&"run-1".to_string())
        );
    }

    #[test]
    fn record_recovery_leaves_metadata_empty_when_none() {
        let mut metadata = HashMap::new();
        record_recovery(&mut metadata, None);
        assert!(metadata.is_empty());
    }

    #[test]
    fn record_recovery_preserves_existing_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("existing".to_string(), "value".to_string());
        record_recovery(&mut metadata, Some("run-2".to_string()));
        assert_eq!(metadata.get("existing"), Some(&"value".to_string()));
        assert_eq!(metadata.len(), 2);
    }

    // Not `test_support::ScriptedRuntime`: these tests assert on the prompt of
    // each spawn and script `PollError` results, which the shared runtime
    // (it records only `resume_session_id` and replays `RunHandle`s) cannot.
    struct ScriptedRuntime {
        requests: Mutex<Vec<(String, Option<String>)>>,
        queue: Mutex<Vec<Result<RunHandle, PollError>>>,
    }

    impl ScriptedRuntime {
        fn new(results: Vec<Result<RunHandle, PollError>>) -> Self {
            let mut q = results;
            q.reverse();
            Self {
                requests: Mutex::new(vec![]),
                queue: Mutex::new(q),
            }
        }
        fn spawns(&self) -> Vec<(String, Option<String>)> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl AgentRuntime for ScriptedRuntime {
        fn spawn_impl(&self, request: &RuntimeRequest, _seal: Seal) -> runkon_runtimes::Result<()> {
            self.requests
                .lock()
                .unwrap()
                .push((request.prompt.clone(), request.resume_session_id.clone()));
            Ok(())
        }
        fn poll(
            &self,
            _run_id: &str,
            _shutdown: Option<&Arc<AtomicBool>>,
            _step_timeout: Duration,
        ) -> Result<RunHandle, PollError> {
            self.queue.lock().unwrap().pop().expect("no queued run")
        }
        fn is_alive(&self, _run: &RunHandle) -> bool {
            false
        }
        fn cancel(&self, _run: &RunHandle) -> runkon_runtimes::Result<()> {
            Ok(())
        }
    }

    fn field(name: &str, field_type: FieldType) -> FieldDef {
        FieldDef {
            name: name.to_string(),
            required: true,
            field_type,
            desc: None,
            examples: None,
        }
    }

    fn schema() -> OutputSchema {
        OutputSchema {
            name: "plan".to_string(),
            fields: vec![field(
                "tasks",
                FieldType::Array {
                    items: ArrayItems::Object(vec![field("id", FieldType::String)]),
                },
            )],
            markers: None,
        }
    }

    fn handle(text: &str, session: Option<&str>, cost: f64, turns: i64) -> RunHandle {
        RunHandle {
            id: "run-1".to_string(),
            session_id: session.map(str::to_string),
            cost_usd: Some(cost),
            num_turns: Some(turns),
            ..scripted_handle(text)
        }
    }

    const BAD: &str =
        "done\n<<<FLOW_OUTPUT>>>\n{\"tasks\":[{\"id\":\"a\"},{}]}\n<<<END_FLOW_OUTPUT>>>";
    const GOOD: &str =
        "fixed\n<<<FLOW_OUTPUT>>>\n{\"tasks\":[{\"id\":\"a\"},{\"id\":\"b\"}]}\n<<<END_FLOW_OUTPUT>>>";

    fn request() -> RuntimeRequest {
        RuntimeRequest {
            run_id: "run-1".to_string(),
            prompt: "original".to_string(),
            ..Default::default()
        }
    }

    fn run(
        rt: &ScriptedRuntime,
        schema: Option<&OutputSchema>,
    ) -> Result<AgentRunOutcome, AgentError> {
        run_agent_with_output_correction(rt, request(), None, Duration::from_secs(1), schema)
    }

    #[test]
    fn corrects_missing_required_field_in_session() {
        let rt = ScriptedRuntime::new(vec![
            Ok(handle(BAD, Some("sess-1"), 0.4, 20)),
            Ok(handle(GOOD, Some("sess-1"), 0.05, 1)),
        ]);
        let s = schema();
        let out = run(&rt, Some(&s)).unwrap();
        assert_eq!(out.correction_attempts, 1);
        let spawns = rt.spawns();
        assert_eq!(spawns.len(), 2);
        assert_eq!(spawns[0].1, None);
        assert_eq!(spawns[1].1.as_deref(), Some("sess-1"));
        assert!(spawns[1].0.contains("Missing required field: 'id'"));
        assert!(out.structured_output.unwrap().contains("\"b\""));
        assert_eq!(out.run.num_turns, Some(21));
        assert!((out.run.cost_usd.unwrap() - 0.45).abs() < 1e-9);
    }

    #[test]
    fn fails_after_max_corrections() {
        let rt = ScriptedRuntime::new(vec![
            Ok(handle(BAD, Some("s"), 0.1, 1)),
            Ok(handle(BAD, Some("s"), 0.1, 1)),
            Ok(handle(BAD, Some("s"), 0.1, 1)),
        ]);
        let s = schema();
        let err = run(&rt, Some(&s)).unwrap_err();
        assert!(
            matches!(
                err,
                AgentError::Output {
                    correction_attempts: 2,
                    error: crate::error::OutputError::Validation { .. },
                }
            ),
            "{err:?}"
        );
        let err = err.to_string();
        assert!(
            err.contains("after 2 output correction attempt(s)"),
            "{err}"
        );
        assert!(err.contains("Missing required field: 'id'"), "{err}");
        assert_eq!(rt.spawns().len(), 3);
    }

    #[test]
    fn no_session_id_means_no_correction() {
        let rt = ScriptedRuntime::new(vec![Ok(handle(BAD, None, 0.1, 1))]);
        let s = schema();
        let err = run(&rt, Some(&s)).unwrap_err();
        assert!(
            matches!(
                err,
                AgentError::Output {
                    correction_attempts: 0,
                    ..
                }
            ),
            "{err:?}"
        );
        let err = err.to_string();
        assert!(err.starts_with("structured output validation: "), "{err}");
        assert!(!err.contains("correction"), "{err}");
        assert_eq!(rt.spawns().len(), 1);
    }

    #[test]
    fn no_schema_means_no_correction() {
        let rt = ScriptedRuntime::new(vec![Ok(handle(BAD, Some("s"), 0.1, 1))]);
        let out = run(&rt, None).unwrap();
        assert_eq!(out.correction_attempts, 0);
        assert_eq!(rt.spawns().len(), 1);
    }

    #[test]
    fn non_completed_run_is_not_corrected() {
        let mut h = handle(BAD, Some("s"), 0.1, 1);
        h.status = RunStatus::Failed;
        let rt = ScriptedRuntime::new(vec![Ok(h)]);
        let s = schema();
        let out = run(&rt, Some(&s)).unwrap();
        assert_eq!(out.correction_attempts, 0);
        assert!(out.structured_output.is_none());
        assert_eq!(rt.spawns().len(), 1);
    }

    #[test]
    fn non_completed_correction_run_keeps_original_error() {
        let mut failed = handle(GOOD, Some("s"), 0.1, 1);
        failed.status = RunStatus::Failed;
        let rt = ScriptedRuntime::new(vec![Ok(handle(BAD, Some("s"), 0.1, 1)), Ok(failed)]);
        let s = schema();
        let err = run(&rt, Some(&s)).unwrap_err();
        assert!(matches!(err, AgentError::Other(_)), "{err:?}");
        let err = err.to_string();
        assert!(err.contains("Failed"), "{err}");
        assert!(err.contains("Missing required field: 'id'"), "{err}");
    }

    fn recover(
        rt: &ScriptedRuntime,
        prior: RunHandle,
    ) -> Option<Result<AgentRunOutcome, AgentError>> {
        let s = schema();
        recover_prior_output(rt, &mut request(), prior, None, Duration::from_secs(1), &s)
    }

    #[test]
    fn recovery_resumes_prior_session_for_missing_field_without_fresh_run() {
        let rt = ScriptedRuntime::new(vec![Ok(handle(GOOD, Some("sess-1"), 0.05, 1))]);
        let out = recover(&rt, handle(BAD, Some("sess-1"), 0.4, 20))
            .expect("recovery applies")
            .unwrap();
        let spawns = rt.spawns();
        assert_eq!(spawns.len(), 1, "one session-resume spawn, no fresh run");
        assert_eq!(spawns[0].1.as_deref(), Some("sess-1"));
        assert!(spawns[0].0.contains("Missing required field: 'id'"));
        assert_ne!(spawns[0].0, "original");
        assert_eq!(out.correction_attempts, 1);
        assert_eq!(out.recovered_from.as_deref(), Some("run-1"));
        assert!(out.structured_output.unwrap().contains("\"b\""));
        // The prior attempt's metrics were already recorded; only the new run counts.
        assert_eq!(out.run.num_turns, Some(1));
        assert!((out.run.cost_usd.unwrap() - 0.05).abs() < 1e-9);
    }

    #[test]
    fn recovery_accepts_now_valid_output_without_spawning() {
        let rt = ScriptedRuntime::new(vec![]);
        let out = recover(&rt, handle(GOOD, Some("s"), 0.4, 20))
            .expect("recovery applies")
            .unwrap();
        assert!(rt.spawns().is_empty());
        assert_eq!(out.correction_attempts, 0);
        assert_eq!(out.recovered_from.as_deref(), Some("run-1"));
        assert!(out.structured_output.unwrap().contains("\"b\""));
        assert_eq!(out.run.cost_usd, None);
        assert_eq!(out.run.num_turns, None);
    }

    #[test]
    fn recovery_fails_after_max_corrections() {
        let rt = ScriptedRuntime::new(vec![
            Ok(handle(BAD, Some("s"), 0.1, 1)),
            Ok(handle(BAD, Some("s"), 0.1, 1)),
        ]);
        let err = recover(&rt, handle(BAD, Some("s"), 0.4, 20))
            .expect("recovery applies")
            .unwrap_err();
        assert!(
            matches!(
                err,
                AgentError::Output {
                    correction_attempts: 2,
                    ..
                }
            ),
            "{err:?}"
        );
        assert_eq!(rt.spawns().len(), 2);
    }

    #[test]
    fn recovery_declines_when_nothing_to_reuse() {
        let rt = ScriptedRuntime::new(vec![]);
        for status in [RunStatus::Failed, RunStatus::Cancelled] {
            let mut h = handle(BAD, Some("s"), 0.1, 1);
            h.status = status;
            assert!(recover(&rt, h).is_none());
        }
        let mut no_text = handle(BAD, Some("s"), 0.1, 1);
        no_text.result_text = None;
        assert!(recover(&rt, no_text).is_none());
        assert!(
            recover(&rt, handle(BAD, None, 0.1, 1)).is_none(),
            "invalid output with no session"
        );
        assert!(rt.spawns().is_empty());
    }

    #[test]
    fn records_correction_attempts_only_when_nonzero() {
        let mut m = HashMap::new();
        record_correction_attempts(&mut m, 0);
        assert!(m.is_empty());
        record_correction_attempts(&mut m, 2);
        assert_eq!(m[metadata_keys::OUTPUT_CORRECTION_ATTEMPTS], "2");
    }

    #[test]
    fn correction_poll_failure_keeps_original_error() {
        let rt = ScriptedRuntime::new(vec![
            Ok(handle(BAD, Some("s"), 0.1, 1)),
            Err(PollError::Failed("boom".to_string())),
        ]);
        let s = schema();
        let err = run(&rt, Some(&s)).unwrap_err();
        assert!(matches!(err, AgentError::Other(_)), "{err:?}");
        let err = err.to_string();
        assert!(err.contains("boom"), "{err}");
        assert!(err.contains("Missing required field: 'id'"), "{err}");
    }

    #[test]
    fn prompt_quotes_error_and_includes_schema_instructions() {
        let p = output_correction_prompt("In 'tasks[1]': Missing required field: 'id'", &schema());
        assert!(p.contains("Missing required field: 'id'"));
        assert!(p.contains("<<<FLOW_OUTPUT>>>"));
        assert!(p.contains("\"tasks\""));
    }
}
