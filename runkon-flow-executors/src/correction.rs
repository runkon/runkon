//! In-session correction of agent output that fails schema validation.
//!
//! When a subprocess agent run completes but its `<<<FLOW_OUTPUT>>>` block does
//! not validate against the step's schema, [`run_agent_with_output_correction`]
//! resumes the same agent session with a short prompt quoting the validation
//! error, instead of failing the step and discarding the finished work.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use runkon_flow::output_schema::OutputSchema;
use runkon_runtimes::{AgentRuntime, PollError, RunHandle, RunStatus, RuntimeRequest};

use crate::agent_loader::generate_prompt_instructions;
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
) -> Result<AgentRunOutcome, String> {
    let mut run = spawn_and_poll(runtime, &request, shutdown, step_timeout)?;
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
                });
            }
            Err(e) => {
                let (Some(schema), Some(session_id)) = (schema, run.session_id.clone()) else {
                    return Err(e);
                };
                if attempts >= MAX_OUTPUT_CORRECTION_ATTEMPTS {
                    return Err(format!("{e} (after {attempts} output correction attempt(s))"));
                }
                attempts += 1;
                request.prompt = output_correction_prompt(&e, schema);
                request.resume_session_id = Some(session_id);
                let next = spawn_and_poll(runtime, &request, shutdown, step_timeout)
                    .map_err(|ce| format!("output correction failed: {ce}; original error: {e}"))?;
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
    use runkon_flow::output_schema::{ArrayItems, FieldDef, FieldType};
    use runkon_runtimes::runtime::private::Seal;
    use std::sync::Mutex;

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
            status: RunStatus::Completed,
            subprocess_pid: None,
            runtime: "claude".to_string(),
            session_id: session.map(str::to_string),
            result_text: Some(text.to_string()),
            started_at: String::new(),
            ended_at: None,
            log_file: None,
            model: None,
            cost_usd: Some(cost),
            num_turns: Some(turns),
            duration_ms: None,
            input_tokens: None,
            output_tokens: None,
            cache_read_input_tokens: None,
            cache_creation_input_tokens: None,
        }
    }

    const BAD: &str = "done\n<<<FLOW_OUTPUT>>>\n{\"tasks\":[{\"id\":\"a\"},{}]}\n<<<END_FLOW_OUTPUT>>>";
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
    ) -> Result<AgentRunOutcome, String> {
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
        assert!(err.contains("after 2 output correction attempt(s)"), "{err}");
        assert!(err.contains("Missing required field: 'id'"), "{err}");
        assert_eq!(rt.spawns().len(), 3);
    }

    #[test]
    fn no_session_id_means_no_correction() {
        let rt = ScriptedRuntime::new(vec![Ok(handle(BAD, None, 0.1, 1))]);
        let s = schema();
        let err = run(&rt, Some(&s)).unwrap_err();
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
    fn correction_poll_failure_keeps_original_error() {
        let rt = ScriptedRuntime::new(vec![
            Ok(handle(BAD, Some("s"), 0.1, 1)),
            Err(PollError::Failed("boom".to_string())),
        ]);
        let s = schema();
        let err = run(&rt, Some(&s)).unwrap_err();
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
