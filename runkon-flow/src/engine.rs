use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cancellation::CancellationToken;
use crate::constants::CONDUCTOR_OUTPUT_INSTRUCTION;
use crate::dsl::{InputType, OnFail, WorkflowDef, WorkflowNode};
use crate::engine_error::{EngineError, Result};
use crate::events::{EngineEvent, EventSink};
use crate::output_schema::OutputSchema;
use crate::status::{WorkflowRunStatus, WorkflowStepStatus};
use crate::traits::action_executor::ActionRegistry;
use crate::traits::item_provider::ItemProviderRegistry;
use crate::traits::persistence::WorkflowPersistence;
use crate::traits::script_env_provider::ScriptEnvProvider;
use crate::types::{
    ContextEntry, StepKey, StepResult, WorkflowExecConfig, WorkflowResult, WorkflowRunStep,
};

/// Input keys that the workflow engine injects automatically from the run context.
pub const ENGINE_INJECTED_KEYS: &[&str] = &[
    "ticket_id",
    "ticket_source_id",
    "ticket_source_type",
    "ticket_title",
    "ticket_body",
    "ticket_url",
    "ticket_raw_json",
    "repo_id",
    "repo_path",
    "repo_name",
    "workflow_run_id",
];

/// Domain-identity context for a single workflow execution.
pub struct WorktreeContext {
    pub worktree_id: Option<String>,
    pub working_dir: String,
    pub repo_path: String,
    pub ticket_id: Option<String>,
    pub repo_id: Option<String>,
    pub extra_plugin_dirs: Vec<String>,
}

/// Pre-loaded context for resuming a workflow run.
pub struct ResumeContext {
    /// Step keys to skip (e.g. `("lint", 0)`).
    pub skip_completed: HashSet<StepKey>,
    /// Completed step records keyed by step key, for O(1) restore.
    pub step_map: HashMap<StepKey, WorkflowRunStep>,
}

/// Mutable runtime state for a workflow execution — no conductor-core deps.
pub struct ExecutionState {
    pub persistence: Arc<dyn WorkflowPersistence>,
    pub action_registry: Arc<ActionRegistry>,
    pub script_env_provider: Arc<dyn ScriptEnvProvider>,
    pub workflow_run_id: String,
    pub workflow_name: String,
    pub worktree_ctx: WorktreeContext,
    pub model: Option<String>,
    pub exec_config: WorkflowExecConfig,
    pub inputs: HashMap<String, String>,
    pub parent_run_id: String,
    pub depth: u32,
    pub target_label: Option<String>,
    // Runtime
    pub step_results: HashMap<String, StepResult>,
    pub contexts: Vec<ContextEntry>,
    pub position: i64,
    pub all_succeeded: bool,
    pub total_cost: f64,
    pub total_turns: i64,
    pub total_duration_ms: i64,
    pub total_input_tokens: i64,
    pub total_output_tokens: i64,
    pub total_cache_read_input_tokens: i64,
    pub total_cache_creation_input_tokens: i64,
    pub last_gate_feedback: Option<String>,
    pub block_output: Option<String>,
    pub block_with: Vec<String>,
    pub resume_ctx: Option<ResumeContext>,
    pub default_bot_name: Option<String>,
    pub triggered_by_hook: bool,
    /// Schema resolver callback — (working_dir, repo_path, schema_name) → OutputSchema
    #[allow(clippy::type_complexity)]
    pub schema_resolver:
        Option<Arc<dyn Fn(&str, &str, &str) -> Result<OutputSchema> + Send + Sync>>,
    /// Runner for child workflows (call workflow nodes).
    pub child_runner: Option<Arc<dyn ChildWorkflowRunner>>,
    pub last_heartbeat_at: Arc<AtomicI64>,
    pub registry: Arc<ItemProviderRegistry>,
    /// Event sinks — slice shared cheaply across sub-workflow states.
    pub event_sinks: Arc<[Arc<dyn EventSink>]>,
    /// Cancellation token for this run. Checked at each step boundary.
    pub cancellation: CancellationToken,
    /// The executor label and step_id of the currently executing action, if any.
    /// Written by execute_call before dispatch; read by FlowEngine::cancel_run()
    /// to fire-and-forget executor.cancel().
    pub current_execution_id: Arc<Mutex<Option<(String, String)>>>,
}

/// Input parameters for child workflow execution.
pub struct ChildWorkflowInput {
    pub inputs: HashMap<String, String>,
    pub iteration: u32,
    pub bot_name: Option<String>,
    pub depth: u32,
    pub parent_step_id: Option<String>,
    /// Child token derived from the parent run's cancellation token.
    /// The child runner sets this as the child `ExecutionState.cancellation`
    /// so that cancelling the parent automatically cancels in-progress child runs.
    pub cancellation: CancellationToken,
}

/// Trait for executing child workflows — allows conductor-core to inject its adapter.
pub trait ChildWorkflowRunner: Send + Sync {
    fn execute_child(
        &self,
        child_def: &WorkflowDef,
        parent_state: &ExecutionState,
        params: ChildWorkflowInput,
    ) -> Result<WorkflowResult>;

    fn resume_child(&self, workflow_run_id: &str, model: Option<&str>) -> Result<WorkflowResult>;

    fn find_resumable_child(
        &self,
        parent_run_id: &str,
        workflow_name: &str,
    ) -> Result<Option<crate::types::WorkflowRun>>;
}

impl ExecutionState {
    /// Create a fresh heartbeat counter, initialized to 0 so the first tick fires immediately.
    pub fn new_heartbeat() -> Arc<AtomicI64> {
        Arc::new(AtomicI64::new(0))
    }

    /// Accumulate individual metrics into this execution state.
    #[allow(clippy::too_many_arguments)]
    pub fn accumulate_metrics(
        &mut self,
        cost: Option<f64>,
        turns: Option<i64>,
        duration: Option<i64>,
        input_tokens: Option<i64>,
        output_tokens: Option<i64>,
        cache_read: Option<i64>,
        cache_create: Option<i64>,
    ) {
        if let Some(c) = cost {
            self.total_cost += c;
        }
        if let Some(t) = turns {
            self.total_turns += t;
        }
        if let Some(d) = duration {
            self.total_duration_ms += d;
        }
        if let Some(t) = input_tokens {
            self.total_input_tokens += t;
        }
        if let Some(t) = output_tokens {
            self.total_output_tokens += t;
        }
        if let Some(t) = cache_read {
            self.total_cache_read_input_tokens += t;
        }
        if let Some(t) = cache_create {
            self.total_cache_creation_input_tokens += t;
        }
    }

    /// Persist the current accumulated metrics to the workflow run row.
    pub fn flush_metrics(&self) -> Result<()> {
        self.persistence.persist_metrics(
            &self.workflow_run_id,
            self.total_input_tokens,
            self.total_output_tokens,
            self.total_cache_read_input_tokens,
            self.total_cache_creation_input_tokens,
            self.total_cost,
            self.total_turns,
            self.total_duration_ms,
        )
    }
}

/// Resolve a schema by name using the schema_resolver callback.
pub fn resolve_schema(state: &ExecutionState, name: &str) -> Result<OutputSchema> {
    match &state.schema_resolver {
        Some(resolver) => resolver(
            &state.worktree_ctx.working_dir,
            &state.worktree_ctx.repo_path,
            name,
        ),
        None => Err(EngineError::Workflow(format!(
            "No schema resolver configured — cannot load schema '{name}'"
        ))),
    }
}

/// Emit an engine event to all registered sinks.
///
/// Each sink is called inside `catch_unwind(AssertUnwindSafe(...))`. Panics are
/// logged via `tracing::warn!` and do not abort the run or skip remaining sinks.
pub fn emit_event(state: &ExecutionState, event: EngineEvent) {
    crate::events::emit_to_sinks(&state.workflow_run_id, event, &state.event_sinks);
}

/// Extract completed step keys from a slice of step records.
pub fn completed_keys_from_steps(steps: &[WorkflowRunStep]) -> HashSet<StepKey> {
    steps
        .iter()
        .filter(|s| s.status == WorkflowStepStatus::Completed)
        .map(|s| (s.step_name.clone(), s.iteration as u32))
        .collect()
}

/// Validate required workflow inputs are present and apply default values.
pub fn apply_workflow_input_defaults(
    workflow: &WorkflowDef,
    inputs: &mut HashMap<String, String>,
) -> Result<()> {
    for input_decl in &workflow.inputs {
        if input_decl.required && !inputs.contains_key(&input_decl.name) {
            return Err(EngineError::Workflow(format!(
                "Missing required input: '{}'. Use --input {}=<value>.",
                input_decl.name, input_decl.name
            )));
        }
        if let Some(ref default) = input_decl.default {
            inputs
                .entry(input_decl.name.clone())
                .or_insert_with(|| default.clone());
        }
        if input_decl.input_type == InputType::Boolean {
            inputs
                .entry(input_decl.name.clone())
                .or_insert_with(|| "false".to_string());
        }
    }
    Ok(())
}

/// Shared orchestration: execute body → always block → build summary → finalize.
pub fn run_workflow_engine(
    state: &mut ExecutionState,
    workflow: &WorkflowDef,
) -> Result<WorkflowResult> {
    // Emit RunStarted or RunResumed (DB write for the run record already happened before this fn).
    if state.resume_ctx.is_some() {
        emit_event(
            state,
            EngineEvent::RunResumed {
                workflow_name: workflow.name.clone(),
            },
        );
    } else {
        emit_event(
            state,
            EngineEvent::RunStarted {
                workflow_name: workflow.name.clone(),
            },
        );
    }

    // Execute main body
    let mut body_error: Option<String> = None;
    let body_result = execute_nodes(state, &workflow.body, true);
    if let Err(ref e) = body_result {
        let msg = e.to_string();
        tracing::error!("Body execution error: {msg}");
        state.all_succeeded = false;
        body_error = Some(msg);
    }

    // Execute always block regardless of outcome
    if !workflow.always.is_empty() {
        let workflow_status = if state.all_succeeded {
            "completed"
        } else {
            "failed"
        };
        state
            .inputs
            .insert("workflow_status".to_string(), workflow_status.to_string());
        // Snapshot all_succeeded so the always block cannot change the terminal status.
        let saved_all_succeeded = state.all_succeeded;
        let always_result = execute_nodes(state, &workflow.always, false);
        state.all_succeeded = saved_all_succeeded;
        if let Err(ref e) = always_result {
            tracing::warn!("Always block error (non-fatal): {e}");
        }
    }

    // Build summary
    let mut summary = crate::helpers::build_workflow_summary(state);
    if let Some(ref err) = body_error {
        summary.push_str(&format!("\nError: {err}"));
    }

    // Finalize run status via persistence
    let wf_run_id = state.workflow_run_id.clone();
    let is_cancelled = matches!(&body_result, Err(EngineError::Cancelled(_)));

    if let Err(e) = state.flush_metrics() {
        tracing::warn!(
            workflow_run_id = %wf_run_id,
            "flush_metrics failed at finalization (non-fatal, metrics may be missing): {e}"
        );
    }
    emit_event(
        state,
        EngineEvent::MetricsUpdated {
            total_cost: state.total_cost,
            total_turns: state.total_turns,
            total_duration_ms: state.total_duration_ms,
        },
    );

    if state.all_succeeded {
        state.persistence.update_run_status(
            &wf_run_id,
            WorkflowRunStatus::Completed,
            Some(&summary),
            None,
        )?;
        tracing::info!("Workflow '{}' completed successfully", workflow.name);
        emit_event(state, EngineEvent::RunCompleted { succeeded: true });
    } else if is_cancelled {
        let cancel_reason = state
            .cancellation
            .reason()
            .unwrap_or(crate::cancellation_reason::CancellationReason::UserRequested(None));
        state.persistence.update_run_status(
            &wf_run_id,
            WorkflowRunStatus::Cancelled,
            Some(&summary),
            body_error.as_deref(),
        )?;
        tracing::warn!("Workflow '{}' was cancelled", workflow.name);
        emit_event(
            state,
            EngineEvent::RunCancelled {
                reason: cancel_reason,
            },
        );
    } else {
        state.persistence.update_run_status(
            &wf_run_id,
            WorkflowRunStatus::Failed,
            Some(&summary),
            body_error.as_deref(),
        )?;
        tracing::warn!("Workflow '{}' finished with failures", workflow.name);
        emit_event(state, EngineEvent::RunCompleted { succeeded: false });
    }

    tracing::info!(
        "Total: ${:.4}, {} turns, {:.1}s",
        state.total_cost,
        state.total_turns,
        state.total_duration_ms as f64 / 1000.0
    );

    Ok(WorkflowResult {
        workflow_run_id: wf_run_id,
        worktree_id: state.worktree_ctx.worktree_id.clone(),
        workflow_name: workflow.name.clone(),
        all_succeeded: state.all_succeeded,
        total_cost: state.total_cost,
        total_turns: state.total_turns,
        total_duration_ms: state.total_duration_ms,
        total_input_tokens: state.total_input_tokens,
        total_output_tokens: state.total_output_tokens,
        total_cache_read_input_tokens: state.total_cache_read_input_tokens,
        total_cache_creation_input_tokens: state.total_cache_creation_input_tokens,
    })
}

/// Walk a list of workflow nodes, dispatching to the appropriate handler.
pub fn execute_single_node(
    state: &mut ExecutionState,
    node: &WorkflowNode,
    iteration: u32,
) -> Result<()> {
    match node {
        WorkflowNode::Call(n) => crate::executors::call::execute_call(state, n, iteration)?,
        WorkflowNode::CallWorkflow(n) => {
            crate::executors::call_workflow::execute_call_workflow(state, n, iteration)?
        }
        WorkflowNode::If(n) => crate::executors::control_flow::execute_if(state, n)?,
        WorkflowNode::Unless(n) => crate::executors::control_flow::execute_unless(state, n)?,
        WorkflowNode::While(n) => crate::executors::control_flow::execute_while(state, n)?,
        WorkflowNode::DoWhile(n) => crate::executors::control_flow::execute_do_while(state, n)?,
        WorkflowNode::Do(n) => crate::executors::control_flow::execute_do(state, n)?,
        WorkflowNode::Parallel(n) => {
            crate::executors::parallel::execute_parallel(state, n, iteration)?
        }
        WorkflowNode::Gate(n) => crate::executors::gate::execute_gate(state, n, iteration)?,
        WorkflowNode::Script(n) => crate::executors::script::execute_script(state, n, iteration)?,
        WorkflowNode::ForEach(n) => {
            crate::executors::foreach::execute_foreach(state, n, iteration)?
        }
        WorkflowNode::Always(n) => {
            // Nested always — just execute body
            execute_nodes(state, &n.body, false)?;
        }
    }
    Ok(())
}

pub fn execute_nodes(
    state: &mut ExecutionState,
    nodes: &[WorkflowNode],
    respect_fail_fast: bool,
) -> Result<()> {
    use crate::cancellation_reason::CancellationReason;
    for node in nodes {
        if respect_fail_fast && !state.all_succeeded && state.exec_config.fail_fast {
            break;
        }
        // Cheap in-memory token check first (no I/O).
        if state.cancellation.is_cancelled() {
            return state.cancellation.error_if_cancelled();
        }
        // Cross-process: DB polling for Cancelling status written by another process.
        match state.persistence.is_run_cancelled(&state.workflow_run_id) {
            Ok(true) => {
                tracing::info!(
                    "Workflow run {} cancelled externally, stopping execution",
                    state.workflow_run_id
                );
                state
                    .cancellation
                    .cancel(CancellationReason::UserRequested(None));
                return Err(EngineError::Cancelled(CancellationReason::UserRequested(
                    None,
                )));
            }
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(
                    "Database error during cancellation check for workflow run {}: {}",
                    state.workflow_run_id,
                    e
                );
            }
        }
        // Throttled heartbeat tick — write at most once every 5 seconds.
        {
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            let last = state.last_heartbeat_at.load(Ordering::Relaxed);
            if now_secs - last >= 5 {
                state.last_heartbeat_at.store(now_secs, Ordering::Relaxed);
                if let Err(e) = state.persistence.tick_heartbeat(&state.workflow_run_id) {
                    tracing::warn!("tick_heartbeat failed (non-fatal): {e}");
                }
            }
        }
        execute_single_node(state, node, 0)?;
    }
    Ok(())
}

/// Record a failed step result and optionally return a fail-fast error.
pub fn record_step_failure(
    state: &mut ExecutionState,
    step_key: String,
    step_label: &str,
    last_error: String,
    max_attempts: u32,
    started: bool,
) -> Result<()> {
    state.all_succeeded = false;
    let step_result = StepResult {
        step_name: step_label.to_string(),
        status: WorkflowStepStatus::Failed,
        result_text: Some(last_error),
        cost_usd: None,
        num_turns: None,
        duration_ms: None,
        markers: Vec::new(),
        context: String::new(),
        child_run_id: None,
        structured_output: None,
        output_file: None,
    };
    state.step_results.insert(step_key, step_result);

    if state.exec_config.fail_fast {
        let msg = if started {
            format!(
                "Step '{}' failed after {} attempts",
                step_label, max_attempts
            )
        } else {
            format!("Step '{}' failed to start (never executed)", step_label)
        };
        return Err(EngineError::Workflow(msg));
    }

    Ok(())
}

/// Record a skipped step (on_fail = continue): insert StepResult with Skipped status.
pub fn record_step_skipped(state: &mut ExecutionState, step_key: String, step_label: &str) {
    tracing::info!("Step '{}' skipped via on_fail = continue", step_label);
    let step_result = StepResult {
        step_name: step_label.to_string(),
        status: WorkflowStepStatus::Skipped,
        result_text: None,
        cost_usd: None,
        num_turns: None,
        duration_ms: None,
        markers: Vec::new(),
        context: String::new(),
        child_run_id: None,
        structured_output: None,
        output_file: None,
    };
    state.step_results.insert(step_key, step_result);
}

/// Record a successful step: accumulate stats, insert StepResult, push context.
#[allow(clippy::too_many_arguments)]
pub fn record_step_success(
    state: &mut ExecutionState,
    step_key: String,
    step_name: &str,
    result_text: Option<String>,
    cost_usd: Option<f64>,
    num_turns: Option<i64>,
    duration_ms: Option<i64>,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    cache_read_input_tokens: Option<i64>,
    cache_creation_input_tokens: Option<i64>,
    markers: Vec<String>,
    context: String,
    child_run_id: Option<String>,
    iteration: u32,
    structured_output: Option<String>,
    output_file: Option<String>,
) {
    state.accumulate_metrics(
        cost_usd,
        num_turns,
        duration_ms,
        input_tokens,
        output_tokens,
        cache_read_input_tokens,
        cache_creation_input_tokens,
    );

    // Best-effort mid-run metrics flush — non-fatal
    if let Err(e) = state.flush_metrics() {
        tracing::warn!("Failed to flush mid-run metrics: {e}");
    }

    let markers_for_ctx = markers.clone();
    let structured_output_for_ctx = structured_output.clone();
    let output_file_for_ctx = output_file.clone();
    let step_result = StepResult {
        step_name: step_name.to_string(),
        status: WorkflowStepStatus::Completed,
        result_text,
        cost_usd,
        num_turns,
        duration_ms,
        markers,
        context: context.clone(),
        child_run_id,
        structured_output,
        output_file,
    };
    state.step_results.insert(step_key, step_result);

    state.contexts.push(ContextEntry {
        step: step_name.to_string(),
        iteration,
        context,
        markers: markers_for_ctx,
        structured_output: structured_output_for_ctx,
        output_file: output_file_for_ctx,
    });
}

/// Resolve child workflow inputs: substitute variables, apply defaults, and
/// check for missing required inputs.
pub fn resolve_child_inputs(
    raw_inputs: &HashMap<String, String>,
    vars: &HashMap<&str, String>,
    input_decls: &[crate::dsl::InputDecl],
) -> std::result::Result<HashMap<String, String>, String> {
    let mut child_inputs = HashMap::new();
    for (k, v) in raw_inputs {
        child_inputs.insert(
            k.clone(),
            crate::prompt_builder::substitute_variables_keep_literal(v, vars),
        );
    }
    for decl in input_decls {
        if !child_inputs.contains_key(&decl.name) {
            if decl.required {
                return Err(decl.name.clone());
            }
            if let Some(ref default) = decl.default {
                child_inputs.insert(decl.name.clone(), default.clone());
            }
            if decl.input_type == crate::dsl::InputType::Boolean {
                child_inputs
                    .entry(decl.name.clone())
                    .or_insert_with(|| "false".to_string());
            }
        }
    }
    Ok(child_inputs)
}

/// Run the on_fail agent after all retries for a step are exhausted.
pub fn run_on_fail_agent(
    state: &mut ExecutionState,
    step_label: &str,
    on_fail_agent: &crate::dsl::AgentRef,
    last_error: &str,
    retries: u32,
    iteration: u32,
) {
    tracing::warn!(
        "All retries exhausted for '{}', running on_fail agent '{}'",
        step_label,
        on_fail_agent.label(),
    );
    state
        .inputs
        .insert("failed_step".to_string(), step_label.to_string());
    state
        .inputs
        .insert("failure_reason".to_string(), last_error.to_string());
    state
        .inputs
        .insert("retry_count".to_string(), retries.to_string());

    let on_fail_node = crate::dsl::CallNode {
        agent: on_fail_agent.clone(),
        retries: 0,
        on_fail: None,
        output: None,
        with: Vec::new(),
        bot_name: None,
        plugin_dirs: Vec::new(),
        timeout: None,
    };
    if let Err(e) = crate::executors::call::execute_call(state, &on_fail_node, iteration) {
        tracing::warn!("on_fail agent '{}' also failed: {e}", on_fail_agent.label(),);
    }

    state.inputs.remove("failed_step");
    state.inputs.remove("failure_reason");
    state.inputs.remove("retry_count");
}

/// Dispatch `on_fail` after all retries are exhausted, then record the failure.
#[allow(clippy::too_many_arguments)]
pub fn handle_on_fail(
    state: &mut ExecutionState,
    step_key: String,
    step_label: &str,
    on_fail: &Option<OnFail>,
    last_error: String,
    retries: u32,
    iteration: u32,
    max_attempts: u32,
) -> Result<()> {
    match on_fail {
        Some(OnFail::Continue) => {
            record_step_skipped(state, step_key, step_label);
            return Ok(());
        }
        Some(OnFail::Agent(ref on_fail_agent)) => {
            run_on_fail_agent(
                state,
                step_label,
                on_fail_agent,
                &last_error,
                retries,
                iteration,
            );
        }
        None => {}
    }
    record_step_failure(state, step_key, step_label, last_error, max_attempts, true)
}

/// Check whether a step should be skipped on resume.
pub fn should_skip(state: &ExecutionState, step_name: &str, iteration: u32) -> bool {
    state.resume_ctx.as_ref().is_some_and(|ctx| {
        ctx.skip_completed
            .contains(&(step_name.to_owned(), iteration))
    })
}

/// Deserialize a `markers_out` JSON string into a `Vec<String>`, logging on error.
fn parse_markers_out(markers_json: Option<&str>, step_name: &str) -> Vec<String> {
    markers_json
        .and_then(|m| {
            serde_json::from_str(m)
                .map_err(|e| {
                    tracing::warn!("Malformed markers_out JSON in step '{step_name}': {e}")
                })
                .ok()
        })
        .unwrap_or_default()
}

/// Temporarily take the `ResumeContext` out of `state` so we can borrow `state`
/// mutably while reading from the context's maps.
pub fn restore_step(state: &mut ExecutionState, key: &str, iteration: u32) {
    let ctx = state.resume_ctx.take();
    if let Some(ref ctx) = ctx {
        restore_completed_step(state, ctx, key, iteration);
    }
    state.resume_ctx = ctx;
}

/// Restore a completed step's results from the resume context into the execution state.
pub fn restore_completed_step(
    state: &mut ExecutionState,
    ctx: &ResumeContext,
    step_key: &str,
    iteration: u32,
) {
    let completed_step = ctx.step_map.get(&(step_key.to_owned(), iteration));

    let Some(step) = completed_step else {
        tracing::warn!(
            "resume: step '{step_key}:{iteration}' in skip set but not found in resume context \
             — downstream variable substitution may be incorrect"
        );
        return;
    };

    let markers = parse_markers_out(step.markers_out.as_deref(), step_key);
    let context = step.context_out.clone().unwrap_or_default();

    // Restore gate feedback if this was a gate step
    if let Some(ref feedback) = step.gate_feedback {
        state.last_gate_feedback = Some(feedback.clone());
    }

    let markers_for_ctx = markers.clone();
    let step_result = StepResult {
        step_name: step_key.to_string(),
        status: WorkflowStepStatus::Completed,
        result_text: step.result_text.clone(),
        cost_usd: None,
        num_turns: None,
        duration_ms: None,
        markers,
        context: context.clone(),
        child_run_id: step.child_run_id.clone(),
        structured_output: step.structured_output.clone(),
        output_file: step.output_file.clone(),
    };
    state.step_results.insert(step_key.to_string(), step_result);

    state.contexts.push(ContextEntry {
        step: step_key.to_string(),
        iteration,
        context,
        markers: markers_for_ctx,
        structured_output: step.structured_output.clone(),
        output_file: step.output_file.clone(),
    });
}

/// Fetch both the final step output (markers + context) and all completed step
/// results for a child workflow run in a single DB query.
pub fn fetch_child_completion_data(
    persistence: &dyn WorkflowPersistence,
    workflow_run_id: &str,
) -> ((Vec<String>, String), HashMap<String, StepResult>) {
    let steps = match persistence.get_steps(workflow_run_id) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(
                "Failed to fetch steps for child workflow run '{}': {e}",
                workflow_run_id,
            );
            return ((Vec::new(), String::new()), HashMap::new());
        }
    };

    // Derive final output from the last completed step.
    let last_completed = steps
        .iter()
        .filter(|s| s.status == WorkflowStepStatus::Completed)
        .max_by_key(|s| s.position);

    let final_output = match last_completed {
        Some(step) => {
            let markers = parse_markers_out(step.markers_out.as_deref(), &step.step_name);
            let context = step.context_out.clone().unwrap_or_default();
            (markers, context)
        }
        None => (Vec::new(), String::new()),
    };

    // Build bubble-up map from all completed steps.
    let child_steps = steps
        .into_iter()
        .filter(|s| s.status == WorkflowStepStatus::Completed)
        .map(|s| {
            let markers = parse_markers_out(s.markers_out.as_deref(), &s.step_name);
            let context = s.context_out.clone().unwrap_or_default();
            let result = StepResult {
                step_name: s.step_name.clone(),
                status: WorkflowStepStatus::Completed,
                result_text: s.result_text.clone(),
                cost_usd: None,
                num_turns: None,
                duration_ms: None,
                markers,
                context,
                child_run_id: s.child_run_id.clone(),
                structured_output: s.structured_output.clone(),
                output_file: s.output_file.clone(),
            };
            (s.step_name, result)
        })
        .collect();

    (final_output, child_steps)
}

/// Check whether the loop is stuck (identical marker sets for `stuck_after` consecutive
/// iterations). Returns `Err` if stuck, `Ok(())` otherwise.
pub fn check_stuck(
    state: &mut ExecutionState,
    prev_marker_sets: &mut Vec<HashSet<String>>,
    step: &str,
    marker: &str,
    stuck_after: u32,
    loop_kind: &str,
) -> Result<()> {
    let current_markers: HashSet<String> = state
        .step_results
        .get(step)
        .map(|r| r.markers.iter().cloned().collect())
        .unwrap_or_default();

    prev_marker_sets.push(current_markers.clone());

    if prev_marker_sets.len() >= stuck_after as usize {
        let window = &prev_marker_sets[prev_marker_sets.len() - stuck_after as usize..];
        if window.iter().all(|s| s == &current_markers) {
            tracing::warn!(
                "{loop_kind} {step}.{marker} — stuck: identical markers for {stuck_after} consecutive iterations",
            );
            state.all_succeeded = false;
            return Err(EngineError::Workflow(format!(
                "{loop_kind} {step}.{marker} stuck after {stuck_after} iterations with identical markers",
            )));
        }
    }

    Ok(())
}

/// Check whether the loop has exceeded `max_iterations`.
pub fn check_max_iterations(
    state: &mut ExecutionState,
    iteration: u32,
    max_iterations: u32,
    on_max_iter: &crate::dsl::OnMaxIter,
    step: &str,
    marker: &str,
    loop_kind: &str,
) -> Result<bool> {
    if iteration >= max_iterations {
        tracing::warn!("{loop_kind} {step}.{marker} — reached max_iterations ({max_iterations})",);
        match on_max_iter {
            crate::dsl::OnMaxIter::Fail => {
                state.all_succeeded = false;
                return Err(EngineError::Workflow(format!(
                    "{loop_kind} {step}.{marker} reached max_iterations ({max_iterations})",
                )));
            }
            crate::dsl::OnMaxIter::Continue => return Ok(true),
        }
    }
    Ok(false)
}

/// Build the variable map from execution state for substitution.
pub fn build_variable_map(state: &ExecutionState) -> HashMap<&str, String> {
    crate::prompt_builder::build_variable_map(state)
}

/// Generate the CONDUCTOR_OUTPUT instruction (used when no schema is set).
pub fn conductor_output_instruction() -> &'static str {
    CONDUCTOR_OUTPUT_INSTRUCTION
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::{InputDecl, InputType, WorkflowDef, WorkflowTrigger};

    fn make_bool_workflow(
        name: &str,
        input_name: &str,
        required: bool,
        default: Option<&str>,
    ) -> WorkflowDef {
        WorkflowDef {
            name: name.to_string(),
            title: None,
            description: String::new(),
            trigger: WorkflowTrigger::Manual,
            targets: vec![],
            group: None,
            inputs: vec![InputDecl {
                name: input_name.to_string(),
                input_type: InputType::Boolean,
                required,
                default: default.map(|s| s.to_string()),
                description: None,
            }],
            body: vec![],
            always: vec![],
            source_path: String::new(),
        }
    }

    #[test]
    fn test_boolean_input_defaults_to_false_when_absent() {
        let workflow = make_bool_workflow("wf", "flag", false, None);
        let mut inputs = HashMap::new();
        apply_workflow_input_defaults(&workflow, &mut inputs).unwrap();
        assert_eq!(inputs.get("flag").map(|s| s.as_str()), Some("false"));
    }

    #[test]
    fn test_boolean_input_uses_explicit_default_over_false() {
        let workflow = make_bool_workflow("wf", "flag", false, Some("true"));
        let mut inputs = HashMap::new();
        apply_workflow_input_defaults(&workflow, &mut inputs).unwrap();
        assert_eq!(inputs.get("flag").map(|s| s.as_str()), Some("true"));
    }

    #[test]
    fn test_boolean_input_caller_value_not_overwritten() {
        let workflow = make_bool_workflow("wf", "flag", false, None);
        let mut inputs = HashMap::new();
        inputs.insert("flag".to_string(), "true".to_string());
        apply_workflow_input_defaults(&workflow, &mut inputs).unwrap();
        assert_eq!(inputs.get("flag").map(|s| s.as_str()), Some("true"));
    }

    #[test]
    fn test_boolean_input_required_and_missing_is_error() {
        let workflow = make_bool_workflow("wf", "flag", true, None);
        let mut inputs = HashMap::new();
        let result = apply_workflow_input_defaults(&workflow, &mut inputs);
        assert!(result.is_err(), "expected error for missing required input");
    }
}
