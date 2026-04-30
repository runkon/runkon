use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cancellation::CancellationToken;
use crate::constants::FLOW_OUTPUT_INSTRUCTION;
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

/// Domain-identity context for a single workflow execution.
#[derive(Clone)]
pub struct WorktreeContext {
    pub worktree_id: Option<String>,
    pub working_dir: String,
    pub repo_path: String,
    pub ticket_id: Option<String>,
    pub repo_id: Option<String>,
    pub extra_plugin_dirs: Vec<String>,
}

/// Pre-loaded context for resuming a workflow run.
#[derive(Clone)]
pub struct ResumeContext {
    /// Completed step records keyed by step key, for O(1) skip check and restore.
    pub step_map: HashMap<StepKey, WorkflowRunStep>,
}

/// Mutable runtime state for a workflow execution — no conductor-core deps.
#[derive(Clone)]
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

/// Subset of `ExecutionState` exposed to `ChildWorkflowRunner` implementations.
///
/// The full `ExecutionState` carries the engine's mutable runtime — registries,
/// accumulators, schema resolver, position pointer, cancellation token —
/// none of which a harness needs to spawn a child workflow run. Passing it
/// across the trait boundary makes every `ExecutionState` field rename or
/// restructuring a breaking change for every `ChildWorkflowRunner` implementor.
///
/// `ChildWorkflowContext` is the narrow, stable surface: every field listed
/// here is something the bridge actually reads when constructing the child
/// run. Build via [`ExecutionState::child_workflow_context`].
#[derive(Clone)]
pub struct ChildWorkflowContext {
    pub worktree_ctx: WorktreeContext,
    pub workflow_run_id: String,
    pub model: Option<String>,
    pub target_label: Option<String>,
    pub exec_config: WorkflowExecConfig,
    pub inputs: HashMap<String, String>,
    pub triggered_by_hook: bool,
    pub event_sinks: Arc<[Arc<dyn EventSink>]>,
}

/// Trait for executing child workflows — allows conductor-core to inject its adapter.
pub trait ChildWorkflowRunner: Send + Sync {
    fn execute_child(
        &self,
        workflow_name: &str,
        parent_ctx: &ChildWorkflowContext,
        params: ChildWorkflowInput,
    ) -> Result<WorkflowResult>;

    fn resume_child(
        &self,
        workflow_run_id: &str,
        model: Option<&str>,
        parent_ctx: &ChildWorkflowContext,
    ) -> Result<WorkflowResult>;

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

    /// Throttled heartbeat tick + external cancel check.
    ///
    /// Bumps `last_heartbeat` in persistence at most once every 5 seconds and
    /// polls for cross-process cancellation via `persistence.is_run_cancelled`.
    /// On external cancel, sets `self.cancellation` and returns
    /// `Err(EngineError::Cancelled)`.
    ///
    /// Callers that own the engine main loop use `?` to propagate cancellation
    /// up. Wait loops that need to drain in-flight work (parallel, foreach)
    /// can call this best-effort and rely on `self.cancellation.is_cancelled()`
    /// for their controlled exit — the cancellation token is set by this
    /// helper before the `Err` is returned.
    ///
    /// Without this being called from inside long-running wait loops (parallel
    /// blocks, foreach fan-out), the heartbeat goes stale during multi-minute
    /// waits and the watchdog reaper races the engine after >60 s — see #2731.
    pub fn tick_heartbeat_throttled(&self) -> Result<()> {
        use crate::cancellation_reason::CancellationReason;

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|e| {
                tracing::warn!("system clock regressed: {e}; heartbeat suppressed");
                e.duration()
            })
            .as_secs() as i64;
        let last = self.last_heartbeat_at.load(Ordering::Relaxed);
        if now_secs - last < 5 {
            return Ok(());
        }
        self.last_heartbeat_at.store(now_secs, Ordering::Relaxed);
        match self.persistence.is_run_cancelled(&self.workflow_run_id) {
            Ok(true) => {
                tracing::info!(
                    "Workflow run {} cancelled externally, stopping execution",
                    self.workflow_run_id
                );
                self.cancellation
                    .cancel(CancellationReason::UserRequested(None));
                return Err(EngineError::Cancelled(CancellationReason::UserRequested(
                    None,
                )));
            }
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(
                    "Database error during cancellation check for workflow run {}: {}",
                    self.workflow_run_id,
                    e
                );
            }
        }
        if let Err(e) = self.persistence.tick_heartbeat(&self.workflow_run_id) {
            tracing::warn!("tick_heartbeat failed (non-fatal): {e}");
        }
        Ok(())
    }

    /// Project this state into the narrow surface a `ChildWorkflowRunner`
    /// implementation needs to spawn a child run.
    pub fn child_workflow_context(&self) -> ChildWorkflowContext {
        ChildWorkflowContext {
            worktree_ctx: self.worktree_ctx.clone(),
            workflow_run_id: self.workflow_run_id.clone(),
            model: self.model.clone(),
            target_label: self.target_label.clone(),
            exec_config: self.exec_config.clone(),
            inputs: self.inputs.clone(),
            triggered_by_hook: self.triggered_by_hook,
            event_sinks: Arc::clone(&self.event_sinks),
        }
    }

    /// Fork a child execution state from this parent.
    ///
    /// Copies shared configuration (persistence, registries, workflow identity) and resets all
    /// runtime accumulators so the child starts with a clean slate.
    pub fn fork_child(&self, cancellation: CancellationToken) -> ExecutionState {
        let mut child = self.clone();
        child.inputs.clear();
        child.step_results.clear();
        child.contexts.clear();
        child.position = 0;
        child.all_succeeded = true;
        child.total_cost = 0.0;
        child.total_turns = 0;
        child.total_duration_ms = 0;
        child.total_input_tokens = 0;
        child.total_output_tokens = 0;
        child.total_cache_read_input_tokens = 0;
        child.total_cache_creation_input_tokens = 0;
        child.last_gate_feedback = None;
        child.block_output = None;
        child.block_with.clear();
        child.resume_ctx = None;
        child.triggered_by_hook = false;
        child.last_heartbeat_at = Self::new_heartbeat();
        child.cancellation = cancellation;
        child.current_execution_id = Arc::new(std::sync::Mutex::new(None));
        child
    }

    /// Accumulate individual metrics into this execution state.
    ///
    /// Returns `true` if at least one metric was present and added.
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
    ) -> bool {
        let mut changed = false;
        if let Some(c) = cost {
            self.total_cost += c;
            changed = true;
        }
        if let Some(t) = turns {
            self.total_turns += t;
            changed = true;
        }
        if let Some(d) = duration {
            self.total_duration_ms += d;
            changed = true;
        }
        if let Some(t) = input_tokens {
            self.total_input_tokens += t;
            changed = true;
        }
        if let Some(t) = output_tokens {
            self.total_output_tokens += t;
            changed = true;
        }
        if let Some(t) = cache_read {
            self.total_cache_read_input_tokens += t;
            changed = true;
        }
        if let Some(t) = cache_create {
            self.total_cache_creation_input_tokens += t;
            changed = true;
        }
        changed
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

/// Input keys that the workflow engine injects automatically from the run context.
///
/// These keys are populated from `WorktreeContext` fields at execution time; callers
/// should treat them as read-only and avoid defining workflow inputs with these names.
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
    for node in nodes {
        if respect_fail_fast && !state.all_succeeded && state.exec_config.fail_fast {
            break;
        }
        // Cheap in-memory token check first (no I/O).
        if state.cancellation.is_cancelled() {
            return state.cancellation.error_if_cancelled();
        }
        state.tick_heartbeat_throttled()?;
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
    let step_result = StepResult::failed(step_label, last_error);
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
    let step_result = StepResult::skipped(step_label);
    state.step_results.insert(step_key, step_result);
}

/// Record a successful step: accumulate stats, insert StepResult, push context.
pub fn record_step_success(
    state: &mut ExecutionState,
    step_key: String,
    success: crate::types::StepSuccess,
) {
    let metrics_changed = state.accumulate_metrics(
        success.cost_usd,
        success.num_turns,
        success.duration_ms,
        success.input_tokens,
        success.output_tokens,
        success.cache_read_input_tokens,
        success.cache_creation_input_tokens,
    );

    // Best-effort mid-run metrics flush — non-fatal, only if something changed
    if metrics_changed {
        if let Err(e) = state.flush_metrics() {
            tracing::warn!("Failed to flush mid-run metrics: {e}");
        }
    }

    let step_result = StepResult::completed(&success);
    state.step_results.insert(step_key, step_result);

    state.contexts.push(success.into());
}

/// Resolve child workflow inputs: substitute variables, apply defaults, and
/// check for missing required inputs.
pub fn resolve_child_inputs(
    raw_inputs: &HashMap<String, String>,
    vars: &HashMap<String, String>,
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
        ctx.step_map
            .contains_key(&(step_name.to_owned(), iteration))
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

    // Accumulate costs from the step's joined agent run metrics.
    state.accumulate_metrics(
        step.cost_usd,
        step.num_turns,
        step.duration_ms,
        step.input_tokens,
        step.output_tokens,
        step.cache_read_input_tokens,
        step.cache_creation_input_tokens,
    );

    // Restore gate feedback if this was a gate step
    if let Some(ref feedback) = step.gate_feedback {
        state.last_gate_feedback = Some(feedback.clone());
    }

    let success = crate::types::StepSuccess::from_workflow_run_step(
        step_key.to_string(),
        step,
        markers,
        context,
        iteration,
    );
    let step_result = StepResult::completed_without_metrics(&success);
    state.step_results.insert(step_key.to_string(), step_result);

    state.contexts.push(success.into());
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

    // Collect completed steps once; derive both final output and bubble-up map from it.
    let completed: Vec<_> = steps
        .into_iter()
        .filter(|s| s.status == WorkflowStepStatus::Completed)
        .collect();

    let final_output = match completed.iter().max_by_key(|s| s.position) {
        Some(step) => {
            let markers = parse_markers_out(step.markers_out.as_deref(), &step.step_name);
            let context = step.context_out.clone().unwrap_or_default();
            (markers, context)
        }
        None => (Vec::new(), String::new()),
    };

    // Build bubble-up map from all completed steps.
    let child_steps = completed
        .into_iter()
        .map(|s| {
            let markers = parse_markers_out(s.markers_out.as_deref(), &s.step_name);
            let context = s.context_out.clone().unwrap_or_default();
            let success = crate::types::StepSuccess::from_workflow_run_step(
                s.step_name.clone(),
                &s,
                markers,
                context,
                0,
            );
            let result = StepResult::completed_without_metrics(&success);
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
pub fn build_variable_map(state: &ExecutionState) -> HashMap<String, String> {
    crate::prompt_builder::build_variable_map(state)
}

/// Generate the FLOW_OUTPUT instruction (used when no schema is set).
pub fn flow_output_instruction() -> &'static str {
    FLOW_OUTPUT_INSTRUCTION
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

    #[test]
    fn fork_child_resets_runtime_state_and_preserves_shared_config() {
        use crate::cancellation::CancellationToken;
        use crate::persistence_memory::InMemoryWorkflowPersistence;
        use crate::traits::script_env_provider::NoOpScriptEnvProvider;
        use crate::types::WorkflowExecConfig;

        struct DummyChildRunner;
        impl ChildWorkflowRunner for DummyChildRunner {
            fn execute_child(
                &self,
                _workflow_name: &str,
                _parent_ctx: &ChildWorkflowContext,
                _params: ChildWorkflowInput,
            ) -> Result<crate::types::WorkflowResult> {
                unimplemented!()
            }
            fn resume_child(
                &self,
                _workflow_run_id: &str,
                _model: Option<&str>,
                _parent_ctx: &ChildWorkflowContext,
            ) -> Result<crate::types::WorkflowResult> {
                unimplemented!()
            }
            fn find_resumable_child(
                &self,
                _parent_run_id: &str,
                _workflow_name: &str,
            ) -> Result<Option<crate::types::WorkflowRun>> {
                unimplemented!()
            }
        }

        let parent = ExecutionState {
            persistence: Arc::new(InMemoryWorkflowPersistence::new()),
            action_registry: Arc::new(crate::traits::action_executor::ActionRegistry::new(
                HashMap::new(),
                None,
            )),
            script_env_provider: Arc::new(NoOpScriptEnvProvider),
            workflow_run_id: "run-1".to_string(),
            workflow_name: "wf".to_string(),
            worktree_ctx: WorktreeContext {
                worktree_id: Some("wt".to_string()),
                working_dir: "/tmp".to_string(),
                repo_path: "/repo".to_string(),
                ticket_id: Some("TICK-1".to_string()),
                repo_id: Some("repo-1".to_string()),
                extra_plugin_dirs: vec!["plugins".to_string()],
            },
            model: Some("gpt-4".to_string()),
            exec_config: WorkflowExecConfig::default(),
            inputs: {
                let mut m = HashMap::new();
                m.insert("key".to_string(), "val".to_string());
                m
            },
            parent_run_id: "parent-1".to_string(),
            depth: 3,
            target_label: Some("label".to_string()),
            step_results: {
                let mut m = HashMap::new();
                m.insert("step".to_string(), StepResult::default());
                m
            },
            contexts: vec![ContextEntry {
                step: "step".to_string(),
                iteration: 1,
                context: "ctx".to_string(),
                markers: vec![],
                structured_output: None,
                output_file: None,
            }],
            position: 42,
            all_succeeded: false,
            total_cost: 1.23,
            total_turns: 5,
            total_duration_ms: 1000,
            total_input_tokens: 100,
            total_output_tokens: 200,
            total_cache_read_input_tokens: 50,
            total_cache_creation_input_tokens: 25,
            last_gate_feedback: Some("feedback".to_string()),
            block_output: Some("output".to_string()),
            block_with: vec!["with".to_string()],
            resume_ctx: None,
            default_bot_name: Some("bot".to_string()),
            triggered_by_hook: true,
            schema_resolver: None,
            child_runner: Some(Arc::new(DummyChildRunner)),
            last_heartbeat_at: ExecutionState::new_heartbeat(),
            registry: Arc::new(crate::traits::item_provider::ItemProviderRegistry::new()),
            event_sinks: Arc::from(vec![]),
            cancellation: CancellationToken::new(),
            current_execution_id: Arc::new(std::sync::Mutex::new(None)),
        };

        let child_cancellation = CancellationToken::new();
        let child = parent.fork_child(child_cancellation.clone());

        // Shared config cloned
        assert_eq!(child.workflow_run_id, "run-1");
        assert_eq!(child.workflow_name, "wf");
        assert_eq!(child.worktree_ctx.working_dir, "/tmp");
        assert_eq!(child.model, Some("gpt-4".to_string()));
        assert_eq!(child.depth, 3);
        assert_eq!(child.target_label, Some("label".to_string()));
        assert_eq!(child.default_bot_name, Some("bot".to_string()));
        assert_eq!(child.parent_run_id, "parent-1");

        // Runtime state reset
        assert!(child.inputs.is_empty(), "inputs should be cleared");
        assert!(
            child.step_results.is_empty(),
            "step_results should be cleared"
        );
        assert!(child.contexts.is_empty(), "contexts should be cleared");
        assert_eq!(child.position, 0);
        assert!(child.all_succeeded);
        assert_eq!(child.total_cost, 0.0);
        assert_eq!(child.total_turns, 0);
        assert_eq!(child.total_duration_ms, 0);
        assert_eq!(child.total_input_tokens, 0);
        assert_eq!(child.total_output_tokens, 0);
        assert_eq!(child.total_cache_read_input_tokens, 0);
        assert_eq!(child.total_cache_creation_input_tokens, 0);
        assert!(child.last_gate_feedback.is_none());
        assert!(child.block_output.is_none());
        assert!(child.block_with.is_empty());
        assert!(child.resume_ctx.is_none());
        assert!(!child.triggered_by_hook);
        assert!(child.schema_resolver.is_none());
        assert!(
            child.child_runner.is_some(),
            "child_runner should be cloned from parent"
        );

        // Cancellation replaced
        assert!(!child.cancellation.is_cancelled());
        assert!(std::sync::Arc::ptr_eq(
            &child.current_execution_id,
            &child.current_execution_id
        ));
    }

    #[test]
    fn child_workflow_context_projects_all_eight_fields() {
        use crate::cancellation::CancellationToken;
        use crate::events::{EngineEventData, EventSink};
        use crate::persistence_memory::InMemoryWorkflowPersistence;
        use crate::traits::script_env_provider::NoOpScriptEnvProvider;
        use crate::types::WorkflowExecConfig;

        struct TestSink;
        impl EventSink for TestSink {
            fn emit(&self, _: &EngineEventData) {}
        }

        let sinks: Arc<[Arc<dyn EventSink>]> = Arc::from(vec![
            Arc::new(TestSink) as Arc<dyn EventSink>,
            Arc::new(TestSink) as Arc<dyn EventSink>,
        ]);

        let mut state_inputs = HashMap::new();
        state_inputs.insert("ticket_id".to_string(), "TICK-42".to_string());
        state_inputs.insert("repo_id".to_string(), "repo-7".to_string());

        // Distinguishable by some non-default field; event_sinks below is the primary check.
        let exec_config = WorkflowExecConfig {
            dry_run: true,
            ..WorkflowExecConfig::default()
        };

        let parent = ExecutionState {
            persistence: Arc::new(InMemoryWorkflowPersistence::new()),
            action_registry: Arc::new(crate::traits::action_executor::ActionRegistry::new(
                HashMap::new(),
                None,
            )),
            script_env_provider: Arc::new(NoOpScriptEnvProvider),
            workflow_run_id: "run-projection-test".to_string(),
            workflow_name: "wf-projection".to_string(),
            worktree_ctx: WorktreeContext {
                worktree_id: Some("wt-9".to_string()),
                working_dir: "/tmp/proj".to_string(),
                repo_path: "/repo/proj".to_string(),
                ticket_id: Some("TICK-42".to_string()),
                repo_id: Some("repo-7".to_string()),
                extra_plugin_dirs: vec!["plugin-a".to_string()],
            },
            model: Some("opus".to_string()),
            exec_config: exec_config.clone(),
            inputs: state_inputs.clone(),
            parent_run_id: "parent-7".to_string(),
            depth: 2,
            target_label: Some("proj-label".to_string()),
            step_results: HashMap::new(),
            contexts: vec![],
            position: 11,
            all_succeeded: false,
            total_cost: 0.0,
            total_turns: 0,
            total_duration_ms: 0,
            total_input_tokens: 0,
            total_output_tokens: 0,
            total_cache_read_input_tokens: 0,
            total_cache_creation_input_tokens: 0,
            last_gate_feedback: None,
            block_output: None,
            block_with: vec![],
            resume_ctx: None,
            default_bot_name: None,
            triggered_by_hook: true,
            schema_resolver: None,
            child_runner: None,
            last_heartbeat_at: ExecutionState::new_heartbeat(),
            registry: Arc::new(crate::traits::item_provider::ItemProviderRegistry::new()),
            event_sinks: Arc::clone(&sinks),
            cancellation: CancellationToken::new(),
            current_execution_id: Arc::new(std::sync::Mutex::new(None)),
        };

        let ctx = parent.child_workflow_context();

        // All eight fields project verbatim.
        assert_eq!(ctx.worktree_ctx.worktree_id.as_deref(), Some("wt-9"));
        assert_eq!(ctx.worktree_ctx.working_dir, "/tmp/proj");
        assert_eq!(ctx.worktree_ctx.repo_path, "/repo/proj");
        assert_eq!(ctx.worktree_ctx.ticket_id.as_deref(), Some("TICK-42"));
        assert_eq!(ctx.worktree_ctx.repo_id.as_deref(), Some("repo-7"));
        assert_eq!(ctx.worktree_ctx.extra_plugin_dirs, vec!["plugin-a"]);
        assert_eq!(ctx.workflow_run_id, "run-projection-test");
        assert_eq!(ctx.model.as_deref(), Some("opus"));
        assert_eq!(ctx.target_label.as_deref(), Some("proj-label"));
        assert!(ctx.exec_config.dry_run);
        assert_eq!(ctx.inputs, state_inputs);
        assert!(ctx.triggered_by_hook);

        // event_sinks slice is shared, not deep-copied.
        assert_eq!(ctx.event_sinks.len(), 2);
        assert!(
            Arc::ptr_eq(&ctx.event_sinks, &sinks),
            "event_sinks slice should be shared via Arc, not cloned"
        );
    }

    use crate::test_helpers::CountingPersistence;

    /// Build a minimal ExecutionState wired to a CountingPersistence.
    fn make_state_with_counting_persistence(
        cp: std::sync::Arc<CountingPersistence>,
        run_id: String,
    ) -> ExecutionState {
        crate::test_helpers::make_test_execution_state(
            cp as Arc<dyn crate::traits::persistence::WorkflowPersistence>,
            run_id,
        )
    }

    /// First call must tick (initial state has last_heartbeat_at = 0, far enough
    /// in the past to clear the 5 s gate). An immediate second call must NOT
    /// tick — it falls inside the 5 s throttle window.
    #[test]
    fn tick_heartbeat_throttled_first_call_ticks_second_call_throttled() {
        let cp = Arc::new(CountingPersistence::new());
        let state = make_state_with_counting_persistence(Arc::clone(&cp), "run-1".into());

        assert_eq!(cp.tick_count(), 0);
        state.tick_heartbeat_throttled().unwrap();
        assert_eq!(cp.tick_count(), 1, "first call must tick");

        // Immediate second call falls inside the 5 s window.
        state.tick_heartbeat_throttled().unwrap();
        assert_eq!(
            cp.tick_count(),
            1,
            "second call within 5s must be throttled, not tick again"
        );
    }

    /// When persistence reports the run cancelled, the helper sets
    /// `state.cancellation` and returns `Err(Cancelled)`.
    #[test]
    fn tick_heartbeat_throttled_propagates_external_cancel() {
        let cp = Arc::new(CountingPersistence::new());
        cp.set_cancelled(true);
        let state = make_state_with_counting_persistence(Arc::clone(&cp), "run-1".into());

        assert!(!state.cancellation.is_cancelled());
        let result = state.tick_heartbeat_throttled();
        assert!(
            matches!(result, Err(EngineError::Cancelled(_))),
            "expected Err(Cancelled), got {result:?}"
        );
        assert!(
            state.cancellation.is_cancelled(),
            "helper must set state.cancellation on external cancel"
        );
    }
}
