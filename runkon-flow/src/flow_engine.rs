use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::cancellation::CancellationToken;
use crate::cancellation_reason::CancellationReason;
use crate::dsl::{detect_workflow_cycles, GateType, ValidationError, WorkflowDef, WorkflowNode};
use crate::engine::{run_workflow_engine, ExecutionState};
use crate::engine_error::EngineError;
use crate::events::EventSink;
use crate::status::WorkflowRunStatus;
use crate::traits::action_executor::{ActionExecutor, ActionRegistry};
use crate::traits::gate_resolver::{GateResolver, GateResolverRegistry};
use crate::traits::item_provider::{ItemProvider, ItemProviderRegistry};
use crate::traits::persistence::WorkflowPersistence;
use crate::traits::script_env_provider::{NoOpScriptEnvProvider, ScriptEnvProvider};
use crate::traits::workflow_resolver::WorkflowResolver;
use crate::types::{WorkflowResult, WorkflowRunStep};
use crate::workflow_resolver_directory::DirectoryWorkflowResolver;

// ---------------------------------------------------------------------------
// FlowEngine
// ---------------------------------------------------------------------------

/// All per-run state needed by `cancel_run()` and `Drop`. Stored atomically in
/// a single `Mutex<HashMap>` so register/deregister/drain are each one lock.
struct ActiveRunEntry {
    token: CancellationToken,
    shutdown: Arc<AtomicBool>,
    persistence: Arc<dyn WorkflowPersistence>,
    registry: Arc<ActionRegistry>,
    /// (executor_label, step_id) of the step currently in flight, if any.
    exec_info: Arc<Mutex<Option<(String, String)>>>,
    /// Stop flag for the lease refresh thread. Set to `true` to ask it to exit.
    refresh_stop: Arc<AtomicBool>,
    /// Thread handle used to `unpark()` the refresh thread for fast teardown.
    refresh_thread: Option<std::thread::Thread>,
    /// Join handle for the refresh thread.
    refresh_handle: Option<std::thread::JoinHandle<()>>,
}

// ---------------------------------------------------------------------------
// Lease refresh thread
// ---------------------------------------------------------------------------

struct RefreshContext {
    persistence: Arc<dyn WorkflowPersistence>,
    run_id: String,
    token: String,
    ttl_seconds: i64,
    refresh_interval: Duration,
    stop: Arc<AtomicBool>,
    cancellation: CancellationToken,
    shutdown: Arc<AtomicBool>,
    registry: Arc<ActionRegistry>,
    exec_info: Arc<Mutex<Option<(String, String)>>>,
}

fn refresh_lease_loop(ctx: RefreshContext) {
    loop {
        std::thread::park_timeout(ctx.refresh_interval);
        if ctx.stop.load(Ordering::Relaxed) {
            return;
        }
        match ctx
            .persistence
            .acquire_lease(&ctx.run_id, &ctx.token, ctx.ttl_seconds)
        {
            Ok(Some(_)) => {} // renewed successfully
            Ok(None) => {
                tracing::warn!(
                    "run {}: lease reclaimed by another engine, aborting",
                    ctx.run_id
                );
                signal_lease_abort(ctx.shutdown, ctx.cancellation, ctx.registry, ctx.exec_info);
                return;
            }
            Err(e) => {
                tracing::warn!("run {}: lease refresh DB error: {e}, aborting", ctx.run_id);
                signal_lease_abort(ctx.shutdown, ctx.cancellation, ctx.registry, ctx.exec_info);
                return;
            }
        }
    }
}

fn signal_lease_abort(
    shutdown: Arc<AtomicBool>,
    cancellation: CancellationToken,
    registry: Arc<ActionRegistry>,
    exec_info: Arc<Mutex<Option<(String, String)>>>,
) {
    shutdown.store(true, Ordering::SeqCst);
    cancellation.cancel(CancellationReason::LeaseLost);
    let snap = exec_info.lock().unwrap_or_else(|e| e.into_inner()).clone();
    if let Some((exec_label, step_id)) = snap {
        std::thread::spawn(move || {
            if let Err(e) = registry.cancel(&exec_label, &step_id) {
                tracing::warn!("lease abort: cancel step '{step_id}' failed: {e}");
            }
        });
    }
}

/// Signal the refresh thread to exit and wake it immediately.
fn stop_refresh_thread(stop: &AtomicBool, thread: Option<&std::thread::Thread>) {
    stop.store(true, Ordering::SeqCst);
    if let Some(t) = thread {
        t.unpark();
    }
}

/// The primary harness for running and validating workflows.
///
/// Produced by [`FlowEngineBuilder::build()`].
pub struct FlowEngine {
    pub(crate) action_registry: ActionRegistry,
    pub(crate) item_provider_registry: ItemProviderRegistry,
    pub(crate) gate_resolver_registry: GateResolverRegistry,
    /// Held for future use when FlowEngine constructs ExecutionState directly.
    #[allow(dead_code)]
    pub(crate) script_env_provider: Arc<dyn ScriptEnvProvider>,
    pub(crate) workflow_resolver: Option<Arc<dyn WorkflowResolver>>,
    pub(crate) event_sinks: Vec<Arc<dyn EventSink>>,
    /// All per-run cancellation state in a single map so register/deregister
    /// are atomic (one lock covers token + shutdown + persistence + registry).
    active_runs: Mutex<HashMap<String, ActiveRunEntry>>,
}

impl FlowEngine {
    /// Validate a workflow definition against the registered executors, providers,
    /// and gate resolvers.
    ///
    /// Collects all errors before returning. Returns `Ok(())` when valid, or
    /// `Err(errors)` with one entry per problem found. Public so CI lint tools
    /// can call it without actually running the workflow.
    ///
    /// # Registry asymmetry with `run()`
    ///
    /// This method validates against the **`FlowEngine`'s own registries** (those
    /// supplied to `FlowEngineBuilder`). `run()`, however, validates against the
    /// **`ExecutionState`'s registries** at call time. Because the two registry
    /// sets are independent, a workflow that passes `validate()` may still be
    /// rejected by `run()` if the `ExecutionState` was built with a different
    /// set of action executors or item providers. Use `validate()` for static
    /// analysis (CI lint, pre-flight checks) when you control both the engine
    /// and execution state; rely on `run()`'s own validation when working with
    /// externally-supplied `ExecutionState` values.
    pub fn validate(&self, def: &WorkflowDef) -> Result<(), Vec<ValidationError>> {
        self.validate_with_registries(
            &self.action_registry,
            &self.item_provider_registry,
            &self.gate_resolver_registry,
            def,
        )
    }

    /// Run a workflow definition with a pre-built execution state.
    ///
    /// Validates against the execution state's own registries (action,
    /// item-provider) so the validation check uses the same source of truth
    /// as dispatch-time lookup.  Gate resolvers are validated against the
    /// FlowEngine's registry because `ExecutionState` carries none — gates
    /// are resolved via persistence callbacks, not the executor pipeline.
    ///
    /// Event sinks registered on the engine are injected into the state for
    /// this run; any sinks already set on `state.event_sinks` are replaced.
    pub fn run(
        &self,
        def: &WorkflowDef,
        state: &mut ExecutionState,
    ) -> crate::engine_error::Result<WorkflowResult> {
        if let Err(validation_errors) = self.validate_with_registries(
            &state.action_registry,
            &state.registry,
            &self.gate_resolver_registry,
            def,
        ) {
            let joined = validation_errors
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("\n");
            return Err(EngineError::Workflow(format!(
                "workflow '{}' failed validation:\n{}",
                def.name, joined
            )));
        }
        state.event_sinks = Arc::from(self.event_sinks.clone());

        let lease_ttl_secs = state.exec_config.lease_ttl_secs;
        let refresh_interval = state.exec_config.lease_refresh_interval;

        // Acquire or re-claim lease (idempotent when token already set by resume()).
        let token = state
            .owner_token
            .get_or_insert_with(|| ulid::Ulid::new().to_string())
            .as_str();
        match state
            .persistence
            .acquire_lease(&state.workflow_run_id, token, lease_ttl_secs)
        {
            Ok(Some(gen)) => {
                state.lease_generation = Some(gen);
            }
            Ok(None) => return Err(EngineError::AlreadyOwned(state.workflow_run_id.clone())),
            Err(e) => return Err(e),
        }

        // Ensure the exec_config.shutdown arc exists so cancel_run() can set it.
        let shutdown_arc = state
            .exec_config
            .shutdown
            .get_or_insert_with(|| Arc::new(AtomicBool::new(false)))
            .clone();

        let run_id = state.workflow_run_id.clone();

        // Spawn the background refresh thread.
        let refresh_stop = Arc::new(AtomicBool::new(false));
        let refresh_handle = {
            let ctx = RefreshContext {
                persistence: Arc::clone(&state.persistence),
                run_id: run_id.clone(),
                token: state
                    .owner_token
                    .clone()
                    .expect("owner_token was just set by get_or_insert_with"),
                ttl_seconds: lease_ttl_secs,
                refresh_interval,
                stop: Arc::clone(&refresh_stop),
                cancellation: state.cancellation.clone(),
                shutdown: Arc::clone(&shutdown_arc),
                registry: Arc::clone(&state.action_registry),
                exec_info: Arc::clone(&state.current_execution_id),
            };
            std::thread::spawn(move || refresh_lease_loop(ctx))
        };
        let refresh_thread = refresh_handle.thread().clone();

        // Register all per-run cancellation state in a single lock so cancel_run()
        // and Drop each see a consistent snapshot.
        {
            let mut runs = self.active_runs.lock().unwrap_or_else(|e| e.into_inner());
            runs.insert(
                run_id.clone(),
                ActiveRunEntry {
                    token: state.cancellation.clone(),
                    shutdown: shutdown_arc,
                    persistence: Arc::clone(&state.persistence),
                    registry: Arc::clone(&state.action_registry),
                    exec_info: Arc::clone(&state.current_execution_id),
                    refresh_stop,
                    refresh_thread: Some(refresh_thread),
                    refresh_handle: Some(refresh_handle),
                },
            );
        }

        let result = run_workflow_engine(state, def);

        // Capture LeaseLost BEFORE stopping the refresh thread to avoid a teardown
        // race where the thread sets LeaseLost after the run has already succeeded.
        let lease_lost_during_run = matches!(
            state.cancellation.reason(),
            Some(CancellationReason::LeaseLost)
        );

        // Stop the refresh thread and join it before deregistering.
        let join_handle = {
            let mut runs = self.active_runs.lock().unwrap_or_else(|e| e.into_inner());
            runs.remove(&run_id).and_then(|entry| {
                stop_refresh_thread(&entry.refresh_stop, entry.refresh_thread.as_ref());
                entry.refresh_handle
            })
        };
        // Join outside the lock to avoid blocking cancel_run callers.
        if let Some(h) = join_handle {
            let _ = h.join();
        }

        if lease_lost_during_run {
            return Err(EngineError::Cancelled(CancellationReason::LeaseLost));
        }

        result
    }

    /// Resume a workflow from the post-reset DB state.
    ///
    /// Reads completed steps from persistence, builds the skip set internally, and
    /// delegates to `run()`. The `state.resume_ctx` must be `None` on entry — this
    /// method owns skip-set construction so that it reads the *post-reset* DB state.
    pub fn resume(
        &self,
        def: &WorkflowDef,
        state: &mut ExecutionState,
    ) -> crate::engine_error::Result<WorkflowResult> {
        if state.resume_ctx.is_some() {
            return Err(EngineError::Workflow(
                "resume() requires resume_ctx to be None on entry".to_string(),
            ));
        }

        // Acquire before any DB reads so concurrent resume() calls race exactly once.
        let token = ulid::Ulid::new().to_string();
        let lease_ttl_secs = state.exec_config.lease_ttl_secs;
        match state
            .persistence
            .acquire_lease(&state.workflow_run_id, &token, lease_ttl_secs)
        {
            Ok(Some(gen)) => {
                state.owner_token = Some(token);
                state.lease_generation = Some(gen);
            }
            Ok(None) => return Err(EngineError::AlreadyOwned(state.workflow_run_id.clone())),
            Err(e) => return Err(e),
        }

        let steps = state
            .persistence
            .get_steps(&state.workflow_run_id)
            .map_err(|e| {
                EngineError::Workflow(format!(
                    "resume: failed to load steps for run '{}': {e}",
                    state.workflow_run_id
                ))
            })?;
        let mut step_map: HashMap<String, HashMap<u32, WorkflowRunStep>> = HashMap::new();
        for s in steps
            .into_iter()
            .filter(|s| s.status == crate::status::WorkflowStepStatus::Completed)
        {
            step_map
                .entry(s.step_name.clone())
                .or_default()
                .insert(s.iteration as u32, s);
        }
        if !step_map.is_empty() {
            state.resume_ctx = Some(crate::engine::ResumeContext { step_map });
        }
        self.run(def, state)
    }

    /// Cancel a running workflow by run ID.
    ///
    /// Marks the DB run as `Cancelling`, signals the in-memory token so the engine
    /// halts at the next step boundary, and fire-and-forgets `executor.cancel()`
    /// for the step currently in flight.
    ///
    /// Returns `Err` if the run is not currently active in this engine instance.
    pub fn cancel_run(
        &self,
        run_id: &str,
        reason: CancellationReason,
    ) -> crate::engine_error::Result<()> {
        // Pull all per-run state out in a single lock.
        let entry = {
            let runs = self.active_runs.lock().unwrap_or_else(|e| e.into_inner());
            runs.get(run_id).map(|e| {
                (
                    e.token.clone(),
                    Arc::clone(&e.shutdown),
                    Arc::clone(&e.persistence),
                    Arc::clone(&e.registry),
                    Arc::clone(&e.exec_info),
                    Arc::clone(&e.refresh_stop),
                    e.refresh_thread.clone(),
                )
            })
        };

        let (token, shutdown, persistence, registry, exec_info, refresh_stop, refresh_thread) =
            match entry {
                Some(e) => e,
                None => {
                    return Err(EngineError::Workflow(format!(
                        "cancel_run: run '{run_id}' is not active in this engine instance"
                    )))
                }
            };

        // Mark DB as Cancelling so cross-process engines also observe the signal.
        if let Err(e) =
            persistence.update_run_status(run_id, WorkflowRunStatus::Cancelling, None, None)
        {
            tracing::warn!("cancel_run: failed to mark run {run_id} as Cancelling in DB: {e}");
        }

        // Set the executor shutdown flag so the in-flight step stops promptly.
        shutdown.store(true, Ordering::SeqCst);

        // Signal the cancellation token so the engine halts at the next step boundary.
        token.cancel(reason);

        // Fire-and-forget: call executor.cancel() on the currently running step, if any.
        let exec_snap = exec_info.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if let Some((exec_label, step_id)) = exec_snap {
            std::thread::spawn(move || {
                if let Err(e) = registry.cancel(&exec_label, &step_id) {
                    tracing::warn!(
                        "cancel_run: executor.cancel() for '{exec_label}' step '{step_id}' failed: {e}"
                    );
                }
            });
        }

        // Stop the refresh thread (does not join — run() teardown handles the join).
        stop_refresh_thread(&refresh_stop, refresh_thread.as_ref());

        Ok(())
    }

    /// Inner validation implementation. Accepts explicit registry references so
    /// both `validate()` (uses builder registries) and `run()` (uses execution
    /// state registries) can call the same logic without risk of divergence.
    fn validate_with_registries(
        &self,
        action_registry: &ActionRegistry,
        item_provider_registry: &ItemProviderRegistry,
        gate_resolver_registry: &GateResolverRegistry,
        def: &WorkflowDef,
    ) -> Result<(), Vec<ValidationError>> {
        let mut errors = Vec::new();

        // Cycle / depth detection — only when a workflow resolver is configured.
        // Without a resolver we cannot traverse sub-workflows, so we degrade gracefully.
        if let Some(resolver) = &self.workflow_resolver {
            let r = Arc::clone(resolver);
            let root_name = def.name.clone();
            // Inject the root def so detect_workflow_cycles can resolve it by name.
            let cycle_loader = |name: &str| -> std::result::Result<WorkflowDef, String> {
                if name == root_name.as_str() {
                    Ok(def.clone())
                } else {
                    r.resolve(name)
                        .map(|arc_def| (*arc_def).clone())
                        .map_err(|e| e.to_string())
                }
            };
            if let Err(cycle_msg) = detect_workflow_cycles(&def.name, &cycle_loader) {
                errors.push(ValidationError {
                    message: cycle_msg,
                    hint: None,
                });
            }
        }

        let ctx = ValidateCtx {
            action_registry,
            item_provider_registry,
            gate_resolver_registry,
            workflow_resolver: &self.workflow_resolver,
        };
        let mut visited: HashSet<String> = HashSet::new();
        validate_workflow_sections(&ctx, &def.body, &def.always, &mut errors, &mut visited);

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

struct ValidateCtx<'a> {
    action_registry: &'a ActionRegistry,
    item_provider_registry: &'a ItemProviderRegistry,
    gate_resolver_registry: &'a GateResolverRegistry,
    workflow_resolver: &'a Option<Arc<dyn WorkflowResolver>>,
}

fn validate_workflow_sections(
    ctx: &ValidateCtx<'_>,
    body: &[WorkflowNode],
    always: &[WorkflowNode],
    errors: &mut Vec<ValidationError>,
    visited: &mut HashSet<String>,
) {
    validate_nodes_impl(ctx, body, errors, visited);
    validate_nodes_impl(ctx, always, errors, visited);
}

fn validate_nodes_impl(
    ctx: &ValidateCtx<'_>,
    nodes: &[WorkflowNode],
    errors: &mut Vec<ValidationError>,
    visited: &mut HashSet<String>,
) {
    for node in nodes {
        match node {
            WorkflowNode::Call(n) => {
                let name = n.agent.label();
                if !ctx.action_registry.has_action(name) {
                    errors.push(ValidationError {
                        message: format!(
                            "call '{}': no registered ActionExecutor for '{}'",
                            n.agent.step_key(),
                            name
                        ),
                        hint: Some(format!(
                            "register an executor named '{}' or add a fallback executor",
                            name
                        )),
                    });
                }
            }
            WorkflowNode::Parallel(n) => {
                for agent_ref in &n.calls {
                    let name = agent_ref.label();
                    if !ctx.action_registry.has_action(name) {
                        errors.push(ValidationError {
                            message: format!(
                                "parallel call '{}': no registered ActionExecutor for '{}'",
                                agent_ref.step_key(),
                                name
                            ),
                            hint: Some(format!(
                                "register an executor named '{}' or add a fallback executor",
                                name
                            )),
                        });
                    }
                }
            }
            WorkflowNode::ForEach(n) => {
                if ctx.item_provider_registry.get(&n.over).is_none() {
                    errors.push(ValidationError {
                        message: format!(
                            "foreach '{}': no registered ItemProvider for '{}'",
                            n.name, n.over
                        ),
                        hint: Some(format!(
                            "register a provider with name '{}' via FlowEngineBuilder::item_provider()",
                            n.over
                        )),
                    });
                }
            }
            WorkflowNode::Gate(n) => {
                // QualityGate is evaluated inline and never goes through a GateResolver.
                if n.gate_type != GateType::QualityGate {
                    let type_str = n.gate_type.to_string();
                    if !ctx.gate_resolver_registry.has_type(&type_str) {
                        errors.push(ValidationError {
                            message: format!(
                                "gate '{}': no registered GateResolver for type '{}'",
                                n.name, type_str
                            ),
                            hint: Some(format!(
                                "register a resolver with gate_type() == '{}' via FlowEngineBuilder::gate_resolver()",
                                type_str
                            )),
                        });
                    }
                }
            }
            WorkflowNode::CallWorkflow(n) => {
                if !visited.contains(&n.workflow) {
                    visited.insert(n.workflow.clone());
                    if let Some(resolver) = ctx.workflow_resolver {
                        match resolver.resolve(&n.workflow).map(|d| (*d).clone()) {
                            Ok(sub_def) => {
                                let mut sub_errors = Vec::new();
                                validate_workflow_sections(
                                    ctx,
                                    &sub_def.body,
                                    &sub_def.always,
                                    &mut sub_errors,
                                    visited,
                                );
                                for sub_err in sub_errors {
                                    errors.push(ValidationError {
                                        message: format!(
                                            "in sub-workflow '{}': {}",
                                            n.workflow, sub_err.message
                                        ),
                                        hint: sub_err.hint,
                                    });
                                }
                            }
                            Err(e) => {
                                // Report every load failure so all missing sub-workflows
                                // surface in one pass, not just the first one.
                                errors.push(ValidationError {
                                    message: format!(
                                        "call workflow '{}': sub-workflow could not be loaded: {}",
                                        n.workflow, e
                                    ),
                                    hint: None,
                                });
                            }
                        }
                    }
                }
            }
            _ => {
                if let Some(body) = node.body() {
                    validate_nodes_impl(ctx, body, errors, visited);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// FlowEngineBuilder
// ---------------------------------------------------------------------------

/// Builder for constructing a [`FlowEngine`].
///
/// Register action executors with `.action()` / `.action_fallback()`,
/// foreach item sources with `.item_provider()`, gate resolvers with
/// `.gate_resolver()`, and environment injection with `.script_env_provider()`.
pub struct FlowEngineBuilder {
    named: HashMap<String, Box<dyn ActionExecutor>>,
    fallback: Option<Box<dyn ActionExecutor>>,
    script_env_provider: Box<dyn ScriptEnvProvider>,
    item_providers: ItemProviderRegistry,
    gate_resolvers: GateResolverRegistry,
    workflow_resolver: Option<Box<dyn WorkflowResolver>>,
    event_sinks: Vec<Arc<dyn EventSink>>,
}

impl FlowEngineBuilder {
    pub fn new() -> Self {
        Self {
            named: HashMap::new(),
            fallback: None,
            script_env_provider: Box::new(NoOpScriptEnvProvider),
            item_providers: ItemProviderRegistry::new(),
            gate_resolvers: GateResolverRegistry::new(),
            workflow_resolver: None,
            event_sinks: Vec::new(),
        }
    }

    /// Register a named executor. The executor's `name()` is used as the lookup key.
    #[allow(dead_code)]
    pub fn action(mut self, executor: Box<dyn ActionExecutor>) -> Self {
        self.named.insert(executor.name().to_string(), executor);
        self
    }

    /// Register the fallback (catch-all) executor.
    ///
    /// Returns `Err` if called more than once — only one fallback is allowed.
    pub fn action_fallback(
        mut self,
        executor: Box<dyn ActionExecutor>,
    ) -> Result<Self, EngineError> {
        if self.fallback.is_some() {
            return Err(EngineError::Workflow(
                "action_fallback already set — only one fallback executor is allowed".to_string(),
            ));
        }
        self.fallback = Some(executor);
        Ok(self)
    }

    /// Register an item provider for foreach fan-outs.
    pub fn item_provider<P: ItemProvider + 'static>(mut self, provider: P) -> Self {
        self.item_providers.register(provider);
        self
    }

    /// Register a gate resolver for a specific gate type.
    pub fn gate_resolver<R: GateResolver + 'static>(mut self, resolver: R) -> Self {
        self.gate_resolvers.register(resolver);
        self
    }

    /// Set the script env provider. Defaults to `NoOpScriptEnvProvider`.
    pub fn script_env_provider(mut self, provider: Box<dyn ScriptEnvProvider>) -> Self {
        self.script_env_provider = provider;
        self
    }

    /// Convenience: register a `DirectoryWorkflowResolver` rooted at `path`.
    ///
    /// `FlowEngine::validate()` will read `<path>/<name>.wf` on each `call workflow`
    /// node it encounters. Re-reads on every call so hot-reload is preserved.
    pub fn workflow_dir(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.workflow_resolver = Some(Box::new(DirectoryWorkflowResolver::new(path)));
        self
    }

    /// Set a custom `WorkflowResolver` for sub-workflow validation and cycle detection.
    ///
    /// Overrides any previous `.workflow_dir()` call.
    pub fn workflow_resolver(mut self, resolver: Box<dyn WorkflowResolver>) -> Self {
        self.workflow_resolver = Some(resolver);
        self
    }

    /// Register an event sink. Multiple calls register multiple sinks; events are
    /// emitted to all sinks in registration order.
    pub fn event_sink(mut self, sink: Box<dyn EventSink>) -> Self {
        self.event_sinks.push(Arc::from(sink));
        self
    }

    /// Register multiple event sinks from an existing `Arc<[Arc<dyn EventSink>]>`.
    /// Sinks are appended in slice order after any already registered.
    pub fn with_event_sinks(mut self, sinks: &Arc<[Arc<dyn EventSink>]>) -> Self {
        self.event_sinks.extend(sinks.iter().cloned());
        self
    }

    /// Consume the builder and produce a [`FlowEngine`].
    pub fn build(self) -> Result<FlowEngine, EngineError> {
        Ok(FlowEngine {
            action_registry: ActionRegistry::new(self.named, self.fallback),
            item_provider_registry: self.item_providers,
            gate_resolver_registry: self.gate_resolvers,
            script_env_provider: Arc::from(self.script_env_provider),
            workflow_resolver: self.workflow_resolver.map(Arc::from),
            event_sinks: self.event_sinks,
            active_runs: Mutex::new(HashMap::new()),
        })
    }
}

impl Default for FlowEngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for FlowEngine {
    fn drop(&mut self) {
        let entries: Vec<ActiveRunEntry> = {
            let mut guard = self.active_runs.lock().unwrap_or_else(|e| e.into_inner());
            guard.drain().map(|(_, e)| e).collect()
        };
        for entry in entries {
            // Stop the refresh thread first so it exits before we cancel the token.
            stop_refresh_thread(&entry.refresh_stop, entry.refresh_thread.as_ref());
            // Dropping refresh_handle detaches the thread; no join in Drop to avoid deadlock.
            entry.shutdown.store(true, Ordering::SeqCst);
            entry.token.cancel(CancellationReason::EngineShutdown);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::{
        ApprovalMode, CallWorkflowNode, ForEachNode, GateNode, GateType, OnChildFail, OnCycle,
        OnTimeout,
    };
    use crate::engine_error::EngineError;
    use crate::test_helpers::{call_node, make_def, make_ectx, make_params, ForwardSink, VecSink};
    use crate::traits::action_executor::{ActionOutput, ActionParams, ExecutionContext};
    use crate::traits::gate_resolver::{GateContext, GateParams, GatePoll};
    use crate::traits::item_provider::{FanOutItem, ProviderContext};
    use crate::traits::run_context::RunContext;
    use crate::workflow_resolver_memory::InMemoryWorkflowResolver;
    use std::collections::HashMap;

    // --- test executors / providers / resolvers ---

    struct AlphaExecutor;
    impl ActionExecutor for AlphaExecutor {
        fn name(&self) -> &str {
            "alpha"
        }
        fn execute(
            &self,
            _ectx: &ExecutionContext,
            _params: &ActionParams,
        ) -> Result<ActionOutput, EngineError> {
            Ok(ActionOutput {
                markers: vec!["alpha".to_string()],
                ..Default::default()
            })
        }
    }

    struct BetaExecutor;
    impl ActionExecutor for BetaExecutor {
        fn name(&self) -> &str {
            "beta"
        }
        fn execute(
            &self,
            _ectx: &ExecutionContext,
            _params: &ActionParams,
        ) -> Result<ActionOutput, EngineError> {
            Ok(ActionOutput {
                markers: vec!["beta".to_string()],
                ..Default::default()
            })
        }
    }

    struct CountingExecutor {
        name: &'static str,
        count: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl ActionExecutor for CountingExecutor {
        fn name(&self) -> &str {
            self.name
        }
        fn execute(
            &self,
            _: &ExecutionContext,
            _: &ActionParams,
        ) -> Result<ActionOutput, EngineError> {
            self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(ActionOutput::default())
        }
    }

    fn make_test_run(
        p: &Arc<crate::persistence_memory::InMemoryWorkflowPersistence>,
    ) -> crate::types::WorkflowRun {
        use crate::traits::persistence::{NewRun, WorkflowPersistence};
        p.create_run(NewRun {
            workflow_name: "wf".to_string(),
            worktree_id: None,
            ticket_id: None,
            repo_id: None,
            parent_run_id: String::new(),
            dry_run: false,
            trigger: "manual".to_string(),
            definition_snapshot: None,
            parent_workflow_run_id: None,
            target_label: None,
        })
        .unwrap()
    }

    /// Build an `ExecutionState` wired with two `CountingExecutor`s (alpha, beta)
    /// and return the counters alongside the state.
    fn make_counting_state(
        persistence: Arc<crate::persistence_memory::InMemoryWorkflowPersistence>,
        run_id: String,
    ) -> (
        Arc<std::sync::atomic::AtomicUsize>,
        Arc<std::sync::atomic::AtomicUsize>,
        crate::engine::ExecutionState,
    ) {
        let alpha_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let beta_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut m = HashMap::new();
        m.insert(
            "alpha".to_string(),
            Box::new(CountingExecutor {
                name: "alpha",
                count: Arc::clone(&alpha_count),
            }) as Box<dyn crate::traits::action_executor::ActionExecutor>,
        );
        m.insert(
            "beta".to_string(),
            Box::new(CountingExecutor {
                name: "beta",
                count: Arc::clone(&beta_count),
            }) as Box<dyn crate::traits::action_executor::ActionExecutor>,
        );
        let mut state = make_bare_state("wf");
        state.persistence = persistence;
        state.action_registry = Arc::new(ActionRegistry::new(m, None));
        state.workflow_run_id = run_id;
        (alpha_count, beta_count, state)
    }

    struct TicketsProvider;
    impl crate::traits::item_provider::ItemProvider for TicketsProvider {
        fn name(&self) -> &str {
            "tickets"
        }
        fn items(
            &self,
            _ctx: &ProviderContext,
            _scope: Option<&crate::dsl::ForeachScope>,
            _filter: &HashMap<String, String>,
            _existing_set: &std::collections::HashSet<String>,
        ) -> Result<Vec<FanOutItem>, EngineError> {
            Ok(vec![])
        }
    }

    struct HumanApprovalResolver;
    impl crate::traits::gate_resolver::GateResolver for HumanApprovalResolver {
        fn gate_type(&self) -> &str {
            "human_approval"
        }
        fn poll(
            &self,
            _run_id: &str,
            _params: &GateParams,
            _ctx: &GateContext,
        ) -> Result<GatePoll, EngineError> {
            Ok(GatePoll::Approved(None))
        }
    }

    // --- helpers ---

    fn foreach_node(step: &str, over: &str) -> WorkflowNode {
        WorkflowNode::ForEach(ForEachNode {
            name: step.to_string(),
            over: over.to_string(),
            scope: None,
            filter: HashMap::new(),
            ordered: false,
            on_cycle: OnCycle::Fail,
            max_parallel: 1,
            workflow: "child_wf".to_string(),
            inputs: HashMap::new(),
            on_child_fail: OnChildFail::Halt,
        })
    }

    fn gate_node(name: &str, gate_type: GateType) -> WorkflowNode {
        WorkflowNode::Gate(GateNode {
            name: name.to_string(),
            gate_type,
            prompt: None,
            min_approvals: 1,
            approval_mode: ApprovalMode::default(),
            timeout_secs: 0,
            on_timeout: OnTimeout::Fail,
            bot_name: None,
            quality_gate: None,
            options: None,
        })
    }

    // --- existing FlowEngineBuilder tests (now produce FlowEngine) ---

    #[test]
    fn build_with_named_executor() {
        let engine = FlowEngineBuilder::new()
            .action(Box::new(AlphaExecutor))
            .build()
            .unwrap();
        let output = engine
            .action_registry
            .dispatch("alpha", &make_ectx(), &make_params("alpha"))
            .unwrap();
        assert_eq!(output.markers, vec!["alpha"]);
    }

    #[test]
    fn build_with_fallback() {
        let engine = FlowEngineBuilder::new()
            .action_fallback(Box::new(BetaExecutor))
            .unwrap()
            .build()
            .unwrap();
        let output = engine
            .action_registry
            .dispatch("anything", &make_ectx(), &make_params("anything"))
            .unwrap();
        assert_eq!(output.markers, vec!["beta"]);
    }

    #[test]
    fn named_takes_precedence_over_fallback() {
        let engine = FlowEngineBuilder::new()
            .action(Box::new(AlphaExecutor))
            .action_fallback(Box::new(BetaExecutor))
            .unwrap()
            .build()
            .unwrap();
        let output = engine
            .action_registry
            .dispatch("alpha", &make_ectx(), &make_params("alpha"))
            .unwrap();
        assert_eq!(output.markers, vec!["alpha"]);
    }

    #[test]
    fn second_action_fallback_returns_err() {
        let result = FlowEngineBuilder::new()
            .action_fallback(Box::new(AlphaExecutor))
            .unwrap()
            .action_fallback(Box::new(BetaExecutor));
        assert!(result.is_err(), "second action_fallback should return Err");
    }

    #[test]
    fn custom_script_env_provider_is_stored_in_bundle() {
        struct FixedEnvProvider;
        impl ScriptEnvProvider for FixedEnvProvider {
            fn env(
                &self,
                _ctx: &dyn RunContext,
                _bot_name: Option<&str>,
            ) -> HashMap<String, String> {
                let mut m = HashMap::new();
                m.insert("CUSTOM_VAR".to_string(), "42".to_string());
                m
            }
        }

        struct NoopCtx;
        impl RunContext for NoopCtx {
            fn injected_variables(&self) -> HashMap<&'static str, String> {
                HashMap::new()
            }
            fn working_dir(&self) -> &std::path::Path {
                std::path::Path::new("/tmp")
            }
            fn repo_path(&self) -> &std::path::Path {
                std::path::Path::new("/tmp")
            }
        }

        let engine = FlowEngineBuilder::new()
            .script_env_provider(Box::new(FixedEnvProvider))
            .build()
            .unwrap();

        let env = engine.script_env_provider.env(&NoopCtx, None);
        assert_eq!(env.get("CUSTOM_VAR").map(String::as_str), Some("42"));
    }

    // --- validate() acceptance criteria ---

    // AC1: missing action name produces error
    #[test]
    fn validate_missing_action_name_produces_error() {
        let def = make_def("wf", vec![call_node("missing_agent")]);
        let engine = FlowEngineBuilder::new().build().unwrap();

        let errors = engine.validate(&def).unwrap_err();
        assert!(
            !errors.is_empty(),
            "expected at least one error for missing action"
        );
        assert!(
            errors.iter().any(|e| e.message.contains("missing_agent")),
            "error should name the missing executor; got: {:?}",
            errors
        );
    }

    // AC2: missing item provider produces error
    #[test]
    fn validate_missing_item_provider_produces_error() {
        let def = make_def("wf", vec![foreach_node("items", "tickets")]);
        let engine = FlowEngineBuilder::new().build().unwrap();

        let errors = engine.validate(&def).unwrap_err();
        assert!(
            errors.iter().any(|e| e.message.contains("tickets")),
            "error should mention the missing provider name; got: {:?}",
            errors
        );
    }

    // AC3: missing gate type produces error
    #[test]
    fn validate_missing_gate_type_produces_error() {
        let def = make_def("wf", vec![gate_node("approval", GateType::HumanApproval)]);
        let engine = FlowEngineBuilder::new().build().unwrap();

        let errors = engine.validate(&def).unwrap_err();
        assert!(
            errors.iter().any(|e| e.message.contains("human_approval")),
            "error should mention the missing gate type; got: {:?}",
            errors
        );
    }

    // AC3b: QualityGate is excluded from resolver checks
    #[test]
    fn validate_quality_gate_does_not_require_resolver() {
        use crate::dsl::{GateNode, GateType, OnFailAction, OnTimeout, QualityGateConfig};
        let gate = WorkflowNode::Gate(GateNode {
            name: "qg".to_string(),
            gate_type: GateType::QualityGate,
            prompt: None,
            min_approvals: 1,
            approval_mode: ApprovalMode::default(),
            timeout_secs: 0,
            on_timeout: OnTimeout::Fail,
            bot_name: None,
            quality_gate: Some(QualityGateConfig {
                source: "step1".to_string(),
                threshold: 80,
                on_fail_action: OnFailAction::Fail,
            }),
            options: None,
        });
        // Also need call step1 to be produced, but validate() only checks harness
        // registrations, not semantic step ordering — so just test the gate alone.
        let def = make_def("wf", vec![gate]);
        let engine = FlowEngineBuilder::new().build().unwrap();
        // No gate resolver registered — but QualityGate must not trigger an error.
        let result = engine.validate(&def);
        // QualityGate check should not produce a gate-resolver error.
        // (There may be no errors at all if no actions are referenced.)
        if let Err(errors) = result {
            assert!(
                !errors.iter().any(|e| e.message.contains("quality_gate")),
                "QualityGate should not produce a resolver error; got: {:?}",
                errors
            );
        }
    }

    // AC4: valid workflow with all registrations passes
    #[test]
    fn validate_valid_workflow_passes() {
        let def = make_def(
            "wf",
            vec![
                call_node("alpha"),
                foreach_node("items", "tickets"),
                gate_node("approval", GateType::HumanApproval),
            ],
        );
        let engine = FlowEngineBuilder::new()
            .action(Box::new(AlphaExecutor))
            .item_provider(TicketsProvider)
            .gate_resolver(HumanApprovalResolver)
            .build()
            .unwrap();

        assert!(
            engine.validate(&def).is_ok(),
            "all registrations present — validation should pass"
        );
    }

    // AC5: sub-workflow validation errors surface with path prefix
    #[test]
    fn validate_sub_workflow_errors_have_path_prefix() {
        let sub_def = make_def("sub_wf", vec![call_node("missing_in_sub")]);
        let engine = FlowEngineBuilder::new()
            .workflow_resolver(Box::new(InMemoryWorkflowResolver::new([(
                "sub_wf", sub_def,
            )])))
            .build()
            .unwrap();

        let root_def = make_def(
            "root",
            vec![WorkflowNode::CallWorkflow(CallWorkflowNode {
                workflow: "sub_wf".to_string(),
                inputs: HashMap::new(),
                retries: 0,
                on_fail: None,
                bot_name: None,
            })],
        );

        let errors = engine.validate(&root_def).unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("in sub-workflow") && e.message.contains("sub_wf")),
            "sub-workflow errors should be prefixed with the sub-workflow name; got: {:?}",
            errors
        );
        assert!(
            errors.iter().any(|e| e.message.contains("missing_in_sub")),
            "error should mention the missing executor from the sub-workflow; got: {:?}",
            errors
        );
    }

    // AC6: cycle detection triggers ValidationError
    #[test]
    fn validate_cycle_detection_triggers_error() {
        // A workflow that calls itself creates a cycle.
        let cycle_def = make_def(
            "cycle_wf",
            vec![WorkflowNode::CallWorkflow(CallWorkflowNode {
                workflow: "cycle_wf".to_string(),
                inputs: HashMap::new(),
                retries: 0,
                on_fail: None,
                bot_name: None,
            })],
        );
        let engine = FlowEngineBuilder::new()
            .workflow_resolver(Box::new(InMemoryWorkflowResolver::new([(
                "cycle_wf",
                cycle_def.clone(),
            )])))
            .build()
            .unwrap();

        let errors = engine.validate(&cycle_def).unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("Circular") || e.message.contains("cycle")),
            "cycle detection should produce an error; got: {:?}",
            errors
        );
    }

    // AC-new-1: WorkflowNotFound error when resolver misses a sub-workflow
    #[test]
    fn resolver_returns_not_found_error_for_missing_sub_workflow() {
        let engine = FlowEngineBuilder::new()
            .workflow_resolver(Box::new(InMemoryWorkflowResolver::new(
                [] as [(String, WorkflowDef); 0]
            )))
            .build()
            .unwrap();

        let root_def = make_def(
            "root",
            vec![WorkflowNode::CallWorkflow(CallWorkflowNode {
                workflow: "missing_sub".to_string(),
                inputs: HashMap::new(),
                retries: 0,
                on_fail: None,
                bot_name: None,
            })],
        );

        let errors = engine.validate(&root_def).unwrap_err();
        assert!(
            errors.iter().any(|e| e.message.contains("missing_sub")),
            "error should mention the missing sub-workflow name; got: {:?}",
            errors
        );
    }

    // AC-new-2: InMemoryWorkflowResolver alone (no filesystem) is sufficient
    #[test]
    fn inmemory_resolver_sufficient_for_full_validation() {
        let sub_def = make_def("sub_wf", vec![call_node("alpha")]);
        let engine = FlowEngineBuilder::new()
            .action(Box::new(AlphaExecutor))
            .workflow_resolver(Box::new(InMemoryWorkflowResolver::new([(
                "sub_wf", sub_def,
            )])))
            .build()
            .unwrap();

        let root_def = make_def(
            "root",
            vec![WorkflowNode::CallWorkflow(CallWorkflowNode {
                workflow: "sub_wf".to_string(),
                inputs: HashMap::new(),
                retries: 0,
                on_fail: None,
                bot_name: None,
            })],
        );

        assert!(
            engine.validate(&root_def).is_ok(),
            "InMemoryWorkflowResolver alone should be sufficient for full validation"
        );
    }

    // Builds a minimal ExecutionState with empty registries for run() tests.
    fn make_bare_state(wf_name: &str) -> crate::engine::ExecutionState {
        use crate::cancellation::CancellationToken;
        use crate::engine::{ExecutionState, WorktreeContext};
        use crate::persistence_memory::InMemoryWorkflowPersistence;
        use crate::traits::script_env_provider::NoOpScriptEnvProvider;
        use crate::types::WorkflowExecConfig;
        let persistence = InMemoryWorkflowPersistence::new();
        persistence.seed_run("test-run");
        ExecutionState {
            persistence: Arc::new(persistence),
            action_registry: Arc::new(ActionRegistry::new(HashMap::new(), None)),
            script_env_provider: Arc::new(NoOpScriptEnvProvider),
            workflow_run_id: "test-run".to_string(),
            workflow_name: wf_name.to_string(),
            worktree_ctx: WorktreeContext {
                worktree_id: None,
                working_dir: String::new(),
                repo_path: String::new(),
                ticket_id: None,
                repo_id: None,
                extra_plugin_dirs: vec![],
            },
            model: None,
            exec_config: WorkflowExecConfig::default(),
            inputs: HashMap::new(),
            parent_run_id: String::new(),
            depth: 0,
            target_label: None,
            step_results: HashMap::new(),
            contexts: vec![],
            position: 0,
            all_succeeded: true,
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
            triggered_by_hook: false,
            schema_resolver: None,
            child_runner: None,
            last_heartbeat_at: ExecutionState::new_heartbeat(),
            registry: Arc::new(ItemProviderRegistry::new()),
            event_sinks: Arc::from(vec![]),
            cancellation: CancellationToken::new(),
            current_execution_id: Arc::new(std::sync::Mutex::new(None)),
            owner_token: None,
            lease_generation: None,
        }
    }

    // AC7a: run() validates against state action registry, not engine registry
    // Engine has "alpha" but ExecutionState doesn't — run() must reject.
    #[test]
    fn run_validates_against_state_registries_not_engine() {
        let def = make_def("wf", vec![call_node("alpha")]);
        // Engine has "alpha" registered — validate() on the engine itself would pass.
        let engine = FlowEngineBuilder::new()
            .action(Box::new(AlphaExecutor))
            .build()
            .unwrap();
        assert!(
            engine.validate(&def).is_ok(),
            "engine validate() should pass"
        );

        // But ExecutionState has no actions — run() must catch the divergence.
        let mut state = make_bare_state("wf");

        let result = engine.run(&def, &mut state);
        assert!(
            result.is_err(),
            "run() must reject when state action registry lacks 'alpha'"
        );
        assert_eq!(state.position, 0, "no side effects on rejection");
    }

    // AC7b: run() validates against state item-provider registry, not engine registry
    // Engine has "tickets" provider but ExecutionState doesn't — run() must reject.
    #[test]
    fn run_validates_item_provider_against_state_registry_not_engine() {
        let def = make_def("wf", vec![foreach_node("items", "tickets")]);
        // Engine has "tickets" registered — validate() on the engine itself would pass.
        let engine = FlowEngineBuilder::new()
            .item_provider(TicketsProvider)
            .build()
            .unwrap();
        assert!(
            engine.validate(&def).is_ok(),
            "engine validate() should pass for tickets provider"
        );

        // ExecutionState has no item providers — run() must catch the divergence.
        let mut state = make_bare_state("wf");

        let result = engine.run(&def, &mut state);
        assert!(
            result.is_err(),
            "run() must reject when state item-provider registry lacks 'tickets'"
        );
        assert_eq!(state.position, 0, "no side effects on rejection");
    }

    // AC7: run() rejects invalid workflows before any side effects
    #[test]
    fn run_rejects_invalid_workflow_before_side_effects() {
        let def = make_def("wf", vec![call_node("unregistered_agent")]);
        let engine = FlowEngineBuilder::new().build().unwrap();

        let mut state = make_bare_state("wf");

        let result = engine.run(&def, &mut state);
        assert!(result.is_err(), "run() must reject an invalid workflow");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("validation"),
            "error should mention validation; got: {err}"
        );
        assert_eq!(
            state.position, 0,
            "no side effects: position must be unchanged when validation fails"
        );
        assert!(
            state.step_results.is_empty(),
            "no side effects: step_results must be empty when validation fails"
        );
    }

    // ---------------------------------------------------------------------------
    // EventSink tests
    // ---------------------------------------------------------------------------

    use crate::events::{EngineEvent, EngineEventData, EventSink};
    use crate::persistence_memory::InMemoryWorkflowPersistence;

    /// A sink that always panics — used to test panic isolation.
    struct PanicSink;

    impl EventSink for PanicSink {
        fn emit(&self, _event: &EngineEventData) {
            panic!("intentional sink panic");
        }
    }

    /// Build a simple 1-step workflow that uses the NoopAlpha executor.
    fn make_single_step_def() -> WorkflowDef {
        make_def("wf", vec![call_node("alpha")])
    }

    /// Build an ExecutionState with a fresh InMemoryWorkflowPersistence.
    fn make_state_with_persistence(wf_name: &str) -> crate::engine::ExecutionState {
        use crate::traits::persistence::{NewRun, WorkflowPersistence};

        let persistence = Arc::new(InMemoryWorkflowPersistence::new());
        // Create a run record so update_run_status doesn't fail; use the returned ID.
        let run = persistence
            .create_run(NewRun {
                workflow_name: wf_name.to_string(),
                worktree_id: None,
                ticket_id: None,
                repo_id: None,
                parent_run_id: String::new(),
                dry_run: false,
                trigger: "manual".to_string(),
                definition_snapshot: None,
                parent_workflow_run_id: None,
                target_label: None,
            })
            .unwrap();

        let mut state = make_bare_state(wf_name);
        state.persistence = persistence;
        state.action_registry = Arc::new(ActionRegistry::new(
            {
                let mut m = HashMap::new();
                m.insert(
                    "alpha".to_string(),
                    Box::new(AlphaExecutor)
                        as Box<dyn crate::traits::action_executor::ActionExecutor>,
                );
                m
            },
            None,
        ));
        state.workflow_run_id = run.id;
        state
    }

    // Test: two sinks both receive all events in registration order
    #[test]
    fn event_sinks_multi_sink_ordering() {
        let sink_a = VecSink::new();
        let sink_b = VecSink::new();

        let sink_a_clone = Arc::clone(&sink_a);
        let sink_b_clone = Arc::clone(&sink_b);

        let engine = FlowEngineBuilder::new()
            .action(Box::new(AlphaExecutor))
            .event_sink(Box::new(ForwardSink(sink_a_clone)))
            .event_sink(Box::new(ForwardSink(sink_b_clone)))
            .build()
            .unwrap();

        let def = make_single_step_def();
        let mut state = make_state_with_persistence("wf");
        let result = engine.run(&def, &mut state);
        assert!(result.is_ok(), "run should succeed: {:?}", result);

        let events_a = sink_a.collected();
        let events_b = sink_b.collected();
        assert!(!events_a.is_empty(), "sink_a should have received events");
        assert_eq!(
            events_a.len(),
            events_b.len(),
            "both sinks should receive the same number of events"
        );
        // Verify at least RunStarted and RunCompleted were received
        let has_run_started = events_a
            .iter()
            .any(|e| matches!(e.event, EngineEvent::RunStarted { .. }));
        let has_run_completed = events_a
            .iter()
            .any(|e| matches!(e.event, EngineEvent::RunCompleted { .. }));
        assert!(has_run_started, "should have RunStarted event");
        assert!(has_run_completed, "should have RunCompleted event");
    }

    // Test: with_event_sinks appends pre-built sinks and they all receive events
    #[test]
    fn with_event_sinks_accumulates_sinks() {
        let sink_a = VecSink::new();
        let sink_b = VecSink::new();

        let pre_built: Arc<[Arc<dyn EventSink>]> = Arc::from(vec![
            Arc::clone(&sink_a) as Arc<dyn EventSink>,
            Arc::clone(&sink_b) as Arc<dyn EventSink>,
        ]);

        let engine = FlowEngineBuilder::new()
            .action(Box::new(AlphaExecutor))
            .with_event_sinks(&pre_built)
            .build()
            .unwrap();

        let def = make_single_step_def();
        let mut state = make_state_with_persistence("wf");
        let result = engine.run(&def, &mut state);
        assert!(result.is_ok(), "run should succeed: {:?}", result);

        let events_a = sink_a.collected();
        let events_b = sink_b.collected();
        assert!(
            !events_a.is_empty(),
            "sink_a registered via with_event_sinks should receive events"
        );
        assert_eq!(
            events_a.len(),
            events_b.len(),
            "both sinks should receive the same number of events"
        );
        assert!(
            events_a
                .iter()
                .any(|e| matches!(e.event, EngineEvent::RunStarted { .. })),
            "should have RunStarted event"
        );
    }

    // Test: mixing event_sink() and with_event_sinks() accumulates all sinks
    #[test]
    fn event_sink_and_with_event_sinks_both_accumulate() {
        let sink_a = VecSink::new();
        let sink_b = VecSink::new();
        let sink_c = VecSink::new();

        let pre_built: Arc<[Arc<dyn EventSink>]> = Arc::from(vec![
            Arc::clone(&sink_b) as Arc<dyn EventSink>,
            Arc::clone(&sink_c) as Arc<dyn EventSink>,
        ]);

        let engine = FlowEngineBuilder::new()
            .action(Box::new(AlphaExecutor))
            .event_sink(Box::new(ForwardSink(Arc::clone(&sink_a))))
            .with_event_sinks(&pre_built)
            .with_event_sinks(&pre_built) // second call appends, not replaces
            .build()
            .unwrap();

        let def = make_single_step_def();
        let mut state = make_state_with_persistence("wf");
        engine.run(&def, &mut state).unwrap();

        // sink_a (via event_sink) and sink_b/sink_c (via with_event_sinks) all fire
        assert!(
            !sink_a.collected().is_empty(),
            "event_sink sink should receive events"
        );
        assert_eq!(
            sink_b.collected().len(),
            sink_a.collected().len() * 2,
            "sink_b registered twice via with_event_sinks should receive 2x events"
        );
        assert_eq!(
            sink_b.collected().len(),
            sink_c.collected().len(),
            "both with_event_sinks sinks should receive the same count"
        );
    }

    // Test: panicking sink doesn't abort the run; the non-panicking sink still receives events
    #[test]
    fn event_sinks_panic_safety() {
        let good_sink = VecSink::new();
        let good_sink_clone = Arc::clone(&good_sink);

        let engine = FlowEngineBuilder::new()
            .action(Box::new(AlphaExecutor))
            .event_sink(Box::new(PanicSink))
            .event_sink(Box::new(ForwardSink(good_sink_clone)))
            .build()
            .unwrap();

        let def = make_single_step_def();
        let mut state = make_state_with_persistence("wf");
        let result = engine.run(&def, &mut state);
        assert!(result.is_ok(), "run should succeed despite panicking sink");

        let events = good_sink.collected();
        assert!(
            !events.is_empty(),
            "good sink should still receive events after panicking sink"
        );
    }

    // Test: integration sequence — RunStarted → StepStarted → StepCompleted → MetricsUpdated → RunCompleted
    #[test]
    fn event_sink_integration_sequence() {
        let sink = VecSink::new();
        let sink_clone = Arc::clone(&sink);

        let engine = FlowEngineBuilder::new()
            .action(Box::new(AlphaExecutor))
            .event_sink(Box::new(ForwardSink(sink_clone)))
            .build()
            .unwrap();

        let def = make_single_step_def();
        let mut state = make_state_with_persistence("wf");
        let result = engine.run(&def, &mut state);
        assert!(result.is_ok(), "run should succeed: {:?}", result);

        let events = sink.collected();
        let kinds: Vec<&str> = events
            .iter()
            .map(|e| match &e.event {
                EngineEvent::RunStarted { .. } => "RunStarted",
                EngineEvent::RunCompleted { .. } => "RunCompleted",
                EngineEvent::RunResumed { .. } => "RunResumed",
                EngineEvent::RunCancelled { .. } => "RunCancelled",
                EngineEvent::StepStarted { .. } => "StepStarted",
                EngineEvent::StepCompleted { .. } => "StepCompleted",
                EngineEvent::StepRetrying { .. } => "StepRetrying",
                EngineEvent::GateWaiting { .. } => "GateWaiting",
                EngineEvent::GateResolved { .. } => "GateResolved",
                EngineEvent::FanOutItemsCollected { .. } => "FanOutItemsCollected",
                EngineEvent::FanOutItemStarted { .. } => "FanOutItemStarted",
                EngineEvent::FanOutItemCompleted { .. } => "FanOutItemCompleted",
                EngineEvent::MetricsUpdated { .. } => "MetricsUpdated",
            })
            .collect();

        assert_eq!(kinds[0], "RunStarted", "first event should be RunStarted");
        assert!(
            kinds.contains(&"StepStarted"),
            "should have StepStarted; got: {:?}",
            kinds
        );
        assert!(
            kinds.contains(&"StepCompleted"),
            "should have StepCompleted; got: {:?}",
            kinds
        );
        assert!(
            kinds.contains(&"MetricsUpdated"),
            "should have MetricsUpdated; got: {:?}",
            kinds
        );
        let last = kinds.last().unwrap();
        assert_eq!(*last, "RunCompleted", "last event should be RunCompleted");
    }

    // ---------------------------------------------------------------------------
    // Cancellation integration tests (Task 16)
    // ---------------------------------------------------------------------------

    /// Executor that always fails — used to trigger fail_fast in parallel tests.
    struct FailingExecutor;
    impl ActionExecutor for FailingExecutor {
        fn name(&self) -> &str {
            "failing"
        }
        fn execute(
            &self,
            _ectx: &ExecutionContext,
            _params: &ActionParams,
        ) -> Result<ActionOutput, EngineError> {
            Err(EngineError::Workflow("intentional failure".to_string()))
        }
    }

    // AC: cancel_run marks run as Cancelling in DB and signals the token.
    #[test]
    fn cancel_run_marks_cancelling_in_db() {
        use crate::persistence_memory::InMemoryWorkflowPersistence;
        use crate::status::WorkflowRunStatus;
        use crate::traits::persistence::WorkflowPersistence;

        let persistence = Arc::new(InMemoryWorkflowPersistence::new());
        let run = make_test_run(&persistence);
        persistence
            .update_run_status(&run.id, WorkflowRunStatus::Running, None, None)
            .unwrap();

        let engine = FlowEngineBuilder::new().build().unwrap();

        // Register a dummy active run entry so cancel_run finds it.
        {
            let mut runs = engine.active_runs.lock().unwrap_or_else(|e| e.into_inner());
            runs.insert(
                run.id.clone(),
                ActiveRunEntry {
                    token: crate::cancellation::CancellationToken::new(),
                    shutdown: Arc::new(AtomicBool::new(false)),
                    persistence: Arc::clone(&persistence) as Arc<dyn WorkflowPersistence>,
                    registry: Arc::new(ActionRegistry::new(HashMap::new(), None)),
                    exec_info: Arc::new(Mutex::new(None)),
                    refresh_stop: Arc::new(AtomicBool::new(false)),
                    refresh_thread: None,
                    refresh_handle: None,
                },
            );
        }

        engine
            .cancel_run(&run.id, CancellationReason::UserRequested(None))
            .unwrap();

        let updated = persistence.get_run(&run.id).unwrap().unwrap();
        assert_eq!(
            updated.status,
            WorkflowRunStatus::Cancelling,
            "DB status should be Cancelling after cancel_run"
        );
    }

    // AC: cancel_run returns Err when run is not active in this engine instance.
    #[test]
    fn cancel_run_returns_err_for_unknown_run() {
        let engine = FlowEngineBuilder::new().build().unwrap();
        let result = engine.cancel_run("nonexistent-run", CancellationReason::UserRequested(None));
        assert!(result.is_err(), "cancel_run on unknown run must return Err");
    }

    // AC: token cancelled before run() starts causes the run to not succeed.
    #[test]
    fn pre_cancelled_token_causes_immediate_failure() {
        let engine = FlowEngineBuilder::new()
            .action(Box::new(AlphaExecutor))
            .build()
            .unwrap();
        let def = make_def("wf", vec![call_node("alpha")]);
        let mut state = make_state_with_persistence("wf");

        // Cancel the token before run() starts.
        state
            .cancellation
            .cancel(CancellationReason::UserRequested(None));

        // The engine handles cancellation internally, returning Ok(WorkflowResult{ all_succeeded: false }).
        let result = engine.run(&def, &mut state);
        let did_not_succeed = match result {
            Ok(wr) => !wr.all_succeeded,
            Err(_) => true,
        };
        assert!(
            did_not_succeed,
            "run with pre-cancelled token should not succeed"
        );
    }

    // AC: fail_fast on a parallel block stops remaining branches after first failure.
    #[test]
    fn parallel_fail_fast_skips_remaining_branches() {
        use crate::dsl::{ParallelNode, WorkflowNode};
        use crate::persistence_memory::InMemoryWorkflowPersistence;

        // Build engine with both alpha and failing executors.
        let engine = FlowEngineBuilder::new()
            .action(Box::new(AlphaExecutor))
            .action(Box::new(FailingExecutor))
            .build()
            .unwrap();

        let parallel = WorkflowNode::Parallel(ParallelNode {
            fail_fast: true,
            min_success: None,
            calls: vec![
                crate::dsl::AgentRef::Name("failing".to_string()),
                crate::dsl::AgentRef::Name("alpha".to_string()),
                crate::dsl::AgentRef::Name("alpha".to_string()),
            ],
            output: None,
            call_outputs: HashMap::new(),
            with: vec![],
            call_with: HashMap::new(),
            call_if: HashMap::new(),
            call_retries: HashMap::new(),
        });

        let def = make_def("wf", vec![parallel]);

        let persistence = Arc::new(InMemoryWorkflowPersistence::new());
        let run = make_test_run(&persistence);
        let persistence: Arc<dyn crate::traits::persistence::WorkflowPersistence> = persistence;

        // Build a state with both executors in the registry.
        let mut m = HashMap::new();
        m.insert(
            "alpha".to_string(),
            Box::new(AlphaExecutor) as Box<dyn crate::traits::action_executor::ActionExecutor>,
        );
        m.insert(
            "failing".to_string(),
            Box::new(FailingExecutor) as Box<dyn crate::traits::action_executor::ActionExecutor>,
        );
        let mut state = make_bare_state("wf");
        state.persistence = Arc::clone(&persistence);
        state.action_registry = Arc::new(ActionRegistry::new(m, None));
        state.workflow_run_id = run.id.clone();

        engine.run(&def, &mut state).ok(); // may fail due to min_success

        // With true parallel execution all branches are spawned simultaneously. The scope
        // token is cancelled as soon as the first failure result is processed; branches
        // that haven't dispatched yet will see the cancellation and return early. At minimum
        // the explicitly-failing branch must be recorded as Failed.
        let steps = persistence.get_steps(&run.id).unwrap();
        let failed = steps
            .iter()
            .filter(|s| s.status == crate::status::WorkflowStepStatus::Failed)
            .count();
        assert!(
            failed >= 1,
            "at least the first (failing) branch should be Failed; got steps: {:?}",
            steps
        );
    }

    // AC: step-level timeout marks step TimedOut when DSL timeout fires.
    #[test]
    fn step_timeout_marks_timed_out() {
        use crate::dsl::{CallNode, WorkflowNode};
        use crate::persistence_memory::InMemoryWorkflowPersistence;

        // Executor that sleeps longer than the DSL timeout.
        struct SlowExecutor;
        impl ActionExecutor for SlowExecutor {
            fn name(&self) -> &str {
                "slow"
            }
            fn execute(
                &self,
                _ectx: &ExecutionContext,
                _params: &ActionParams,
            ) -> Result<ActionOutput, EngineError> {
                std::thread::sleep(std::time::Duration::from_millis(100));
                Ok(ActionOutput::default())
            }
        }

        let engine = FlowEngineBuilder::new()
            .action(Box::new(SlowExecutor))
            .build()
            .unwrap();

        let timed_out_call = WorkflowNode::Call(CallNode {
            agent: crate::dsl::AgentRef::Name("slow".to_string()),
            retries: 0,
            on_fail: None,
            output: None,
            with: vec![],
            bot_name: None,
            plugin_dirs: vec![],
            timeout: Some("10ms".to_string()),
        });

        let def = make_def("wf", vec![timed_out_call]);

        let persistence = Arc::new(InMemoryWorkflowPersistence::new());
        let run = make_test_run(&persistence);
        let persistence: Arc<dyn crate::traits::persistence::WorkflowPersistence> = persistence;

        let mut m = HashMap::new();
        m.insert(
            "slow".to_string(),
            Box::new(SlowExecutor) as Box<dyn crate::traits::action_executor::ActionExecutor>,
        );
        let mut state = make_bare_state("wf");
        state.persistence = Arc::clone(&persistence);
        state.action_registry = Arc::new(ActionRegistry::new(m, None));
        state.workflow_run_id = run.id.clone();

        engine.run(&def, &mut state).ok();

        let steps = persistence.get_steps(&run.id).unwrap();
        let timed_out = steps
            .iter()
            .any(|s| s.status == crate::status::WorkflowStepStatus::TimedOut);
        assert!(
            timed_out,
            "step should be marked TimedOut; got: {:?}",
            steps
        );
    }

    // ---------------------------------------------------------------------------
    // FlowEngine::resume() tests
    // ---------------------------------------------------------------------------

    // AC: resume() reads completed steps from DB and skips them; pending steps run.
    #[test]
    fn resume_skips_completed_steps() {
        use crate::persistence_memory::InMemoryWorkflowPersistence;
        use crate::status::WorkflowStepStatus;
        use crate::traits::persistence::{NewStep, StepUpdate, WorkflowPersistence};
        use std::sync::atomic::Ordering;

        let persistence = Arc::new(InMemoryWorkflowPersistence::new());
        let run = make_test_run(&persistence);

        // Pre-seed alpha as a completed step so resume() will skip it.
        let step_id = persistence
            .insert_step(NewStep {
                workflow_run_id: run.id.clone(),
                step_name: "alpha".to_string(),
                role: "actor".to_string(),
                can_commit: false,
                position: 0,
                iteration: 0,
                retry_count: Some(0),
            })
            .unwrap();
        persistence
            .update_step(
                &step_id,
                StepUpdate {
                    generation: 0,
                    status: WorkflowStepStatus::Completed,
                    child_run_id: None,
                    result_text: None,
                    context_out: None,
                    markers_out: None,
                    retry_count: None,
                    structured_output: None,
                    step_error: None,
                },
            )
            .unwrap();

        let (alpha_count, beta_count, mut state) =
            make_counting_state(Arc::clone(&persistence), run.id);

        let engine = FlowEngineBuilder::new().build().unwrap();
        let def = make_def("wf", vec![call_node("alpha"), call_node("beta")]);
        engine.resume(&def, &mut state).unwrap();

        assert_eq!(
            alpha_count.load(Ordering::SeqCst),
            0,
            "alpha was pre-completed and should be skipped"
        );
        assert_eq!(
            beta_count.load(Ordering::SeqCst),
            1,
            "beta should execute once"
        );
    }

    // AC: resume() accumulates metrics from pre-completed steps into WorkflowResult totals.
    #[test]
    fn resume_accumulates_metrics_from_completed_steps() {
        use crate::persistence_memory::InMemoryWorkflowPersistence;
        use crate::status::WorkflowStepStatus;
        use crate::traits::persistence::{NewStep, StepUpdate, WorkflowPersistence};

        let persistence = Arc::new(InMemoryWorkflowPersistence::new());
        let run = make_test_run(&persistence);

        // Pre-seed alpha as a completed step with non-zero metrics.
        let step_id = persistence
            .insert_step(NewStep {
                workflow_run_id: run.id.clone(),
                step_name: "alpha".to_string(),
                role: "actor".to_string(),
                can_commit: false,
                position: 0,
                iteration: 0,
                retry_count: Some(0),
            })
            .unwrap();
        persistence
            .update_step(
                &step_id,
                StepUpdate {
                    generation: 0,
                    status: WorkflowStepStatus::Completed,
                    child_run_id: None,
                    result_text: None,
                    context_out: None,
                    markers_out: None,
                    retry_count: None,
                    structured_output: None,
                    step_error: None,
                },
            )
            .unwrap();
        // Inject non-zero cost and turn metrics directly into the step record.
        persistence.set_step_metrics_for_test(
            &step_id,
            Some(1.23),
            Some(5),
            Some(4000),
            Some(100),
            Some(200),
        );

        let (_, _, mut state) = make_counting_state(Arc::clone(&persistence), run.id);

        let engine = FlowEngineBuilder::new().build().unwrap();
        let def = make_def("wf", vec![call_node("alpha"), call_node("beta")]);
        let result = engine.resume(&def, &mut state).unwrap();

        assert!(
            (result.total_cost - 1.23).abs() < 1e-9,
            "total_cost should include alpha's pre-completed cost; got {}",
            result.total_cost
        );
        assert_eq!(
            result.total_turns, 5,
            "total_turns should include alpha's pre-completed turns"
        );
        assert_eq!(
            result.total_input_tokens, 100,
            "total_input_tokens should include alpha's pre-completed tokens"
        );
    }

    // AC: resume() with no completed steps runs all steps (same behaviour as run()).
    #[test]
    fn resume_empty_skip_set_runs_all() {
        use crate::persistence_memory::InMemoryWorkflowPersistence;
        use std::sync::atomic::Ordering;

        let persistence = Arc::new(InMemoryWorkflowPersistence::new());
        let run = make_test_run(&persistence);

        let (alpha_count, beta_count, mut state) = make_counting_state(persistence, run.id);

        let engine = FlowEngineBuilder::new().build().unwrap();
        let def = make_def("wf", vec![call_node("alpha"), call_node("beta")]);
        engine.resume(&def, &mut state).unwrap();

        assert_eq!(
            alpha_count.load(Ordering::SeqCst),
            1,
            "alpha should execute once when no completed steps exist"
        );
        assert_eq!(
            beta_count.load(Ordering::SeqCst),
            1,
            "beta should execute once when no completed steps exist"
        );
    }

    // AC: resume() with a while loop fast-forwards past completed iterations and only
    // executes body steps for the first incomplete iteration.
    #[test]
    fn resume_while_loop_starts_at_first_incomplete_iteration() {
        use crate::dsl::{OnMaxIter, WhileNode, WorkflowNode};
        use crate::persistence_memory::InMemoryWorkflowPersistence;
        use crate::status::WorkflowStepStatus;
        use crate::traits::persistence::{NewStep, StepUpdate, WorkflowPersistence};
        use std::sync::atomic::{AtomicUsize, Ordering};

        let persistence = Arc::new(InMemoryWorkflowPersistence::new());
        let run = make_test_run(&persistence);

        // Pre-seed the condition step (outside the while loop) as completed with a "continue" marker.
        let cond_id = persistence
            .insert_step(NewStep {
                workflow_run_id: run.id.clone(),
                step_name: "cond".to_string(),
                role: "actor".to_string(),
                can_commit: false,
                position: 0,
                iteration: 0,
                retry_count: Some(0),
            })
            .unwrap();
        persistence
            .update_step(
                &cond_id,
                StepUpdate {
                    generation: 0,
                    status: WorkflowStepStatus::Completed,
                    child_run_id: None,
                    result_text: None,
                    context_out: None,
                    markers_out: Some(r#"["continue"]"#.to_string()),
                    retry_count: None,
                    structured_output: None,
                    step_error: None,
                },
            )
            .unwrap();

        // Pre-seed body_a and body_b for iterations 0 and 1.
        for iter in 0i64..2 {
            for (pos_offset, name) in [(0i64, "body_a"), (1, "body_b")] {
                let sid = persistence
                    .insert_step(NewStep {
                        workflow_run_id: run.id.clone(),
                        step_name: name.to_string(),
                        role: "actor".to_string(),
                        can_commit: false,
                        position: iter * 2 + pos_offset + 1,
                        iteration: iter,
                        retry_count: Some(0),
                    })
                    .unwrap();
                persistence
                    .update_step(
                        &sid,
                        StepUpdate {
                            generation: 0,
                            status: WorkflowStepStatus::Completed,
                            child_run_id: None,
                            result_text: None,
                            context_out: None,
                            markers_out: None,
                            retry_count: None,
                            structured_output: None,
                            step_error: None,
                        },
                    )
                    .unwrap();
            }
        }

        // Build state with CountingExecutors for body_a and body_b.
        let a_count = Arc::new(AtomicUsize::new(0));
        let b_count = Arc::new(AtomicUsize::new(0));
        let mut m = HashMap::new();
        m.insert(
            "body_a".to_string(),
            Box::new(CountingExecutor {
                name: "body_a",
                count: Arc::clone(&a_count),
            }) as Box<dyn crate::traits::action_executor::ActionExecutor>,
        );
        m.insert(
            "body_b".to_string(),
            Box::new(CountingExecutor {
                name: "body_b",
                count: Arc::clone(&b_count),
            }) as Box<dyn crate::traits::action_executor::ActionExecutor>,
        );
        // Also register "cond" with a counting executor (it is pre-completed and will
        // be skipped, but must be present in the registry so validation passes).
        m.insert(
            "cond".to_string(),
            Box::new(CountingExecutor {
                name: "cond",
                count: Arc::new(AtomicUsize::new(0)),
            }) as Box<dyn crate::traits::action_executor::ActionExecutor>,
        );
        let mut state = make_bare_state("wf");
        state.persistence = Arc::clone(&persistence) as Arc<dyn WorkflowPersistence>;
        state.action_registry = Arc::new(ActionRegistry::new(m, None));
        state.workflow_run_id = run.id.clone();

        // Workflow: cond (outside) -> while(cond.continue, max=3) { body_a, body_b }
        let while_node = WorkflowNode::While(WhileNode {
            step: "cond".to_string(),
            marker: "continue".to_string(),
            max_iterations: 3,
            stuck_after: None,
            on_max_iter: OnMaxIter::Continue,
            body: vec![call_node("body_a"), call_node("body_b")],
        });
        let def = make_def("wf", vec![call_node("cond"), while_node]);

        let engine = FlowEngineBuilder::new().build().unwrap();
        engine.resume(&def, &mut state).unwrap();

        assert_eq!(
            a_count.load(Ordering::SeqCst),
            1,
            "body_a should execute only for the third iteration (first incomplete)"
        );
        assert_eq!(
            b_count.load(Ordering::SeqCst),
            1,
            "body_b should execute only for the third iteration (first incomplete)"
        );
    }

    // AC: resume() propagates persistence errors from get_steps().
    #[test]
    fn resume_propagates_get_steps_error() {
        use crate::persistence_memory::InMemoryWorkflowPersistence;

        let persistence = Arc::new(InMemoryWorkflowPersistence::new());
        persistence.seed_run("run-123");
        persistence.set_fail_get_steps(true);

        let engine = FlowEngineBuilder::new().build().unwrap();
        let def = make_def("wf", vec![call_node("alpha")]);
        let mut state = make_bare_state("wf");
        state.persistence = persistence;
        state.workflow_run_id = "run-123".to_string();

        let err = engine.resume(&def, &mut state).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("resume: failed to load steps for run"),
            "error should contain the prefix; got: {msg}"
        );
        assert!(
            msg.contains("run-123"),
            "error should contain the run ID; got: {msg}"
        );
    }

    // AC: resume() returns Err when called with a pre-seeded resume_ctx.
    #[test]
    fn resume_rejects_pre_seeded_resume_ctx() {
        use crate::engine::ResumeContext;
        use std::collections::HashMap;

        let engine = FlowEngineBuilder::new().build().unwrap();
        let def = make_def("wf", vec![call_node("alpha")]);
        let mut state = make_bare_state("wf");
        state.resume_ctx = Some(ResumeContext {
            step_map: HashMap::new(),
        });
        state.workflow_run_id = "run-precond".to_string();

        let err = engine.resume(&def, &mut state).unwrap_err();
        assert!(
            err.to_string().contains("resume_ctx"),
            "error should mention resume_ctx; got: {err}"
        );
    }

    // ---------------------------------------------------------------------------
    // Lease acquisition tests
    // ---------------------------------------------------------------------------

    // AC: run() sets owner_token and lease_generation after a successful acquire.
    #[test]
    fn run_sets_lease_fields_on_success() {
        use crate::persistence_memory::InMemoryWorkflowPersistence;

        let persistence = Arc::new(InMemoryWorkflowPersistence::new());
        let run = make_test_run(&persistence);

        let engine = FlowEngineBuilder::new()
            .action(Box::new(AlphaExecutor))
            .build()
            .unwrap();
        let def = make_def("wf", vec![call_node("alpha")]);

        let mut state = make_bare_state("wf");
        state.persistence =
            Arc::clone(&persistence) as Arc<dyn crate::traits::persistence::WorkflowPersistence>;
        state.action_registry = Arc::new(ActionRegistry::new(
            {
                let mut m = HashMap::new();
                m.insert(
                    "alpha".to_string(),
                    Box::new(AlphaExecutor)
                        as Box<dyn crate::traits::action_executor::ActionExecutor>,
                );
                m
            },
            None,
        ));
        state.workflow_run_id = run.id.clone();

        engine.run(&def, &mut state).unwrap();

        assert!(
            state.owner_token.is_some(),
            "owner_token should be set after run()"
        );
        assert_eq!(
            state.lease_generation,
            Some(1),
            "lease_generation should be 1 after first acquire"
        );
    }

    // AC: two concurrent FlowEngine::run calls on same run_id → exactly one succeeds.
    #[test]
    fn two_concurrent_runs_exactly_one_succeeds() {
        use crate::persistence_memory::InMemoryWorkflowPersistence;
        use crate::traits::persistence::WorkflowPersistence;
        use std::thread;

        let persistence = Arc::new(InMemoryWorkflowPersistence::new());
        let run = make_test_run(&persistence);
        let run_id = run.id.clone();

        let persistence: Arc<dyn WorkflowPersistence> = persistence;

        // Build a state factory
        let make_state_for_run = |run_id: String, p: Arc<dyn WorkflowPersistence>| {
            let mut s = make_bare_state("wf");
            s.persistence = p;
            s.action_registry = Arc::new(ActionRegistry::new(
                {
                    let mut m = HashMap::new();
                    m.insert(
                        "alpha".to_string(),
                        Box::new(AlphaExecutor)
                            as Box<dyn crate::traits::action_executor::ActionExecutor>,
                    );
                    m
                },
                None,
            ));
            s.workflow_run_id = run_id;
            s
        };

        let def = make_def("wf", vec![call_node("alpha")]);

        // Use a barrier so both threads start run() at the same time.
        let barrier = Arc::new(std::sync::Barrier::new(2));

        let p1 = Arc::clone(&persistence);
        let run_id1 = run_id.clone();
        let barrier1 = Arc::clone(&barrier);
        let def1 = def.clone();
        let t1 = thread::spawn(move || {
            let engine = FlowEngineBuilder::new()
                .action(Box::new(AlphaExecutor))
                .build()
                .unwrap();
            let mut state = make_state_for_run(run_id1, p1);
            barrier1.wait();
            engine.run(&def1, &mut state)
        });

        let p2 = Arc::clone(&persistence);
        let run_id2 = run_id.clone();
        let barrier2 = Arc::clone(&barrier);
        let def2 = def.clone();
        let t2 = thread::spawn(move || {
            let engine = FlowEngineBuilder::new()
                .action(Box::new(AlphaExecutor))
                .build()
                .unwrap();
            let mut state = make_state_for_run(run_id2, p2);
            barrier2.wait();
            engine.run(&def2, &mut state)
        });

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        let successes = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
        let already_owned = [&r1, &r2]
            .iter()
            .filter(|r| matches!(r, Err(EngineError::AlreadyOwned(_))))
            .count();

        assert_eq!(
            successes, 1,
            "exactly one run should succeed; got r1={r1:?}, r2={r2:?}"
        );
        assert_eq!(
            already_owned, 1,
            "exactly one run should fail with AlreadyOwned; got r1={r1:?}, r2={r2:?}"
        );
    }

    // AC: resume() acquires lease before get_steps() — a pre-held lease blocks resume().
    #[test]
    fn resume_acquires_before_get_steps() {
        use crate::persistence_memory::InMemoryWorkflowPersistence;
        use crate::traits::persistence::WorkflowPersistence;

        let persistence = Arc::new(InMemoryWorkflowPersistence::new());
        let run = make_test_run(&persistence);

        // Manually acquire the lease for another token (TTL = 1 hour, won't expire).
        persistence
            .acquire_lease(&run.id, "other-engine-token", 3600)
            .unwrap();

        let engine = FlowEngineBuilder::new()
            .action(Box::new(AlphaExecutor))
            .build()
            .unwrap();
        let def = make_def("wf", vec![call_node("alpha")]);

        let mut state = make_bare_state("wf");
        state.persistence = Arc::clone(&persistence) as Arc<dyn WorkflowPersistence>;
        state.workflow_run_id = run.id.clone();

        let err = engine.resume(&def, &mut state).unwrap_err();
        assert!(
            matches!(err, EngineError::AlreadyOwned(_)),
            "resume() with a pre-held lease should fail with AlreadyOwned; got {err:?}"
        );
    }

    // AC: existing single-engine workflow still completes normally.
    #[test]
    fn single_engine_workflow_still_completes() {
        let engine = FlowEngineBuilder::new()
            .action(Box::new(AlphaExecutor))
            .build()
            .unwrap();
        let def = make_single_step_def();
        let mut state = make_state_with_persistence("wf");
        let result = engine.run(&def, &mut state).unwrap();
        assert!(
            result.all_succeeded,
            "single-engine workflow should complete successfully"
        );
    }

    // AC: refresh_lease_loop Err path — DB error during refresh triggers LeaseLost abort.
    //
    // Setup: executor blocks until its shutdown flag is set; fail_acquire_lease is flipped
    // from a side thread once the executor is running so the initial acquire() in run()
    // still succeeds. The first refresh tick then returns Err, calling signal_lease_abort
    // which sets shutdown=true. The executor exits, and run() returns Err(Cancelled(LeaseLost)).
    #[test]
    fn refresh_db_error_causes_lease_lost_abort() {
        use crate::persistence_memory::InMemoryWorkflowPersistence;
        use crate::traits::action_executor::{ActionOutput, ActionParams, ExecutionContext};
        use crate::traits::persistence::WorkflowPersistence;
        use std::sync::atomic::Ordering;
        use std::thread;
        use std::time::Duration;

        struct BlockingExecutor {
            started: Arc<AtomicBool>,
        }
        impl ActionExecutor for BlockingExecutor {
            fn name(&self) -> &str {
                "alpha"
            }
            fn execute(
                &self,
                ectx: &ExecutionContext,
                _: &ActionParams,
            ) -> Result<ActionOutput, EngineError> {
                self.started.store(true, Ordering::SeqCst);
                // Spin until the engine's shutdown flag is set by signal_lease_abort.
                loop {
                    if ectx
                        .shutdown
                        .as_ref()
                        .is_some_and(|s| s.load(Ordering::Relaxed))
                    {
                        return Ok(ActionOutput::default());
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }

        let persistence = Arc::new(InMemoryWorkflowPersistence::new());
        let run = make_test_run(&persistence);

        let started = Arc::new(AtomicBool::new(false));
        let started_clone = Arc::clone(&started);
        let persistence_clone = Arc::clone(&persistence);

        // Side thread: wait until the executor has started (initial acquire done),
        // then flip fail_acquire_lease so the next refresh tick returns Err.
        let watcher = thread::spawn(move || {
            while !started_clone.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(1));
            }
            persistence_clone.set_fail_acquire_lease(true);
        });

        let mut m = HashMap::new();
        m.insert(
            "alpha".to_string(),
            Box::new(BlockingExecutor {
                started: Arc::clone(&started),
            }) as Box<dyn crate::traits::action_executor::ActionExecutor>,
        );
        let mut state = make_bare_state("wf");
        state.persistence = Arc::clone(&persistence) as Arc<dyn WorkflowPersistence>;
        state.action_registry = Arc::new(ActionRegistry::new(m, None));
        state.workflow_run_id = run.id.clone();
        // Short refresh interval so the error is detected quickly.
        state.exec_config.lease_refresh_interval = Duration::from_millis(15);

        let engine = FlowEngineBuilder::new().build().unwrap();
        let def = make_def("wf", vec![call_node("alpha")]);

        let result = engine.run(&def, &mut state);
        watcher.join().unwrap();

        assert!(
            matches!(
                result,
                Err(EngineError::Cancelled(CancellationReason::LeaseLost))
            ),
            "DB error in refresh should abort with LeaseLost; got {result:?}"
        );
    }

    // AC: stale generation on step write — when another engine steals the lease mid-run,
    // the next executor step write sees a generation mismatch, returns LeaseLost, and
    // the engine aborts cleanly without duplicate side effects.
    #[test]
    fn stale_generation_on_step_write_aborts_with_lease_lost() {
        use crate::persistence_memory::InMemoryWorkflowPersistence;
        use crate::traits::action_executor::{ActionOutput, ActionParams, ExecutionContext};
        use crate::traits::persistence::WorkflowPersistence;
        use std::sync::atomic::Ordering;
        use std::thread;
        use std::time::Duration;

        // Executor that signals when started, waits for proceed, then returns Ok.
        // The stealer steals the lease between started and proceed so the step
        // write (persist_completed_step) sees a stale generation.
        struct LatchedExecutor {
            started: Arc<AtomicBool>,
            proceed: Arc<AtomicBool>,
        }
        impl ActionExecutor for LatchedExecutor {
            fn name(&self) -> &str {
                "alpha"
            }
            fn execute(
                &self,
                _ectx: &ExecutionContext,
                _: &ActionParams,
            ) -> Result<ActionOutput, EngineError> {
                self.started.store(true, Ordering::SeqCst);
                while !self.proceed.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Ok(ActionOutput::default())
            }
        }

        let persistence = Arc::new(InMemoryWorkflowPersistence::new());
        let run = make_test_run(&persistence);
        let run_id = run.id.clone();

        let started = Arc::new(AtomicBool::new(false));
        let proceed = Arc::new(AtomicBool::new(false));
        let started_clone = Arc::clone(&started);
        let proceed_clone = Arc::clone(&proceed);
        let persistence_clone = Arc::clone(&persistence);

        // Side thread: waits for the executor to start, steals the lease (bumps
        // generation), then lets the executor proceed so it tries to write the step.
        let stealer = thread::spawn(move || {
            while !started_clone.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(1));
            }
            persistence_clone.expire_and_steal_lease(&run_id, "thief-token");
            proceed_clone.store(true, Ordering::SeqCst);
        });

        let mut m = HashMap::new();
        m.insert(
            "alpha".to_string(),
            Box::new(LatchedExecutor {
                started: Arc::clone(&started),
                proceed: Arc::clone(&proceed),
            }) as Box<dyn crate::traits::action_executor::ActionExecutor>,
        );

        let mut state = make_bare_state("wf");
        state.persistence = Arc::clone(&persistence) as Arc<dyn WorkflowPersistence>;
        state.action_registry = Arc::new(ActionRegistry::new(m, None));
        state.workflow_run_id = run.id.clone();
        // Short refresh interval so the refresh thread also detects the steal quickly.
        state.exec_config.lease_refresh_interval = Duration::from_millis(15);

        let engine = FlowEngineBuilder::new().build().unwrap();
        let def = make_def("wf", vec![call_node("alpha")]);

        let result = engine.run(&def, &mut state);
        stealer.join().unwrap();

        assert!(
            matches!(
                result,
                Err(EngineError::Cancelled(CancellationReason::LeaseLost))
            ),
            "stale generation on step write should abort with LeaseLost; got {result:?}"
        );
    }

    // AC: cross-process cancel — is_run_cancelled returns true for Cancelling status.
    #[test]
    fn cross_process_cancel_via_db_poll() {
        use crate::persistence_memory::InMemoryWorkflowPersistence;
        use crate::status::WorkflowRunStatus;
        use crate::traits::persistence::WorkflowPersistence;

        let persistence = Arc::new(InMemoryWorkflowPersistence::new());
        let run = make_test_run(&persistence);

        // Simulate cross-process cancel by directly writing Cancelling to DB.
        persistence
            .update_run_status(&run.id, WorkflowRunStatus::Cancelling, None, None)
            .unwrap();

        // is_run_cancelled must return true for Cancelling status.
        assert!(
            persistence.is_run_cancelled(&run.id).unwrap(),
            "is_run_cancelled should return true when status is Cancelling"
        );
    }
}
