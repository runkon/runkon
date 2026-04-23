use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

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
use crate::types::WorkflowResult;
use crate::workflow_resolver_directory::DirectoryWorkflowResolver;

// ---------------------------------------------------------------------------
// EngineBundle (kept for source compatibility)
// ---------------------------------------------------------------------------

/// Produced by earlier versions of `FlowEngineBuilder::build()`.
///
/// Kept so that importers of `runkon_flow::EngineBundle` continue to compile.
/// New code should use `FlowEngine` instead.
pub struct EngineBundle {
    pub action_registry: ActionRegistry,
    pub script_env_provider: Arc<dyn ScriptEnvProvider>,
}

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

        // Ensure the exec_config.shutdown arc exists so cancel_run() can set it.
        let shutdown_arc = state
            .exec_config
            .shutdown
            .get_or_insert_with(|| Arc::new(AtomicBool::new(false)))
            .clone();

        // Register all per-run cancellation state in a single lock so cancel_run()
        // and Drop each see a consistent snapshot.
        let run_id = state.workflow_run_id.clone();
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
                },
            );
        }

        let result = run_workflow_engine(state, def);

        // Deregister on completion regardless of outcome.
        {
            let mut runs = self.active_runs.lock().unwrap_or_else(|e| e.into_inner());
            runs.remove(&run_id);
        }

        result
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
                )
            })
        };

        let (token, shutdown, persistence, registry, exec_info) = match entry {
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
            let root_def_clone = def.clone();
            // Inject the root def so detect_workflow_cycles can resolve it by name.
            let cycle_loader = move |name: &str| -> std::result::Result<WorkflowDef, String> {
                if name == root_name.as_str() {
                    Ok(root_def_clone.clone())
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

        let mut visited: HashSet<String> = HashSet::new();
        validate_nodes_impl(
            action_registry,
            item_provider_registry,
            gate_resolver_registry,
            &self.workflow_resolver,
            &def.body,
            &mut errors,
            &mut visited,
        );
        validate_nodes_impl(
            action_registry,
            item_provider_registry,
            gate_resolver_registry,
            &self.workflow_resolver,
            &def.always,
            &mut errors,
            &mut visited,
        );

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

fn validate_nodes_impl(
    action_registry: &ActionRegistry,
    item_provider_registry: &ItemProviderRegistry,
    gate_resolver_registry: &GateResolverRegistry,
    workflow_resolver: &Option<Arc<dyn WorkflowResolver>>,
    nodes: &[WorkflowNode],
    errors: &mut Vec<ValidationError>,
    visited: &mut HashSet<String>,
) {
    for node in nodes {
        match node {
            WorkflowNode::Call(n) => {
                let name = n.agent.label();
                if !action_registry.has_action(name) {
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
                    if !action_registry.has_action(name) {
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
                if item_provider_registry.get(&n.over).is_none() {
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
                    if !gate_resolver_registry.has_type(&type_str) {
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
                    if let Some(resolver) = workflow_resolver {
                        match resolver.resolve(&n.workflow).map(|d| (*d).clone()) {
                            Ok(sub_def) => {
                                let mut sub_errors = Vec::new();
                                validate_nodes_impl(
                                    action_registry,
                                    item_provider_registry,
                                    gate_resolver_registry,
                                    workflow_resolver,
                                    &sub_def.body,
                                    &mut sub_errors,
                                    visited,
                                );
                                validate_nodes_impl(
                                    action_registry,
                                    item_provider_registry,
                                    gate_resolver_registry,
                                    workflow_resolver,
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
            WorkflowNode::If(n) => {
                validate_nodes_impl(
                    action_registry,
                    item_provider_registry,
                    gate_resolver_registry,
                    workflow_resolver,
                    &n.body,
                    errors,
                    visited,
                );
            }
            WorkflowNode::Unless(n) => {
                validate_nodes_impl(
                    action_registry,
                    item_provider_registry,
                    gate_resolver_registry,
                    workflow_resolver,
                    &n.body,
                    errors,
                    visited,
                );
            }
            WorkflowNode::While(n) => {
                validate_nodes_impl(
                    action_registry,
                    item_provider_registry,
                    gate_resolver_registry,
                    workflow_resolver,
                    &n.body,
                    errors,
                    visited,
                );
            }
            WorkflowNode::DoWhile(n) => {
                validate_nodes_impl(
                    action_registry,
                    item_provider_registry,
                    gate_resolver_registry,
                    workflow_resolver,
                    &n.body,
                    errors,
                    visited,
                );
            }
            WorkflowNode::Do(n) => {
                validate_nodes_impl(
                    action_registry,
                    item_provider_registry,
                    gate_resolver_registry,
                    workflow_resolver,
                    &n.body,
                    errors,
                    visited,
                );
            }
            WorkflowNode::Always(n) => {
                validate_nodes_impl(
                    action_registry,
                    item_provider_registry,
                    gate_resolver_registry,
                    workflow_resolver,
                    &n.body,
                    errors,
                    visited,
                );
            }
            WorkflowNode::Script(_) => {}
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
            entry.shutdown.store(true, Ordering::SeqCst);
            entry.token.cancel(CancellationReason::EngineShutdown);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::{
        AgentRef, ApprovalMode, CallNode, CallWorkflowNode, ForEachNode, GateNode, GateType,
        OnChildFail, OnCycle, OnTimeout, WorkflowTrigger,
    };
    use crate::engine_error::EngineError;
    use crate::test_helpers::{make_ectx, make_params};
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

    #[allow(dead_code)]
    fn make_def(name: &str, body: Vec<WorkflowNode>) -> WorkflowDef {
        WorkflowDef {
            name: name.to_string(),
            title: None,
            description: String::new(),
            trigger: WorkflowTrigger::Manual,
            targets: vec![],
            group: None,
            inputs: vec![],
            body,
            always: vec![],
            source_path: "test.wf".to_string(),
        }
    }

    fn call_node(agent: &str) -> WorkflowNode {
        WorkflowNode::Call(CallNode {
            agent: AgentRef::Name(agent.to_string()),
            retries: 0,
            on_fail: None,
            output: None,
            with: vec![],
            bot_name: None,
            plugin_dirs: vec![],
            timeout: None,
        })
    }

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
            fn env(&self, _ctx: &dyn RunContext) -> HashMap<String, String> {
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

        let env = engine.script_env_provider.env(&NoopCtx);
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
        ExecutionState {
            persistence: Arc::new(InMemoryWorkflowPersistence::new()),
            action_registry: Arc::new(ActionRegistry::new(HashMap::new(), None)),
            script_env_provider: Arc::new(NoOpScriptEnvProvider),
            workflow_run_id: "test-run".to_string(),
            workflow_name: wf_name.to_string(),
            worktree_ctx: WorktreeContext {
                worktree_id: None,
                working_dir: String::new(),
                worktree_slug: String::new(),
                repo_path: String::new(),
                ticket_id: None,
                repo_id: None,
                conductor_bin_dir: None,
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
    use std::sync::Mutex;

    /// A VecSink that collects all received events for inspection.
    struct VecSink {
        events: Mutex<Vec<EngineEventData>>,
    }

    impl VecSink {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                events: Mutex::new(Vec::new()),
            })
        }

        fn collected(&self) -> Vec<EngineEventData> {
            self.events.lock().unwrap().clone()
        }
    }

    impl EventSink for VecSink {
        fn emit(&self, event: &EngineEventData) {
            self.events.lock().unwrap().push(event.clone());
        }
    }

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
        use crate::cancellation::CancellationToken;
        use crate::engine::{ExecutionState, WorktreeContext};
        use crate::traits::persistence::{NewRun, WorkflowPersistence};
        use crate::traits::script_env_provider::NoOpScriptEnvProvider;
        use crate::types::WorkflowExecConfig;

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

        ExecutionState {
            persistence,
            action_registry: Arc::new(ActionRegistry::new(
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
            )),
            script_env_provider: Arc::new(NoOpScriptEnvProvider),
            workflow_run_id: run.id,
            workflow_name: wf_name.to_string(),
            worktree_ctx: WorktreeContext {
                worktree_id: None,
                working_dir: String::new(),
                worktree_slug: String::new(),
                repo_path: String::new(),
                ticket_id: None,
                repo_id: None,
                conductor_bin_dir: None,
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
            last_heartbeat_at: crate::engine::ExecutionState::new_heartbeat(),
            registry: Arc::new(crate::traits::item_provider::ItemProviderRegistry::new()),
            event_sinks: Arc::from(vec![]),
            cancellation: CancellationToken::new(),
            current_execution_id: Arc::new(std::sync::Mutex::new(None)),
        }
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

    /// A sink that forwards events to a VecSink behind an Arc.
    struct ForwardSink(Arc<VecSink>);

    impl EventSink for ForwardSink {
        fn emit(&self, event: &EngineEventData) {
            self.0.emit(event);
        }
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
        use crate::traits::persistence::{NewRun, WorkflowPersistence};

        let persistence = Arc::new(InMemoryWorkflowPersistence::new());
        let run = persistence
            .create_run(NewRun {
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
            .unwrap();
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
        use crate::engine::{ExecutionState, WorktreeContext};
        use crate::persistence_memory::InMemoryWorkflowPersistence;
        use crate::traits::persistence::NewRun;
        use crate::types::WorkflowExecConfig;

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
        });

        let def = make_def("wf", vec![parallel]);

        let persistence: Arc<dyn crate::traits::persistence::WorkflowPersistence> =
            Arc::new(InMemoryWorkflowPersistence::new());
        let run = persistence
            .create_run(NewRun {
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
            .unwrap();

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
        let mut state = ExecutionState {
            persistence: Arc::clone(&persistence),
            action_registry: Arc::new(ActionRegistry::new(m, None)),
            script_env_provider: Arc::new(
                crate::traits::script_env_provider::NoOpScriptEnvProvider,
            ),
            workflow_run_id: run.id.clone(),
            workflow_name: "wf".to_string(),
            worktree_ctx: WorktreeContext {
                worktree_id: None,
                working_dir: String::new(),
                worktree_slug: String::new(),
                repo_path: String::new(),
                ticket_id: None,
                repo_id: None,
                conductor_bin_dir: None,
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
            registry: Arc::new(crate::traits::item_provider::ItemProviderRegistry::new()),
            event_sinks: Arc::from(vec![]),
            cancellation: crate::cancellation::CancellationToken::new(),
            current_execution_id: Arc::new(std::sync::Mutex::new(None)),
        };

        engine.run(&def, &mut state).ok(); // may fail due to min_success

        // The fail_fast scope token skips branches after the first failure.
        // Exactly one branch should have been dispatched and failed; the rest are skipped.
        let steps = persistence.get_steps(&run.id).unwrap();
        let failed = steps
            .iter()
            .filter(|s| s.status == crate::status::WorkflowStepStatus::Failed)
            .count();
        assert_eq!(
            failed, 1,
            "only the first (failing) branch should be Failed; got steps: {:?}",
            steps
        );
    }

    // AC: step-level timeout marks step TimedOut when DSL timeout fires.
    #[test]
    fn step_timeout_marks_timed_out() {
        use crate::dsl::{CallNode, WorkflowNode};
        use crate::engine::{ExecutionState, WorktreeContext};
        use crate::persistence_memory::InMemoryWorkflowPersistence;
        use crate::traits::persistence::NewRun;
        use crate::types::WorkflowExecConfig;

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

        let persistence: Arc<dyn crate::traits::persistence::WorkflowPersistence> =
            Arc::new(InMemoryWorkflowPersistence::new());
        let run = persistence
            .create_run(NewRun {
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
            .unwrap();

        let mut m = HashMap::new();
        m.insert(
            "slow".to_string(),
            Box::new(SlowExecutor) as Box<dyn crate::traits::action_executor::ActionExecutor>,
        );
        let mut state = ExecutionState {
            persistence: Arc::clone(&persistence),
            action_registry: Arc::new(ActionRegistry::new(m, None)),
            script_env_provider: Arc::new(
                crate::traits::script_env_provider::NoOpScriptEnvProvider,
            ),
            workflow_run_id: run.id.clone(),
            workflow_name: "wf".to_string(),
            worktree_ctx: WorktreeContext {
                worktree_id: None,
                working_dir: String::new(),
                worktree_slug: String::new(),
                repo_path: String::new(),
                ticket_id: None,
                repo_id: None,
                conductor_bin_dir: None,
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
            registry: Arc::new(crate::traits::item_provider::ItemProviderRegistry::new()),
            event_sinks: Arc::from(vec![]),
            cancellation: crate::cancellation::CancellationToken::new(),
            current_execution_id: Arc::new(std::sync::Mutex::new(None)),
        };

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

    // AC: cross-process cancel — is_run_cancelled returns true for Cancelling status.
    #[test]
    fn cross_process_cancel_via_db_poll() {
        use crate::persistence_memory::InMemoryWorkflowPersistence;
        use crate::status::WorkflowRunStatus;
        use crate::traits::persistence::{NewRun, WorkflowPersistence};

        let persistence = Arc::new(InMemoryWorkflowPersistence::new());
        let run = persistence
            .create_run(NewRun {
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
            .unwrap();

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
