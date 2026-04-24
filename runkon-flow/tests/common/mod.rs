#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use runkon_flow::cancellation::CancellationToken;
use runkon_flow::dsl::{
    AgentRef, ApprovalMode, CallNode, ForEachNode, GateNode, GateType, OnChildFail, OnCycle,
    OnTimeout, WorkflowDef, WorkflowNode, WorkflowTrigger,
};
use runkon_flow::engine::{
    ChildWorkflowInput, ChildWorkflowRunner, ExecutionState, ResumeContext, WorktreeContext,
};
use runkon_flow::engine_error::EngineError;
use runkon_flow::events::{EngineEventData, EventSink};
use runkon_flow::persistence_memory::InMemoryWorkflowPersistence;
pub use runkon_flow::traits::action_executor::ActionExecutor;
use runkon_flow::traits::action_executor::{
    ActionOutput, ActionParams, ActionRegistry, ExecutionContext,
};
use runkon_flow::traits::item_provider::{FanOutItem, ItemProvider, ProviderContext};
use runkon_flow::traits::persistence::{NewRun, WorkflowPersistence};
use runkon_flow::traits::script_env_provider::NoOpScriptEnvProvider;
use runkon_flow::types::{WorkflowExecConfig, WorkflowResult};
use runkon_flow::CancellationReason;
use runkon_flow::ItemProviderRegistry;

// ---------------------------------------------------------------------------
// Mock executors
// ---------------------------------------------------------------------------

/// Returns configurable markers on every execution.
pub struct MockExecutor {
    pub label: String,
    pub markers: Vec<String>,
}

impl MockExecutor {
    pub fn new(name: &str) -> Self {
        Self {
            label: name.to_string(),
            markers: vec![],
        }
    }

    pub fn with_markers(name: &str, markers: &[&str]) -> Self {
        Self {
            label: name.to_string(),
            markers: markers.iter().map(|s| s.to_string()).collect(),
        }
    }
}

impl ActionExecutor for MockExecutor {
    fn name(&self) -> &str {
        &self.label
    }

    fn execute(
        &self,
        _ectx: &ExecutionContext,
        _params: &ActionParams,
    ) -> Result<ActionOutput, EngineError> {
        Ok(ActionOutput {
            markers: self.markers.clone(),
            ..Default::default()
        })
    }
}

/// Always returns an engine error — used to test failure propagation.
pub struct FailingExecutor;

impl ActionExecutor for FailingExecutor {
    fn name(&self) -> &str {
        "failing"
    }

    fn execute(
        &self,
        _ectx: &ExecutionContext,
        _params: &ActionParams,
    ) -> Result<ActionOutput, EngineError> {
        Err(EngineError::Workflow(
            "intentional test failure".to_string(),
        ))
    }
}

// ---------------------------------------------------------------------------
// Event sink helpers
// ---------------------------------------------------------------------------

/// Collects all emitted events for post-run inspection.
pub struct VecSink {
    events: Mutex<Vec<EngineEventData>>,
}

impl VecSink {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
        })
    }

    pub fn collected(&self) -> Vec<EngineEventData> {
        self.events.lock().unwrap().clone()
    }
}

impl EventSink for VecSink {
    fn emit(&self, event: &EngineEventData) {
        self.events.lock().unwrap().push(event.clone());
    }
}

/// Forwards events to a `VecSink` owned behind an `Arc`.
///
/// Used because `FlowEngineBuilder::event_sink` takes `Box<dyn EventSink>`,
/// while the test needs to keep an `Arc<VecSink>` to read the collected events
/// after `run()` completes.
pub struct ForwardSink(pub Arc<VecSink>);

impl EventSink for ForwardSink {
    fn emit(&self, event: &EngineEventData) {
        self.0.emit(event);
    }
}

// ---------------------------------------------------------------------------
// State construction helpers
// ---------------------------------------------------------------------------

pub fn make_persistence() -> Arc<InMemoryWorkflowPersistence> {
    Arc::new(InMemoryWorkflowPersistence::new())
}

/// Build an `ExecutionState` with a pre-created run record in `persistence`.
///
/// `named_executors` maps action name → executor; supply an empty `HashMap`
/// for workflows with no `call` steps.
pub fn make_state(
    wf_name: &str,
    persistence: Arc<InMemoryWorkflowPersistence>,
    named_executors: HashMap<String, Box<dyn ActionExecutor>>,
) -> ExecutionState {
    let run = persistence
        .create_run(NewRun {
            workflow_name: wf_name.to_string(),
            worktree_id: None,
            ticket_id: None,
            repo_id: None,
            parent_run_id: String::new(),
            dry_run: false,
            trigger: "test".to_string(),
            definition_snapshot: None,
            parent_workflow_run_id: None,
            target_label: None,
        })
        .expect("create_run failed");

    ExecutionState {
        persistence: Arc::clone(&persistence) as Arc<dyn WorkflowPersistence>,
        action_registry: Arc::new(ActionRegistry::from_executors(named_executors, None)),
        script_env_provider: Arc::new(NoOpScriptEnvProvider),
        workflow_run_id: run.id,
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
        current_execution_id: Arc::new(Mutex::new(None)),
    }
}

/// Wrap `make_state` and set `resume_ctx` to signal a workflow resume.
///
/// The `skip_completed` set is empty so all steps execute normally — tests that
/// need skipping behaviour should populate it directly after calling this helper.
pub fn make_state_with_resume_ctx(
    wf_name: &str,
    persistence: Arc<InMemoryWorkflowPersistence>,
    named_executors: HashMap<String, Box<dyn ActionExecutor>>,
) -> ExecutionState {
    let mut state = make_state(wf_name, persistence, named_executors);
    state.resume_ctx = Some(ResumeContext {
        skip_completed: HashSet::new(),
        step_map: HashMap::new(),
    });
    state
}

// ---------------------------------------------------------------------------
// WorkflowDef construction helpers
// ---------------------------------------------------------------------------

pub fn make_def(name: &str, body: Vec<WorkflowNode>) -> WorkflowDef {
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

pub fn make_def_with_always(
    name: &str,
    body: Vec<WorkflowNode>,
    always: Vec<WorkflowNode>,
) -> WorkflowDef {
    WorkflowDef {
        name: name.to_string(),
        title: None,
        description: String::new(),
        trigger: WorkflowTrigger::Manual,
        targets: vec![],
        group: None,
        inputs: vec![],
        body,
        always,
        source_path: "test.wf".to_string(),
    }
}

pub fn call_node(agent: &str) -> WorkflowNode {
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

pub fn gate_node(name: &str) -> WorkflowNode {
    WorkflowNode::Gate(GateNode {
        name: name.to_string(),
        gate_type: GateType::HumanApproval,
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

/// Build a `HumanApproval` gate named `"approval"` with `timeout_secs = 0`.
/// Used by the two gate-timeout tests whose only difference is `on_timeout`.
pub fn timeout_gate(on_timeout: OnTimeout) -> WorkflowNode {
    WorkflowNode::Gate(GateNode {
        name: "approval".to_string(),
        gate_type: GateType::HumanApproval,
        prompt: None,
        min_approvals: 1,
        approval_mode: ApprovalMode::default(),
        timeout_secs: 0,
        on_timeout,
        bot_name: None,
        quality_gate: None,
        options: None,
    })
}

/// Build a named-executor map keyed by each executor's `name()`.
///
/// Eliminates the repeated `HashMap::new()` + multiple `insert()` pattern in tests.
pub fn named_executors(
    executors: impl IntoIterator<Item = Box<dyn ActionExecutor>>,
) -> HashMap<String, Box<dyn ActionExecutor>> {
    executors
        .into_iter()
        .map(|e| (e.name().to_string(), e))
        .collect()
}

// ---------------------------------------------------------------------------
// foreach test helpers
// ---------------------------------------------------------------------------

/// Mock child workflow runner.
///
/// Reads `params.inputs["item.id"]` to determine success from the pre-configured
/// outcomes map. Records dispatch order in `call_log` for verification.
pub struct MockChildRunner {
    outcomes: HashMap<String, bool>,
    pub call_log: Mutex<Vec<String>>,
}

impl MockChildRunner {
    pub fn new(outcomes: HashMap<String, bool>) -> Self {
        Self {
            outcomes,
            call_log: Mutex::new(Vec::new()),
        }
    }

    /// Convenience: build a runner where every listed item_id succeeds.
    pub fn all_succeed(item_ids: &[&str]) -> Self {
        Self::new(item_ids.iter().map(|id| (id.to_string(), true)).collect())
    }
}

impl ChildWorkflowRunner for MockChildRunner {
    fn execute_child(
        &self,
        child_def: &WorkflowDef,
        _parent_state: &ExecutionState,
        params: ChildWorkflowInput,
    ) -> runkon_flow::engine_error::Result<WorkflowResult> {
        let item_id = params.inputs.get("item.id").cloned().unwrap_or_default();
        self.call_log.lock().unwrap().push(item_id.clone());
        let succeeded = self.outcomes.get(&item_id).copied().unwrap_or(true);
        Ok(WorkflowResult {
            workflow_run_id: format!("mock-run-{}", item_id),
            worktree_id: None,
            workflow_name: child_def.name.clone(),
            all_succeeded: succeeded,
            total_cost: 0.0,
            total_turns: 0,
            total_duration_ms: 0,
            total_input_tokens: 0,
            total_output_tokens: 0,
            total_cache_read_input_tokens: 0,
            total_cache_creation_input_tokens: 0,
        })
    }

    fn resume_child(
        &self,
        _workflow_run_id: &str,
        _model: Option<&str>,
    ) -> runkon_flow::engine_error::Result<WorkflowResult> {
        unimplemented!("MockChildRunner does not support resume_child")
    }

    fn find_resumable_child(
        &self,
        _parent_run_id: &str,
        _workflow_name: &str,
    ) -> runkon_flow::engine_error::Result<Option<runkon_flow::types::WorkflowRun>> {
        Ok(None)
    }
}

/// Mock item provider returning a fixed list of items.
pub struct MockItemProvider {
    name: String,
    items: Vec<(String, String, String)>, // (item_type, item_id, item_ref)
}

impl MockItemProvider {
    pub fn new(name: &str, items: Vec<(&str, &str, &str)>) -> Self {
        Self {
            name: name.to_string(),
            items: items
                .into_iter()
                .map(|(t, i, r)| (t.to_string(), i.to_string(), r.to_string()))
                .collect(),
        }
    }
}

impl ItemProvider for MockItemProvider {
    fn name(&self) -> &str {
        &self.name
    }

    fn items(
        &self,
        _ctx: &ProviderContext,
        _scope: Option<&runkon_flow::dsl::ForeachScope>,
        _filter: &HashMap<String, String>,
        existing_set: &HashSet<String>,
    ) -> Result<Vec<FanOutItem>, EngineError> {
        Ok(self
            .items
            .iter()
            .filter(|(_, id, _)| !existing_set.contains(id))
            .map(|(t, i, r)| FanOutItem {
                item_type: t.clone(),
                item_id: i.clone(),
                item_ref: r.clone(),
            })
            .collect())
    }
}

/// Build a `ForEachNode` with the most common test parameters.
pub fn foreach_node(
    name: &str,
    provider: &str,
    workflow: &str,
    max_parallel: u32,
    on_child_fail: OnChildFail,
) -> ForEachNode {
    ForEachNode {
        name: name.to_string(),
        over: provider.to_string(),
        scope: None,
        filter: HashMap::new(),
        ordered: false,
        on_cycle: OnCycle::Fail,
        max_parallel,
        workflow: workflow.to_string(),
        inputs: HashMap::new(),
        on_child_fail,
    }
}

/// Like `foreach_node` but with `ordered = true`.
pub fn ordered_foreach_node(
    name: &str,
    provider: &str,
    workflow: &str,
    max_parallel: u32,
    on_child_fail: OnChildFail,
) -> ForEachNode {
    ForEachNode {
        name: name.to_string(),
        over: provider.to_string(),
        scope: None,
        filter: HashMap::new(),
        ordered: true,
        on_cycle: OnCycle::Fail,
        max_parallel,
        workflow: workflow.to_string(),
        inputs: HashMap::new(),
        on_child_fail,
    }
}

/// Build an `ExecutionState` wired with a `MockChildRunner` and an item provider.
///
/// Sets `fail_fast = false` so tests can inspect state after step failures.
pub fn make_foreach_state<P: ItemProvider + 'static>(
    wf_name: &str,
    persistence: Arc<InMemoryWorkflowPersistence>,
    child_runner: MockChildRunner,
    provider: P,
) -> ExecutionState {
    let mut state = make_state(wf_name, Arc::clone(&persistence), HashMap::new());
    state.child_runner = Some(Arc::new(child_runner));
    state.exec_config.fail_fast = false;

    let mut registry = ItemProviderRegistry::new();
    registry.register(provider);
    state.registry = Arc::new(registry);

    state
}

/// Like `make_foreach_state` but uses a `CancellingMockRunner` and a caller-supplied
/// `CancellationToken` so tests can trigger cancellation mid-dispatch.
pub fn make_foreach_state_cancellable(
    wf_name: &str,
    persistence: Arc<InMemoryWorkflowPersistence>,
    child_runner: CancellingMockRunner,
    provider: MockItemProvider,
    cancellation: CancellationToken,
) -> ExecutionState {
    let mut state = make_state(wf_name, Arc::clone(&persistence), HashMap::new());
    state.child_runner = Some(Arc::new(child_runner));
    state.exec_config.fail_fast = false;
    state.cancellation = cancellation;

    let mut registry = ItemProviderRegistry::new();
    registry.register(provider);
    state.registry = Arc::new(registry);

    state
}

// ---------------------------------------------------------------------------
// Additional mock types for ordered / cancellation tests
// ---------------------------------------------------------------------------

/// Item provider that supports ordered execution and returns configurable dependencies.
pub struct MockOrderedItemProvider {
    name: String,
    items: Vec<(String, String, String)>,
    deps: Vec<(String, String)>,
}

impl MockOrderedItemProvider {
    pub fn new(name: &str, items: Vec<(&str, &str, &str)>, deps: Vec<(&str, &str)>) -> Self {
        Self {
            name: name.to_string(),
            items: items
                .into_iter()
                .map(|(t, i, r)| (t.to_string(), i.to_string(), r.to_string()))
                .collect(),
            deps: deps
                .into_iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect(),
        }
    }
}

impl ItemProvider for MockOrderedItemProvider {
    fn name(&self) -> &str {
        &self.name
    }

    fn items(
        &self,
        _ctx: &ProviderContext,
        _scope: Option<&runkon_flow::dsl::ForeachScope>,
        _filter: &HashMap<String, String>,
        existing_set: &HashSet<String>,
    ) -> Result<Vec<FanOutItem>, EngineError> {
        Ok(self
            .items
            .iter()
            .filter(|(_, id, _)| !existing_set.contains(id))
            .map(|(t, i, r)| FanOutItem {
                item_type: t.clone(),
                item_id: i.clone(),
                item_ref: r.clone(),
            })
            .collect())
    }

    fn dependencies(&self, _step_id: &str) -> Result<Vec<(String, String)>, EngineError> {
        Ok(self.deps.clone())
    }

    fn supports_ordered(&self) -> bool {
        true
    }
}

/// Ordered item provider whose `dependencies()` always returns an error.
/// Used to verify that the executor propagates dependency fetch failures.
/// Delegates name/items/supports_ordered to `MockOrderedItemProvider`.
pub struct FailingOrderedItemProvider {
    inner: MockOrderedItemProvider,
}

impl FailingOrderedItemProvider {
    pub fn new(name: &str, items: Vec<(&str, &str, &str)>) -> Self {
        Self {
            inner: MockOrderedItemProvider::new(name, items, vec![]),
        }
    }
}

impl ItemProvider for FailingOrderedItemProvider {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn items(
        &self,
        ctx: &ProviderContext,
        scope: Option<&runkon_flow::dsl::ForeachScope>,
        filter: &HashMap<String, String>,
        existing_set: &HashSet<String>,
    ) -> Result<Vec<FanOutItem>, EngineError> {
        self.inner.items(ctx, scope, filter, existing_set)
    }

    fn dependencies(&self, _step_id: &str) -> Result<Vec<(String, String)>, EngineError> {
        Err(EngineError::Workflow(
            "injected dependency fetch failure".to_string(),
        ))
    }

    fn supports_ordered(&self) -> bool {
        self.inner.supports_ordered()
    }
}

/// Child runner that cancels a `CancellationToken` after `cancel_after` calls.
pub struct CancellingMockRunner {
    outcomes: HashMap<String, bool>,
    cancel_after: usize,
    call_count: Mutex<usize>,
    token: CancellationToken,
}

impl CancellingMockRunner {
    pub fn new(
        outcomes: HashMap<String, bool>,
        cancel_after: usize,
        token: CancellationToken,
    ) -> Self {
        Self {
            outcomes,
            cancel_after,
            call_count: Mutex::new(0),
            token,
        }
    }
}

impl ChildWorkflowRunner for CancellingMockRunner {
    fn execute_child(
        &self,
        child_def: &WorkflowDef,
        _parent_state: &ExecutionState,
        params: ChildWorkflowInput,
    ) -> runkon_flow::engine_error::Result<WorkflowResult> {
        let item_id = params.inputs.get("item.id").cloned().unwrap_or_default();
        let mut count = self.call_count.lock().unwrap();
        *count += 1;
        if *count >= self.cancel_after {
            self.token.cancel(CancellationReason::UserRequested(None));
        }
        let succeeded = self.outcomes.get(&item_id).copied().unwrap_or(true);
        Ok(WorkflowResult {
            workflow_run_id: format!("mock-run-{}", item_id),
            worktree_id: None,
            workflow_name: child_def.name.clone(),
            all_succeeded: succeeded,
            total_cost: 0.0,
            total_turns: 0,
            total_duration_ms: 0,
            total_input_tokens: 0,
            total_output_tokens: 0,
            total_cache_read_input_tokens: 0,
            total_cache_creation_input_tokens: 0,
        })
    }

    fn resume_child(
        &self,
        _workflow_run_id: &str,
        _model: Option<&str>,
    ) -> runkon_flow::engine_error::Result<WorkflowResult> {
        unimplemented!("CancellingMockRunner does not support resume_child")
    }

    fn find_resumable_child(
        &self,
        _parent_run_id: &str,
        _workflow_name: &str,
    ) -> runkon_flow::engine_error::Result<Option<runkon_flow::types::WorkflowRun>> {
        Ok(None)
    }
}
