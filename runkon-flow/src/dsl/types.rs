use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// AST types
// ---------------------------------------------------------------------------

/// A complete workflow definition parsed from a `.wf` file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowDef {
    pub name: String,
    #[serde(default)]
    pub title: Option<String>,
    pub description: String,
    pub trigger: WorkflowTrigger,
    #[serde(default)]
    pub targets: Vec<String>,
    #[serde(default)]
    pub group: Option<String>,
    pub inputs: Vec<InputDecl>,
    pub body: Vec<WorkflowNode>,
    pub always: Vec<WorkflowNode>,
    pub source_path: String,
}

impl WorkflowDef {
    /// Returns the human-readable display name for this workflow.
    /// Falls back to `name` if no `title` is set.
    pub fn display_name(&self) -> &str {
        self.title.as_deref().unwrap_or(&self.name)
    }

    /// Total number of nodes across body and always blocks.
    pub fn total_nodes(&self) -> usize {
        count_nodes(&self.body) + count_nodes(&self.always)
    }

    /// Number of top-level steps (body + always, non-recursive).
    /// Better for user-facing progress display than `total_nodes()`.
    pub fn top_level_steps(&self) -> usize {
        self.body.len() + self.always.len()
    }

    /// Find the `max_iterations` of the do-while or while loop that owns
    /// the step with the given name. Returns `None` if the step is not
    /// inside a loop or the step name is not found.
    pub fn max_iterations_for_step(&self, step_name: &str) -> Option<u32> {
        fn search(nodes: &[WorkflowNode], name: &str) -> Option<u32> {
            for node in nodes {
                match node {
                    WorkflowNode::DoWhile(n) => {
                        if n.step == name {
                            return Some(n.max_iterations);
                        }
                        if let Some(v) = search(&n.body, name) {
                            return Some(v);
                        }
                    }
                    WorkflowNode::While(n) => {
                        if n.step == name {
                            return Some(n.max_iterations);
                        }
                        if let Some(v) = search(&n.body, name) {
                            return Some(v);
                        }
                    }
                    WorkflowNode::If(n) => {
                        if let Some(v) = search(&n.body, name) {
                            return Some(v);
                        }
                    }
                    WorkflowNode::Unless(n) => {
                        if let Some(v) = search(&n.body, name) {
                            return Some(v);
                        }
                    }
                    WorkflowNode::Do(n) => {
                        if let Some(v) = search(&n.body, name) {
                            return Some(v);
                        }
                    }
                    WorkflowNode::Always(n) => {
                        if let Some(v) = search(&n.body, name) {
                            return Some(v);
                        }
                    }
                    _ => {}
                }
            }
            None
        }
        search(&self.body, step_name).or_else(|| search(&self.always, step_name))
    }

    /// Collect all prompt snippet references across body and always blocks, sorted and deduplicated.
    pub fn collect_all_snippet_refs(&self) -> Vec<String> {
        let mut refs = collect_snippet_refs(&self.body);
        refs.extend(collect_snippet_refs(&self.always));
        refs.sort();
        refs.dedup();
        refs
    }

    /// Collect all output schema references across body and always blocks, sorted and deduplicated.
    pub fn collect_all_schema_refs(&self) -> Vec<String> {
        let mut refs = collect_schema_refs(&self.body);
        refs.extend(collect_schema_refs(&self.always));
        refs.sort();
        refs.dedup();
        refs
    }

    /// Collect all agent references across body and always blocks, sorted and deduplicated.
    pub fn collect_all_agent_refs(&self) -> Vec<AgentRef> {
        let mut refs = collect_agent_names(&self.body);
        refs.extend(collect_agent_names(&self.always));
        refs.sort();
        refs.dedup();
        refs
    }

    /// Collect all bot names referenced across body and always blocks, sorted and deduplicated.
    pub fn collect_all_bot_names(&self) -> Vec<String> {
        let mut names = collect_bot_names(&self.body);
        names.extend(collect_bot_names(&self.always));
        names.sort();
        names.dedup();
        names
    }

    /// Collect all plugin_dirs from call nodes across body and always blocks, sorted and deduplicated.
    pub fn collect_all_plugin_dirs(&self) -> Vec<String> {
        let mut dirs = collect_plugin_dirs(&self.body);
        dirs.extend(collect_plugin_dirs(&self.always));
        dirs.sort();
        dirs.dedup();
        dirs
    }
}

/// A structured parse warning produced when a `.wf` file fails to load.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowWarning {
    /// The filename (e.g. `bad.wf`) that failed to parse.
    pub file: String,
    /// Human-readable description of the parse error.
    pub message: String,
}

/// Trigger type for when a workflow should run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowTrigger {
    Manual,
    Pr,
    Scheduled,
}

impl std::fmt::Display for WorkflowTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Manual => write!(f, "manual"),
            Self::Pr => write!(f, "pr"),
            Self::Scheduled => write!(f, "scheduled"),
        }
    }
}

impl std::str::FromStr for WorkflowTrigger {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "manual" => Ok(Self::Manual),
            "pr" => Ok(Self::Pr),
            "scheduled" => Ok(Self::Scheduled),
            _ => Err(format!("unknown trigger: {s}")),
        }
    }
}

/// The type of a workflow input.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum InputType {
    #[default]
    String,
    Boolean,
}

/// An input declaration for a workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputDecl {
    pub name: String,
    pub required: bool,
    pub default: Option<String>,
    pub description: Option<String>,
    #[serde(default)]
    pub input_type: InputType,
}

/// A node in the workflow execution graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkflowNode {
    Call(CallNode),
    CallWorkflow(CallWorkflowNode),
    If(IfNode),
    Unless(UnlessNode),
    While(WhileNode),
    DoWhile(DoWhileNode),
    Do(DoNode),
    Parallel(ParallelNode),
    Gate(GateNode),
    Always(AlwaysNode),
    Script(ScriptNode),
    ForEach(ForEachNode),
}

/// A foreach step node — fans out a child workflow over a collection of items.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForEachNode {
    /// Step name used as the key in step_results and resume skip sets.
    pub name: String,
    /// The collection type to fan out over.
    pub over: ForeachOver,
    /// Scope filter for ticket or worktree fan-outs.
    pub scope: Option<ForeachScope>,
    /// Generic filter map (required for workflow_run fan-outs, reserved for repos).
    #[serde(default)]
    pub filter: HashMap<String, String>,
    /// Whether to use dependency-ordered dispatch (tickets only).
    pub ordered: bool,
    /// What to do when a ticket cycle is detected (tickets + ordered only).
    pub on_cycle: OnCycle,
    /// Maximum number of child workflows to run concurrently.
    pub max_parallel: u32,
    /// Name of the child workflow to invoke for each item.
    pub workflow: String,
    /// Input map passed to each child workflow invocation.
    /// Values may contain `{{item.*}}` template references.
    #[serde(default)]
    pub inputs: HashMap<String, String>,
    /// How to handle a child workflow failure.
    pub on_child_fail: OnChildFail,
}

/// The collection type for a foreach step — the registered provider name.
pub type ForeachOver = String;

/// Unified scope for foreach fan-outs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "scope_type", content = "scope_value", rename_all = "snake_case")]
pub enum ForeachScope {
    Ticket(TicketScope),
    Worktree(WorktreeScope),
}

/// Scope selector for ticket fan-outs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum TicketScope {
    /// All direct children (parent_of edges) of the given ticket.
    TicketId(String),
    /// All open tickets with the given label in the repo.
    Label(String),
    /// All open tickets with no entries in ticket_labels.
    Unlabeled,
}

/// Scope selector for worktree fan-outs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeScope {
    #[serde(default)]
    pub base_branch: Option<String>,
    #[serde(default)]
    pub has_open_pr: Option<bool>,
}

/// What to do when a child workflow fails.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnChildFail {
    /// Cancel in-flight runs and fail the step immediately.
    Halt,
    /// Log the failure and keep dispatching remaining items.
    Continue,
    /// Mark the failed item's transitive dependents as skipped (ordered tickets only).
    SkipDependents,
}

/// What to do when a ticket dependency cycle is detected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnCycle {
    /// Abort with an error naming the cycle.
    Fail,
    /// Log a warning, break the back-edge, and continue.
    Warn,
}

/// A script step node — runs a shell script directly (no agent/LLM).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptNode {
    /// Step name used as the step key in step_results and resume skip sets.
    pub name: String,
    /// Path to the script to run (supports `{{variable}}` substitution).
    /// Resolved in order: worktree dir → repo dir → `~/.claude/skills/`.
    pub run: String,
    /// Environment variable overrides (values support `{{variable}}` substitution).
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Optional timeout in seconds. If the script does not complete within this
    /// duration it is killed and the step is marked `TimedOut`.
    pub timeout: Option<u64>,
    /// Number of retry attempts after the first failure (0 = no retries).
    #[serde(default)]
    pub retries: u32,
    /// Action to take if all attempts fail.
    pub on_fail: Option<OnFail>,
    /// Named GitHub App bot identity to use for this script (matches `[github.apps.<name>]`).
    /// When set, the resolved installation token is injected as `GH_TOKEN` so the script
    /// uses that bot identity for all `gh` CLI calls.
    pub bot_name: Option<String>,
}

/// The action to take when all retries for a `call`, `script`, or `call workflow` step exhaust.
///
/// - `Agent`: invoke a fallback agent (existing behaviour).
/// - `Continue`: skip the step without marking the workflow failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum OnFail {
    Agent(AgentRef),
    Continue,
}

/// Reference to an agent — either a short name or an explicit file path.
///
/// - `Name`: bare identifier (e.g. `plan`) resolved via the search order.
/// - `Path`: quoted string (e.g. `".claude/agents/plan.md"`) resolved directly
///   relative to the repository root.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum AgentRef {
    Name(String),
    Path(String),
}

impl AgentRef {
    /// Human-readable label for display and logging (the inner string value).
    pub fn label(&self) -> &str {
        match self {
            Self::Name(s) | Self::Path(s) => s.as_str(),
        }
    }

    /// Key used to store and look up results in `step_results`.
    ///
    /// - `Name` variants return the name as-is.
    /// - `Path` variants return the file stem without extension
    ///   (e.g. `"plan"` from `".claude/agents/plan.md"`), so that `if`/`while`
    ///   conditions can reference path-based agents by their short name.
    pub fn step_key(&self) -> String {
        match self {
            Self::Name(s) => s.clone(),
            Self::Path(s) => Path::new(s)
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or(s.as_str())
                .to_string(),
        }
    }
}

impl std::fmt::Display for AgentRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallNode {
    pub agent: AgentRef,
    #[serde(default)]
    pub retries: u32,
    pub on_fail: Option<OnFail>,
    /// Optional output schema reference for structured output.
    pub output: Option<String>,
    /// Prompt snippet references to append to the agent prompt.
    #[serde(default)]
    pub with: Vec<String>,
    /// Named GitHub App bot identity to use for this call (matches `[github.apps.<name>]`).
    pub bot_name: Option<String>,
    /// Per-step plugin directories from the `.wf` file. Merged with repo-level
    /// `extra_plugin_dirs` at execution time to give this agent access to
    /// specialist plugins (e.g. `/usr/local/bsg/agent-architecture/planner`).
    #[serde(default)]
    pub plugin_dirs: Vec<String>,
    /// Optional per-step timeout (e.g. "5m", "30s", "1h"). If the step does not
    /// complete within this duration it is cancelled with `CancellationReason::Timeout`.
    pub timeout: Option<String>,
}

/// A sub-workflow invocation node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallWorkflowNode {
    pub workflow: String,
    #[serde(default)]
    pub inputs: HashMap<String, String>,
    #[serde(default)]
    pub retries: u32,
    pub on_fail: Option<OnFail>,
    /// Named GitHub App bot identity inherited by child call nodes.
    pub bot_name: Option<String>,
}

/// A condition in an `if`/`unless` block.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Condition {
    /// References a marker produced by a prior step: `step.marker`.
    StepMarker { step: String, marker: String },
    /// References a boolean input directly: `input_name`.
    BoolInput { input: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IfNode {
    pub condition: Condition,
    pub body: Vec<WorkflowNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnlessNode {
    pub condition: Condition,
    pub body: Vec<WorkflowNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhileNode {
    pub step: String,
    pub marker: String,
    pub max_iterations: u32,
    pub stuck_after: Option<u32>,
    pub on_max_iter: OnMaxIter,
    pub body: Vec<WorkflowNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoWhileNode {
    pub step: String,
    pub marker: String,
    pub max_iterations: u32,
    pub stuck_after: Option<u32>,
    pub on_max_iter: OnMaxIter,
    pub body: Vec<WorkflowNode>,
}

/// A plain sequential grouping block (`do { ... }`), with optional `output` and `with`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoNode {
    /// Optional output schema reference for structured output.
    pub output: Option<String>,
    /// Prompt snippet references applied to all calls inside the block.
    #[serde(default)]
    pub with: Vec<String>,
    pub body: Vec<WorkflowNode>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnMaxIter {
    Fail,
    Continue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParallelNode {
    #[serde(default = "default_true")]
    pub fail_fast: bool,
    pub min_success: Option<u32>,
    pub calls: Vec<AgentRef>,
    /// Block-level output schema reference (applies to all calls unless overridden).
    pub output: Option<String>,
    /// Per-call output schema overrides, keyed by index (as string) in `calls`.
    /// String keys are used because JSON object keys are always strings and serde_json
    /// cannot coerce them back to integer types on deserialization.
    #[serde(default)]
    pub call_outputs: HashMap<String, String>,
    /// Block-level prompt snippet references (applied to all calls).
    #[serde(default)]
    pub with: Vec<String>,
    /// Per-call prompt snippet additions, keyed by index (as string) in `calls`.
    #[serde(default)]
    pub call_with: HashMap<String, Vec<String>>,
    /// Per-call `if` conditions keyed by index (as string) in `calls`.
    /// Value is (step_name, marker_name). Run the call only if that marker is present.
    #[serde(default)]
    pub call_if: HashMap<String, (String, String)>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalMode {
    #[default]
    MinApprovals,
    ReviewDecision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnFailAction {
    Fail,
    Continue,
}

/// Configuration specific to `GateType::QualityGate` nodes.
///
/// Grouped into a single struct so non-quality-gate construction sites need
/// only `quality_gate: None` instead of three separate optional fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityGateConfig {
    /// Step key whose structured output is evaluated.
    pub source: String,
    /// Minimum confidence score (0-100) required to pass.
    pub threshold: u32,
    /// Action when the gate fails (score below threshold).
    #[serde(default = "default_on_fail")]
    pub on_fail_action: OnFailAction,
}

fn default_on_fail() -> OnFailAction {
    OnFailAction::Fail
}

/// Specifies the set of options for a multi-select gate.
///
/// - `Static`: a literal list of option strings defined in the workflow file.
/// - `StepRef`: a `"step.field"` reference resolved at runtime from a prior step's
///   structured output (the field must be a JSON array of strings).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GateOptions {
    Static(Vec<String>),
    /// Raw `"step.field"` dotted reference — resolved at execution time.
    StepRef(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateNode {
    pub name: String,
    pub gate_type: GateType,
    pub prompt: Option<String>,
    #[serde(default = "default_one")]
    pub min_approvals: u32,
    #[serde(default)]
    pub approval_mode: ApprovalMode,
    pub timeout_secs: u64,
    pub on_timeout: OnTimeout,
    /// Named GitHub App bot identity used for `gh` calls inside this gate.
    pub bot_name: Option<String>,
    /// Quality gate-specific configuration. Present only when `gate_type == QualityGate`.
    #[serde(flatten)]
    pub quality_gate: Option<QualityGateConfig>,
    /// Optional multi-select options for human_approval / human_review gates.
    pub options: Option<GateOptions>,
}

fn default_one() -> u32 {
    1
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateType {
    HumanApproval,
    HumanReview,
    PrApproval,
    PrChecks,
    QualityGate,
}

impl std::fmt::Display for GateType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HumanApproval => write!(f, "human_approval"),
            Self::HumanReview => write!(f, "human_review"),
            Self::PrApproval => write!(f, "pr_approval"),
            Self::PrChecks => write!(f, "pr_checks"),
            Self::QualityGate => write!(f, "quality_gate"),
        }
    }
}

impl std::str::FromStr for GateType {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "human_approval" => Ok(Self::HumanApproval),
            "human_review" => Ok(Self::HumanReview),
            "pr_approval" => Ok(Self::PrApproval),
            "pr_checks" => Ok(Self::PrChecks),
            "quality_gate" => Ok(Self::QualityGate),
            _ => Err(format!("unknown gate type: {s}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnTimeout {
    Fail,
    Continue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlwaysNode {
    pub body: Vec<WorkflowNode>,
}

// ---------------------------------------------------------------------------
// Tree-walking helpers
// ---------------------------------------------------------------------------

/// Count the total number of nodes in a node list (for display).
pub(crate) fn count_nodes(nodes: &[WorkflowNode]) -> usize {
    let mut count = 0;
    for node in nodes {
        count += 1;
        match node {
            WorkflowNode::Call(_) | WorkflowNode::CallWorkflow(_) | WorkflowNode::Script(_) => {}
            WorkflowNode::If(n) => count += count_nodes(&n.body),
            WorkflowNode::Unless(n) => count += count_nodes(&n.body),
            WorkflowNode::While(n) => count += count_nodes(&n.body),
            WorkflowNode::DoWhile(n) => count += count_nodes(&n.body),
            WorkflowNode::Do(n) => count += count_nodes(&n.body),
            WorkflowNode::Parallel(n) => count += n.calls.len(),
            WorkflowNode::Gate(_) => {}
            WorkflowNode::Always(n) => count += count_nodes(&n.body),
            WorkflowNode::ForEach(_) => {}
        }
    }
    count
}

/// Collect all agent references in a node tree (for validation before execution).
pub fn collect_agent_names(nodes: &[WorkflowNode]) -> Vec<AgentRef> {
    let mut refs = Vec::new();
    for node in nodes {
        match node {
            WorkflowNode::Call(n) => {
                refs.push(n.agent.clone());
                if let Some(OnFail::Agent(ref a)) = n.on_fail {
                    refs.push(a.clone());
                }
            }
            WorkflowNode::CallWorkflow(n) => {
                if let Some(OnFail::Agent(ref a)) = n.on_fail {
                    refs.push(a.clone());
                }
            }
            WorkflowNode::Script(n) => {
                if let Some(OnFail::Agent(ref a)) = n.on_fail {
                    refs.push(a.clone());
                }
            }
            WorkflowNode::If(n) => refs.extend(collect_agent_names(&n.body)),
            WorkflowNode::Unless(n) => refs.extend(collect_agent_names(&n.body)),
            WorkflowNode::While(n) => refs.extend(collect_agent_names(&n.body)),
            WorkflowNode::DoWhile(n) => refs.extend(collect_agent_names(&n.body)),
            WorkflowNode::Do(n) => refs.extend(collect_agent_names(&n.body)),
            WorkflowNode::Parallel(n) => refs.extend(n.calls.iter().cloned()),
            WorkflowNode::Gate(_) => {}
            WorkflowNode::Always(n) => refs.extend(collect_agent_names(&n.body)),
            WorkflowNode::ForEach(_) => {}
        }
    }
    refs
}

/// Collect all prompt snippet references (`with` values) from a node tree.
pub(crate) fn collect_snippet_refs(nodes: &[WorkflowNode]) -> Vec<String> {
    let mut refs = Vec::new();
    for node in nodes {
        match node {
            WorkflowNode::Call(n) => refs.extend(n.with.iter().cloned()),
            WorkflowNode::Parallel(n) => {
                refs.extend(n.with.iter().cloned());
                for extra in n.call_with.values() {
                    refs.extend(extra.iter().cloned());
                }
            }
            WorkflowNode::If(n) => refs.extend(collect_snippet_refs(&n.body)),
            WorkflowNode::Unless(n) => refs.extend(collect_snippet_refs(&n.body)),
            WorkflowNode::While(n) => refs.extend(collect_snippet_refs(&n.body)),
            WorkflowNode::DoWhile(n) => refs.extend(collect_snippet_refs(&n.body)),
            WorkflowNode::Do(n) => {
                refs.extend(n.with.iter().cloned());
                refs.extend(collect_snippet_refs(&n.body));
            }
            WorkflowNode::Always(n) => refs.extend(collect_snippet_refs(&n.body)),
            WorkflowNode::CallWorkflow(_) | WorkflowNode::Gate(_) | WorkflowNode::Script(_) => {}
            WorkflowNode::ForEach(_) => {}
        }
    }
    refs
}

/// Collect all `call workflow` references in a node tree (for cycle detection).
pub fn collect_workflow_refs(nodes: &[WorkflowNode]) -> Vec<String> {
    let mut refs = Vec::new();
    for node in nodes {
        match node {
            WorkflowNode::Call(_) | WorkflowNode::Gate(_) | WorkflowNode::Script(_) => {}
            WorkflowNode::CallWorkflow(n) => refs.push(n.workflow.clone()),
            WorkflowNode::If(n) => refs.extend(collect_workflow_refs(&n.body)),
            WorkflowNode::Unless(n) => refs.extend(collect_workflow_refs(&n.body)),
            WorkflowNode::While(n) => refs.extend(collect_workflow_refs(&n.body)),
            WorkflowNode::DoWhile(n) => refs.extend(collect_workflow_refs(&n.body)),
            WorkflowNode::Do(n) => refs.extend(collect_workflow_refs(&n.body)),
            WorkflowNode::Parallel(_) => {} // parallel only contains agent calls
            WorkflowNode::Always(n) => refs.extend(collect_workflow_refs(&n.body)),
            // ForEach references a child workflow — include it for cycle detection
            WorkflowNode::ForEach(n) => refs.push(n.workflow.clone()),
        }
    }
    refs
}

/// Collect all output schema references (`output =` values) from a node tree.
pub(crate) fn collect_schema_refs(nodes: &[WorkflowNode]) -> Vec<String> {
    let mut refs = Vec::new();
    for node in nodes {
        match node {
            WorkflowNode::Call(n) => {
                if let Some(ref s) = n.output {
                    refs.push(s.clone());
                }
            }
            WorkflowNode::Do(n) => {
                if let Some(ref s) = n.output {
                    refs.push(s.clone());
                }
                refs.extend(collect_schema_refs(&n.body));
            }
            WorkflowNode::Parallel(n) => {
                if let Some(ref s) = n.output {
                    refs.push(s.clone());
                }
                refs.extend(n.call_outputs.values().cloned());
            }
            WorkflowNode::If(n) => refs.extend(collect_schema_refs(&n.body)),
            WorkflowNode::Unless(n) => refs.extend(collect_schema_refs(&n.body)),
            WorkflowNode::While(n) => refs.extend(collect_schema_refs(&n.body)),
            WorkflowNode::DoWhile(n) => refs.extend(collect_schema_refs(&n.body)),
            WorkflowNode::Always(n) => refs.extend(collect_schema_refs(&n.body)),
            WorkflowNode::CallWorkflow(_) | WorkflowNode::Gate(_) | WorkflowNode::Script(_) => {}
            WorkflowNode::ForEach(_) => {}
        }
    }
    refs
}

/// Collect all bot names (`bot_name =` values) from a node tree.
pub(crate) fn collect_bot_names(nodes: &[WorkflowNode]) -> Vec<String> {
    let mut names = Vec::new();
    for node in nodes {
        match node {
            WorkflowNode::Call(n) => {
                if let Some(ref b) = n.bot_name {
                    names.push(b.clone());
                }
            }
            WorkflowNode::CallWorkflow(n) => {
                if let Some(ref b) = n.bot_name {
                    names.push(b.clone());
                }
            }
            WorkflowNode::Gate(n) => {
                if let Some(ref b) = n.bot_name {
                    names.push(b.clone());
                }
            }
            WorkflowNode::If(n) => names.extend(collect_bot_names(&n.body)),
            WorkflowNode::Unless(n) => names.extend(collect_bot_names(&n.body)),
            WorkflowNode::While(n) => names.extend(collect_bot_names(&n.body)),
            WorkflowNode::DoWhile(n) => names.extend(collect_bot_names(&n.body)),
            WorkflowNode::Do(n) => names.extend(collect_bot_names(&n.body)),
            WorkflowNode::Parallel(_) => {}
            WorkflowNode::Script(n) => {
                if let Some(ref b) = n.bot_name {
                    names.push(b.clone());
                }
            }
            WorkflowNode::Always(n) => names.extend(collect_bot_names(&n.body)),
            WorkflowNode::ForEach(_) => {}
        }
    }
    names
}

/// Collect all per-step plugin_dirs from call nodes in a node tree.
pub(crate) fn collect_plugin_dirs(nodes: &[WorkflowNode]) -> Vec<String> {
    let mut dirs = Vec::new();
    for node in nodes {
        match node {
            WorkflowNode::Call(n) => dirs.extend(n.plugin_dirs.iter().cloned()),
            WorkflowNode::If(n) => dirs.extend(collect_plugin_dirs(&n.body)),
            WorkflowNode::Unless(n) => dirs.extend(collect_plugin_dirs(&n.body)),
            WorkflowNode::While(n) => dirs.extend(collect_plugin_dirs(&n.body)),
            WorkflowNode::DoWhile(n) => dirs.extend(collect_plugin_dirs(&n.body)),
            WorkflowNode::Do(n) => dirs.extend(collect_plugin_dirs(&n.body)),
            WorkflowNode::Always(n) => dirs.extend(collect_plugin_dirs(&n.body)),
            WorkflowNode::CallWorkflow(_)
            | WorkflowNode::Gate(_)
            | WorkflowNode::Parallel(_)
            | WorkflowNode::Script(_)
            | WorkflowNode::ForEach(_) => {}
        }
    }
    dirs
}
