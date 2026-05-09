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
                    _ => {
                        if let Some(body) = node.body() {
                            if let Some(v) = search(body, name) {
                                return Some(v);
                            }
                        }
                    }
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

    /// Collect all as_identity values referenced across body and always blocks, sorted and deduplicated.
    pub fn collect_all_as_identities(&self) -> Vec<String> {
        let mut names = collect_as_identities(&self.body);
        names.extend(collect_as_identities(&self.always));
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

impl WorkflowNode {
    /// Returns the child body slice for block-node variants, or `None` for leaf nodes.
    pub fn body(&self) -> Option<&[WorkflowNode]> {
        match self {
            WorkflowNode::If(n) => Some(&n.body),
            WorkflowNode::Unless(n) => Some(&n.body),
            WorkflowNode::While(n) => Some(&n.body),
            WorkflowNode::DoWhile(n) => Some(&n.body),
            WorkflowNode::Do(n) => Some(&n.body),
            WorkflowNode::Always(n) => Some(&n.body),
            _ => None,
        }
    }
}

/// A foreach step node — fans out a child workflow over a collection of items.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForEachNode {
    /// Step name used as the key in step_results and resume skip sets.
    pub name: String,
    /// The collection type to fan out over.
    pub over: ForeachOver,
    /// Raw scope key-value map passed to the provider's `parse_scope` method.
    pub scope: Option<HashMap<String, String>>,
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
    pub as_identity: Option<String>,
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
    pub as_identity: Option<String>,
    /// Per-step plugin directories from the `.wf` file. Merged with repo-level
    /// `extra_plugin_dirs` at execution time to give this agent access to
    /// specialist plugins (e.g. `/usr/local/bsg/agent-architecture/planner`).
    #[serde(default)]
    pub plugin_dirs: Vec<String>,
    /// Optional per-step timeout (e.g. "5m", "30s", "1h"). If the step does not
    /// complete within this duration it is cancelled with `CancellationReason::Timeout`.
    pub timeout: Option<String>,
    /// Optional per-step host-enforced turn cap. Overrides the workflow-level default.
    /// `None` defers to `DEFAULT_MAX_TURNS` applied by the executor.
    #[serde(default)]
    pub max_turns: Option<u32>,
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
    pub as_identity: Option<String>,
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
    /// Per-call retry counts keyed by index (as string) in `calls`. 0 = no retries.
    #[serde(default)]
    pub call_retries: HashMap<String, u32>,
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

/// Configuration specific to `quality_gate` nodes.
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
/// - `Static`: a literal key-value map of option strings defined in the workflow file.
/// - `StepRef`: a `"step.field"` reference resolved at runtime from a prior step's
///   structured output (the field must be a JSON object with string values).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GateOptions {
    Static(HashMap<String, String>),
    /// Raw `"step.field"` dotted reference — resolved at execution time.
    StepRef(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateNode {
    pub name: String,
    pub gate_type: String,
    pub prompt: Option<String>,
    #[serde(default = "default_one")]
    pub min_approvals: u32,
    #[serde(default)]
    pub approval_mode: ApprovalMode,
    pub timeout_secs: u64,
    pub on_timeout: OnTimeout,
    /// Named GitHub App bot identity used for `gh` calls inside this gate.
    pub as_identity: Option<String>,
    /// Quality gate-specific configuration. Present only when `gate_type == QUALITY_GATE_TYPE`.
    #[serde(flatten)]
    pub quality_gate: Option<QualityGateConfig>,
    /// Optional multi-select options for human_approval / human_review gates.
    pub options: Option<GateOptions>,
}

fn default_one() -> u32 {
    1
}

pub const QUALITY_GATE_TYPE: &str = "quality_gate";

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
            WorkflowNode::Parallel(n) => count += n.calls.len(),
            _ => {
                if let Some(body) = node.body() {
                    count += count_nodes(body);
                }
            }
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
            WorkflowNode::Parallel(n) => refs.extend(n.calls.iter().cloned()),
            _ => {
                if let Some(body) = node.body() {
                    refs.extend(collect_agent_names(body));
                }
            }
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
            WorkflowNode::Do(n) => {
                refs.extend(n.with.iter().cloned());
                refs.extend(collect_snippet_refs(&n.body));
            }
            _ => {
                if let Some(body) = node.body() {
                    refs.extend(collect_snippet_refs(body));
                }
            }
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
            _ => {
                if let Some(body) = node.body() {
                    refs.extend(collect_schema_refs(body));
                }
            }
        }
    }
    refs
}

/// Collect all as_identity values from a node tree.
pub(crate) fn collect_as_identities(nodes: &[WorkflowNode]) -> Vec<String> {
    let mut names = Vec::new();
    for node in nodes {
        match node {
            WorkflowNode::Call(n) => {
                if let Some(ref b) = n.as_identity {
                    names.push(b.clone());
                }
            }
            WorkflowNode::CallWorkflow(n) => {
                if let Some(ref b) = n.as_identity {
                    names.push(b.clone());
                }
            }
            WorkflowNode::Gate(n) => {
                if let Some(ref b) = n.as_identity {
                    names.push(b.clone());
                }
            }
            WorkflowNode::Script(n) => {
                if let Some(ref b) = n.as_identity {
                    names.push(b.clone());
                }
            }
            _ => {
                if let Some(body) = node.body() {
                    names.extend(collect_as_identities(body));
                }
            }
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
            _ => {
                if let Some(body) = node.body() {
                    dirs.extend(collect_plugin_dirs(body));
                }
            }
        }
    }
    dirs
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    // ── helpers ──────────────────────────────────────────────────────────────

    fn simple_wf(body: Vec<WorkflowNode>) -> WorkflowDef {
        WorkflowDef {
            name: "test_wf".to_string(),
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

    fn call(agent: &str) -> WorkflowNode {
        WorkflowNode::Call(CallNode {
            agent: AgentRef::Name(agent.to_string()),
            retries: 0,
            on_fail: None,
            output: None,
            with: vec![],
            as_identity: None,
            plugin_dirs: vec![],
            timeout: None,
            max_turns: None,
        })
    }

    fn call_with_output(agent: &str, output: &str) -> WorkflowNode {
        WorkflowNode::Call(CallNode {
            agent: AgentRef::Name(agent.to_string()),
            output: Some(output.to_string()),
            retries: 0,
            on_fail: None,
            with: vec![],
            as_identity: None,
            plugin_dirs: vec![],
            timeout: None,
            max_turns: None,
        })
    }

    fn call_with_snippets(agent: &str, snippets: &[&str]) -> WorkflowNode {
        WorkflowNode::Call(CallNode {
            agent: AgentRef::Name(agent.to_string()),
            with: snippets.iter().map(|s| s.to_string()).collect(),
            retries: 0,
            on_fail: None,
            output: None,
            as_identity: None,
            plugin_dirs: vec![],
            timeout: None,
            max_turns: None,
        })
    }

    fn call_with_plugin_dirs(agent: &str, dirs: &[&str]) -> WorkflowNode {
        WorkflowNode::Call(CallNode {
            agent: AgentRef::Name(agent.to_string()),
            plugin_dirs: dirs.iter().map(|s| s.to_string()).collect(),
            retries: 0,
            on_fail: None,
            output: None,
            with: vec![],
            as_identity: None,
            timeout: None,
            max_turns: None,
        })
    }

    fn call_with_identity(agent: &str, identity: &str) -> WorkflowNode {
        WorkflowNode::Call(CallNode {
            agent: AgentRef::Name(agent.to_string()),
            as_identity: Some(identity.to_string()),
            retries: 0,
            on_fail: None,
            output: None,
            with: vec![],
            plugin_dirs: vec![],
            timeout: None,
            max_turns: None,
        })
    }

    fn do_while_node(step: &str, max_iter: u32, body: Vec<WorkflowNode>) -> WorkflowNode {
        WorkflowNode::DoWhile(DoWhileNode {
            step: step.to_string(),
            marker: "done".to_string(),
            max_iterations: max_iter,
            stuck_after: None,
            on_max_iter: OnMaxIter::Fail,
            body,
        })
    }

    fn while_node(step: &str, max_iter: u32, body: Vec<WorkflowNode>) -> WorkflowNode {
        WorkflowNode::While(WhileNode {
            step: step.to_string(),
            marker: "needs_revision".to_string(),
            max_iterations: max_iter,
            stuck_after: None,
            on_max_iter: OnMaxIter::Fail,
            body,
        })
    }

    fn if_node(step: &str, marker: &str, body: Vec<WorkflowNode>) -> WorkflowNode {
        WorkflowNode::If(IfNode {
            condition: Condition::StepMarker {
                step: step.to_string(),
                marker: marker.to_string(),
            },
            body,
        })
    }

    fn call_workflow(name: &str) -> WorkflowNode {
        WorkflowNode::CallWorkflow(CallWorkflowNode {
            workflow: name.to_string(),
            inputs: HashMap::new(),
            retries: 0,
            on_fail: None,
            as_identity: None,
        })
    }

    fn script_node(name: &str, run: &str) -> WorkflowNode {
        WorkflowNode::Script(ScriptNode {
            name: name.to_string(),
            run: run.to_string(),
            env: HashMap::new(),
            timeout: None,
            retries: 0,
            on_fail: None,
            as_identity: None,
        })
    }

    // ── WorkflowDef::display_name ─────────────────────────────────────────────

    #[test]
    fn display_name_returns_title_when_set() {
        let mut wf = simple_wf(vec![]);
        wf.title = Some("My Workflow".to_string());
        assert_eq!(wf.display_name(), "My Workflow");
    }

    #[test]
    fn display_name_falls_back_to_name_when_no_title() {
        let wf = simple_wf(vec![]);
        assert_eq!(wf.display_name(), "test_wf");
    }

    // ── WorkflowDef::total_nodes ──────────────────────────────────────────────

    #[test]
    fn total_nodes_flat_list() {
        let wf = simple_wf(vec![call("a"), call("b"), call("c")]);
        assert_eq!(wf.total_nodes(), 3);
    }

    #[test]
    fn total_nodes_includes_nested_nodes() {
        let nested = if_node("a", "done", vec![call("b"), call("c")]);
        let wf = simple_wf(vec![call("a"), nested]);
        assert_eq!(wf.total_nodes(), 4);
    }

    #[test]
    fn total_nodes_includes_always_block() {
        let mut wf = simple_wf(vec![call("a")]);
        wf.always = vec![call("cleanup")];
        assert_eq!(wf.total_nodes(), 2);
    }

    // ── WorkflowDef::top_level_steps ─────────────────────────────────────────

    #[test]
    fn top_level_steps_returns_only_direct_children() {
        let nested = if_node("a", "done", vec![call("b"), call("c")]);
        let wf = simple_wf(vec![call("a"), nested]);
        assert_eq!(wf.top_level_steps(), 2);
    }

    #[test]
    fn top_level_steps_includes_always_block() {
        let mut wf = simple_wf(vec![call("a"), call("b")]);
        wf.always = vec![call("cleanup")];
        assert_eq!(wf.top_level_steps(), 3);
    }

    // ── WorkflowDef::max_iterations_for_step ─────────────────────────────────

    #[test]
    fn max_iterations_for_step_found_in_do_while() {
        let wf = simple_wf(vec![do_while_node("reviewer", 5, vec![call("reviewer")])]);
        assert_eq!(wf.max_iterations_for_step("reviewer"), Some(5));
    }

    #[test]
    fn max_iterations_for_step_found_in_while() {
        let wf = simple_wf(vec![
            call("reviewer"),
            while_node("reviewer", 3, vec![call("fix")]),
        ]);
        assert_eq!(wf.max_iterations_for_step("reviewer"), Some(3));
    }

    #[test]
    fn max_iterations_for_step_not_found_returns_none() {
        let wf = simple_wf(vec![call("a"), call("b")]);
        assert_eq!(wf.max_iterations_for_step("a"), None);
    }

    #[test]
    fn max_iterations_for_step_nested_loop() {
        let inner = do_while_node("inner", 2, vec![call("inner")]);
        let outer = while_node("outer", 10, vec![call("outer"), inner]);
        let wf = simple_wf(vec![outer]);
        assert_eq!(wf.max_iterations_for_step("inner"), Some(2));
        assert_eq!(wf.max_iterations_for_step("outer"), Some(10));
    }

    // ── count_nodes ──────────────────────────────────────────────────────────

    #[test]
    fn count_nodes_flat_list() {
        let nodes = vec![call("a"), call("b")];
        assert_eq!(count_nodes(&nodes), 2);
    }

    #[test]
    fn count_nodes_parallel_counts_calls() {
        let parallel = WorkflowNode::Parallel(ParallelNode {
            fail_fast: true,
            min_success: None,
            calls: vec![
                AgentRef::Name("a".to_string()),
                AgentRef::Name("b".to_string()),
            ],
            output: None,
            call_outputs: HashMap::new(),
            with: vec![],
            call_with: HashMap::new(),
            call_if: HashMap::new(),
            call_retries: HashMap::new(),
        });
        let nodes = vec![parallel];
        assert_eq!(count_nodes(&nodes), 3); // 1 parallel node + 2 calls
    }

    #[test]
    fn count_nodes_recursive_into_if_body() {
        let nested = if_node("a", "done", vec![call("b"), call("c")]);
        assert_eq!(count_nodes(&[nested]), 3); // if + 2 body
    }

    // ── collect_agent_names ───────────────────────────────────────────────────

    #[test]
    fn collect_agent_names_flat_call_nodes() {
        let nodes = vec![call("agent_a"), call("agent_b")];
        let refs = collect_agent_names(&nodes);
        let names: Vec<&str> = refs.iter().map(|r| r.label()).collect();
        assert!(names.contains(&"agent_a"));
        assert!(names.contains(&"agent_b"));
    }

    #[test]
    fn collect_agent_names_deduplication_when_sorted() {
        let nodes = vec![call("agent_a"), call("agent_a"), call("agent_b")];
        let mut refs = collect_agent_names(&nodes);
        refs.sort();
        refs.dedup();
        assert_eq!(refs.len(), 2);
    }

    #[test]
    fn collect_agent_names_parallel_node() {
        let parallel = WorkflowNode::Parallel(ParallelNode {
            fail_fast: true,
            min_success: None,
            calls: vec![
                AgentRef::Name("par_a".to_string()),
                AgentRef::Name("par_b".to_string()),
            ],
            output: None,
            call_outputs: HashMap::new(),
            with: vec![],
            call_with: HashMap::new(),
            call_if: HashMap::new(),
            call_retries: HashMap::new(),
        });
        let refs = collect_agent_names(&[parallel]);
        let names: Vec<&str> = refs.iter().map(|r| r.label()).collect();
        assert!(names.contains(&"par_a"));
        assert!(names.contains(&"par_b"));
    }

    #[test]
    fn collect_all_agent_refs_deduplicates_and_sorts() {
        let wf = simple_wf(vec![call("z_agent"), call("a_agent"), call("z_agent")]);
        let refs = wf.collect_all_agent_refs();
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].label(), "a_agent");
        assert_eq!(refs[1].label(), "z_agent");
    }

    // ── collect_snippet_refs ──────────────────────────────────────────────────

    #[test]
    fn collect_snippet_refs_from_call_with() {
        let nodes = vec![call_with_snippets("agent", &["ctx_a", "ctx_b"])];
        let refs = collect_snippet_refs(&nodes);
        assert!(refs.contains(&"ctx_a".to_string()));
        assert!(refs.contains(&"ctx_b".to_string()));
    }

    #[test]
    fn collect_all_snippet_refs_deduplicates() {
        let wf = simple_wf(vec![
            call_with_snippets("a", &["shared"]),
            call_with_snippets("b", &["shared", "unique"]),
        ]);
        let refs = wf.collect_all_snippet_refs();
        assert_eq!(refs.iter().filter(|s| *s == "shared").count(), 1);
        assert_eq!(refs.len(), 2);
    }

    // ── collect_workflow_refs ─────────────────────────────────────────────────

    #[test]
    fn collect_workflow_refs_from_call_workflow() {
        let nodes = vec![call_workflow("child_wf"), call_workflow("other_wf")];
        let refs = collect_workflow_refs(&nodes);
        assert!(refs.contains(&"child_wf".to_string()));
        assert!(refs.contains(&"other_wf".to_string()));
    }

    #[test]
    fn collect_workflow_refs_skips_call_nodes() {
        let nodes = vec![call("agent"), call_workflow("child_wf")];
        let refs = collect_workflow_refs(&nodes);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0], "child_wf");
    }

    // ── collect_schema_refs ───────────────────────────────────────────────────

    #[test]
    fn collect_schema_refs_from_call_output() {
        let nodes = vec![call_with_output("agent", "my_schema")];
        let refs = collect_schema_refs(&nodes);
        assert!(refs.contains(&"my_schema".to_string()));
    }

    #[test]
    fn collect_all_schema_refs_deduplicates() {
        let wf = simple_wf(vec![
            call_with_output("a", "schema"),
            call_with_output("b", "schema"),
        ]);
        let refs = wf.collect_all_schema_refs();
        assert_eq!(refs.iter().filter(|s| *s == "schema").count(), 1);
    }

    // ── collect_as_identities ─────────────────────────────────────────────────

    #[test]
    fn collect_as_identities_from_call_nodes() {
        let nodes = vec![call_with_identity("agent", "bot-app")];
        let names = collect_as_identities(&nodes);
        assert!(names.contains(&"bot-app".to_string()));
    }

    #[test]
    fn collect_all_as_identities_deduplicates() {
        let wf = simple_wf(vec![
            call_with_identity("a", "bot"),
            call_with_identity("b", "bot"),
        ]);
        let names = wf.collect_all_as_identities();
        assert_eq!(names.iter().filter(|n| *n == "bot").count(), 1);
    }

    // ── collect_plugin_dirs ───────────────────────────────────────────────────

    #[test]
    fn collect_plugin_dirs_from_call_nodes() {
        let nodes = vec![call_with_plugin_dirs("agent", &["/opt/plugins"])];
        let dirs = collect_plugin_dirs(&nodes);
        assert!(dirs.contains(&"/opt/plugins".to_string()));
    }

    #[test]
    fn collect_all_plugin_dirs_deduplicates() {
        let wf = simple_wf(vec![
            call_with_plugin_dirs("a", &["/opt/shared"]),
            call_with_plugin_dirs("b", &["/opt/shared", "/opt/unique"]),
        ]);
        let dirs = wf.collect_all_plugin_dirs();
        assert_eq!(dirs.iter().filter(|d| *d == "/opt/shared").count(), 1);
        assert_eq!(dirs.len(), 2);
    }

    // ── AgentRef::step_key ────────────────────────────────────────────────────

    #[test]
    fn agent_ref_name_step_key_returns_name() {
        let r = AgentRef::Name("my_agent".to_string());
        assert_eq!(r.step_key(), "my_agent");
    }

    #[test]
    fn agent_ref_path_step_key_returns_file_stem() {
        let r = AgentRef::Path(".claude/agents/plan.md".to_string());
        assert_eq!(r.step_key(), "plan");
    }

    #[test]
    fn agent_ref_label_returns_inner_string() {
        assert_eq!(AgentRef::Name("foo".to_string()).label(), "foo");
        assert_eq!(
            AgentRef::Path("bar/baz.md".to_string()).label(),
            "bar/baz.md"
        );
    }

    // ── WorkflowTrigger serde ─────────────────────────────────────────────────

    #[test]
    fn workflow_trigger_serde_round_trip() {
        for (variant, expected_json) in [
            (WorkflowTrigger::Manual, r#""manual""#),
            (WorkflowTrigger::Pr, r#""pr""#),
            (WorkflowTrigger::Scheduled, r#""scheduled""#),
        ] {
            let json = serde_json::to_string(&variant).unwrap();
            assert_eq!(json, expected_json, "display mismatch for {variant:?}");
            let back: WorkflowTrigger = serde_json::from_str(&json).unwrap();
            assert_eq!(back, variant);
        }
    }

    // ── Enum serde round-trips ────────────────────────────────────────────────

    #[test]
    fn on_max_iter_serde_round_trip() {
        let json = serde_json::to_string(&OnMaxIter::Continue).unwrap();
        assert_eq!(json, r#""continue""#);
        let back: OnMaxIter = serde_json::from_str(&json).unwrap();
        assert_eq!(back, OnMaxIter::Continue);
    }

    #[test]
    fn on_timeout_serde_round_trip() {
        let json = serde_json::to_string(&OnTimeout::Fail).unwrap();
        let back: OnTimeout = serde_json::from_str(&json).unwrap();
        assert_eq!(back, OnTimeout::Fail);
    }

    #[test]
    fn on_child_fail_serde_all_variants() {
        for variant in [
            OnChildFail::Halt,
            OnChildFail::Continue,
            OnChildFail::SkipDependents,
        ] {
            let json = serde_json::to_string(&variant).unwrap();
            let back: OnChildFail = serde_json::from_str(&json).unwrap();
            assert_eq!(back, variant);
        }
    }

    #[test]
    fn on_cycle_serde_all_variants() {
        for variant in [OnCycle::Fail, OnCycle::Warn] {
            let json = serde_json::to_string(&variant).unwrap();
            let back: OnCycle = serde_json::from_str(&json).unwrap();
            assert_eq!(back, variant);
        }
    }

    #[test]
    fn approval_mode_serde_all_variants() {
        for variant in [ApprovalMode::MinApprovals, ApprovalMode::ReviewDecision] {
            let json = serde_json::to_string(&variant).unwrap();
            let back: ApprovalMode = serde_json::from_str(&json).unwrap();
            assert_eq!(back, variant);
        }
    }

    #[test]
    fn on_fail_action_serde_all_variants() {
        for variant in [OnFailAction::Fail, OnFailAction::Continue] {
            let json = serde_json::to_string(&variant).unwrap();
            let back: OnFailAction = serde_json::from_str(&json).unwrap();
            assert_eq!(back, variant);
        }
    }

    #[test]
    fn on_fail_agent_variant_serde() {
        let val = OnFail::Agent(AgentRef::Name("fallback".to_string()));
        let json = serde_json::to_string(&val).unwrap();
        assert!(json.contains("agent"), "got: {json}");
        let back: OnFail = serde_json::from_str(&json).unwrap();
        assert_eq!(back, OnFail::Agent(AgentRef::Name("fallback".to_string())));
    }

    #[test]
    fn on_fail_continue_variant_serde() {
        let json = serde_json::to_string(&OnFail::Continue).unwrap();
        let back: OnFail = serde_json::from_str(&json).unwrap();
        assert_eq!(back, OnFail::Continue);
    }

    #[test]
    fn input_type_serde_all_variants() {
        assert_eq!(
            serde_json::to_string(&InputType::String).unwrap(),
            r#""string""#
        );
        assert_eq!(
            serde_json::to_string(&InputType::Boolean).unwrap(),
            r#""boolean""#
        );
    }

    // ── Script node helper ────────────────────────────────────────────────────

    #[test]
    fn script_node_collect_included_in_total() {
        let wf = simple_wf(vec![script_node("lint", "./scripts/lint.sh")]);
        assert_eq!(wf.total_nodes(), 1);
    }
}
