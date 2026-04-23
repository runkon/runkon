//! Recursive descent parser for the `.wf` workflow DSL.

mod api;
mod lexer;
mod parser;
mod script_utils;
mod types;
mod validation;

// Re-export everything that is currently public
pub use api::{
    detect_workflow_cycles, load_workflow_by_name, load_workflow_defs, validate_workflow_name,
    MAX_WORKFLOW_DEPTH,
};
#[allow(unused_imports)]
pub(crate) use parser::parse_duration_str;
pub(crate) use parser::parse_workflow_file;
pub use parser::parse_workflow_str;
pub use script_utils::{default_skills_dir, make_script_resolver, resolve_script_path};
#[allow(unused_imports)]
pub use types::QualityGateConfig;
#[allow(unused_imports)]
pub use types::WorktreeScope;
pub use types::{
    collect_agent_names, collect_workflow_refs, AgentRef, AlwaysNode, ApprovalMode, CallNode,
    CallWorkflowNode, Condition, DoNode, DoWhileNode, ForEachNode, ForeachScope, GateNode,
    GateOptions, GateType, IfNode, InputDecl, InputType, OnChildFail, OnCycle, OnFail,
    OnFailAction, OnMaxIter, OnTimeout, ParallelNode, ScriptNode, TicketScope, UnlessNode,
    WhileNode, WorkflowDef, WorkflowNode, WorkflowTrigger, WorkflowWarning,
};
pub use validation::{
    validate_script_steps, validate_workflow_semantics, ValidationError, ValidationReport,
};
