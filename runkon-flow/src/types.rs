use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::dsl::GateType;
use crate::status::{WorkflowRunStatus, WorkflowStepStatus};

/// A step key is a `(name, iteration)` pair used for skip-set and step-map lookups.
#[allow(dead_code)]
pub(crate) type StepKey = (String, u32);

/// Describes what a workflow run is currently blocked on when in `Waiting` status.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BlockedOn {
    HumanApproval {
        gate_name: String,
        prompt: Option<String>,
        #[serde(default)]
        options: Vec<String>,
    },
    HumanReview {
        gate_name: String,
        prompt: Option<String>,
        #[serde(default)]
        options: Vec<String>,
    },
    PrApproval {
        gate_name: String,
        approvals_needed: u32,
    },
    PrChecks {
        gate_name: String,
    },
}

/// A workflow run record from the database.
#[derive(Debug, Clone, Serialize)]
pub struct WorkflowRun {
    pub id: String,
    pub workflow_name: String,
    pub worktree_id: Option<String>,
    pub parent_run_id: String,
    pub status: WorkflowRunStatus,
    pub dry_run: bool,
    pub trigger: String,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub result_summary: Option<String>,
    pub error: Option<String>,
    pub definition_snapshot: Option<String>,
    pub inputs: HashMap<String, String>,
    pub ticket_id: Option<String>,
    pub repo_id: Option<String>,
    pub parent_workflow_run_id: Option<String>,
    pub target_label: Option<String>,
    pub default_bot_name: Option<String>,
    pub iteration: i64,
    pub blocked_on: Option<BlockedOn>,
    pub workflow_title: Option<String>,
    pub total_input_tokens: Option<i64>,
    pub total_output_tokens: Option<i64>,
    pub total_cache_read_input_tokens: Option<i64>,
    pub total_cache_creation_input_tokens: Option<i64>,
    pub total_turns: Option<i64>,
    pub total_cost_usd: Option<f64>,
    pub total_duration_ms: Option<i64>,
    pub model: Option<String>,
    pub dismissed: bool,
}

/// Extract the human-readable title from a workflow definition snapshot JSON string.
pub fn extract_workflow_title(snapshot: Option<&str>) -> Option<String> {
    let s = snapshot?;
    match serde_json::from_str::<serde_json::Value>(s) {
        Ok(v) => v["title"].as_str().map(String::from),
        Err(e) => {
            tracing::warn!(
                "Malformed definition_snapshot JSON — could not extract workflow title: {e}"
            );
            None
        }
    }
}

impl WorkflowRun {
    /// Whether this run was triggered by a workflow hook (prevents infinite chains).
    pub fn is_triggered_by_hook(&self) -> bool {
        self.trigger == "hook"
    }

    /// Returns the human-readable display name for this run.
    pub fn display_name(&self) -> &str {
        self.workflow_title
            .as_deref()
            .unwrap_or(&self.workflow_name)
    }
}

/// A workflow step execution record from the database.
#[derive(Debug, Clone, Default, Serialize)]
pub struct WorkflowRunStep {
    pub id: String,
    pub workflow_run_id: String,
    pub step_name: String,
    pub role: String,
    pub can_commit: bool,
    pub condition_expr: Option<String>,
    pub status: WorkflowStepStatus,
    pub child_run_id: Option<String>,
    pub position: i64,
    pub started_at: Option<String>,
    pub ended_at: Option<String>,
    pub result_text: Option<String>,
    pub condition_met: Option<bool>,
    pub iteration: i64,
    pub parallel_group_id: Option<String>,
    pub context_out: Option<String>,
    pub markers_out: Option<String>,
    pub retry_count: i64,
    pub gate_type: Option<GateType>,
    pub gate_prompt: Option<String>,
    pub gate_timeout: Option<String>,
    pub gate_approved_by: Option<String>,
    pub gate_approved_at: Option<String>,
    pub gate_feedback: Option<String>,
    pub structured_output: Option<String>,
    pub output_file: Option<String>,
    pub gate_options: Option<String>,
    pub gate_selections: Option<String>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cache_read_input_tokens: Option<i64>,
    pub cache_creation_input_tokens: Option<i64>,
    pub fan_out_total: Option<i64>,
    pub fan_out_completed: i64,
    pub fan_out_failed: i64,
    pub fan_out_skipped: i64,
    pub step_error: Option<String>,
}

/// Lightweight summary of the currently-running step for a workflow run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStepSummary {
    pub step_name: String,
    pub iteration: i64,
    pub workflow_chain: Vec<String>,
}

/// Configuration for workflow execution.
#[derive(Debug, Clone)]
pub struct WorkflowExecConfig {
    pub poll_interval: Duration,
    pub step_timeout: Duration,
    pub fail_fast: bool,
    pub dry_run: bool,
    pub shutdown: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
}

impl Default for WorkflowExecConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(5),
            step_timeout: Duration::from_secs(12 * 60 * 60),
            fail_fast: true,
            dry_run: false,
            shutdown: None,
        }
    }
}

/// Result of executing a workflow.
#[derive(Debug, Clone)]
pub struct WorkflowResult {
    pub workflow_run_id: String,
    pub worktree_id: Option<String>,
    pub workflow_name: String,
    pub all_succeeded: bool,
    pub total_cost: f64,
    pub total_turns: i64,
    pub total_duration_ms: i64,
    pub total_input_tokens: i64,
    pub total_output_tokens: i64,
    pub total_cache_read_input_tokens: i64,
    pub total_cache_creation_input_tokens: i64,
}

/// Result of a single step execution (kept in memory during execution).
#[derive(Debug, Clone)]
pub struct StepResult {
    pub step_name: String,
    pub status: WorkflowStepStatus,
    pub result_text: Option<String>,
    pub cost_usd: Option<f64>,
    pub num_turns: Option<i64>,
    pub duration_ms: Option<i64>,
    pub markers: Vec<String>,
    pub context: String,
    pub child_run_id: Option<String>,
    pub structured_output: Option<String>,
    pub output_file: Option<String>,
}

/// An entry in the accumulated context history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextEntry {
    pub step: String,
    pub iteration: u32,
    pub context: String,
    #[serde(default)]
    pub markers: Vec<String>,
    #[serde(default)]
    pub structured_output: Option<String>,
    #[serde(default)]
    pub output_file: Option<String>,
}

/// A single row in the `workflow_run_step_fan_out_items` table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanOutItemRow {
    pub id: String,
    pub step_run_id: String,
    pub item_type: String,
    pub item_id: String,
    pub item_ref: String,
    pub child_run_id: Option<String>,
    pub status: String,
    pub dispatched_at: Option<String>,
    pub completed_at: Option<String>,
}

/// Resolve the directory containing the current executable.
pub fn resolve_conductor_bin_dir() -> Option<std::path::PathBuf> {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
}
