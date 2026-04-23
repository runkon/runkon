use crate::engine_error::EngineError;
use crate::status::{WorkflowRunStatus, WorkflowStepStatus};
use crate::types::{FanOutItemRow, WorkflowRun, WorkflowRunStep};

/// Parameters for creating a new workflow run.
pub struct NewRun {
    pub workflow_name: String,
    pub worktree_id: Option<String>,
    pub ticket_id: Option<String>,
    pub repo_id: Option<String>,
    pub parent_run_id: String,
    pub dry_run: bool,
    pub trigger: String,
    pub definition_snapshot: Option<String>,
    pub parent_workflow_run_id: Option<String>,
    pub target_label: Option<String>,
}

/// Parameters for inserting a new workflow step.
///
/// When `retry_count` is `Some`, the step is inserted with `status = 'running'`
/// and `started_at` set atomically. When `None`, the step starts as `'pending'`.
pub struct NewStep {
    pub workflow_run_id: String,
    pub step_name: String,
    pub role: String,
    pub can_commit: bool,
    pub position: i64,
    pub iteration: i64,
    /// `Some(n)` → insert as running with retry_count=n; `None` → insert as pending.
    pub retry_count: Option<i64>,
}

/// Fields to update on an existing workflow step.
pub struct StepUpdate {
    pub status: WorkflowStepStatus,
    pub child_run_id: Option<String>,
    pub result_text: Option<String>,
    pub context_out: Option<String>,
    pub markers_out: Option<String>,
    pub retry_count: Option<i64>,
    pub structured_output: Option<String>,
    pub step_error: Option<String>,
}

/// Status values for fan-out items, mirroring the string constants stored in the DB.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanOutItemStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Skipped,
}

impl FanOutItemStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
        }
    }
}

impl TryFrom<&str> for FanOutItemStatus {
    type Error = String;
    fn try_from(s: &str) -> std::result::Result<Self, Self::Error> {
        match s {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "skipped" => Ok(Self::Skipped),
            other => Err(format!("unknown FanOutItemStatus: {other}")),
        }
    }
}

/// Update payload for a fan-out item, mapping the two existing update variants.
pub enum FanOutItemUpdate {
    Running { child_run_id: String },
    Terminal { status: FanOutItemStatus },
}

/// Current approval state of a gate step.
#[derive(Debug, Clone)]
pub enum GateApprovalState {
    Pending,
    Approved {
        feedback: Option<String>,
        selections: Option<Vec<String>>,
    },
    Rejected {
        feedback: Option<String>,
    },
}

pub fn gate_approval_state_from_fields(
    approved_at: Option<&str>,
    status: WorkflowStepStatus,
    feedback: Option<String>,
    selections: Option<Vec<String>>,
) -> GateApprovalState {
    if approved_at.is_some() || status == WorkflowStepStatus::Completed {
        return GateApprovalState::Approved {
            feedback,
            selections,
        };
    }
    if status == WorkflowStepStatus::Failed {
        return GateApprovalState::Rejected { feedback };
    }
    GateApprovalState::Pending
}

/// Abstracts all persistence reads and writes needed by the workflow engine.
///
/// `Send + Sync` are required for use behind `Arc<dyn WorkflowPersistence>`.
/// All methods acquire a lock internally; no external synchronization is needed.
pub trait WorkflowPersistence: Send + Sync {
    // --- Run lifecycle ---

    fn create_run(&self, new_run: NewRun) -> Result<WorkflowRun, EngineError>;
    fn get_run(&self, run_id: &str) -> Result<Option<WorkflowRun>, EngineError>;
    fn list_active_runs(
        &self,
        statuses: &[WorkflowRunStatus],
    ) -> Result<Vec<WorkflowRun>, EngineError>;
    /// Update run status.
    ///
    /// NOTE: must not be called with `WorkflowRunStatus::Waiting` — use the engine's
    /// `set_waiting_blocked_on` path instead.
    fn update_run_status(
        &self,
        run_id: &str,
        status: WorkflowRunStatus,
        result_summary: Option<&str>,
        error: Option<&str>,
    ) -> Result<(), EngineError>;

    // --- Steps ---

    fn insert_step(&self, new_step: NewStep) -> Result<String, EngineError>;
    fn update_step(&self, step_id: &str, update: StepUpdate) -> Result<(), EngineError>;
    fn get_steps(&self, run_id: &str) -> Result<Vec<WorkflowRunStep>, EngineError>;

    // --- Fan-out ---

    fn insert_fan_out_item(
        &self,
        step_run_id: &str,
        item_type: &str,
        item_id: &str,
        item_ref: &str,
    ) -> Result<String, EngineError>;
    fn update_fan_out_item(
        &self,
        item_id: &str,
        update: FanOutItemUpdate,
    ) -> Result<(), EngineError>;
    fn get_fan_out_items(
        &self,
        step_run_id: &str,
        status_filter: Option<FanOutItemStatus>,
    ) -> Result<Vec<FanOutItemRow>, EngineError>;

    // --- Gate approval ---

    fn get_gate_approval(&self, step_id: &str) -> Result<GateApprovalState, EngineError>;
    fn approve_gate(
        &self,
        step_id: &str,
        approved_by: &str,
        feedback: Option<&str>,
        selections: Option<&[String]>,
    ) -> Result<(), EngineError>;
    fn reject_gate(
        &self,
        step_id: &str,
        rejected_by: &str,
        feedback: Option<&str>,
    ) -> Result<(), EngineError>;

    // --- Engine lifecycle hooks ---

    /// Returns true if the run has been cancelled (e.g. via external request).
    fn is_run_cancelled(&self, run_id: &str) -> Result<bool, EngineError>;

    /// Heartbeat tick — update last-seen timestamp so the run is not considered stale.
    fn tick_heartbeat(&self, run_id: &str) -> Result<(), EngineError>;

    /// Persist per-step token/cost metrics to the run record.
    #[allow(clippy::too_many_arguments)]
    fn persist_metrics(
        &self,
        run_id: &str,
        input_tokens: i64,
        output_tokens: i64,
        cache_read_input_tokens: i64,
        cache_creation_input_tokens: i64,
        cost_usd: f64,
        num_turns: i64,
        duration_ms: i64,
    ) -> Result<(), EngineError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::WorkflowStepStatus;

    #[test]
    fn pending_when_status_is_waiting_and_no_approved_at() {
        let state = gate_approval_state_from_fields(None, WorkflowStepStatus::Waiting, None, None);
        assert!(matches!(state, GateApprovalState::Pending));
    }

    #[test]
    fn approved_when_approved_at_is_set_regardless_of_status() {
        let state = gate_approval_state_from_fields(
            Some("2025-01-01T00:00:00Z"),
            WorkflowStepStatus::Waiting,
            Some("lgtm".into()),
            None,
        );
        match state {
            GateApprovalState::Approved { feedback, .. } => {
                assert_eq!(feedback.as_deref(), Some("lgtm"));
            }
            other => panic!("expected Approved, got {other:?}"),
        }
    }

    #[test]
    fn approved_when_status_completed_and_no_approved_at() {
        let selections = vec!["option-a".to_string()];
        let state = gate_approval_state_from_fields(
            None,
            WorkflowStepStatus::Completed,
            None,
            Some(selections.clone()),
        );
        match state {
            GateApprovalState::Approved {
                feedback,
                selections: s,
            } => {
                assert!(feedback.is_none());
                assert_eq!(s, Some(selections));
            }
            other => panic!("expected Approved, got {other:?}"),
        }
    }

    #[test]
    fn rejected_when_status_failed_surfaces_feedback() {
        let state = gate_approval_state_from_fields(
            None,
            WorkflowStepStatus::Failed,
            Some("not ready".into()),
            None,
        );
        match state {
            GateApprovalState::Rejected { feedback } => {
                assert_eq!(feedback.as_deref(), Some("not ready"));
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[test]
    fn rejected_with_no_feedback_when_none_stored() {
        let state = gate_approval_state_from_fields(None, WorkflowStepStatus::Failed, None, None);
        match state {
            GateApprovalState::Rejected { feedback } => {
                assert!(feedback.is_none());
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }
}
