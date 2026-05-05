use crate::engine_error::EngineError;
use crate::status::WorkflowStepStatus;

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

/// Abstracts human-approval gate reads and writes for a persistence backend.
///
/// Extracted from `WorkflowPersistence` as a supertrait so the engine's gate
/// executor can call `state.persistence.get_gate_approval()` without any
/// structural changes to `ExecutionState` — any `dyn WorkflowPersistence` is
/// also a `dyn GateApprovalStore` through the supertrait bound.
pub trait GateApprovalStore: Send + Sync {
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
}
