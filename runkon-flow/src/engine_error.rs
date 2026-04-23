use crate::cancellation_reason::CancellationReason;
use thiserror::Error;

pub type Result<T, E = EngineError> = std::result::Result<T, E>;

#[allow(dead_code)]
#[derive(Debug, Error)]
pub enum EngineError {
    #[error("workflow cancelled: {0:?}")]
    Cancelled(CancellationReason),
    #[error("persistence error: {0}")]
    Persistence(String),
    #[error("workflow error: {0}")]
    Workflow(String),
    #[error("workflow not found: {0}")]
    WorkflowNotFound(String),
}
