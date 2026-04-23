use std::sync::Arc;

use crate::dsl::WorkflowDef;
use crate::engine_error::EngineError;

pub trait WorkflowResolver: Send + Sync {
    fn resolve(&self, name: &str) -> Result<Arc<WorkflowDef>, EngineError>;
}
