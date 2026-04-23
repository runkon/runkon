use std::collections::HashMap;
use std::sync::Arc;

use crate::dsl::WorkflowDef;
use crate::engine_error::EngineError;
use crate::traits::workflow_resolver::WorkflowResolver;

pub struct InMemoryWorkflowResolver {
    defs: HashMap<String, WorkflowDef>,
}

impl InMemoryWorkflowResolver {
    pub fn new(defs: impl IntoIterator<Item = (impl Into<String>, WorkflowDef)>) -> Self {
        Self {
            defs: defs.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        }
    }
}

impl WorkflowResolver for InMemoryWorkflowResolver {
    fn resolve(&self, name: &str) -> Result<Arc<WorkflowDef>, EngineError> {
        self.defs
            .get(name)
            .map(|d| Arc::new(d.clone()))
            .ok_or_else(|| EngineError::WorkflowNotFound(name.to_string()))
    }
}
