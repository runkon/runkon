use std::collections::HashMap;

use crate::traits::run_context::RunContext;

pub trait ScriptEnvProvider: Send + Sync {
    fn env(&self, ctx: &dyn RunContext) -> HashMap<String, String>;
}

/// No-op default — returns empty env when no provider is configured.
pub struct NoOpScriptEnvProvider;

impl ScriptEnvProvider for NoOpScriptEnvProvider {
    fn env(&self, _ctx: &dyn RunContext) -> HashMap<String, String> {
        HashMap::new()
    }
}
