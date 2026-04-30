use std::collections::HashMap;

use crate::traits::run_context::RunContext;

/// Builds the per-script-step environment.
///
/// `bot_name`, when `Some`, identifies a host-configured bot identity
/// (e.g. a GitHub App installation) the host should resolve into auth
/// material — typically a `GH_TOKEN` env var so the script's `gh` calls
/// run as that bot rather than the conductor user. Provider impls that
/// don't support bot identities should ignore the parameter.
pub trait ScriptEnvProvider: Send + Sync {
    fn env(&self, ctx: &dyn RunContext, bot_name: Option<&str>) -> HashMap<String, String>;
}

/// No-op default — returns empty env when no provider is configured.
pub struct NoOpScriptEnvProvider;

impl ScriptEnvProvider for NoOpScriptEnvProvider {
    fn env(&self, _ctx: &dyn RunContext, _bot_name: Option<&str>) -> HashMap<String, String> {
        HashMap::new()
    }
}
