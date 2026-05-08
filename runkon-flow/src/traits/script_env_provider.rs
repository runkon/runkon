use std::collections::HashMap;

use crate::traits::run_context::RunContext;

/// Builds the per-script-step environment.
///
/// `as_identity`, when `Some`, names the identity this step should act as.
/// Providers resolve the identity into harness-defined auth material —
/// typically by injecting credentials as env vars. Examples:
///
/// - GitHub App installation name → `GH_TOKEN` (conductor's default impl)
/// - AWS service account ID → `AWS_ACCESS_KEY_ID` / related vars
/// - Slack bot user ID → `SLACK_BOT_TOKEN`
/// - Agent persona key → API key scoped to that persona
///
/// Providers that don't model named identities ignore the parameter.
pub trait ScriptEnvProvider: Send + Sync {
    fn env(&self, ctx: &dyn RunContext, as_identity: Option<&str>) -> HashMap<String, String>;
}

/// No-op default — returns empty env when no provider is configured.
pub struct NoOpScriptEnvProvider;

impl ScriptEnvProvider for NoOpScriptEnvProvider {
    fn env(&self, _ctx: &dyn RunContext, _as_identity: Option<&str>) -> HashMap<String, String> {
        HashMap::new()
    }
}
