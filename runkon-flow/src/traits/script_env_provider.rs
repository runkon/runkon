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

    /// Prefix for the env vars that carry the workflow's inputs into script
    /// steps: input `ticket_id` is exposed as `<prefix>TICKET_ID`. Defaults to
    /// [`DEFAULT_INPUT_ENV_PREFIX`] (`FLOW_`); a host whose scripts expect
    /// another prefix overrides this. Names that would land on a
    /// security-sensitive variable such as `PATH` or `GH_TOKEN` are skipped.
    fn input_env_prefix(&self) -> &str {
        DEFAULT_INPUT_ENV_PREFIX
    }
}

/// Default [`ScriptEnvProvider::input_env_prefix`].
pub const DEFAULT_INPUT_ENV_PREFIX: &str = "FLOW_";

/// No-op default — returns empty env when no provider is configured.
pub struct NoOpScriptEnvProvider;

impl ScriptEnvProvider for NoOpScriptEnvProvider {
    fn env(&self, _ctx: &dyn RunContext, _as_identity: Option<&str>) -> HashMap<String, String> {
        HashMap::new()
    }
}
