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
    ///
    /// The prefix must be empty or contain only ASCII letters, digits and `_`
    /// (see [`validate_input_env_prefix`]); the engine refuses to start a run
    /// with any other prefix.
    fn input_env_prefix(&self) -> &str {
        DEFAULT_INPUT_ENV_PREFIX
    }
}

/// Default [`ScriptEnvProvider::input_env_prefix`].
pub const DEFAULT_INPUT_ENV_PREFIX: &str = "FLOW_";

/// Check a [`ScriptEnvProvider::input_env_prefix`] value: empty, or only ASCII
/// letters, digits and `_`. Anything else could produce a different variable
/// than intended (`PATH=` + `TICKET_ID` sets `PATH`) or fail every spawn (a
/// null byte), so it's rejected rather than corrected.
pub fn validate_input_env_prefix(prefix: &str) -> Result<(), String> {
    if prefix
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        Ok(())
    } else {
        Err(format!(
            "invalid script input env prefix {prefix:?} from ScriptEnvProvider::input_env_prefix(): \
             it may contain only ASCII letters, digits and '_'"
        ))
    }
}

/// No-op default — returns empty env when no provider is configured.
pub struct NoOpScriptEnvProvider;

impl ScriptEnvProvider for NoOpScriptEnvProvider {
    fn env(&self, _ctx: &dyn RunContext, _as_identity: Option<&str>) -> HashMap<String, String> {
        HashMap::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_input_env_prefix_accepts_env_var_safe_prefixes() {
        for prefix in ["", DEFAULT_INPUT_ENV_PREFIX, "CONDUCTOR_", "app2_"] {
            assert!(validate_input_env_prefix(prefix).is_ok(), "{prefix:?}");
        }
    }

    #[test]
    fn validate_input_env_prefix_rejects_other_characters() {
        for prefix in ["PATH=", "BAD\0", "MY-APP_", "WITH SPACE_", "ÜNI_"] {
            let err = validate_input_env_prefix(prefix).unwrap_err();
            assert!(
                err.contains("invalid script input env prefix"),
                "{prefix:?}: {err}"
            );
        }
    }
}
