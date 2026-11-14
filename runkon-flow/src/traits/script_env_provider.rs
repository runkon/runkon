use std::collections::HashMap;
use std::path::PathBuf;

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
    /// The prefix must be empty, or start with an ASCII letter or `_` followed
    /// by letters, digits or `_` (see [`validate_input_env_prefix`]); the engine
    /// refuses to start a run with any other prefix.
    fn input_env_prefix(&self) -> &str {
        DEFAULT_INPUT_ENV_PREFIX
    }

    /// Where to write a script step's log, or `None` (the default) to keep none.
    ///
    /// Called once per attempt, since every attempt has its own step row and
    /// `step_id`. When it returns a path, the executor writes a log there on
    /// every outcome (success, non-zero exit, timeout, FLOW_OUTPUT parse
    /// failure): a `--- stderr ---` section and a `--- stdout (outside
    /// FLOW_OUTPUT) ---` section, only the non-empty ones. The log is capped at
    /// about 64 KiB; the oldest bytes are dropped and the section starts with
    /// `… [truncated N bytes]`. The executor creates parent directories and
    /// overwrites an existing file.
    ///
    /// The path is recorded in the step row's `output_file` column, not in the
    /// step's context, so `{{prior_output_file}}` is unaffected. Nothing is
    /// written, and no path is recorded, when both sections are empty or the
    /// write fails.
    fn script_log_path(
        &self,
        _ctx: &dyn RunContext,
        _step_id: &str,
        _step_name: &str,
        _iteration: u32,
    ) -> Option<PathBuf> {
        None
    }
}

/// Default [`ScriptEnvProvider::input_env_prefix`].
pub const DEFAULT_INPUT_ENV_PREFIX: &str = "FLOW_";

/// Check a [`ScriptEnvProvider::input_env_prefix`] value: empty, or an ASCII
/// letter or `_` followed by letters, digits or `_`. Anything else could
/// produce a different variable than intended (`PATH=` + `TICKET_ID` sets
/// `PATH`), fail every spawn (a null byte), or produce names a script can't
/// read (`$1_TICKET_ID` expands `$1`), so it's rejected rather than corrected.
pub fn validate_input_env_prefix(prefix: &str) -> Result<(), String> {
    let mut chars = prefix.chars();
    let valid = match chars.next() {
        None => true,
        Some(first) => {
            (first.is_ascii_alphabetic() || first == '_')
                && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
    };
    if valid {
        Ok(())
    } else {
        Err(format!(
            "invalid script input env prefix {prefix:?} from ScriptEnvProvider::input_env_prefix(): \
             it must be empty or start with an ASCII letter or '_', followed by letters, \
             digits or '_'"
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
        for prefix in [
            "",
            DEFAULT_INPUT_ENV_PREFIX,
            "CONDUCTOR_",
            "app2_",
            "_",
            "_X_",
            "_1",
            "_PREFIX_2",
        ] {
            assert!(validate_input_env_prefix(prefix).is_ok(), "{prefix:?}");
        }
    }

    #[test]
    fn validate_input_env_prefix_rejects_other_characters() {
        for prefix in [
            "PATH=",
            "BAD\0",
            "MY-APP_",
            "WITH SPACE_",
            "ÜNI_",
            "1_",
            "2FLOW_",
        ] {
            let err = validate_input_env_prefix(prefix).unwrap_err();
            assert!(
                err.contains("invalid script input env prefix"),
                "{prefix:?}: {err}"
            );
        }
    }
}
