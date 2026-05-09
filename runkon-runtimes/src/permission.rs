use std::borrow::Cow;

use serde::Serialize;

/// Opaque permission mode forwarded through to the headless agent CLI.
///
/// `runkon-runtimes` deliberately does not encode any specific permission
/// taxonomy (Claude's `plan` / `repo-safe`, etc.). Vendor-specific values
/// live in the host crate (e.g. conductor-core's `AgentPermissionMode`) and
/// are converted into this opaque form when constructing a `RuntimeRequest`.
///
/// `Default` produces no permission flag value; `Other(s)` forwards `s` as-is
/// as the value argument to whichever permission flag the host adds to the
/// CLI invocation.
///
/// `Deserialize` is intentionally absent: `PermissionMode::Other` must only
/// be created from compile-time-known strings via the host crate's typed enum
/// (e.g. `AgentPermissionMode::to_runtime_permission_mode`). Allowing
/// deserialization from untrusted input would bypass that invariant and could
/// inject arbitrary strings as CLI flags.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub enum PermissionMode {
    /// No permission flag value forwarded.
    #[default]
    Default,
    /// Forward this raw flag value to the headless arg builder.
    Other(Cow<'static, str>),
}

impl PermissionMode {
    /// Returns the optional value argument that follows the host-specific
    /// permission flag in the headless CLI invocation. `None` means no
    /// permission flag is appended to the args.
    pub fn cli_flag_value(&self) -> Option<&str> {
        match self {
            Self::Default => None,
            Self::Other(s) => Some(s.as_ref()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;

    #[test]
    fn default_cli_flag_value_is_none() {
        assert_eq!(PermissionMode::Default.cli_flag_value(), None);
    }

    #[test]
    fn other_cli_flag_value_returns_inner_string() {
        let mode = PermissionMode::Other(Cow::Borrowed("bypassPermissions"));
        assert_eq!(mode.cli_flag_value(), Some("bypassPermissions"));
    }

    #[test]
    fn other_owned_cli_flag_value() {
        let mode = PermissionMode::Other(Cow::Owned("plan".to_string()));
        assert_eq!(mode.cli_flag_value(), Some("plan"));
    }

    #[test]
    fn default_variant_is_the_default() {
        assert_eq!(PermissionMode::default(), PermissionMode::Default);
    }

    #[test]
    fn default_serializes_without_panic() {
        let json = serde_json::to_string(&PermissionMode::Default).unwrap();
        assert!(!json.is_empty());
    }

    #[test]
    fn other_serializes_and_contains_value() {
        let mode = PermissionMode::Other(Cow::Borrowed("bypassPermissions"));
        let json = serde_json::to_string(&mode).unwrap();
        assert!(json.contains("bypass"), "got: {json}");
    }
}
