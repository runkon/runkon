use std::collections::HashMap;
use std::path::PathBuf;

use runkon_flow::traits::run_context::RunContext;
use runkon_flow::traits::script_env_provider::ScriptEnvProvider;

/// A `ScriptEnvProvider` that prepends directories to `PATH`.
///
/// Bot-name / GH_TOKEN resolution is conductor-specific and intentionally
/// omitted — use `ConductorScriptEnvProvider` (in `conductor-core`) when
/// GitHub App token injection is required.
pub struct PathPrependingEnvProvider {
    prepend_dirs: Vec<PathBuf>,
}

impl PathPrependingEnvProvider {
    pub fn new(prepend_dirs: Vec<PathBuf>) -> Self {
        Self { prepend_dirs }
    }
}

impl ScriptEnvProvider for PathPrependingEnvProvider {
    fn env(&self, _ctx: &dyn RunContext, _bot_name: Option<&str>) -> HashMap<String, String> {
        let mut env = HashMap::new();
        if !self.prepend_dirs.is_empty() {
            let existing = std::env::var("PATH").unwrap_or_default();
            let mut parts: Vec<String> = self
                .prepend_dirs
                .iter()
                .map(|p| p.display().to_string())
                .collect();
            if !existing.is_empty() {
                parts.push(existing);
            }
            env.insert("PATH".to_string(), parts.join(":"));
        }
        env
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runkon_flow::traits::run_context::NoopRunContext;

    fn ctx() -> NoopRunContext {
        NoopRunContext::default()
    }

    #[test]
    fn empty_prepend_dirs_produces_no_env() {
        let provider = PathPrependingEnvProvider::new(vec![]);
        let env = provider.env(&ctx(), None);
        assert!(env.is_empty(), "expected empty env, got: {env:?}");
    }

    #[test]
    fn single_dir_prepends_to_path() {
        let dir = PathBuf::from("/usr/local/mybin");
        let provider = PathPrependingEnvProvider::new(vec![dir]);
        let env = provider.env(&ctx(), None);
        let path = env.get("PATH").expect("PATH should be set");
        assert!(
            path.starts_with("/usr/local/mybin"),
            "PATH should start with the prepended dir, got: {path}"
        );
    }

    #[test]
    fn multiple_dirs_preserve_order() {
        let dirs = vec![
            PathBuf::from("/first"),
            PathBuf::from("/second"),
            PathBuf::from("/third"),
        ];
        let provider = PathPrependingEnvProvider::new(dirs);
        let env = provider.env(&ctx(), None);
        let path = env.get("PATH").expect("PATH should be set");
        let parts: Vec<&str> = path.splitn(4, ':').collect();
        assert_eq!(parts[0], "/first");
        assert_eq!(parts[1], "/second");
        assert_eq!(parts[2], "/third");
    }

    #[test]
    fn inherited_path_appended_after_prepend_dirs() {
        // Set a known PATH in the environment for this test.
        // We use std::env::set_var under a cfg(test) guard.
        // The existing PATH from the process is appended; we verify the
        // prepended dir comes first regardless of the inherited value.
        let dir = PathBuf::from("/prepended");
        let provider = PathPrependingEnvProvider::new(vec![dir]);
        let env = provider.env(&ctx(), None);
        let path = env.get("PATH").expect("PATH should be set");
        assert!(
            path.starts_with("/prepended"),
            "prepended dir must come before inherited PATH, got: {path}"
        );
        // If the process has a non-empty PATH, it must appear after the
        // prepended segment.
        let existing = std::env::var("PATH").unwrap_or_default();
        if !existing.is_empty() {
            assert!(
                path.contains(&existing),
                "inherited PATH should be appended, got: {path}"
            );
        }
    }
}
