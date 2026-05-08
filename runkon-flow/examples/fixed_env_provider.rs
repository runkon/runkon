use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{atomic::AtomicBool, Arc};

use runkon_flow::traits::run_context::RunContext;
use runkon_flow::traits::script_env_provider::ScriptEnvProvider;

/// `ScriptEnvProvider` that returns a fixed, pre-configured environment map.
struct FixedEnvProvider(HashMap<String, String>);

impl ScriptEnvProvider for FixedEnvProvider {
    fn env(&self, _ctx: &dyn RunContext, _as_identity: Option<&str>) -> HashMap<String, String> {
        self.0.clone()
    }
}

struct StubCtx(PathBuf);

impl RunContext for StubCtx {
    fn injected_variables(&self) -> HashMap<&'static str, String> {
        HashMap::new()
    }
    fn working_dir(&self) -> &Path {
        &self.0
    }
    fn working_dir_str(&self) -> String {
        self.0.to_string_lossy().into_owned()
    }
    fn get(&self, _: &str) -> Option<String> {
        None
    }
    fn run_id(&self) -> &str {
        "env-run"
    }
    fn workflow_name(&self) -> &str {
        "env-example"
    }
    fn parent_run_id(&self) -> Option<&str> {
        None
    }
    fn shutdown(&self) -> Option<&Arc<AtomicBool>> {
        None
    }
}

fn main() {
    let provider = FixedEnvProvider(
        [
            ("CI".to_string(), "true".to_string()),
            ("RUST_LOG".to_string(), "info".to_string()),
        ]
        .into_iter()
        .collect(),
    );

    let ctx = StubCtx(std::env::temp_dir());
    let env = provider.env(&ctx, None);

    println!("env ({} vars):", env.len());
    let mut pairs: Vec<_> = env.iter().collect();
    pairs.sort_by_key(|(k, _)| k.as_str());
    for (k, v) in pairs {
        println!("  {}={}", k, v);
    }
}
