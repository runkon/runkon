use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{atomic::AtomicBool, Arc};
use std::time::Duration;

use runkon_flow::engine_error::EngineError;
use runkon_flow::traits::action_executor::{ActionExecutor, ActionOutput, ActionParams, StepInfo};
use runkon_flow::traits::run_context::RunContext;

/// Minimal `ActionExecutor` that echoes its input back as output.
struct EchoExecutor;

impl ActionExecutor for EchoExecutor {
    fn name(&self) -> &str {
        "echo"
    }

    fn execute(
        &self,
        _ctx: &dyn RunContext,
        _info: &StepInfo,
        params: &ActionParams,
    ) -> Result<ActionOutput, EngineError> {
        let text = params
            .inputs
            .get("text")
            .cloned()
            .unwrap_or_else(|| params.name.clone());
        Ok(ActionOutput {
            result_text: Some(text),
            ..Default::default()
        })
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
        "echo-run"
    }
    fn workflow_name(&self) -> &str {
        "echo-example"
    }
    fn parent_run_id(&self) -> Option<&str> {
        None
    }
    fn shutdown(&self) -> Option<&Arc<AtomicBool>> {
        None
    }
}

fn main() {
    let executor = EchoExecutor;
    let ctx = StubCtx(std::env::temp_dir());
    let info = StepInfo {
        step_id: "step-1".into(),
        step_timeout: Duration::from_secs(60),
    };
    let inputs: Arc<HashMap<String, String>> = Arc::new(
        [("text".to_string(), "hello from echo".to_string())]
            .into_iter()
            .collect(),
    );
    let params = ActionParams {
        name: "echo".into(),
        inputs,
        retries_remaining: 0,
        retry_error: None,
        snippets: vec![],
        dry_run: false,
        gate_feedback: None,
        extensions: Default::default(),
        model: None,
        as_identity: None,
        plugin_dirs: vec![],
    };
    let output = executor
        .execute(&ctx, &info, &params)
        .expect("execute failed");
    println!("result_text: {:?}", output.result_text);
}
