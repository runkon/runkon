use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{atomic::AtomicBool, Arc};

use runkon_flow::engine::{ChildWorkflowContext, ChildWorkflowInput, ChildWorkflowRunner};
use runkon_flow::engine_error::EngineError;
use runkon_flow::traits::run_context::RunContext;
use runkon_flow::types::{WorkflowExecConfig, WorkflowResult};
use runkon_flow::CancellationToken;

/// `ChildWorkflowRunner` that logs each call and returns a stub result.
struct LoggingChildRunner;

fn stub_result(run_id: &str, workflow_name: &str) -> WorkflowResult {
    WorkflowResult {
        workflow_run_id: run_id.to_string(),
        workflow_name: workflow_name.to_string(),
        all_succeeded: true,
        total_cost: 0.0,
        total_turns: 0,
        total_duration_ms: 0,
        total_input_tokens: 0,
        total_output_tokens: 0,
        total_cache_read_input_tokens: 0,
        total_cache_creation_input_tokens: 0,
    }
}

impl ChildWorkflowRunner for LoggingChildRunner {
    fn execute_child(
        &self,
        workflow_name: &str,
        parent_ctx: &ChildWorkflowContext,
        params: ChildWorkflowInput,
    ) -> Result<WorkflowResult, EngineError> {
        println!(
            "execute_child: workflow={} inputs={}",
            workflow_name,
            params.inputs.len()
        );
        Ok(stub_result(&parent_ctx.workflow_run_id, workflow_name))
    }

    fn resume_child(
        &self,
        workflow_run_id: &str,
        _model: Option<&str>,
        parent_ctx: &ChildWorkflowContext,
    ) -> Result<WorkflowResult, EngineError> {
        println!("resume_child: run_id={}", workflow_run_id);
        Ok(stub_result(
            workflow_run_id,
            parent_ctx.run_ctx.workflow_name(),
        ))
    }

    fn find_resumable_child(
        &self,
        _parent_run_id: &str,
        _workflow_name: &str,
    ) -> Result<Option<runkon_flow::types::WorkflowRun>, EngineError> {
        Ok(None)
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
        "parent-run-001"
    }
    fn workflow_name(&self) -> &str {
        "parent-workflow"
    }
    fn parent_run_id(&self) -> Option<&str> {
        None
    }
    fn shutdown(&self) -> Option<&Arc<AtomicBool>> {
        None
    }
}

fn main() {
    let runner = LoggingChildRunner;
    let ctx = Arc::new(StubCtx(std::env::temp_dir()));
    let parent_ctx = ChildWorkflowContext {
        run_ctx: Arc::clone(&ctx) as Arc<dyn RunContext>,
        extra_plugin_dirs: vec![],
        workflow_run_id: "parent-run-001".into(),
        model: None,
        exec_config: WorkflowExecConfig::default(),
        inputs: HashMap::new(),
        event_sinks: Arc::from(vec![]),
    };
    let params = ChildWorkflowInput {
        inputs: HashMap::new(),
        iteration: 0,
        bot_name: None,
        depth: 1,
        parent_step_id: None,
        cancellation: CancellationToken::new(),
    };
    let result = runner
        .execute_child("child-workflow", &parent_ctx, params)
        .expect("execute_child failed");
    println!("succeeded: {}", result.all_succeeded);
}
