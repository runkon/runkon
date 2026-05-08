use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{atomic::AtomicBool, Arc};

use runkon_flow::dsl::ApprovalMode;
use runkon_flow::engine_error::EngineError;
use runkon_flow::traits::gate_resolver::{GateParams, GatePoll, GateResolver};
use runkon_flow::traits::run_context::RunContext;

/// `GateResolver` that immediately approves every gate poll.
struct AlwaysApproveGateResolver;

impl GateResolver for AlwaysApproveGateResolver {
    fn gate_type(&self) -> &str {
        "always-approve"
    }

    fn poll(
        &self,
        _run_id: &str,
        _params: &GateParams,
        _ctx: &dyn RunContext,
    ) -> Result<GatePoll, EngineError> {
        Ok(GatePoll::Approved(None))
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
        "gate-run"
    }
    fn workflow_name(&self) -> &str {
        "gate-example"
    }
    fn parent_run_id(&self) -> Option<&str> {
        None
    }
    fn shutdown(&self) -> Option<&Arc<AtomicBool>> {
        None
    }
}

fn main() {
    let resolver = AlwaysApproveGateResolver;
    let ctx = StubCtx(std::env::temp_dir());
    let params = GateParams {
        gate_name: "code-review".into(),
        prompt: Some("Approve this change?".into()),
        min_approvals: 1,
        approval_mode: ApprovalMode::MinApprovals,
        options: HashMap::new(),
        timeout_secs: 300,
        as_identity: None,
        step_id: "gate-step-1".into(),
    };

    match resolver
        .poll("run-001", &params, &ctx)
        .expect("poll failed")
    {
        GatePoll::Approved(feedback) => println!("approved (feedback: {:?})", feedback),
        GatePoll::Rejected(reason) => println!("rejected: {}", reason),
        GatePoll::Pending => println!("pending"),
    }
}
