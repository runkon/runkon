use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use crate::traits::action_executor::{ActionParams, ExecutionContext};

pub fn make_ectx() -> ExecutionContext {
    ExecutionContext {
        run_id: "r1".to_string(),
        working_dir: PathBuf::from("/tmp"),
        repo_path: "/tmp/repo".to_string(),
        step_timeout: Duration::from_secs(60),
        shutdown: None,
        model: None,
        bot_name: None,
        plugin_dirs: vec![],
        workflow_name: "wf".to_string(),
        worktree_id: None,
        parent_run_id: "parent-run-1".to_string(),
        step_id: "step-1".to_string(),
    }
}

pub fn make_params(name: &str) -> ActionParams {
    ActionParams {
        name: name.to_string(),
        inputs: HashMap::new(),
        retries_remaining: 0,
        retry_error: None,
        snippets: vec![],
        dry_run: false,
        gate_feedback: None,
        schema: None,
    }
}
