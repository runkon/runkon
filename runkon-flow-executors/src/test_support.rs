//! Scripted runtime scaffolding shared by the agent executors' tests.
//! Available with the `test-utils` feature.

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use runkon_runtimes::runtime::private::Seal;
use runkon_runtimes::{
    AgentRuntime, PollError, Result as RkResult, RunHandle, RunStatus, RuntimeRequest,
    RuntimeResolver,
};

/// Runtime that replays queued [`RunHandle`]s and records each spawn's
/// `resume_session_id` into `resumes`.
pub struct ScriptedRuntime {
    pub results: Mutex<Vec<RunHandle>>,
    pub resumes: Arc<Mutex<Vec<Option<String>>>>,
}

impl AgentRuntime for ScriptedRuntime {
    fn spawn_impl(&self, request: &RuntimeRequest, _seal: Seal) -> RkResult<()> {
        self.resumes
            .lock()
            .unwrap()
            .push(request.resume_session_id.clone());
        Ok(())
    }
    fn poll(
        &self,
        _run_id: &str,
        _shutdown: Option<&Arc<AtomicBool>>,
        _step_timeout: Duration,
    ) -> Result<RunHandle, PollError> {
        Ok(self.results.lock().unwrap().remove(0))
    }
    fn is_alive(&self, _run: &RunHandle) -> bool {
        false
    }
    fn cancel(&self, _run: &RunHandle) -> RkResult<()> {
        Ok(())
    }
}

/// Resolver that hands out a [`ScriptedRuntime`] seeded with `results`.
pub struct ScriptedResolver {
    pub results: Vec<RunHandle>,
    pub resumes: Arc<Mutex<Vec<Option<String>>>>,
}

impl RuntimeResolver for ScriptedResolver {
    fn resolve(&self, _name: &str) -> RkResult<Box<dyn AgentRuntime>> {
        Ok(Box::new(ScriptedRuntime {
            results: Mutex::new(self.results.clone()),
            resumes: self.resumes.clone(),
        }))
    }
}

/// A completed run with session id `sess-1` and the given result text.
pub fn scripted_handle(text: &str) -> RunHandle {
    RunHandle {
        id: "test-run".to_string(),
        status: RunStatus::Completed,
        subprocess_pid: None,
        runtime: "claude".to_string(),
        session_id: Some("sess-1".to_string()),
        result_text: Some(text.to_string()),
        started_at: String::new(),
        ended_at: None,
        log_file: None,
        model: None,
        cost_usd: None,
        num_turns: Some(1),
        duration_ms: None,
        input_tokens: None,
        output_tokens: None,
        cache_read_input_tokens: None,
        cache_creation_input_tokens: None,
    }
}
