use crate::error::RuntimeError;
use crate::run::RunHandle;

/// Lifecycle tracking for a spawned agent run.
///
/// Vendor-neutral — every spawned agent has a PID, can be cancelled,
/// can exit without a result, and the host wants the final row back.
pub trait RunTracker: Send + Sync {
    fn record_pid(&self, run_id: &str, pid: u32) -> Result<(), RuntimeError>;
    fn record_runtime(&self, run_id: &str, runtime_name: &str) -> Result<(), RuntimeError>;
    fn mark_cancelled(&self, run_id: &str) -> Result<(), RuntimeError>;
    fn mark_failed_if_running(&self, run_id: &str, reason: &str) -> Result<(), RuntimeError>;
    fn get_run(&self, run_id: &str) -> Result<Option<RunHandle>, RuntimeError>;
}

/// Best-effort streaming progress sink for agent stdout events.
pub trait RunEventSink: Send + Sync {
    /// Best-effort. The drain thread will not propagate errors from this
    /// call — implementors should log internally.
    fn on_event(&self, run_id: &str, event: RuntimeEvent);
}

/// Events emitted by a running agent process, parsed from its stdout stream.
#[derive(Debug, Clone)]
pub enum RuntimeEvent {
    /// Agent has reported its model and session id (Claude: `system.init`).
    Init {
        model: Option<String>,
        session_id: Option<String>,
    },

    /// Incremental token usage from a single agent message
    /// (Claude: `assistant` event, fires per chunk).
    Tokens {
        input: i64,
        output: i64,
        cache_read: i64,
        cache_create: i64,
    },

    /// Agent finished successfully. Field set is the union of what current
    /// vendors emit; non-applicable fields are `None`.
    Completed {
        result_text: Option<String>,
        session_id: Option<String>,
        cost_usd: Option<f64>,
        num_turns: Option<i64>,
        duration_ms: Option<i64>,
        input_tokens: Option<i64>,
        output_tokens: Option<i64>,
        cache_read_input_tokens: Option<i64>,
        cache_creation_input_tokens: Option<i64>,
    },

    /// Agent reported an in-band error result.
    Failed {
        error: String,
        session_id: Option<String>,
    },
}

/// A no-op event sink for hosts that don't care about progress events.
pub struct NoopEventSink;

impl RunEventSink for NoopEventSink {
    fn on_event(&self, _run_id: &str, _event: RuntimeEvent) {}
}
