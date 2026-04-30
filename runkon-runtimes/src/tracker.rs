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

/// Base event sink for synchronous (single-thread) contexts.
pub trait EventSink {
    fn on_event(&self, run_id: &str, event: RuntimeEvent);
    fn on_raw_value(&self, _run_id: &str, _value: &serde_json::Value) {}
}

/// Thread-safe event sink (blanket-impl'd for all EventSink + Send + Sync types).
pub trait RunEventSink: EventSink + Send + Sync {}
impl<T: EventSink + Send + Sync> RunEventSink for T {}

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

impl EventSink for NoopEventSink {
    fn on_event(&self, _run_id: &str, _event: RuntimeEvent) {}
}
