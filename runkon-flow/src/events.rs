use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cancellation_reason::CancellationReason;

/// A single workflow engine event with timestamp and run identity.
#[derive(Debug, Clone)]
pub struct EngineEventData {
    /// Unix timestamp (seconds) when the event was emitted.
    pub timestamp: u64,
    /// The workflow run ID this event belongs to.
    pub run_id: String,
    /// The event payload.
    pub event: EngineEvent,
}

impl EngineEventData {
    pub fn new(run_id: String, event: EngineEvent) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            timestamp,
            run_id,
            event,
        }
    }
}

/// Workflow engine event variants emitted after each DB-write state transition.
///
/// Marked `#[non_exhaustive]` so downstream crates must handle an `_` arm —
/// future variants can be added without breaking existing sinks.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum EngineEvent {
    // Run lifecycle
    RunStarted {
        workflow_name: String,
    },
    RunCompleted {
        succeeded: bool,
    },
    RunResumed {
        workflow_name: String,
    },
    RunCancelled {
        reason: CancellationReason,
    },
    // Step lifecycle
    StepStarted {
        step_name: String,
    },
    StepCompleted {
        step_name: String,
        succeeded: bool,
    },
    StepRetrying {
        step_name: String,
        attempt: u32,
    },
    // Gate
    GateWaiting {
        gate_name: String,
    },
    GateResolved {
        gate_name: String,
        approved: bool,
    },
    // Fan-out
    FanOutItemsCollected {
        count: usize,
    },
    FanOutItemStarted {
        item_id: String,
    },
    FanOutItemCompleted {
        item_id: String,
        succeeded: bool,
    },
    // Metrics
    MetricsUpdated {
        total_cost: f64,
        total_turns: i64,
        total_duration_ms: i64,
    },
}

/// Observability sink that receives engine events after each DB-write state transition.
///
/// # Contract
///
/// - **DB writes happen before emit**: subscribers never observe pre-persistence state.
/// - **Slow sinks block the engine**: sinks that need async offload must implement it
///   internally (e.g. send over a channel, not await a future).
/// - **Panics are caught**: the engine wraps each `emit` call in `catch_unwind` and
///   logs panics; they do not abort the run.
pub trait EventSink: Send + Sync + 'static {
    fn emit(&self, event: &EngineEventData);
}

/// Emit an event to all sinks, catching and logging any panics.
///
/// Panics are caught per-sink so one bad sink cannot abort the run or silence
/// subsequent sinks. The `run_id` is included in the warning for debuggability.
pub fn emit_to_sinks(run_id: &str, event: EngineEvent, sinks: &[Arc<dyn EventSink>]) {
    if sinks.is_empty() {
        return;
    }
    let data = EngineEventData::new(run_id.to_string(), event);
    for sink in sinks.iter() {
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            sink.emit(&data);
        }))
        .is_err()
        {
            tracing::warn!(
                run_id = %run_id,
                "EventSink::emit panicked — continuing with remaining sinks"
            );
        }
    }
}
