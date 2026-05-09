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

/// A no-op run tracker for contexts that don't need lifecycle persistence.
pub struct NoopTracker;

impl RunTracker for NoopTracker {
    fn record_pid(&self, _run_id: &str, _pid: u32) -> Result<(), RuntimeError> {
        Ok(())
    }
    fn record_runtime(&self, _run_id: &str, _name: &str) -> Result<(), RuntimeError> {
        Ok(())
    }
    fn mark_cancelled(&self, _run_id: &str) -> Result<(), RuntimeError> {
        Ok(())
    }
    fn mark_failed_if_running(&self, _run_id: &str, _reason: &str) -> Result<(), RuntimeError> {
        Ok(())
    }
    fn get_run(&self, _run_id: &str) -> Result<Option<RunHandle>, RuntimeError> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn noop_tracker_record_pid_returns_ok() {
        assert!(NoopTracker.record_pid("run-1", 1234).is_ok());
    }

    #[test]
    fn noop_tracker_record_runtime_returns_ok() {
        assert!(NoopTracker.record_runtime("run-1", "claude").is_ok());
    }

    #[test]
    fn noop_tracker_mark_cancelled_returns_ok() {
        assert!(NoopTracker.mark_cancelled("run-1").is_ok());
    }

    #[test]
    fn noop_tracker_mark_failed_if_running_returns_ok() {
        assert!(NoopTracker
            .mark_failed_if_running("run-1", "some reason")
            .is_ok());
    }

    #[test]
    fn noop_tracker_get_run_returns_none() {
        let result = NoopTracker.get_run("run-1").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn noop_event_sink_on_event_all_variants_do_not_panic() {
        let sink = NoopEventSink;
        sink.on_event(
            "run-1",
            RuntimeEvent::Init {
                model: None,
                session_id: None,
            },
        );
        sink.on_event(
            "run-1",
            RuntimeEvent::Tokens {
                input: 10,
                output: 20,
                cache_read: 5,
                cache_create: 3,
            },
        );
        sink.on_event(
            "run-1",
            RuntimeEvent::Completed {
                result_text: Some("done".to_string()),
                session_id: None,
                cost_usd: Some(0.01),
                num_turns: Some(1),
                duration_ms: Some(1000),
                input_tokens: Some(100),
                output_tokens: Some(50),
                cache_read_input_tokens: None,
                cache_creation_input_tokens: None,
            },
        );
        sink.on_event(
            "run-1",
            RuntimeEvent::Failed {
                error: "oops".to_string(),
                session_id: None,
            },
        );
    }

    #[test]
    fn noop_event_sink_on_raw_value_does_not_panic() {
        let val = serde_json::json!({"key": "value"});
        NoopEventSink.on_raw_value("run-1", &val);
    }

    #[test]
    fn runtime_event_tokens_fields_are_preserved() {
        let event = RuntimeEvent::Tokens {
            input: 42,
            output: 100,
            cache_read: 5,
            cache_create: 10,
        };
        match event {
            RuntimeEvent::Tokens {
                input,
                output,
                cache_read,
                cache_create,
            } => {
                assert_eq!(input, 42);
                assert_eq!(output, 100);
                assert_eq!(cache_read, 5);
                assert_eq!(cache_create, 10);
            }
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn noop_event_sink_satisfies_run_event_sink_bound() {
        let _sink: Arc<dyn RunEventSink> = Arc::new(NoopEventSink);
    }
}
