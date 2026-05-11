use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

/// Type-erased extension map for passing executor-specific data through `ActionParams`
/// without polluting the generic API surface.
///
/// Values are stored as `Arc<dyn Any + Send + Sync>` so cloning is cheap (Arc ref-count bump)
/// and the map can cross thread boundaries (required by `parallel.rs`'s per-thread copies).
#[derive(Default, Clone)]
pub struct Extensions {
    map: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl std::fmt::Debug for Extensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extensions")
            .field("len", &self.map.len())
            .finish()
    }
}

impl Extensions {
    /// Insert a value of type `T`, replacing any previously inserted value of the same type.
    pub fn insert<T: Any + Send + Sync + 'static>(&mut self, value: T) {
        self.map.insert(TypeId::of::<T>(), Arc::new(value));
    }

    /// Retrieve a cloned `Arc<T>` for a value of type `T`, if one was inserted.
    pub fn get<T: Any + Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        self.map
            .get(&TypeId::of::<T>())
            .and_then(|arc| arc.clone().downcast::<T>().ok())
    }
}

/// Claude-specific per-step parameters, passed through `ActionParams.extensions`.
/// Same convention as `OutputSchema`: executor-specific types live in extensions,
/// not on the shared `ActionParams` surface.
pub struct ClaudeActionParams {
    pub max_turns: Option<u32>,
}

/// LLM-runtime rollup metrics, stored in the `Extensions` map on both
/// `WorkflowRun` and `WorkflowResult`. Only present when at least one
/// LLM-backed step ran and reported metrics via `metadata_keys`.
///
/// `model` is included here rather than on the harness-neutral `WorkflowRun`
/// because every LLM has a model identifier while non-LLM executors do not.
/// (Issue #2987 may revisit if a non-LLM "model" concept emerges.)
pub struct LlmRunMetrics {
    pub total_input_tokens: Option<i64>,
    pub total_output_tokens: Option<i64>,
    pub total_cache_read_input_tokens: Option<i64>,
    pub total_cache_creation_input_tokens: Option<i64>,
    pub total_turns: Option<i64>,
    pub total_cost_usd: Option<f64>,
    pub model: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get_returns_value() {
        let mut ext = Extensions::default();
        ext.insert(42u32);
        let v = ext.get::<u32>().expect("should find u32");
        assert_eq!(*v, 42u32);
    }

    #[test]
    fn get_missing_type_returns_none() {
        let ext = Extensions::default();
        assert!(ext.get::<u32>().is_none());
    }

    #[test]
    fn insert_replaces_previous_value() {
        let mut ext = Extensions::default();
        ext.insert(1u32);
        ext.insert(2u32);
        let v = ext.get::<u32>().expect("should find u32");
        assert_eq!(*v, 2u32);
    }

    #[test]
    fn different_types_are_stored_independently() {
        let mut ext = Extensions::default();
        ext.insert(10u32);
        ext.insert("hello");
        assert_eq!(*ext.get::<u32>().unwrap(), 10u32);
        assert_eq!(*ext.get::<&str>().unwrap(), "hello");
    }

    #[test]
    fn clone_shares_arc_not_data() {
        let mut ext = Extensions::default();
        ext.insert(String::from("shared"));
        let cloned = ext.clone();
        let a = ext.get::<String>().unwrap();
        let b = cloned.get::<String>().unwrap();
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn claude_action_params_round_trips() {
        let mut ext = Extensions::default();
        ext.insert(ClaudeActionParams {
            max_turns: Some(50),
        });
        let v = ext
            .get::<ClaudeActionParams>()
            .expect("should find ClaudeActionParams");
        assert_eq!(v.max_turns, Some(50));
    }

    #[test]
    fn llm_run_metrics_round_trips() {
        let mut ext = Extensions::default();
        ext.insert(LlmRunMetrics {
            total_input_tokens: Some(100),
            total_output_tokens: Some(200),
            total_cache_read_input_tokens: Some(50),
            total_cache_creation_input_tokens: Some(25),
            total_turns: Some(3),
            total_cost_usd: Some(0.05),
            model: Some("claude-opus-4".to_string()),
        });
        let v = ext
            .get::<LlmRunMetrics>()
            .expect("should find LlmRunMetrics");
        assert_eq!(v.total_input_tokens, Some(100));
        assert_eq!(v.total_output_tokens, Some(200));
        assert_eq!(v.total_cache_read_input_tokens, Some(50));
        assert_eq!(v.total_cache_creation_input_tokens, Some(25));
        assert_eq!(v.total_turns, Some(3));
        assert_eq!(v.total_cost_usd, Some(0.05));
        assert_eq!(v.model.as_deref(), Some("claude-opus-4"));
    }
}
