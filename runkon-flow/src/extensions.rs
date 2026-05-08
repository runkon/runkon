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
}
