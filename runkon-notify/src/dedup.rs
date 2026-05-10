use crate::error::Result;

/// Deduplication store for fire-at-most-once notification semantics.
///
/// Implement this trait to back [`HookRunner::fire_with_dedup`] with any
/// storage engine (SQLite, Redis, Postgres, etc.). The in-memory
/// [`HashSetDedupStore`] is available under the `test-utils` feature.
///
/// Storage stays in the consumer; this crate only owns the contract.
pub trait DedupStore: Send + Sync {
    /// Returns `Ok(true)` on the first claim for `(entity_id, event_type)`.
    /// Returns `Ok(false)` on every subsequent call for the same pair.
    fn try_claim(&self, entity_id: &str, event_type: &str) -> Result<bool>;
}

/// In-memory dedup store for tests and examples.
///
/// Uses a `Mutex<HashSet>` to track claimed `(entity_id, event_type)` pairs.
/// Not persistent — cleared on drop.
#[cfg(any(test, feature = "test-utils"))]
pub struct HashSetDedupStore {
    inner: std::sync::Mutex<std::collections::HashSet<(String, String)>>,
}

#[cfg(any(test, feature = "test-utils"))]
impl Default for HashSetDedupStore {
    fn default() -> Self {
        Self {
            inner: std::sync::Mutex::new(std::collections::HashSet::new()),
        }
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl HashSetDedupStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl DedupStore for HashSetDedupStore {
    fn try_claim(&self, entity_id: &str, event_type: &str) -> Result<bool> {
        use crate::error::NotifyError;
        let mut set = self
            .inner
            .lock()
            .map_err(|e| NotifyError::Dispatch(format!("dedup: lock poisoned: {e}")))?;
        Ok(set.insert((entity_id.to_string(), event_type.to_string())))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn first_claim_returns_true() {
        let store = HashSetDedupStore::new();
        assert!(store.try_claim("entity-1", "event.completed").unwrap());
    }

    #[test]
    fn second_claim_same_key_returns_false() {
        let store = HashSetDedupStore::new();
        assert!(store.try_claim("entity-1", "event.completed").unwrap());
        assert!(!store.try_claim("entity-1", "event.completed").unwrap());
    }

    #[test]
    fn different_entity_id_returns_true() {
        let store = HashSetDedupStore::new();
        assert!(store.try_claim("entity-1", "event.completed").unwrap());
        assert!(store.try_claim("entity-2", "event.completed").unwrap());
    }

    #[test]
    fn different_event_type_returns_true() {
        let store = HashSetDedupStore::new();
        assert!(store.try_claim("entity-1", "event.completed").unwrap());
        assert!(store.try_claim("entity-1", "event.failed").unwrap());
    }

    #[test]
    fn concurrent_claims_produce_exactly_one_true() {
        let store = Arc::new(HashSetDedupStore::new());
        let n = 10;
        let handles: Vec<_> = (0..n)
            .map(|_| {
                let s = Arc::clone(&store);
                std::thread::spawn(move || s.try_claim("entity-1", "event.completed").unwrap())
            })
            .collect();
        let results: Vec<bool> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(results.iter().filter(|&&v| v).count(), 1);
    }
}
