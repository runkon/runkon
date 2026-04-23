use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::dsl::ForeachScope;
use crate::engine_error::EngineError;

/// An item returned by an `ItemProvider` during fan-out.
pub struct FanOutItem {
    pub item_type: String,
    pub item_id: String,
    pub item_ref: String,
}

/// Context passed to providers during item collection.
pub struct ProviderContext {
    pub repo_id: Option<String>,
    pub worktree_id: Option<String>,
}

/// Trait for a foreach item source registered with the engine.
pub trait ItemProvider: Send + Sync {
    fn name(&self) -> &str;

    fn items(
        &self,
        ctx: &ProviderContext,
        scope: Option<&ForeachScope>,
        filter: &HashMap<String, String>,
        existing_set: &HashSet<String>,
    ) -> Result<Vec<FanOutItem>, EngineError>;

    fn dependencies(&self, step_id: &str) -> Result<Vec<(String, String)>, EngineError> {
        let _ = step_id;
        Ok(vec![])
    }

    fn supports_ordered(&self) -> bool {
        false
    }
}

/// Registry mapping provider names to implementations.
pub struct ItemProviderRegistry {
    providers: HashMap<String, Arc<dyn ItemProvider>>,
}

impl ItemProviderRegistry {
    pub fn new() -> Self {
        Self {
            providers: HashMap::new(),
        }
    }

    pub fn register<P: ItemProvider + 'static>(&mut self, provider: P) {
        let name = provider.name().to_string();
        self.providers.insert(name, Arc::new(provider));
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn ItemProvider>> {
        self.providers.get(name).cloned()
    }
}

impl Default for ItemProviderRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyProvider;
    impl ItemProvider for DummyProvider {
        fn name(&self) -> &str {
            "dummy"
        }
        fn items(
            &self,
            _ctx: &ProviderContext,
            _scope: Option<&ForeachScope>,
            _filter: &HashMap<String, String>,
            _existing_set: &HashSet<String>,
        ) -> Result<Vec<FanOutItem>, EngineError> {
            Ok(vec![FanOutItem {
                item_type: "dummy".to_string(),
                item_id: "d1".to_string(),
                item_ref: "ref1".to_string(),
            }])
        }
    }

    #[test]
    fn test_registry_register_and_get() {
        let mut registry = ItemProviderRegistry::new();
        registry.register(DummyProvider);
        let p = registry.get("dummy");
        assert!(
            p.is_some(),
            "registered provider should be retrievable by name"
        );
        let missing = registry.get("nonexistent");
        assert!(
            missing.is_none(),
            "unregistered provider should return None"
        );
    }

    #[test]
    fn test_registry_get_returns_same_name() {
        let mut registry = ItemProviderRegistry::new();
        registry.register(DummyProvider);
        let p = registry.get("dummy").unwrap();
        assert_eq!(p.name(), "dummy");
    }
}
