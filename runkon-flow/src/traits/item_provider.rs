use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use crate::engine_error::EngineError;
use crate::traits::run_context::RunContext;

/// An item returned by an `ItemProvider` during fan-out.
#[derive(Default)]
pub struct FanOutItem {
    pub item_type: String,
    pub item_id: String,
    pub item_ref: String,
    /// Arbitrary per-item key/value data injected into child workflow inputs as `item.<key>`.
    pub context: HashMap<String, String>,
}

/// Engine-populated per-call info for a foreach provider invocation.
pub struct ProviderInfo {
    pub step_id: String,
}

/// Trait for a foreach item source registered with the engine.
pub trait ItemProvider: Send + Sync {
    fn name(&self) -> &str;

    /// Parse a raw scope KV map into a provider-specific opaque value.
    ///
    /// Called at validation time and at execution time. The default impl
    /// rejects any non-`None` scope (provider does not support scope).
    fn parse_scope(
        &self,
        raw: Option<&HashMap<String, String>>,
    ) -> Result<Option<Box<dyn Any>>, String> {
        match raw {
            None => Ok(None),
            Some(_) => Err(format!("provider '{}' does not support scope", self.name())),
        }
    }

    /// Return warnings about the scope (e.g. "no scope; falling back to context").
    fn scope_warnings(&self, _raw: Option<&HashMap<String, String>>) -> Vec<String> {
        vec![]
    }

    /// Whether a `filter` block is required for this provider.
    fn requires_filter(&self) -> bool {
        false
    }

    /// Validate filter key/value pairs. Return `Err(message)` on invalid input.
    fn validate_filter(&self, _filter: &HashMap<String, String>) -> Result<(), String> {
        Ok(())
    }

    fn items(
        &self,
        ctx: &dyn RunContext,
        info: &ProviderInfo,
        scope: Option<&dyn Any>,
        filter: &HashMap<String, String>,
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

    pub fn iter(&self) -> impl Iterator<Item = &Arc<dyn ItemProvider>> {
        self.providers.values()
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
    use crate::traits::run_context::NoopRunContext;

    struct DummyProvider;
    impl ItemProvider for DummyProvider {
        fn name(&self) -> &str {
            "dummy"
        }
        fn items(
            &self,
            _ctx: &dyn RunContext,
            _info: &ProviderInfo,
            _scope: Option<&dyn Any>,
            _filter: &HashMap<String, String>,
        ) -> Result<Vec<FanOutItem>, EngineError> {
            Ok(vec![FanOutItem {
                item_type: "dummy".to_string(),
                item_id: "d1".to_string(),
                item_ref: "ref1".to_string(),
                context: HashMap::new(),
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

    #[test]
    fn test_dummy_provider_returns_items() {
        let ctx = NoopRunContext::default();
        let info = ProviderInfo {
            step_id: "s1".to_string(),
        };
        let provider = DummyProvider;
        let items = provider.items(&ctx, &info, None, &HashMap::new()).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].item_id, "d1");
    }
}
