use std::collections::HashMap;
use std::sync::Arc;

use crate::dsl::ApprovalMode;
use crate::engine_error::EngineError;

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

/// Outcome of a single poll tick from a `GateResolver`.
#[derive(Debug)]
pub enum GatePoll {
    Approved(Option<String>),
    Rejected(String),
    Pending,
}

/// All gate configuration passed to `GateResolver::poll`.
#[allow(dead_code)] // fields are available for resolver use; not all are consumed in Phase 1
pub struct GateParams {
    pub gate_name: String,
    pub prompt: Option<String>,
    pub min_approvals: u32,
    pub approval_mode: ApprovalMode,
    /// Resolved options list (StepRef already expanded by the dispatcher).
    pub options: Vec<String>,
    pub timeout_secs: u64,
    pub bot_name: Option<String>,
    pub step_id: String,
}

/// Transient context passed to each `GateResolver::poll` call.
#[allow(dead_code)]
pub struct GateContext {
    pub working_dir: String,
    pub default_bot_name: Option<String>,
}

// ---------------------------------------------------------------------------
// GateResolver trait
// ---------------------------------------------------------------------------

pub trait GateResolver: Send + Sync {
    fn gate_type(&self) -> &str;
    fn poll(
        &self,
        run_id: &str,
        params: &GateParams,
        ctx: &GateContext,
    ) -> Result<GatePoll, EngineError>;
}

// ---------------------------------------------------------------------------
// GateResolverRegistry
// ---------------------------------------------------------------------------

/// Registry mapping gate type strings to `GateResolver` implementations.
///
/// Mirrors the pattern of `ItemProviderRegistry`. Used by `FlowEngine::validate()`
/// to check that every `gate <type>` node (excluding `QualityGate`) has a
/// registered resolver before execution starts.
pub struct GateResolverRegistry {
    resolvers: HashMap<String, Arc<dyn GateResolver>>,
}

impl GateResolverRegistry {
    pub fn new() -> Self {
        Self {
            resolvers: HashMap::new(),
        }
    }

    /// Register a resolver. The `gate_type()` string is used as the lookup key.
    pub fn register<R: GateResolver + 'static>(&mut self, resolver: R) {
        let gate_type = resolver.gate_type().to_string();
        self.resolvers.insert(gate_type, Arc::new(resolver));
    }

    /// Returns `true` if a resolver is registered for `gate_type`.
    pub fn has_type(&self, gate_type: &str) -> bool {
        self.resolvers.contains_key(gate_type)
    }

    /// Returns all registered gate type strings, sorted alphabetically.
    pub fn registered_types(&self) -> Vec<String> {
        let mut types: Vec<String> = self.resolvers.keys().cloned().collect();
        types.sort();
        types
    }
}

impl Default for GateResolverRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockResolver {
        gate_type: &'static str,
    }

    impl GateResolver for MockResolver {
        fn gate_type(&self) -> &str {
            self.gate_type
        }
        fn poll(
            &self,
            _run_id: &str,
            _params: &GateParams,
            _ctx: &GateContext,
        ) -> Result<GatePoll, EngineError> {
            Ok(GatePoll::Approved(None))
        }
    }

    #[test]
    fn register_and_has_type_roundtrip() {
        let mut registry = GateResolverRegistry::new();
        registry.register(MockResolver {
            gate_type: "human_approval",
        });
        assert!(registry.has_type("human_approval"));
        assert!(!registry.has_type("pr_checks"));
    }

    #[test]
    fn missing_type_returns_false() {
        let registry = GateResolverRegistry::new();
        assert!(!registry.has_type("nonexistent"));
    }

    #[test]
    fn registered_types_is_sorted() {
        let mut registry = GateResolverRegistry::new();
        registry.register(MockResolver {
            gate_type: "pr_checks",
        });
        registry.register(MockResolver {
            gate_type: "human_approval",
        });
        let types = registry.registered_types();
        assert_eq!(types, vec!["human_approval", "pr_checks"]);
    }

    #[test]
    fn default_registry_is_empty() {
        let registry = GateResolverRegistry::default();
        assert!(registry.registered_types().is_empty());
    }
}
