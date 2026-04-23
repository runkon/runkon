use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::cancellation_reason::CancellationReason;
use crate::engine_error::EngineError;

#[allow(dead_code)]
struct CancellationInner {
    cancelled: AtomicBool,
    reason: Mutex<Option<CancellationReason>>,
    parent: Option<Arc<CancellationInner>>,
}

impl CancellationInner {
    fn find_in_chain<T>(&self, f: impl Fn(&CancellationInner) -> Option<T>) -> Option<T> {
        let mut node = self;
        loop {
            if let Some(val) = f(node) {
                return Some(val);
            }
            match &node.parent {
                Some(p) => node = p,
                None => return None,
            }
        }
    }
}

#[derive(Clone)]
pub struct CancellationToken(Arc<CancellationInner>);

impl CancellationToken {
    pub fn new() -> Self {
        Self(Arc::new(CancellationInner {
            cancelled: AtomicBool::new(false),
            reason: Mutex::new(None),
            parent: None,
        }))
    }

    pub fn child(&self) -> Self {
        Self(Arc::new(CancellationInner {
            cancelled: AtomicBool::new(false),
            reason: Mutex::new(None),
            parent: Some(self.0.clone()),
        }))
    }

    pub fn cancel(&self, reason: CancellationReason) {
        if self
            .0
            .cancelled
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            *self.0.reason.lock().unwrap_or_else(|e| e.into_inner()) = Some(reason);
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.0
            .find_in_chain(|n| n.cancelled.load(Ordering::SeqCst).then_some(()))
            .is_some()
    }

    pub fn reason(&self) -> Option<CancellationReason> {
        self.0
            .find_in_chain(|n| n.reason.lock().unwrap_or_else(|e| e.into_inner()).clone())
    }

    pub fn error_if_cancelled(&self) -> Result<(), EngineError> {
        if self.is_cancelled() {
            Err(EngineError::Cancelled(
                self.reason().unwrap_or(CancellationReason::ParentCancelled),
            ))
        } else {
            Ok(())
        }
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
pub(crate) struct ExecutionContext<'a> {
    pub run: &'a dyn crate::traits::run_context::RunContext,
    pub cancellation: &'a CancellationToken,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_token_is_not_cancelled() {
        let tok = CancellationToken::new();
        assert!(!tok.is_cancelled());
        assert!(tok.reason().is_none());
        assert!(tok.error_if_cancelled().is_ok());
    }

    #[test]
    fn cancel_sets_cancelled_flag() {
        let tok = CancellationToken::new();
        tok.cancel(CancellationReason::UserRequested(None));
        assert!(tok.is_cancelled());
    }

    #[test]
    fn reason_is_preserved_after_cancel() {
        let tok = CancellationToken::new();
        tok.cancel(CancellationReason::Timeout);
        assert!(matches!(tok.reason(), Some(CancellationReason::Timeout)));
    }

    #[test]
    fn first_cancel_wins_subsequent_ignored() {
        let tok = CancellationToken::new();
        tok.cancel(CancellationReason::Timeout);
        tok.cancel(CancellationReason::UserRequested(None));
        assert!(matches!(tok.reason(), Some(CancellationReason::Timeout)));
    }

    #[test]
    fn error_if_cancelled_returns_err_with_reason() {
        let tok = CancellationToken::new();
        tok.cancel(CancellationReason::FailFast);
        let err = tok.error_if_cancelled().unwrap_err();
        assert!(matches!(
            err,
            EngineError::Cancelled(CancellationReason::FailFast)
        ));
    }

    #[test]
    fn parent_cancel_propagates_to_child() {
        let parent = CancellationToken::new();
        let child = parent.child();
        assert!(!child.is_cancelled());
        parent.cancel(CancellationReason::UserRequested(None));
        assert!(child.is_cancelled());
    }

    #[test]
    fn parent_cancel_propagates_reason_to_child() {
        let parent = CancellationToken::new();
        let child = parent.child();
        parent.cancel(CancellationReason::EngineShutdown);
        assert!(matches!(
            child.reason(),
            Some(CancellationReason::EngineShutdown)
        ));
    }

    #[test]
    fn child_cancel_does_not_affect_parent() {
        let parent = CancellationToken::new();
        let child = parent.child();
        child.cancel(CancellationReason::FailFast);
        assert!(child.is_cancelled());
        assert!(!parent.is_cancelled());
    }

    #[test]
    fn sibling_cancel_does_not_affect_other_sibling() {
        let parent = CancellationToken::new();
        let sibling_a = parent.child();
        let sibling_b = parent.child();
        sibling_a.cancel(CancellationReason::FailFast);
        assert!(sibling_a.is_cancelled());
        assert!(!sibling_b.is_cancelled(), "sibling_b must not be affected");
    }

    #[test]
    fn grandchild_sees_grandparent_cancel() {
        let grandparent = CancellationToken::new();
        let parent = grandparent.child();
        let child = parent.child();
        grandparent.cancel(CancellationReason::Timeout);
        assert!(child.is_cancelled());
        assert!(matches!(child.reason(), Some(CancellationReason::Timeout)));
    }

    #[test]
    fn clone_shares_same_cancellation_state() {
        let tok = CancellationToken::new();
        let clone = tok.clone();
        tok.cancel(CancellationReason::UserRequested(None));
        assert!(clone.is_cancelled());
    }
}
