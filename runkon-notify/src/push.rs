use serde::{Deserialize, Serialize};

use crate::error::Result;

/// A web-push subscription record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Subscription {
    pub id: String,
    pub endpoint: String,
    pub p256dh: String,
    pub auth: String,
    pub created_at: String,
    pub updated_at: String,
}

/// Trait for storing and retrieving web-push subscriptions.
///
/// The in-process implementation (`InMemoryPushStore`) is provided for
/// tests and examples. Production implementations (e.g. SQLite-backed)
/// live in the consuming crate.
pub trait PushSubscriptionStore: Send + Sync {
    /// Return all stored subscriptions.
    fn list(&self) -> Result<Vec<Subscription>>;

    /// Insert or update a subscription identified by `endpoint`.
    ///
    /// If a subscription with the same `endpoint` already exists, its
    /// `p256dh`, `auth`, and `updated_at` fields are updated in place;
    /// `id` and `created_at` are preserved.
    fn upsert(&self, endpoint: &str, p256dh: &str, auth: &str) -> Result<Subscription>;

    /// Delete the subscription with the given `endpoint`.
    ///
    /// Returns `true` if a subscription was found and removed, `false` if
    /// no subscription with that endpoint existed.
    fn delete(&self, endpoint: &str) -> Result<bool>;
}

/// In-memory push subscription store for tests and examples.
#[cfg(any(test, feature = "test-utils"))]
pub struct InMemoryPushStore {
    inner: std::sync::Mutex<Vec<Subscription>>,
    next_id: std::sync::atomic::AtomicU64,
}

#[cfg(any(test, feature = "test-utils"))]
impl Default for InMemoryPushStore {
    fn default() -> Self {
        Self {
            inner: std::sync::Mutex::new(Vec::new()),
            next_id: std::sync::atomic::AtomicU64::new(1),
        }
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl InMemoryPushStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock_subs(&self) -> Result<std::sync::MutexGuard<'_, Vec<Subscription>>> {
        use crate::error::NotifyError;
        self.inner
            .lock()
            .map_err(|e| NotifyError::Subscription(format!("lock poisoned: {e}")))
    }
}

#[cfg(any(test, feature = "test-utils"))]
fn now_str() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}

#[cfg(any(test, feature = "test-utils"))]
impl PushSubscriptionStore for InMemoryPushStore {
    fn list(&self) -> Result<Vec<Subscription>> {
        Ok(self.inner.lock().unwrap().clone())
    }

    fn upsert(&self, endpoint: &str, p256dh: &str, auth: &str) -> Result<Subscription> {
        let mut subs = self.lock_subs()?;

        if let Some(existing) = subs.iter_mut().find(|s| s.endpoint == endpoint) {
            existing.p256dh = p256dh.to_string();
            existing.auth = auth.to_string();
            existing.updated_at = now_str();
            return Ok(existing.clone());
        }

        let id = format!(
            "sub-{}",
            self.next_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        let now = now_str();
        let sub = Subscription {
            id,
            endpoint: endpoint.to_string(),
            p256dh: p256dh.to_string(),
            auth: auth.to_string(),
            created_at: now.clone(),
            updated_at: now,
        };
        subs.push(sub.clone());
        Ok(sub)
    }

    fn delete(&self, endpoint: &str) -> Result<bool> {
        let mut subs = self.lock_subs()?;
        let len_before = subs.len();
        subs.retain(|s| s.endpoint != endpoint);
        Ok(subs.len() < len_before)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upsert_subscription() {
        let store = InMemoryPushStore::new();

        let sub = store
            .upsert("https://example.com/push", "p256dh_key", "auth_secret")
            .unwrap();

        assert_eq!(sub.endpoint, "https://example.com/push");
        assert_eq!(sub.p256dh, "p256dh_key");
        assert_eq!(sub.auth, "auth_secret");
    }

    #[test]
    fn test_upsert_subscription_update() {
        let store = InMemoryPushStore::new();

        let inserted = store
            .upsert("https://example.com/push", "p256dh_key", "auth_secret")
            .unwrap();

        let updated = store
            .upsert("https://example.com/push", "p256dh_new", "auth_new")
            .unwrap();

        assert_eq!(inserted.id, updated.id);
        assert_eq!(inserted.created_at, updated.created_at);
        assert_eq!(updated.p256dh, "p256dh_new");
        assert_eq!(updated.auth, "auth_new");

        let all = store.list().unwrap();
        assert_eq!(all.len(), 1);
    }

    #[test]
    fn test_delete_subscription() {
        let store = InMemoryPushStore::new();

        store
            .upsert("https://example.com/push", "p256dh_key", "auth_secret")
            .unwrap();

        let deleted = store.delete("https://example.com/push").unwrap();
        assert!(deleted);

        let subscriptions = store.list().unwrap();
        assert!(subscriptions.is_empty());
    }

    #[test]
    fn test_get_all_subscriptions() {
        let store = InMemoryPushStore::new();

        store
            .upsert("https://example1.com/push", "p256dh_key1", "auth_secret1")
            .unwrap();
        store
            .upsert("https://example2.com/push", "p256dh_key2", "auth_secret2")
            .unwrap();

        let subscriptions = store.list().unwrap();
        assert_eq!(subscriptions.len(), 2);
    }

    #[test]
    fn test_delete_nonexistent_returns_false() {
        let store = InMemoryPushStore::new();
        let deleted = store.delete("https://does-not-exist.com/push").unwrap();
        assert!(!deleted);
    }
}
