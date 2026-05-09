//! Demonstrates the PushSubscriptionStore trait with InMemoryPushStore.
//!
//! Run with: `cargo run --example web_push -p runkon-notify --features test-utils`

use runkon_notify::InMemoryPushStore;
use runkon_notify::PushSubscriptionStore;

fn main() {
    let store = InMemoryPushStore::new();

    // Insert a subscription.
    let sub = store
        .upsert("https://example.com/push", "p256dh_key", "auth_secret")
        .expect("upsert should succeed");

    assert_eq!(sub.endpoint, "https://example.com/push");
    assert_eq!(sub.p256dh, "p256dh_key");

    let all = store.list().expect("list should succeed");
    assert_eq!(all.len(), 1, "store should contain one subscription");

    println!("Inserted subscription: {}", sub.endpoint);

    // Upsert updates key material, preserves id and created_at.
    let updated = store
        .upsert("https://example.com/push", "p256dh_new", "auth_new")
        .expect("update upsert should succeed");

    assert_eq!(sub.id, updated.id, "id must not change on upsert update");
    assert_eq!(
        sub.created_at, updated.created_at,
        "created_at must not change on upsert update"
    );
    assert_eq!(updated.p256dh, "p256dh_new");

    // Delete the subscription.
    let deleted = store
        .delete("https://example.com/push")
        .expect("delete should succeed");
    assert!(deleted, "delete must return true when the endpoint existed");

    let remaining = store.list().expect("list after delete should succeed");
    assert!(remaining.is_empty(), "store must be empty after delete");

    println!("web_push example passed — subscription CRUD round-trip verified");
}
