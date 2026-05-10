//! Domain-neutral notification dispatch primitives.
//!
//! `runkon-notify` provides a generic event envelope and hook execution
//! machinery that can be used by any harness regardless of domain. Callers
//! map their own event types to [`Event`] before dispatch; hook scripts and
//! HTTP endpoints receive the generic envelope.
//!
//! # Quick start
//!
//! ```no_run
//! use std::collections::HashMap;
//! use runkon_notify::{Event, Severity, HookConfig, HookRunner};
//!
//! let event = Event {
//!     kind: "stage.completed".into(),
//!     title: "Stage finished".into(),
//!     body: "All steps passed.".into(),
//!     severity: Severity::Info,
//!     fields: HashMap::new(),
//! };
//!
//! let hooks = vec![HookConfig {
//!     on: "stage.*".into(),
//!     run: Some("notify-send \"$RUNKON_NOTIFY_TITLE\"".into()),
//!     ..Default::default()
//! }];
//!
//! HookRunner::new(&hooks).fire(&event);
//! ```

pub mod dedup;
pub mod error;
pub mod event;
pub mod hooks;
pub mod push;

pub use dedup::DedupStore;
pub use error::{NotifyError, Result};
pub use event::{Event, Severity};
pub use hooks::{HookConfig, HookFilter, HookRunner};
pub use push::{PushSubscriptionStore, Subscription};

#[cfg(any(test, feature = "test-utils"))]
pub use dedup::HashSetDedupStore;
#[cfg(any(test, feature = "test-utils"))]
pub use push::InMemoryPushStore;
