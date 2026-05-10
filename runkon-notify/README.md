# runkon-notify

Domain-neutral notification dispatch primitives — generic event envelope, shell/HTTP hook execution, glob pattern matching, and web-push subscription store.

[![crates.io](https://img.shields.io/crates/v/runkon-notify.svg)](https://crates.io/crates/runkon-notify)
[![docs.rs](https://docs.rs/runkon-notify/badge.svg)](https://docs.rs/runkon-notify)
[![CI](https://github.com/runkon/runkon/actions/workflows/ci.yml/badge.svg)](https://github.com/runkon/runkon/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)

## What it is

`runkon-notify` provides the dispatch layer for notifying humans when something happens in a pipeline — without coupling to any specific orchestrator's domain types.

- **`Event`** — generic envelope (`kind`, `title`, `body`, `severity`, `fields`)
- **`HookConfig` / `HookRunner`** — glob-based pattern matching, fire-and-forget shell/HTTP hooks, and synchronous test-capture mode
- **`PushSubscriptionStore`** — trait for web-push subscription storage (CRUD)
- **`InMemoryPushStore`** — in-memory impl for tests and examples (enabled by the `test-utils` feature)
- **`DedupStore`** — trait for "fire at most once per `(entity_id, event_type)`" deduplication; storage stays in the consumer
- **`HashSetDedupStore`** — in-memory dedup impl for tests and examples (enabled by the `test-utils` feature)

Each harness maps its own domain events to `Event` before calling `HookRunner::fire`. Hook scripts and HTTP endpoints always receive the generic envelope.

## Install

```toml
[dependencies]
runkon-notify = "0.2.0-alpha"
```

Optional features:

- `test-utils` — enables `InMemoryPushStore` and `HashSetDedupStore` for use in tests and examples

## Examples

```sh
# Fire a shell hook and assert it saw the event kind
cargo run --example hook_fires -p runkon-notify

# Demonstrate subscription CRUD with InMemoryPushStore
cargo run --example web_push -p runkon-notify --features test-utils
```

### Minimal usage

```rust
use std::collections::HashMap;
use runkon_notify::{Event, Severity, HookConfig, HookRunner};

let event = Event {
    kind: "stage.completed".into(),
    title: "Stage finished".into(),
    body: "All steps passed.".into(),
    severity: Severity::Info,
    fields: HashMap::new(),
};

let hooks = vec![HookConfig {
    on: "stage.*".into(),
    run: Some("notify-send \"$RUNKON_NOTIFY_TITLE\"".into()),
    ..Default::default()
}];

HookRunner::new(&hooks).fire(&event);
```

### Deduplication (`DedupStore`)

Use `fire_with_dedup` to skip duplicate `(entity_id, event_type)` pairs — useful when the same event may be produced multiple times (e.g. retries, fan-out pipelines):

```rust
use std::sync::Arc;
use runkon_notify::{DedupStore, HookConfig, HookRunner, Event, Severity};
use std::collections::HashMap;

// In production: implement DedupStore against SQLite, Redis, Postgres, etc.
// In tests: use HashSetDedupStore from the `test-utils` feature.

// runner.fire_with_dedup(&event, "run-42", "stage.completed");
// ^ fires on first call; subsequent calls with the same key are no-ops.
```

When no store is attached (`HookRunner::new` without `with_dedup_store`), `fire_with_dedup` behaves identically to `fire` — no dedup guard, every call fires.

## Filtering

### Declarative field predicates

`HookConfig` supports five optional `when_field_*` predicates that match against `Event::fields`. All predicates AND together with the `on:` glob — every constraint must pass for the hook to fire.

| Field | Semantics |
|---|---|
| `when_field_in` | Field value must be one of the listed strings |
| `when_field_eq` | Field value must equal the given string exactly |
| `when_field_glob` | Field value must match the given glob (`*`, `prefix.*`, `prefix/*`) |
| `when_field_gte` | Field value, parsed as `f64`, must be ≥ the threshold |
| `when_field_lte` | Field value, parsed as `f64`, must be ≤ the threshold |

**Missing-field semantics:** if a constrained field is absent from `Event::fields`, the hook does **not** fire. An unconstrained hook (no `when_field_*` set) fires for everything that matches `on:`.

Example — fire only on `workflow_run.completed` events for `main`, in the `runkon/runkon` repo, when the run took ≥ 60 seconds:

```toml
[[hooks]]
on = "workflow_run.completed"
url = "https://example.com/notify"

[hooks.when_field_eq]
branch = "main"
repo   = "runkon/runkon"

[hooks.when_field_gte]
duration_ms = 60000
```

Conductor (or any consumer) maps its domain fields — `branch`, `repo`, `step`, etc. — into `Event::fields` before calling `fire`. The runner does the filtering; no pre-`fire()` resolver needed.

### Custom filter escape hatch

For predicates that don't fit the declarative form, implement the `HookFilter` trait:

```rust
use std::sync::Arc;
use runkon_notify::{HookConfig, HookFilter, HookRunner, Event, Severity};
use std::collections::HashMap;

/// Only fire hooks between 08:00 and 18:00 UTC (business hours).
struct BusinessHoursOnly;

impl HookFilter for BusinessHoursOnly {
    fn allow(&self, _hook: &HookConfig, _event: &Event) -> bool {
        // Replace with a timezone-aware crate (e.g. `chrono`) in production.
        let hour = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| (d.as_secs() / 3600) % 24)
            .unwrap_or(0);
        hour >= 8 && hour < 18
    }
}

# let hooks: Vec<HookConfig> = vec![];
let runner = HookRunner::new_with_filter(&hooks, Arc::new(BusinessHoursOnly));
// or: HookRunner::new(&hooks).with_filter(Arc::new(BusinessHoursOnly))
```

The custom filter is ANDed with `on:` and all `when_field_*` predicates — all three must allow for the hook to fire. Implementations must be `Send + Sync` because hooks execute in spawned threads.

## Hook-script protocol

Shell hooks are invoked via `sh -c <command>` with the following environment variables injected. All keys are `RUNKON_NOTIFY_*`-prefixed.

### Common fields (all events)

| Variable | Value |
|---|---|
| `RUNKON_NOTIFY_KIND` | Dotted event kind, e.g. `"stage.completed"` |
| `RUNKON_NOTIFY_TITLE` | Human-readable title |
| `RUNKON_NOTIFY_BODY` | Longer description |
| `RUNKON_NOTIFY_SEVERITY` | `"info"`, `"warning"`, `"error"`, or `"critical"` |

### Custom fields

Each entry in `Event::fields` is exposed as `RUNKON_NOTIFY_FIELD_<UPPER_KEY>`. For example, a field `("run_id", "abc123")` becomes `RUNKON_NOTIFY_FIELD_RUN_ID=abc123`.

### HTTP hook payload

HTTP hooks receive `Event::to_json()` as the POST body — the JSON serialization of the envelope with `kind`, `title`, `body`, `severity`, and `fields`. Header values starting with `$` are resolved from the host process environment (e.g. `Authorization: $SLACK_TOKEN`).

### `on` pattern matching

The `on` field accepts a comma-separated list of glob patterns:

| Pattern | Matches |
|---|---|
| `*` | All events |
| `stage.*` | Any `stage.` event |
| `permit.approved` | Exact event kind |
| `stage.*:root` | Fires only when `event.fields["is_root"] == "true"` |
| `feature/*` | Branch-style glob |

#### `:root` enforcement

When a pattern includes the `:root` suffix (e.g. `workflow_run.completed:root`), the runner fires the hook only if the event carries `fields["is_root"] == "true"` (case-sensitive, exact string match). Any other value — including `"false"`, missing, or empty — is treated as "not root" and the hook does not fire.

Consumers should set `event.fields["is_root"] = "true"` for any event type where root-vs-sub distinction matters. Events without `is_root` are treated as not root — the runner fails closed, matching the missing-field semantics of `when_field_*` predicates.

**Migration note:** Prior to this change, `:root` was silently a no-op — hooks fired regardless of whether the event was root. If you use `:root` patterns, set `event.fields["is_root"] = "true"` for root events to preserve the intended behavior.

### Security model

Shell hooks run user-configured commands via `sh -c`. Event data is passed safely through environment variables, but the hook command itself runs with full shell privileges. Treat hook configuration files as trusted input.

## License

Dual-licensed under either:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
