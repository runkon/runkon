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

Each harness maps its own domain events to `Event` before calling `HookRunner::fire`. Hook scripts and HTTP endpoints always receive the generic envelope.

## Install

```toml
[dependencies]
runkon-notify = "0.2.0-alpha"
```

Optional features:

- `test-utils` — enables `InMemoryPushStore` for use in tests and examples

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
| `stage.*:root` | `:root` suffix recognized (no-op in generic context) |
| `feature/*` | Branch-style glob |

### Security model

Shell hooks run user-configured commands via `sh -c`. Event data is passed safely through environment variables, but the hook command itself runs with full shell privileges. Treat hook configuration files as trusted input.

## License

Dual-licensed under either:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
