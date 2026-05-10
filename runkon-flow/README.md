# runkon-flow

Portable workflow execution engine — DSL, traits, and in-memory reference implementations.

[![crates.io](https://img.shields.io/crates/v/runkon-flow.svg)](https://crates.io/crates/runkon-flow)
[![docs.rs](https://docs.rs/runkon-flow/badge.svg)](https://docs.rs/runkon-flow)
[![CI](https://github.com/runkon/runkon/actions/workflows/ci.yml/badge.svg)](https://github.com/runkon/runkon/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)

## What it is

`runkon-flow` is the engine core for the runkon workflow ecosystem:

- **A workflow DSL** — `.wf` text files describing scripted, branching, and parallel agentic work
- **Trait-based extension points** — `ActionExecutor`, `ItemProvider`, `GateResolver`, `WorkflowResolver`, `ScriptEnvProvider`, `ChildWorkflowRunner`, `EventSink`, `WorkflowPersistence`
- **Reference implementations** — in-memory persistence, in-memory workflow resolver, plus runnable per-trait examples under `examples/`

The crate is harness-neutral: no Anthropic-specific, GitHub-specific, or otherwise vendor-coupled code. Vendor integrations live in sibling crates (`runkon-anthropic`, etc.).

## Install

```toml
[dependencies]
runkon-flow = "0.1.0-alpha"
```

Optional features:

- `sqlite` — SQLite-backed `WorkflowPersistence`
- `diagnostics` — per-run engine log + panic capture (pulls in `tracing-subscriber`)
- `test-utils` — additional test fixtures (`test_helpers`, `CountingPersistence`) for downstream integration tests. The in-memory persistence backend (`persistence_memory::InMemoryWorkflowPersistence`) is available without this feature.
- `utoipa` — OpenAPI schema derives on shared types

## Minimal example

See [`examples/full_engine_minimal.rs`](examples/full_engine_minimal.rs) for an end-to-end engine wiring with an `EchoExecutor`. Single-trait reference impls live alongside it (one file per trait).

```bash
cargo run --example full_engine_minimal --features test-utils
```

## License

Dual-licensed under either:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
