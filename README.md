# runkon

[![CI](https://github.com/runkon/runkon/actions/workflows/ci.yml/badge.svg)](https://github.com/runkon/runkon/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)

Portable workflow execution engine and supporting harness crates for building agentic pipelines. Engine and harness primitives are vendor-neutral; vendor integrations live in dedicated sibling crates so consumers pull in only what they use.

## Crates

| Crate | Purpose |
|---|---|
| [`runkon-flow`](runkon-flow/) [![crates.io](https://img.shields.io/crates/v/runkon-flow.svg)](https://crates.io/crates/runkon-flow) [![docs.rs](https://docs.rs/runkon-flow/badge.svg)](https://docs.rs/runkon-flow) | Workflow DSL, traits, engine core. In-memory + SQLite persistence. Reference impls per trait. |
| [`runkon-runtimes`](runkon-runtimes/) [![crates.io](https://img.shields.io/crates/v/runkon-runtimes.svg)](https://crates.io/crates/runkon-runtimes) [![docs.rs](https://docs.rs/runkon-runtimes/badge.svg)](https://docs.rs/runkon-runtimes) | Spawn-poll-cancel agent runtime harness. Built-in runtimes: `cli`, `script`. Vendor runtimes (Claude, Gemini, …) live in their respective vendor crates. |
| [`runkon-flow-executors`](runkon-flow-executors/) [![crates.io](https://img.shields.io/crates/v/runkon-flow-executors.svg)](https://crates.io/crates/runkon-flow-executors) [![docs.rs](https://docs.rs/runkon-flow-executors/badge.svg)](https://docs.rs/runkon-flow-executors) | Vendor-neutral executor primitives (event sinks, env providers, output parsing, agent loader). |
| [`runkon-anthropic`](runkon-anthropic/) [![crates.io](https://img.shields.io/crates/v/runkon-anthropic.svg)](https://crates.io/crates/runkon-anthropic) [![docs.rs](https://docs.rs/runkon-anthropic/badge.svg)](https://docs.rs/runkon-anthropic) | Anthropic API client + Claude agent executor (API and CLI subprocess modes). |
| [`runkon-google`](runkon-google/) [![crates.io](https://img.shields.io/crates/v/runkon-google.svg)](https://crates.io/crates/runkon-google) [![docs.rs](https://docs.rs/runkon-google/badge.svg)](https://docs.rs/runkon-google) | Google Gemini API client + agent executor (API and generic subprocess modes). |

Future vendor integrations (`runkon-openai`, `runkon-codex`, etc.) follow the same shape — depend on `runkon-flow`/`runkon-flow-executors`/`runkon-runtimes`, export vendor-specific executors.

## Recipes

- **Gemini CLI** — see [docs/recipes/gemini-cli.md](docs/recipes/gemini-cli.md) for a ready-to-use `CliRuntime` binding and the Phase 2 `GeminiRuntime` (stream-json, per-turn telemetry).

Status: pre-1.0 alpha. APIs may change between minor versions until 0.1.0 stable.

## Build

```bash
cargo build --workspace
cargo test --workspace --features runkon-flow/test-utils
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all --check
```

## Examples

Each runnable trait reference impl lives under `runkon-flow/examples/`. The end-to-end engine wiring is `full_engine_minimal.rs`:

```bash
cargo run --example full_engine_minimal --features test-utils
```

Anthropic-specific composition lives in `runkon-anthropic/examples/standalone_flow.rs`.

Google Gemini composition lives in `runkon-google/examples/standalone_flow.rs`.

## License

Dual-licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
