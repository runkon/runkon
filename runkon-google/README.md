# runkon-google

Google Gemini API client and agent executor for `runkon-flow`. Supports both direct API calls and generic subprocess runtimes.

[![crates.io](https://img.shields.io/crates/v/runkon-google.svg)](https://crates.io/crates/runkon-google)
[![docs.rs](https://docs.rs/runkon-google/badge.svg)](https://docs.rs/runkon-google)
[![CI](https://github.com/runkon/runkon/actions/workflows/ci.yml/badge.svg)](https://github.com/runkon/runkon/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)

## What it is

Two ways to invoke Gemini inside a `runkon-flow` workflow:

- **`GeminiApiCallExecutor`** — direct calls to `https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent`. Uses JSON mode (`responseMimeType: application/json`) with a `responseSchema` derived from the step's `OutputSchema`. Works with any Gemini API key.
- **`GeminiAgentExecutor`** — dispatches to `GeminiApiCallExecutor` when a schema and API key are both present (fast path); otherwise falls back to spawning a subprocess via the injected `RuntimeResolver` (useful with `cli` or `script` runtimes).

Auth is via the `x-goog-api-key` header. Pass `GEMINI_API_KEY` (or any env var you prefer) to the executor at construction; no env-var lookup happens inside the library.

## Install

```toml
[dependencies]
runkon-google = "0.6.0"
```

Optional features:

- `test-utils` — re-exports `runkon-flow/test-utils` for downstream integration tests

## Standalone example

[`examples/standalone_flow.rs`](examples/standalone_flow.rs) wires both executors into a minimal `FlowEngine` to demonstrate composition with `runkon-flow` and `runkon-flow-executors`. Compilation alone proves the wiring; no real workflow is executed.

```bash
cargo run --example standalone_flow --features test-utils
```

## License

Dual-licensed under either:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
