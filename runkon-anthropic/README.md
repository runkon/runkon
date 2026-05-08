# runkon-anthropic

Anthropic API client and Claude agent executor for `runkon-flow`. Supports both direct API calls and headless Claude CLI subprocesses.

[![crates.io](https://img.shields.io/crates/v/runkon-anthropic.svg)](https://crates.io/crates/runkon-anthropic)
[![docs.rs](https://docs.rs/runkon-anthropic/badge.svg)](https://docs.rs/runkon-anthropic)
[![CI](https://github.com/runkon/runkon/actions/workflows/ci.yml/badge.svg)](https://github.com/runkon/runkon/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)

## What it is

Two ways to invoke Claude inside a `runkon-flow` workflow, both implementing `runkon_flow::traits::action_executor::ActionExecutor`:

- **`ApiCallExecutor`** — direct calls to `https://api.anthropic.com/v1/messages`. Tool-use response shape with structured-output schema enforcement. Works for any Anthropic-API-compatible model.
- **`ClaudeAgentExecutor`** — drives the headless Claude CLI as a subprocess (via `runkon-runtimes`). Useful when you want the agent to have tools, file-system access, etc.

## Install

```toml
[dependencies]
runkon-anthropic = "0.1.0-alpha"
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
