# runkon-runtimes

Portable agent runtime harness — spawn, poll, and cancel agents without depending on a fuller orchestrator's domain types.

[![crates.io](https://img.shields.io/crates/v/runkon-runtimes.svg)](https://crates.io/crates/runkon-runtimes)
[![docs.rs](https://docs.rs/runkon-runtimes/badge.svg)](https://docs.rs/runkon-runtimes)
[![CI](https://github.com/runkon/runkon/actions/workflows/ci.yml/badge.svg)](https://github.com/runkon/runkon/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)

## What it is

`runkon-runtimes` provides the lifecycle primitives needed to run an agent process and observe its progress:

- **`AgentRuntime` trait** — the common shape of "spawn, poll, cancel"
- **Built-in runtimes** — `cli` (generic CLI tool), `script` (shell-script step). Vendor-specific runtimes (Claude CLI, Gemini CLI, …) live in their respective vendor crates and are wired in via the `RuntimeResolver` trait.
- **`RunTracker`** — process liveness + last-event timestamp, with stall detection
- **`RunEventSink`** — streaming output capture during agent execution

Designed to compose with `runkon-flow` (a workflow engine consumes this crate via the `ActionExecutor` trait), but standalone usable for any "spawn-and-supervise" need.

## Install

```toml
[dependencies]
runkon-runtimes = "0.1.0-alpha"
```

Optional features:

- `utoipa` — OpenAPI schema derives on shared types

## License

Dual-licensed under either:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
