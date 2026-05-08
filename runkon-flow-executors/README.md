# runkon-flow-executors

Portable executor primitives for `runkon-flow` — event sinks, env providers, structured output parsing, and agent definition loading. No vendor coupling.

[![crates.io](https://img.shields.io/crates/v/runkon-flow-executors.svg)](https://crates.io/crates/runkon-flow-executors)
[![docs.rs](https://docs.rs/runkon-flow-executors/badge.svg)](https://docs.rs/runkon-flow-executors)
[![CI](https://github.com/runkon/runkon/actions/workflows/ci.yml/badge.svg)](https://github.com/runkon/runkon/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)

## What it is

A grab-bag of vendor-neutral building blocks downstream harnesses pick from when wiring a `runkon-flow` engine:

- **`ChannelEventSink`** — forwards engine events over an `mpsc::Sender<EngineEventData>`
- **`PathPrependingEnvProvider`** — `ScriptEnvProvider` that prepends directories to `PATH`
- **`output`** — structured-output parsers (`<<<FLOW_OUTPUT>>>` block extraction, JSON normalization, marker derivation)
- **`agent_loader`** — agent-definition loading from frontmatter `.md` files + prompt-instruction generation from `OutputSchema`

Vendor-specific executors (Anthropic API, OpenAI API, Codex CLI, etc.) live in sibling crates so consumers pull in only the integrations they use.

## Install

```toml
[dependencies]
runkon-flow-executors = "0.1.0-alpha"
```

Optional features:

- `test-utils` — re-exports `runkon-flow/test-utils` for downstream integration tests

## License

Dual-licensed under either:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
