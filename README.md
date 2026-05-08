# runkon

Portable workflow execution engine and supporting harness crates.

This workspace contains three crates:

- **`runkon-flow`** — Workflow DSL, traits, and engine core. Persistence-pluggable
  (in-memory + SQLite). Reference implementations of every public trait under
  `runkon-flow/examples/`.
- **`runkon-flow-executors`** — Portable executor implementations (event sinks,
  env providers, step utilities) that don't depend on any specific orchestration
  harness.
- **`runkon-runtimes`** — Portable agent runtime harness — spawn, poll, and cancel
  agents without depending on a fuller orchestrator's domain types.

Status: pre-1.0 alpha. APIs may change between minor versions until 0.1.0 stable.

## Build

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

## License

Dual-licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  http://opensource.org/licenses/MIT)

at your option.
