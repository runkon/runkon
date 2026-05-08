# runkon-flow Examples

One minimal correct implementation per public trait — demonstrates the shape of each
extension point and doubles as an adoption guide for embedders building their own
harness on top of runkon-flow.

> **Alpha stability warning**: runkon-flow is pre-1.0. Trait signatures may change
> in a minor version. Pin to an exact version until a stable 1.0 is published.

---

## Examples

| File | Trait | Description | Run command |
|---|---|---|---|
| `echo_executor.rs` | `ActionExecutor` | Returns the `text` input as `result_text`; falls back to the action name. | `cargo run --example echo_executor -p runkon-flow` |
| `static_items_provider.rs` | `ItemProvider` | Returns a fixed list of `FanOutItem`s for foreach fan-out steps. | `cargo run --example static_items_provider -p runkon-flow` |
| `always_approve_gate.rs` | `GateResolver` | Unconditionally returns `GatePoll::Approved(None)` on every poll. | `cargo run --example always_approve_gate -p runkon-flow` |
| `fixed_env_provider.rs` | `ScriptEnvProvider` | Returns a hard-coded `HashMap` of env vars regardless of identity. | `cargo run --example fixed_env_provider -p runkon-flow` |
| `stdout_event_sink.rs` | `EventSink` | Prints a one-line summary of each engine event to stdout. | `cargo run --example stdout_event_sink -p runkon-flow` |
| `static_workflow_resolver.rs` | `WorkflowResolver` | Resolves a single named `WorkflowDef`; returns `WorkflowNotFound` for all others. | `cargo run --example static_workflow_resolver -p runkon-flow` |
| `logging_child_runner.rs` | `ChildWorkflowRunner` | Logs each child workflow call and returns a stub `WorkflowResult`. | `cargo run --example logging_child_runner -p runkon-flow` |
| `full_engine_minimal.rs` | (end-to-end) | Wires `EchoExecutor` into a `FlowEngine` and executes a 2-step workflow end-to-end. | `cargo run --example full_engine_minimal -p runkon-flow --features test-utils` |

---

## In-tree reference implementations

Two traits are already covered by in-tree implementations that live behind the
`test-utils` feature flag — they are too large for a standalone `< 100 LOC` example
but are the canonical reference for implementors:

- **`WorkflowPersistence`** — `InMemoryWorkflowPersistence` in
  [`src/persistence_memory.rs`](../src/persistence_memory.rs).
  Implements all 12+ methods (including the `GateApprovalStore` supertrait)
  using in-memory `Mutex<HashMap>` maps. Used by `full_engine_minimal` above.

- **`RunContext`** — `NoopRunContext` in
  [`src/traits/run_context.rs`](../src/traits/run_context.rs).
  Returns empty injected variables and `/tmp` as the working directory.
  Used by `full_engine_minimal` via `make_test_execution_state`.

---

## Building all examples

```bash
# Build all examples (no feature flags required for 7 of 8):
cargo build --examples -p runkon-flow

# Build the end-to-end example (requires test-utils):
cargo build --example full_engine_minimal -p runkon-flow --features test-utils
```
