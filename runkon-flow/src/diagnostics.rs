//! Per-run engine diagnostics: scoped tracing subscriber and panic capture.
//!
//! Each engine thread gets its own log file. `tracing::subscriber::with_default`
//! installs a thread-local subscriber that writes to the per-run file. A
//! `GlobalForwardLayer` included in that subscriber forwards every event to the
//! host binary's global dispatch as well.
//!
//! Panics that escape the engine are caught, emitted as `EngineEvent::Panicked`
//! (so all registered sinks — including the conductor-core `PanicDbSink` — receive
//! them), then re-raised so the host binary's panic handler still fires.

use std::panic::UnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter, Layer, Registry};

use crate::events::{emit_to_sinks, EngineEvent, EventSink};

/// Forwards every event to a captured `tracing::Dispatch` so the host binary's
/// global subscriber still receives events when `with_default` overrides the
/// thread-local dispatcher.
///
/// # Span context limitation
///
/// Only events are forwarded — span lifecycle (`on_new_span`, `on_enter`, etc.) is not.
/// Span IDs are scoped to the Subscriber that created them: forwarding our local span IDs
/// to the host's Subscriber would arrive without a matching `new_span` and produce broken
/// span trees on the host side. Engine code in this codebase emits ad-hoc events
/// (`tracing::info!`, `tracing::error!`) rather than rich span trees, so the events-only
/// forward covers the common case. Span fields that need to reach the host log should be
/// attached to events directly (e.g. `tracing::info!(run_id = %id, "msg")`).
struct GlobalForwardLayer(tracing::Dispatch);

impl<S: tracing::Subscriber> Layer<S> for GlobalForwardLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        self.0.event(event);
    }
}

/// Run `f` on the current thread with a per-run tracing subscriber installed and a
/// top-level `catch_unwind` guard.
///
/// On panic:
/// 1. The panic message + backtrace are emitted via `tracing::error!` into the per-run log.
/// 2. `EngineEvent::Panicked` is emitted to all `sinks` (DB write, notifications, etc.).
/// 3. The panic is re-raised with `std::panic::resume_unwind` so the host binary's panic
///    handler still fires.
///
/// The `log_dir` parameter specifies where to write the per-run log file
/// (`<log_dir>/<run_id>.log`). Pass `crate::config::workflow_log_dir()` in production;
/// pass a tempdir in tests.
pub fn run_with_per_run_log<F: FnOnce() + UnwindSafe>(
    run_id: &str,
    log_dir: PathBuf,
    sinks: Vec<Arc<dyn EventSink>>,
    f: F,
) {
    let (subscriber, guard) = setup_per_run_tracing(run_id, &log_dir);

    let panic_payload = tracing::subscriber::with_default(subscriber, || {
        let result = std::panic::catch_unwind(f);
        match result {
            Ok(()) => None,
            Err(payload) => {
                let msg = extract_panic_msg(&payload);
                let bt = std::backtrace::Backtrace::force_capture();
                tracing::error!(run_id = %run_id, "engine panic: {msg}\n{bt}");
                let event = EngineEvent::Panicked {
                    message: msg,
                    backtrace: bt.to_string(),
                };
                emit_to_sinks(run_id, event, &sinks);
                Some(payload)
            }
        }
    });

    // Drop the guard to flush the log file before re-raising the panic.
    drop(guard);

    if let Some(payload) = panic_payload {
        std::panic::resume_unwind(payload);
    }
}

fn setup_per_run_tracing(
    run_id: &str,
    log_dir: &Path,
) -> (impl tracing::Subscriber + Send + Sync, WorkerGuard) {
    // Create directory; best-effort — if it fails, the file open below will also fail
    // and we'll fall back to a no-op writer.
    let _ = std::fs::create_dir_all(log_dir);

    // Validate run_id via ULID parse (defense-in-depth against path traversal).
    // In practice run_id is always a freshly-minted ULID so the error branch is
    // unreachable; the guard catches any malformed or adversarial input.
    let log_basename = match ulid::Ulid::from_string(run_id) {
        Ok(_) => format!("{run_id}.log"),
        Err(e) => {
            tracing::warn!(
                run_id = %run_id,
                "diagnostics: invalid run_id, using fallback log path: {e}"
            );
            "invalid-run-id.log".to_string()
        }
    };
    let log_path = log_dir.join(log_basename);

    // Open (or create) the log file in append mode so resumed engine threads extend
    // rather than overwrite an existing log from a prior attempt on the same run_id.
    let writer: Box<dyn std::io::Write + Send + 'static> = match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
    {
        Ok(f) => Box::new(f),
        Err(e) => {
            tracing::warn!(run_id = %run_id, "diagnostics: cannot open log file {}: {e}", log_path.display());
            Box::new(std::io::sink())
        }
    };

    let (non_blocking, guard) = tracing_appender::non_blocking(writer);

    // Respect RUST_LOG but default to `info` so postmortem evidence is preserved even
    // when the host binary's filter is stricter (e.g. `warn`).
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Capture the current global dispatch so events are forwarded to the host binary's
    // subscriber even while with_default overrides the thread-local dispatcher.
    // This get_default call MUST happen before with_default installs the per-run
    // subscriber — capturing while it is still the current dispatch is load-bearing.
    let global_dispatch = tracing::dispatcher::get_default(|d| d.clone());

    let subscriber = Registry::default()
        .with(filter)
        .with(fmt::Layer::new().with_writer(non_blocking).with_ansi(false))
        .with(GlobalForwardLayer(global_dispatch));

    (subscriber, guard)
}

fn extract_panic_msg(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_file_created_on_normal_run() {
        let dir = tempfile::tempdir().unwrap();
        let run_id = ulid::Ulid::new().to_string();
        let log_dir = dir.path().join("workflow-logs");

        let run_id_clone = run_id.clone();
        run_with_per_run_log(&run_id, log_dir.clone(), vec![], move || {
            tracing::info!(run_id = %run_id_clone, "normal engine event");
        });

        let log_path = log_dir.join(format!("{run_id}.log"));
        assert!(
            log_path.exists(),
            "per-run log file should be created on normal run"
        );
    }

    #[test]
    fn unwritable_log_dir_does_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let blocker_file = dir.path().join("not-a-dir");
        std::fs::write(&blocker_file, b"").unwrap();
        let log_dir = blocker_file.join("workflow-logs"); // can't be created — parent is a file

        let run_id = ulid::Ulid::new().to_string();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_with_per_run_log(&run_id, log_dir, vec![], || {
                panic!("no log file but still panicking")
            });
        }));

        assert!(result.is_err(), "panic must still propagate");
    }
}
