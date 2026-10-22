//! Portable agent runtime harness — spawn, poll, and cancel agents
//! without depending on conductor's full domain.

pub mod agent_def;
pub mod config;
pub mod error;
pub mod headless;
pub mod permission;
pub mod process_utils;
pub mod run;
pub mod runtime;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_support;
pub mod text_util;
pub mod tracker;

pub use agent_def::{AgentDef, AgentRole};
pub use config::RuntimeConfig;
pub use error::{Result, RuntimeError};
pub use headless::{
    resolve_completed_run, DrainOutcome, LineEventParser, ParseSignal, TerminalEventCapture,
};
pub use permission::PermissionMode;
pub use run::{RunHandle, RunStatus};
pub use runtime::{AgentRuntime, PollError, RuntimeOptions, RuntimeRequest, RuntimeResolver};
pub use tracker::{EventSink, NoopEventSink, RunEventSink, RunTracker, RuntimeEvent};
