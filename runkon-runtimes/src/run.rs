use serde::{Deserialize, Serialize};

/// Lifecycle status of a runtime-spawned agent.
///
/// Vendor-neutral: only the four states that any runtime in this crate
/// (`ClaudeRuntime`, `CliRuntime`, `ScriptRuntime`) needs to emit. Host
/// applications layering richer states on top (conductor's
/// `WaitingForFeedback` for paused-for-human-input runs, etc.) keep them in
/// their own enum and convert at the boundary.
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl std::fmt::Display for RunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        };
        write!(f, "{s}")
    }
}

impl std::str::FromStr for RunStatus {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(format!("unknown RunStatus: {s}")),
        }
    }
}

/// Handle to a runtime-spawned agent run, carrying only the fields any
/// runtime in this crate (or a host like conductor) actually reads through
/// the [`AgentRuntime`](crate::runtime::AgentRuntime) trait.
///
/// Richer host-domain records (e.g. conductor's `AgentRun` with
/// `worktree_id`, `repo_id`, `prompt`, plan steps, etc.) live in the host
/// crate and are converted to / from `RunHandle` at the boundary by the
/// [`RunTracker`](crate::tracker::RunTracker) implementation.
///
/// `session_id` is the host-supplied resume identifier (e.g. Claude's
/// session id) — the field name is generic so non-Claude runtimes can store
/// their own resumable identifier here without a vendor-named field
/// surfacing in this crate.
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunHandle {
    pub id: String,
    pub status: RunStatus,
    pub subprocess_pid: Option<i64>,
    /// Name of the runtime that spawned this run (`"claude"`, `"cli"`,
    /// `"script"`, or a host-defined value).
    pub runtime: String,
    /// Resumable session id captured by the runtime (vendor-neutral name).
    pub session_id: Option<String>,
    pub result_text: Option<String>,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub log_file: Option<String>,
    pub model: Option<String>,
    pub cost_usd: Option<f64>,
    pub num_turns: Option<i64>,
    pub duration_ms: Option<i64>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cache_read_input_tokens: Option<i64>,
    pub cache_creation_input_tokens: Option<i64>,
}
