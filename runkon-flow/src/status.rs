use serde::{Deserialize, Serialize};

/// Status of a workflow run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowRunStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    Waiting,
    /// Transient staging state: the classifier has determined this run is
    /// eligible for automatic resume. The watchdog picks it up on the next
    /// tick, CAS-flips it back to `failed`, and spawns a resume thread.
    /// Neither active nor terminal — consumed within one background tick.
    NeedsResume,
    /// Transient state: a cancel signal has been sent; the engine is cleaning up.
    /// Neither active nor terminal — the engine transitions to `Cancelled` once cleanup completes.
    Cancelling,
}

impl std::fmt::Display for WorkflowRunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Waiting => "waiting",
            Self::NeedsResume => "needs_resume",
            Self::Cancelling => "cancelling",
        };
        write!(f, "{s}")
    }
}

impl std::str::FromStr for WorkflowRunStatus {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "waiting" => Ok(Self::Waiting),
            "needs_resume" => Ok(Self::NeedsResume),
            "cancelling" => Ok(Self::Cancelling),
            _ => Err(format!("unknown WorkflowRunStatus: {s}")),
        }
    }
}

impl WorkflowRunStatus {
    /// Canonical set of statuses that constitute an "active" run.
    pub const ACTIVE: [WorkflowRunStatus; 3] = [
        WorkflowRunStatus::Pending,
        WorkflowRunStatus::Running,
        WorkflowRunStatus::Waiting,
    ];

    /// Returns the SQL string representations of all active statuses.
    pub fn active_strings() -> Vec<String> {
        Self::ACTIVE.iter().map(|s| s.to_string()).collect()
    }

    /// Whether this status is terminal (no further transitions expected).
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    /// Whether this status is active (run is in progress or waiting).
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Pending | Self::Running | Self::Waiting)
    }
}

/// Status of a single workflow step execution.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStepStatus {
    #[default]
    Pending,
    Running,
    Completed,
    Failed,
    Skipped,
    Waiting,
    TimedOut,
}

impl std::fmt::Display for WorkflowStepStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::Waiting => "waiting",
            Self::TimedOut => "timed_out",
        };
        write!(f, "{s}")
    }
}

impl WorkflowStepStatus {
    /// Short display label used in summaries and status columns.
    pub fn short_label(&self) -> &'static str {
        match self {
            Self::Completed => "ok",
            Self::Failed => "FAIL",
            Self::Skipped => "skip",
            Self::Running => "...",
            Self::Pending => "-",
            Self::Waiting => "wait",
            Self::TimedOut => "tout",
        }
    }

    /// Whether this status is terminal (no further transitions expected).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Skipped | Self::TimedOut
        )
    }
}

impl std::str::FromStr for WorkflowStepStatus {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "skipped" => Ok(Self::Skipped),
            "waiting" => Ok(Self::Waiting),
            "timed_out" => Ok(Self::TimedOut),
            _ => Err(format!("unknown WorkflowStepStatus: {s}")),
        }
    }
}
