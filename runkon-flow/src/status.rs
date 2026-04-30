use serde::{Deserialize, Serialize};

/// Status of a workflow run.
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
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
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
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

    /// Whether this status represents a step that is starting (running or waiting).
    pub fn is_starting(&self) -> bool {
        matches!(self, Self::Running | Self::Waiting)
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

#[cfg(feature = "rusqlite")]
mod sql_impls {
    use super::{WorkflowRunStatus, WorkflowStepStatus};
    use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};

    fn status_to_sql(status: &impl std::fmt::Display) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(status.to_string()))
    }

    fn status_from_sql<T>(value: ValueRef<'_>) -> FromSqlResult<T>
    where
        T: std::str::FromStr<Err = String>,
    {
        let s = String::column_result(value)?;
        s.parse().map_err(|e: String| {
            FromSqlError::Other(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e,
            )))
        })
    }

    impl ToSql for WorkflowRunStatus {
        fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
            status_to_sql(self)
        }
    }

    impl FromSql for WorkflowRunStatus {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            status_from_sql(value)
        }
    }

    impl ToSql for WorkflowStepStatus {
        fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
            status_to_sql(self)
        }
    }

    impl FromSql for WorkflowStepStatus {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            status_from_sql(value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_terminal_states() {
        assert!(WorkflowRunStatus::Completed.is_terminal());
        assert!(WorkflowRunStatus::Failed.is_terminal());
        assert!(WorkflowRunStatus::Cancelled.is_terminal());
        assert!(!WorkflowRunStatus::Pending.is_terminal());
        assert!(!WorkflowRunStatus::Running.is_terminal());
        assert!(!WorkflowRunStatus::Waiting.is_terminal());
        // NeedsResume is a transient staging state — not terminal.
        assert!(!WorkflowRunStatus::NeedsResume.is_terminal());
    }

    #[test]
    fn run_active_states() {
        assert!(WorkflowRunStatus::Pending.is_active());
        assert!(WorkflowRunStatus::Running.is_active());
        assert!(WorkflowRunStatus::Waiting.is_active());
        assert!(!WorkflowRunStatus::Completed.is_active());
        assert!(!WorkflowRunStatus::Failed.is_active());
        assert!(!WorkflowRunStatus::Cancelled.is_active());
        // NeedsResume is a transient staging state — not active.
        assert!(!WorkflowRunStatus::NeedsResume.is_active());
    }

    #[test]
    fn step_terminal_states() {
        assert!(WorkflowStepStatus::Completed.is_terminal());
        assert!(WorkflowStepStatus::Failed.is_terminal());
        assert!(WorkflowStepStatus::Skipped.is_terminal());
        assert!(WorkflowStepStatus::TimedOut.is_terminal());
        assert!(!WorkflowStepStatus::Pending.is_terminal());
        assert!(!WorkflowStepStatus::Running.is_terminal());
        assert!(!WorkflowStepStatus::Waiting.is_terminal());
    }

    #[test]
    fn step_starting_states() {
        assert!(WorkflowStepStatus::Running.is_starting());
        assert!(WorkflowStepStatus::Waiting.is_starting());
        assert!(!WorkflowStepStatus::Pending.is_starting());
        assert!(!WorkflowStepStatus::Completed.is_starting());
        assert!(!WorkflowStepStatus::Failed.is_starting());
        assert!(!WorkflowStepStatus::Skipped.is_starting());
        assert!(!WorkflowStepStatus::TimedOut.is_starting());
    }

    #[test]
    fn timed_out_is_not_a_valid_run_status() {
        use std::str::FromStr;
        // 'timed_out' is valid only for workflow_run_steps (WorkflowStepStatus::TimedOut).
        // workflow_runs.status must never be 'timed_out'; the schema CHECK constraint
        // (migration 080) enforces this at the DB level.
        assert!(WorkflowRunStatus::from_str("timed_out").is_err());
    }

    #[test]
    fn run_terminal_and_active_are_mutually_exclusive() {
        // These statuses must be exactly one of terminal or active.
        let exactly_one = [
            WorkflowRunStatus::Pending,
            WorkflowRunStatus::Running,
            WorkflowRunStatus::Completed,
            WorkflowRunStatus::Failed,
            WorkflowRunStatus::Cancelled,
            WorkflowRunStatus::Waiting,
        ];
        for s in exactly_one {
            assert!(
                s.is_terminal() != s.is_active(),
                "{s} should be exactly one of terminal or active"
            );
        }
        // NeedsResume is a transient staging state — neither terminal nor active.
        // At most one of is_terminal / is_active may be true for any status.
        assert!(
            !(WorkflowRunStatus::NeedsResume.is_terminal()
                && WorkflowRunStatus::NeedsResume.is_active()),
            "NeedsResume must not be both terminal and active"
        );
    }

    #[cfg(feature = "rusqlite")]
    mod rusqlite_roundtrip {
        use super::*;
        use rusqlite::types::{FromSql, ToSql};

        fn roundtrip_all<T>(conn: &rusqlite::Connection, variants: &[T])
        where
            T: ToSql + FromSql + std::fmt::Display + PartialEq + std::fmt::Debug,
        {
            conn.execute("CREATE TABLE IF NOT EXISTS t (status TEXT)", [])
                .unwrap();
            for variant in variants {
                conn.execute("INSERT INTO t (status) VALUES (?1)", [variant])
                    .unwrap();
                let recovered: T = conn
                    .query_row("SELECT status FROM t", [], |row| row.get(0))
                    .unwrap();
                assert_eq!(*variant, recovered, "round-trip failed for {variant}");
                conn.execute("DELETE FROM t", []).unwrap();
            }
        }

        fn invalid_string_errors<T>(conn: &rusqlite::Connection)
        where
            T: FromSql,
        {
            conn.execute("CREATE TABLE IF NOT EXISTS t (status TEXT)", [])
                .unwrap();
            conn.execute("INSERT INTO t (status) VALUES (?1)", ["not_a_status"])
                .unwrap();
            let result = conn.query_row::<T, _, _>("SELECT status FROM t", [], |row| row.get(0));
            assert!(result.is_err(), "expected error for invalid status string");
        }

        #[test]
        fn workflow_run_status_roundtrip() {
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            roundtrip_all(
                &conn,
                &[
                    WorkflowRunStatus::Pending,
                    WorkflowRunStatus::Running,
                    WorkflowRunStatus::Completed,
                    WorkflowRunStatus::Failed,
                    WorkflowRunStatus::Cancelled,
                    WorkflowRunStatus::Waiting,
                    WorkflowRunStatus::NeedsResume,
                    WorkflowRunStatus::Cancelling,
                ],
            );
        }

        #[test]
        fn workflow_step_status_roundtrip() {
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            roundtrip_all(
                &conn,
                &[
                    WorkflowStepStatus::Pending,
                    WorkflowStepStatus::Running,
                    WorkflowStepStatus::Completed,
                    WorkflowStepStatus::Failed,
                    WorkflowStepStatus::Skipped,
                    WorkflowStepStatus::Waiting,
                    WorkflowStepStatus::TimedOut,
                ],
            );
        }

        #[test]
        fn workflow_run_status_invalid_string_errors() {
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            invalid_string_errors::<WorkflowRunStatus>(&conn);
        }

        #[test]
        fn workflow_step_status_invalid_string_errors() {
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            invalid_string_errors::<WorkflowStepStatus>(&conn);
        }
    }
}
