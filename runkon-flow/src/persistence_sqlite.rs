//! SQLite-backed implementation of [`WorkflowPersistence`] (Phase 4 step 4.3).
//!
//! Gated on the `rusqlite` cargo feature. The module is fully self-contained:
//! row mappers, column constants, and the small set of gate / json helpers
//! all live here so a harness only needs to enable `rusqlite` to get a
//! production-ready persistence backend without writing any SQL itself.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use chrono::Utc;
use rusqlite::{named_params, Connection, OptionalExtension};

use crate::cancellation_reason::CancellationReason;
use crate::constants::{RUN_COLUMNS, STEP_COLUMNS, TERMINAL_STATUSES_SQL};
use crate::engine_error::EngineError;
use crate::status::{WorkflowRunStatus, WorkflowStepStatus};
use crate::traits::persistence::{
    gate_approval_state_from_fields, FanOutItemRow, FanOutItemStatus, FanOutItemUpdate,
    GateApprovalState, NewRun, NewStep, StepUpdate, WorkflowPersistence,
};
use crate::types::{extract_workflow_title, BlockedOn, WorkflowRun, WorkflowRunStep};

/// `s.`-prefixed variant of [`STEP_COLUMNS`] for queries that JOIN additional
/// context tables (workflow_runs, worktrees, etc.) and need disambiguated column
/// references. Computed once at first access.
static STEP_COLUMNS_WITH_PREFIX: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    STEP_COLUMNS
        .split(", ")
        .map(|c| format!("s.{c}"))
        .collect::<Vec<_>>()
        .join(", ")
});

/// Precomputed SQL for the terminal-status UPDATE in `update_step`. Allocated once so
/// the hot-path call site carries no per-invocation heap cost.
static SQL_UPDATE_TERMINAL: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    format!(
        "UPDATE workflow_run_steps SET status = :status, \
         child_run_id = COALESCE(:child_run_id, child_run_id), \
         ended_at = :ended_at, result_text = :result_text, context_out = :context_out, \
         markers_out = :markers_out, \
         retry_count = COALESCE(:retry_count, retry_count), \
         structured_output = :structured_output, step_error = :step_error \
         WHERE id = :id \
         AND (SELECT generation FROM workflow_runs \
              WHERE id = workflow_run_steps.workflow_run_id) = :generation \
         AND status NOT IN ({TERMINAL_STATUSES_SQL})"
    )
});

/// Precomputed SQL for the already-terminal disambiguation check in `update_step`.
static SQL_CHECK_TERMINAL: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    format!(
        "SELECT 1 FROM workflow_run_steps \
         WHERE id = :id \
         AND (SELECT generation FROM workflow_runs \
              WHERE id = workflow_run_steps.workflow_run_id) = :generation \
         AND status IN ({TERMINAL_STATUSES_SQL})"
    )
});

// ---------------------------------------------------------------------------
// Row mappers (rusqlite::Row → runkon-flow type)
// ---------------------------------------------------------------------------

fn json_or_warn<T: serde::de::DeserializeOwned + Default>(
    json: Option<&str>,
    context: impl FnOnce() -> String,
) -> T {
    match json {
        None => T::default(),
        Some(s) => serde_json::from_str(s).unwrap_or_else(|e| {
            tracing::warn!("{}: {e}", context());
            T::default()
        }),
    }
}

fn row_to_run(row: &rusqlite::Row) -> rusqlite::Result<WorkflowRun> {
    let id: String = row.get("id")?;
    let dry_run_int: i64 = row.get("dry_run")?;
    let inputs_json: Option<String> = row.get("inputs")?;
    let inputs: HashMap<String, String> = json_or_warn(inputs_json.as_deref(), || {
        format!("Malformed inputs JSON in workflow run {id}")
    });
    let blocked_on_json: Option<String> = row.get("blocked_on")?;
    let blocked_on: Option<BlockedOn> = json_or_warn(blocked_on_json.as_deref(), || {
        format!("Malformed blocked_on JSON in workflow run {id}")
    });
    let dismissed_int: i64 = row.get("dismissed")?;
    let definition_snapshot: Option<String> = row.get("definition_snapshot")?;
    let workflow_title: Option<String> = row.get("workflow_title")?;
    Ok(WorkflowRun {
        id,
        workflow_name: row.get("workflow_name")?,
        worktree_id: row.get::<_, Option<String>>("worktree_id")?,
        parent_run_id: row.get("parent_run_id")?,
        status: row.get("status")?,
        dry_run: dry_run_int != 0,
        trigger: row.get("trigger")?,
        started_at: row.get("started_at")?,
        ended_at: row.get("ended_at")?,
        result_summary: row.get("result_summary")?,
        error: row.get("error")?,
        definition_snapshot,
        inputs,
        ticket_id: row.get("ticket_id")?,
        repo_id: row.get("repo_id")?,
        parent_workflow_run_id: row.get("parent_workflow_run_id")?,
        target_label: row.get("target_label")?,
        default_bot_name: row.get("default_bot_name")?,
        iteration: row.get("iteration")?,
        blocked_on,
        workflow_title,
        total_input_tokens: row.get("total_input_tokens")?,
        total_output_tokens: row.get("total_output_tokens")?,
        total_cache_read_input_tokens: row.get("total_cache_read_input_tokens")?,
        total_cache_creation_input_tokens: row.get("total_cache_creation_input_tokens")?,
        total_turns: row.get("total_turns")?,
        total_cost_usd: row.get("total_cost_usd")?,
        total_duration_ms: row.get("total_duration_ms")?,
        model: row.get("model")?,
        dismissed: dismissed_int != 0,
        owner_token: row.get("owner_token")?,
        lease_until: row.get("lease_until")?,
        generation: row.get("generation")?,
    })
}

fn row_to_step(row: &rusqlite::Row) -> rusqlite::Result<WorkflowRunStep> {
    let can_commit_int: i64 = row.get("can_commit")?;
    let condition_met_int: Option<i64> = row.get("condition_met")?;
    Ok(WorkflowRunStep {
        id: row.get("id")?,
        workflow_run_id: row.get("workflow_run_id")?,
        step_name: row.get("step_name")?,
        role: row.get("role")?,
        can_commit: can_commit_int != 0,
        condition_expr: row.get("condition_expr")?,
        status: row.get("status")?,
        child_run_id: row.get("child_run_id")?,
        position: row.get("position")?,
        started_at: row.get("started_at")?,
        ended_at: row.get("ended_at")?,
        result_text: row.get("result_text")?,
        condition_met: condition_met_int.map(|v| v != 0),
        iteration: row.get("iteration")?,
        parallel_group_id: row.get("parallel_group_id")?,
        context_out: row.get("context_out")?,
        markers_out: row.get("markers_out")?,
        retry_count: row.get("retry_count")?,
        gate_type: {
            let s: Option<String> = row.get("gate_type")?;
            s.as_deref().and_then(|s| s.parse().ok())
        },
        gate_prompt: row.get("gate_prompt")?,
        gate_timeout: row.get("gate_timeout")?,
        gate_approved_by: row.get("gate_approved_by")?,
        gate_approved_at: row.get("gate_approved_at")?,
        gate_feedback: row.get("gate_feedback")?,
        structured_output: row.get("structured_output")?,
        output_file: row.get("output_file")?,
        gate_options: row.get("gate_options")?,
        gate_selections: row.get("gate_selections")?,
        input_tokens: row.get("input_tokens")?,
        output_tokens: row.get("output_tokens")?,
        cache_read_input_tokens: row.get("cache_read_input_tokens")?,
        cache_creation_input_tokens: row.get("cache_creation_input_tokens")?,
        cost_usd: row.get("cost_usd")?,
        num_turns: row.get("num_turns")?,
        duration_ms: row.get("duration_ms")?,
        fan_out_total: row.get("fan_out_total")?,
        fan_out_completed: row.get::<_, Option<i64>>("fan_out_completed")?.unwrap_or(0),
        fan_out_failed: row.get::<_, Option<i64>>("fan_out_failed")?.unwrap_or(0),
        fan_out_skipped: row.get::<_, Option<i64>>("fan_out_skipped")?.unwrap_or(0),
        step_error: row.get("step_error")?,
    })
}

fn row_to_fan_out_item(row: &rusqlite::Row) -> rusqlite::Result<FanOutItemRow> {
    Ok(FanOutItemRow {
        id: row.get("id")?,
        step_run_id: row.get("step_run_id")?,
        item_type: row.get("item_type")?,
        item_id: row.get("item_id")?,
        item_ref: row.get("item_ref")?,
        child_run_id: row.get("child_run_id")?,
        status: row.get("status")?,
        dispatched_at: row.get("dispatched_at")?,
        completed_at: row.get("completed_at")?,
    })
}

// ---------------------------------------------------------------------------
// Gate helpers
// ---------------------------------------------------------------------------

fn format_gate_selection_context(items: &[String]) -> String {
    let mut out = String::from("User selected the following items:\n");
    for item in items {
        out.push_str("- ");
        out.push_str(item);
        out.push('\n');
    }
    out
}

fn serialize_gate_selections(selections: Option<&[String]>) -> Result<Option<String>, EngineError> {
    match selections {
        None => Ok(None),
        Some(s) => serde_json::to_string(s).map(Some).map_err(|e| {
            EngineError::Persistence(format!("gate selections serialization failed: {e}"))
        }),
    }
}

// ---------------------------------------------------------------------------
// SqliteWorkflowPersistence
// ---------------------------------------------------------------------------

/// SQLite-backed implementation of [`WorkflowPersistence`].
///
/// Wraps a [`rusqlite::Connection`] behind `Arc<Mutex<_>>` so it satisfies the
/// `Send + Sync` requirement of the trait. Each method acquires the lock and
/// runs its own SQL directly against the connection — no manager-layer
/// indirection. The required schema is defined by the conductor migrations
/// for now (`workflow_runs`, `workflow_run_steps`,
/// `workflow_run_step_fan_out_items`); a built-in `create_tables()` helper is
/// tracked as a follow-up under Phase 4 step 4.4.
pub struct SqliteWorkflowPersistence {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteWorkflowPersistence {
    /// Open a new SQLite connection at `path`, configured with sensible defaults
    /// for a workflow store: WAL journal mode, foreign-key enforcement, and a
    /// 5-second busy timeout. Creates the file if it does not exist.
    ///
    /// Most callers in production should construct the connection themselves
    /// (alongside their own schema migrations) and pass it via
    /// [`from_shared_connection`](Self::from_shared_connection); this helper is
    /// for tests and small standalone harnesses.
    pub fn open(path: &std::path::Path) -> Result<Self, EngineError> {
        let conn = Connection::open(path).map_err(db_err)?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(db_err)?;
        conn.pragma_update(None, "foreign_keys", true)
            .map_err(db_err)?;
        conn.pragma_update(None, "busy_timeout", 5000)
            .map_err(db_err)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Wrap an existing shared connection. Used by callers that need to share
    /// one [`Connection`] between setup and the engine (e.g.
    /// `execute_workflow_standalone` in conductor-core).
    pub fn from_shared_connection(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    fn lock(&self) -> Result<MutexGuard<'_, Connection>, EngineError> {
        self.conn.lock().map_err(|_| {
            EngineError::Persistence("SqliteWorkflowPersistence: mutex poisoned".into())
        })
    }
}

fn db_err(e: rusqlite::Error) -> EngineError {
    EngineError::Persistence(e.to_string())
}

fn new_id() -> String {
    ulid::Ulid::new().to_string()
}

impl WorkflowPersistence for SqliteWorkflowPersistence {
    fn create_run(&self, new_run: NewRun) -> Result<WorkflowRun, EngineError> {
        let conn = self.lock()?;
        let id = new_id();
        let now = Utc::now().to_rfc3339();

        let workflow_title = extract_workflow_title(new_run.definition_snapshot.as_deref());
        conn.execute(
            "INSERT INTO workflow_runs (id, workflow_name, worktree_id, ticket_id, repo_id, \
             parent_run_id, status, dry_run, trigger, started_at, definition_snapshot, \
             parent_workflow_run_id, target_label, workflow_title) \
             VALUES (:id, :workflow_name, :worktree_id, :ticket_id, :repo_id, :parent_run_id, \
             :status, :dry_run, :trigger, :started_at, :definition_snapshot, \
             :parent_workflow_run_id, :target_label, :workflow_title)",
            named_params![
                ":id": id,
                ":workflow_name": new_run.workflow_name,
                ":worktree_id": new_run.worktree_id,
                ":ticket_id": new_run.ticket_id,
                ":repo_id": new_run.repo_id,
                ":parent_run_id": new_run.parent_run_id,
                ":status": "pending",
                ":dry_run": new_run.dry_run as i64,
                ":trigger": new_run.trigger,
                ":started_at": now,
                ":definition_snapshot": new_run.definition_snapshot,
                ":parent_workflow_run_id": new_run.parent_workflow_run_id,
                ":target_label": new_run.target_label,
                ":workflow_title": workflow_title,
            ],
        )
        .map_err(db_err)?;
        Ok(WorkflowRun {
            id,
            workflow_name: new_run.workflow_name,
            worktree_id: new_run.worktree_id,
            parent_run_id: new_run.parent_run_id,
            status: WorkflowRunStatus::Pending,
            dry_run: new_run.dry_run,
            trigger: new_run.trigger,
            started_at: now,
            ended_at: None,
            result_summary: None,
            error: None,
            definition_snapshot: new_run.definition_snapshot,
            inputs: HashMap::new(),
            ticket_id: new_run.ticket_id,
            repo_id: new_run.repo_id,
            parent_workflow_run_id: new_run.parent_workflow_run_id,
            target_label: new_run.target_label,
            default_bot_name: None,
            iteration: 0,
            blocked_on: None,
            workflow_title,
            total_input_tokens: None,
            total_output_tokens: None,
            total_cache_read_input_tokens: None,
            total_cache_creation_input_tokens: None,
            total_turns: None,
            total_cost_usd: None,
            total_duration_ms: None,
            model: None,
            dismissed: false,
            owner_token: None,
            lease_until: None,
            generation: 0,
        })
    }

    fn get_run(&self, run_id: &str) -> Result<Option<WorkflowRun>, EngineError> {
        let conn = self.lock()?;
        conn.query_row(
            &format!("SELECT {RUN_COLUMNS} FROM workflow_runs WHERE id = :id"),
            named_params! { ":id": run_id },
            row_to_run,
        )
        .optional()
        .map_err(db_err)
    }

    fn list_active_runs(
        &self,
        statuses: &[WorkflowRunStatus],
    ) -> Result<Vec<WorkflowRun>, EngineError> {
        let conn = self.lock()?;
        let effective: &[WorkflowRunStatus] = if statuses.is_empty() {
            &WorkflowRunStatus::ACTIVE
        } else {
            statuses
        };
        let placeholders = (1..=effective.len())
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {RUN_COLUMNS} FROM workflow_runs \
             WHERE status IN ({placeholders}) \
             ORDER BY started_at DESC \
             LIMIT 500"
        );
        let status_strings: Vec<String> = effective.iter().map(|s| s.to_string()).collect();
        let mut stmt = conn.prepare(&sql).map_err(db_err)?;
        let rows = stmt
            .query_map(
                rusqlite::params_from_iter(status_strings.iter()),
                row_to_run,
            )
            .map_err(db_err)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(db_err)
    }

    fn update_run_status(
        &self,
        run_id: &str,
        status: WorkflowRunStatus,
        result_summary: Option<&str>,
        error: Option<&str>,
    ) -> Result<(), EngineError> {
        if matches!(status, WorkflowRunStatus::Waiting) {
            return Err(EngineError::Persistence(
                "Use set_waiting_blocked_on() to transition a workflow run to Waiting status"
                    .into(),
            ));
        }
        let conn = self.lock()?;
        let now = Utc::now().to_rfc3339();
        let is_terminal = matches!(
            status,
            WorkflowRunStatus::Completed | WorkflowRunStatus::Failed | WorkflowRunStatus::Cancelled
        );
        let ended_at = if is_terminal {
            Some(now.as_str())
        } else {
            None
        };

        conn.execute(
            "UPDATE workflow_runs SET status = :status, result_summary = :result_summary, \
             ended_at = :ended_at, blocked_on = NULL, error = :error WHERE id = :id",
            named_params![
                ":status": status,
                ":result_summary": result_summary,
                ":ended_at": ended_at,
                ":error": error,
                ":id": run_id,
            ],
        )
        .map_err(db_err)?;
        Ok(())
    }

    fn insert_step(&self, new_step: NewStep) -> Result<String, EngineError> {
        let conn = self.lock()?;
        let id = new_id();
        if let Some(retry_count) = new_step.retry_count {
            let now = Utc::now().to_rfc3339();
            conn.execute(
                "INSERT INTO workflow_run_steps \
                 (id, workflow_run_id, step_name, role, can_commit, status, position, iteration, \
                  started_at, retry_count) \
                 VALUES (:id, :workflow_run_id, :step_name, :role, :can_commit, 'running', :position, :iteration, :started_at, :retry_count)",
                named_params![
                    ":id": id,
                    ":workflow_run_id": new_step.workflow_run_id,
                    ":step_name": new_step.step_name,
                    ":role": new_step.role,
                    ":can_commit": new_step.can_commit as i64,
                    ":position": new_step.position,
                    ":iteration": new_step.iteration,
                    ":started_at": now,
                    ":retry_count": retry_count,
                ],
            )
            .map_err(db_err)?;
        } else {
            conn.execute(
                "INSERT INTO workflow_run_steps \
                 (id, workflow_run_id, step_name, role, can_commit, status, position, iteration) \
                 VALUES (:id, :workflow_run_id, :step_name, :role, :can_commit, :status, :position, :iteration)",
                named_params![
                    ":id": id,
                    ":workflow_run_id": new_step.workflow_run_id,
                    ":step_name": new_step.step_name,
                    ":role": new_step.role,
                    ":can_commit": new_step.can_commit as i64,
                    ":status": "pending",
                    ":position": new_step.position,
                    ":iteration": new_step.iteration,
                ],
            )
            .map_err(db_err)?;
        }
        Ok(id)
    }

    fn update_step(&self, step_id: &str, update: StepUpdate) -> Result<(), EngineError> {
        let conn = self.lock()?;
        let affected = if update.status.is_starting() {
            let now = Utc::now().to_rfc3339();
            conn.execute(
                "UPDATE workflow_run_steps SET status = :status, child_run_id = :child_run_id, \
                 started_at = :started_at WHERE id = :id \
                 AND (SELECT generation FROM workflow_runs \
                      WHERE id = workflow_run_steps.workflow_run_id) = :generation",
                named_params![
                    ":status": update.status,
                    ":child_run_id": update.child_run_id,
                    ":started_at": now,
                    ":id": step_id,
                    ":generation": update.generation,
                ],
            )
            .map_err(db_err)?
        } else if update.status.is_terminal() {
            let now = Utc::now().to_rfc3339();
            let n = conn
                .execute(
                    &SQL_UPDATE_TERMINAL,
                    named_params![
                        ":status": update.status,
                        ":child_run_id": update.child_run_id,
                        ":ended_at": now,
                        ":result_text": update.result_text,
                        ":context_out": update.context_out,
                        ":markers_out": update.markers_out,
                        ":retry_count": update.retry_count,
                        ":structured_output": update.structured_output,
                        ":step_error": update.step_error,
                        ":id": step_id,
                        ":generation": update.generation,
                    ],
                )
                .map_err(db_err)?;
            if n == 0 {
                // Disambiguate: already terminal with correct generation (benign no-op) vs
                // generation truly mismatched (caller lost the lease).
                let already_terminal = conn
                    .query_row(
                        &SQL_CHECK_TERMINAL,
                        named_params![":id": step_id, ":generation": update.generation],
                        |_| Ok(()),
                    )
                    .optional()
                    .map_err(db_err)?
                    .is_some();
                if already_terminal {
                    tracing::debug!(
                        step_id = %step_id,
                        "update_step: step already terminal, skipping overwrite"
                    );
                    return Ok(());
                }
                return Err(EngineError::Cancelled(CancellationReason::LeaseLost));
            }
            n
        } else {
            conn.execute(
                "UPDATE workflow_run_steps SET status = :status WHERE id = :id \
                 AND (SELECT generation FROM workflow_runs \
                      WHERE id = workflow_run_steps.workflow_run_id) = :generation",
                named_params![
                    ":status": update.status,
                    ":id": step_id,
                    ":generation": update.generation,
                ],
            )
            .map_err(db_err)?
        };
        if affected == 0 {
            return Err(EngineError::Cancelled(CancellationReason::LeaseLost));
        }
        Ok(())
    }

    fn get_steps(&self, run_id: &str) -> Result<Vec<WorkflowRunStep>, EngineError> {
        let conn = self.lock()?;
        let sql = format!(
            "SELECT {cols} FROM workflow_run_steps s \
             WHERE s.workflow_run_id = :workflow_run_id \
             ORDER BY s.position",
            cols = &*STEP_COLUMNS_WITH_PREFIX,
        );
        let mut stmt = conn.prepare(&sql).map_err(db_err)?;
        let rows = stmt
            .query_map(named_params! { ":workflow_run_id": run_id }, row_to_step)
            .map_err(db_err)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(db_err)
    }

    fn insert_fan_out_item(
        &self,
        step_run_id: &str,
        item_type: &str,
        item_id: &str,
        item_ref: &str,
    ) -> Result<String, EngineError> {
        let conn = self.lock()?;
        let id = new_id();
        conn.execute(
            "INSERT OR IGNORE INTO workflow_run_step_fan_out_items \
             (id, step_run_id, item_type, item_id, item_ref, status) \
             VALUES (:id, :step_run_id, :item_type, :item_id, :item_ref, 'pending')",
            named_params![
                ":id": id,
                ":step_run_id": step_run_id,
                ":item_type": item_type,
                ":item_id": item_id,
                ":item_ref": item_ref,
            ],
        )
        .map_err(db_err)?;
        Ok(id)
    }

    fn update_fan_out_item(
        &self,
        item_id: &str,
        update: FanOutItemUpdate,
    ) -> Result<(), EngineError> {
        let conn = self.lock()?;
        let now = Utc::now().to_rfc3339();
        match update {
            FanOutItemUpdate::Running { child_run_id } => {
                conn.execute(
                    "UPDATE workflow_run_step_fan_out_items \
                     SET status = 'running', child_run_id = :child_run_id, dispatched_at = :now \
                     WHERE id = :id",
                    named_params![
                        ":child_run_id": child_run_id,
                        ":now": now,
                        ":id": item_id,
                    ],
                )
                .map_err(db_err)?;
            }
            FanOutItemUpdate::Terminal { status } => {
                conn.execute(
                    "UPDATE workflow_run_step_fan_out_items \
                     SET status = :status, completed_at = :now \
                     WHERE id = :id",
                    named_params![
                        ":status": status.as_str(),
                        ":now": now,
                        ":id": item_id,
                    ],
                )
                .map_err(db_err)?;
            }
        }
        Ok(())
    }

    fn batch_update_fan_out_items(
        &self,
        updates: &[(String, FanOutItemUpdate)],
    ) -> Result<(), EngineError> {
        if updates.is_empty() {
            return Ok(());
        }
        let conn = self.lock()?;
        let tx = conn.unchecked_transaction().map_err(db_err)?;
        let now = Utc::now().to_rfc3339();
        for (item_id, update) in updates {
            let rows_changed = match update {
                FanOutItemUpdate::Running { child_run_id } => tx
                    .execute(
                        "UPDATE workflow_run_step_fan_out_items \
                         SET status = 'running', child_run_id = :child_run_id, dispatched_at = :now \
                         WHERE id = :id",
                        named_params![
                            ":child_run_id": child_run_id,
                            ":now": now,
                            ":id": item_id,
                        ],
                    )
                    .map_err(db_err)?,
                FanOutItemUpdate::Terminal { status } => tx
                    .execute(
                        "UPDATE workflow_run_step_fan_out_items \
                         SET status = :status, completed_at = :now \
                         WHERE id = :id",
                        named_params![
                            ":status": status.as_str(),
                            ":now": now,
                            ":id": item_id,
                        ],
                    )
                    .map_err(db_err)?,
            };
            if rows_changed == 0 {
                return Err(EngineError::Persistence(format!(
                    "fan-out item {item_id} not found"
                )));
            }
        }
        tx.commit().map_err(db_err)?;
        Ok(())
    }

    fn get_fan_out_items(
        &self,
        step_run_id: &str,
        status_filter: Option<FanOutItemStatus>,
    ) -> Result<Vec<FanOutItemRow>, EngineError> {
        let conn = self.lock()?;
        let select = "SELECT id, step_run_id, item_type, item_id, item_ref, child_run_id, \
                      status, dispatched_at, completed_at \
                      FROM workflow_run_step_fan_out_items";
        if let Some(status) = status_filter {
            let sql = format!(
                "{select} WHERE step_run_id = :step_run_id AND status = :status ORDER BY id ASC"
            );
            let mut stmt = conn.prepare(&sql).map_err(db_err)?;
            let rows = stmt
                .query_map(
                    named_params![":step_run_id": step_run_id, ":status": status.as_str()],
                    row_to_fan_out_item,
                )
                .map_err(db_err)?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(db_err)
        } else {
            let sql = format!("{select} WHERE step_run_id = :step_run_id ORDER BY id ASC");
            let mut stmt = conn.prepare(&sql).map_err(db_err)?;
            let rows = stmt
                .query_map(
                    named_params![":step_run_id": step_run_id],
                    row_to_fan_out_item,
                )
                .map_err(db_err)?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(db_err)
        }
    }

    fn get_gate_approval(&self, step_id: &str) -> Result<GateApprovalState, EngineError> {
        let conn = self.lock()?;
        #[allow(clippy::type_complexity)]
        let row: Option<(Option<String>, String, Option<String>, Option<String>)> = conn
            .query_row(
                "SELECT gate_approved_at, status, gate_feedback, gate_selections \
                 FROM workflow_run_steps WHERE id = ?1",
                rusqlite::params![step_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(db_err)?;

        let Some((approved_at, status_str, feedback, selections_json)) = row else {
            return Ok(GateApprovalState::Pending);
        };
        let status = status_str
            .parse::<WorkflowStepStatus>()
            .unwrap_or_else(|_| {
                tracing::warn!(
                    step_id = %step_id,
                    status = %status_str,
                    "get_gate_approval_state: unrecognised step status; treating as Waiting",
                );
                WorkflowStepStatus::Waiting
            });
        let selections = selections_json.and_then(|json| {
            serde_json::from_str::<Vec<String>>(&json)
                .map_err(|e| {
                    tracing::warn!(
                        step_id = %step_id,
                        "get_gate_approval_state: failed to deserialize gate_selections: {e}",
                    );
                })
                .ok()
        });
        Ok(gate_approval_state_from_fields(
            approved_at.as_deref(),
            status,
            feedback,
            selections,
        ))
    }

    fn approve_gate(
        &self,
        step_id: &str,
        approved_by: &str,
        feedback: Option<&str>,
        selections: Option<&[String]>,
    ) -> Result<(), EngineError> {
        let context_out = selections
            .filter(|s| !s.is_empty())
            .map(format_gate_selection_context);

        let conn = self.lock()?;
        if let Some(sels) = selections {
            validate_gate_selections(&conn, step_id, sels)?;
        }
        let selections_json = serialize_gate_selections(selections)?;
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE workflow_run_steps SET gate_approved_at = :now, gate_approved_by = :approved_by, \
             gate_feedback = :feedback, gate_selections = :selections_json, \
             context_out = COALESCE(:context_out, context_out), \
             status = 'completed', ended_at = :now WHERE id = :id",
            named_params![
                ":now": now,
                ":approved_by": approved_by,
                ":feedback": feedback,
                ":selections_json": selections_json,
                ":context_out": context_out,
                ":id": step_id,
            ],
        )
        .map_err(db_err)?;
        Ok(())
    }

    fn reject_gate(
        &self,
        step_id: &str,
        rejected_by: &str,
        feedback: Option<&str>,
    ) -> Result<(), EngineError> {
        let conn = self.lock()?;
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE workflow_run_steps SET gate_approved_by = :rejected_by, gate_feedback = :feedback, \
             status = 'failed', ended_at = :ended_at WHERE id = :id",
            named_params![
                ":rejected_by": rejected_by,
                ":feedback": feedback,
                ":ended_at": now,
                ":id": step_id,
            ],
        )
        .map_err(db_err)?;
        Ok(())
    }

    fn is_run_cancelled(&self, run_id: &str) -> Result<bool, EngineError> {
        let conn = self.lock()?;
        let status: Option<String> = conn
            .query_row(
                "SELECT status FROM workflow_runs WHERE id = ?1",
                rusqlite::params![run_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(db_err)?;
        Ok(matches!(
            status.as_deref(),
            Some("cancelled") | Some("cancelling")
        ))
    }

    fn tick_heartbeat(&self, run_id: &str) -> Result<(), EngineError> {
        let conn = self.lock()?;
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE workflow_runs SET last_heartbeat = :now \
             WHERE id = :id AND status = 'running'",
            named_params![":now": now, ":id": run_id],
        )
        .map_err(db_err)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn acquire_lease(
        &self,
        run_id: &str,
        token: &str,
        ttl_seconds: i64,
    ) -> Result<Option<i64>, EngineError> {
        let conn = self.lock()?;
        conn.execute(
            "UPDATE workflow_runs \
             SET owner_token = :token, \
                 lease_until = datetime('now', '+' || :ttl || ' seconds'), \
                 generation  = CASE WHEN owner_token = :token THEN generation ELSE generation + 1 END \
             WHERE id = :run_id \
               AND (owner_token IS NULL \
                    OR lease_until < datetime('now') \
                    OR owner_token = :token)",
            named_params! { ":token": token, ":ttl": ttl_seconds, ":run_id": run_id },
        )
        .map_err(|e| EngineError::Persistence(format!("acquire_lease UPDATE: {e}")))?;

        if conn.changes() == 0u64 {
            return Ok(None);
        }
        let gen: i64 = conn
            .query_row(
                "SELECT generation FROM workflow_runs WHERE id = ?1",
                [run_id],
                |r| r.get(0),
            )
            .map_err(|e| EngineError::Persistence(format!("acquire_lease read generation: {e}")))?;
        Ok(Some(gen))
    }

    fn persist_metrics(
        &self,
        run_id: &str,
        input_tokens: i64,
        output_tokens: i64,
        cache_read_input_tokens: i64,
        cache_creation_input_tokens: i64,
        cost_usd: f64,
        num_turns: i64,
        duration_ms: i64,
    ) -> Result<(), EngineError> {
        let conn = self.lock()?;
        conn.execute(
            "UPDATE workflow_runs SET \
             total_input_tokens = :total_input_tokens, \
             total_output_tokens = :total_output_tokens, \
             total_cache_read_input_tokens = :total_cache_read_input_tokens, \
             total_cache_creation_input_tokens = :total_cache_creation_input_tokens, \
             total_turns = :total_turns, \
             total_cost_usd = :total_cost_usd, \
             total_duration_ms = :total_duration_ms, \
             model = :model \
             WHERE id = :id",
            named_params![
                ":total_input_tokens": input_tokens,
                ":total_output_tokens": output_tokens,
                ":total_cache_read_input_tokens": cache_read_input_tokens,
                ":total_cache_creation_input_tokens": cache_creation_input_tokens,
                ":total_turns": num_turns,
                ":total_cost_usd": cost_usd,
                ":total_duration_ms": duration_ms,
                ":model": Option::<&str>::None,
                ":id": run_id,
            ],
        )
        .map_err(db_err)?;
        Ok(())
    }
}

/// Validate that gate selections are within the allowed options for this step.
///
/// The allowed-values set is parsed from the step's `gate_options` JSON column,
/// which the engine writes when a gate starts. Returns an
/// `EngineError::Persistence` describing the violation when a selection is
/// outside the configured option set.
fn validate_gate_selections(
    conn: &Connection,
    step_id: &str,
    selections: &[String],
) -> Result<(), EngineError> {
    let gate_options: Option<String> = conn
        .query_row(
            "SELECT gate_options FROM workflow_run_steps WHERE id = :id",
            named_params![":id": step_id],
            |row| row.get::<_, Option<String>>("gate_options"),
        )
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => {
                EngineError::Persistence(format!("Step not found: {step_id}"))
            }
            other => db_err(other),
        })?;

    let options_json = match gate_options {
        Some(json) => json,
        None => {
            if !selections.is_empty() {
                return Err(EngineError::Persistence(
                    "Gate selections provided but no options configured for this gate".into(),
                ));
            }
            return Ok(());
        }
    };

    let allowed_options: Vec<serde_json::Value> =
        serde_json::from_str(&options_json).map_err(|e| {
            EngineError::Persistence(format!("Invalid gate options JSON in database: {e}"))
        })?;

    let allowed_set: HashSet<String> = allowed_options
        .iter()
        .filter_map(|opt| {
            opt.get("value")
                .and_then(|v| v.as_str().map(|s| s.to_string()))
        })
        .collect();

    if allowed_set.is_empty() {
        return Err(EngineError::Persistence(
            "No valid options found in gate configuration".into(),
        ));
    }

    for selection in selections {
        if !allowed_set.contains(selection.as_str()) {
            let mut sorted: Vec<&str> = allowed_set.iter().map(|s| s.as_str()).collect();
            sorted.sort_unstable();
            return Err(EngineError::Persistence(format!(
                "Invalid gate selection '{selection}' - not in allowed options: [{}]",
                sorted.join(", ")
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::SqliteWorkflowPersistence;
    use crate::traits::persistence::WorkflowPersistence;

    /// Create an in-memory SQLite DB with the minimal schema for acquire_lease tests.
    fn make_lease_db() -> (SqliteWorkflowPersistence, String) {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE workflow_runs (
                id TEXT PRIMARY KEY,
                owner_token TEXT,
                lease_until TEXT,
                generation INTEGER NOT NULL DEFAULT 0
            );
            INSERT INTO workflow_runs (id) VALUES ('run-1');",
        )
        .unwrap();
        let p = SqliteWorkflowPersistence::from_shared_connection(Arc::new(Mutex::new(conn)));
        (p, "run-1".to_string())
    }

    #[test]
    fn acquire_lease_increments_generation_on_new_owner() {
        let (p, run_id) = make_lease_db();

        let gen1 = p.acquire_lease(&run_id, "tok", 60).unwrap();
        assert_eq!(gen1, Some(1), "first acquire should return generation 1");

        // Same-token renewal must NOT increment generation.
        let gen2 = p.acquire_lease(&run_id, "tok", 60).unwrap();
        assert_eq!(
            gen2,
            Some(1),
            "same-token re-acquire must not increment generation"
        );
    }

    /// create a minimal DB with workflow_runs + workflow_run_steps for update_step tests.
    fn make_step_db() -> (SqliteWorkflowPersistence, String, String) {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE workflow_runs (
                id TEXT PRIMARY KEY,
                owner_token TEXT,
                lease_until TEXT,
                generation INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE workflow_run_steps (
                id TEXT PRIMARY KEY,
                workflow_run_id TEXT NOT NULL REFERENCES workflow_runs(id),
                step_name TEXT NOT NULL DEFAULT '',
                role TEXT NOT NULL DEFAULT 'actor',
                can_commit INTEGER NOT NULL DEFAULT 0,
                condition_expr TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                started_at TEXT,
                ended_at TEXT,
                result_text TEXT,
                context_out TEXT,
                markers_out TEXT,
                retry_count INTEGER NOT NULL DEFAULT 0,
                structured_output TEXT,
                step_error TEXT,
                child_run_id TEXT,
                position INTEGER NOT NULL DEFAULT 0,
                iteration INTEGER NOT NULL DEFAULT 0,
                output_file TEXT,
                gate_options TEXT
            );
            INSERT INTO workflow_runs (id, generation) VALUES ('run-1', 1);
            INSERT INTO workflow_run_steps (id, workflow_run_id, status) VALUES ('step-1', 'run-1', 'running');",
        )
        .unwrap();
        let p = SqliteWorkflowPersistence::from_shared_connection(Arc::new(Mutex::new(conn)));
        (p, "run-1".to_string(), "step-1".to_string())
    }

    #[test]
    fn update_step_starting_branch_returns_lease_lost_on_stale_generation() {
        use crate::cancellation_reason::CancellationReason;
        use crate::engine_error::EngineError;
        use crate::status::WorkflowStepStatus;
        use crate::traits::persistence::StepUpdate;

        let (p, _run_id, step_id) = make_step_db();

        // DB has generation=1; send generation=0 (stale).
        let result = p.update_step(
            &step_id,
            StepUpdate {
                generation: 0,
                status: WorkflowStepStatus::Running,
                child_run_id: None,
                result_text: None,
                context_out: None,
                markers_out: None,
                retry_count: None,
                structured_output: None,
                step_error: None,
            },
        );
        assert!(
            matches!(
                result,
                Err(EngineError::Cancelled(CancellationReason::LeaseLost))
            ),
            "starting branch should return LeaseLost on stale generation; got {result:?}"
        );
    }

    #[test]
    fn update_step_terminal_branch_returns_lease_lost_on_stale_generation() {
        use crate::cancellation_reason::CancellationReason;
        use crate::engine_error::EngineError;
        use crate::status::WorkflowStepStatus;
        use crate::traits::persistence::StepUpdate;

        let (p, _run_id, step_id) = make_step_db();

        // DB has generation=1; send generation=0 (stale).
        let result = p.update_step(
            &step_id,
            StepUpdate {
                generation: 0,
                status: WorkflowStepStatus::Completed,
                child_run_id: None,
                result_text: None,
                context_out: None,
                markers_out: None,
                retry_count: None,
                structured_output: None,
                step_error: None,
            },
        );
        assert!(
            matches!(
                result,
                Err(EngineError::Cancelled(CancellationReason::LeaseLost))
            ),
            "terminal branch should return LeaseLost on stale generation; got {result:?}"
        );
    }

    #[test]
    fn update_step_terminal_branch_noop_when_already_completed() {
        use crate::status::WorkflowStepStatus;
        use crate::traits::persistence::StepUpdate;

        let (p, run_id, step_id) = make_step_db();

        // Mark the step as completed (generation=1 matches the DB seed).
        p.update_step(
            &step_id,
            StepUpdate {
                generation: 1,
                status: WorkflowStepStatus::Completed,
                child_run_id: None,
                result_text: Some("success".to_string()),
                context_out: None,
                markers_out: None,
                retry_count: None,
                structured_output: None,
                step_error: None,
            },
        )
        .expect("first update to completed must succeed");

        // Capture the ended_at written by the first update.
        let (status_after_first, ended_at_after_first): (String, String) = p
            .lock()
            .unwrap()
            .query_row(
                "SELECT status, ended_at FROM workflow_run_steps WHERE id = ?",
                rusqlite::params![step_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("row must exist");
        assert_eq!(status_after_first, "completed");

        // Try to overwrite with Failed — must be a no-op.
        let result = p.update_step(
            &step_id,
            StepUpdate {
                generation: 1,
                status: WorkflowStepStatus::Failed,
                child_run_id: None,
                result_text: Some("overwrite attempt".to_string()),
                context_out: None,
                markers_out: None,
                retry_count: None,
                structured_output: None,
                step_error: Some("cancelled".to_string()),
            },
        );
        assert!(
            result.is_ok(),
            "update_step on already-completed step must return Ok; got {result:?}"
        );

        // Row must be unchanged.
        let (status_final, ended_at_final): (String, String) = p
            .lock()
            .unwrap()
            .query_row(
                "SELECT status, ended_at FROM workflow_run_steps WHERE id = ?",
                rusqlite::params![step_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("row must still exist");
        assert_eq!(status_final, "completed", "status must remain completed");
        assert_eq!(
            ended_at_final, ended_at_after_first,
            "ended_at must be unchanged after no-op"
        );

        // run_id is read by other tests; suppress the unused warning.
        let _ = run_id;
    }

    #[test]
    fn update_step_status_only_branch_returns_lease_lost_on_stale_generation() {
        use crate::cancellation_reason::CancellationReason;
        use crate::engine_error::EngineError;
        use crate::status::WorkflowStepStatus;
        use crate::traits::persistence::StepUpdate;

        let (p, _run_id, step_id) = make_step_db();

        // DB has generation=1; send generation=0 (stale). Status=Pending is the
        // only value that is neither `is_starting()` nor `is_terminal()`, so
        // it's the only one that hits the third (status-only) UPDATE branch.
        let result = p.update_step(
            &step_id,
            StepUpdate {
                generation: 0,
                status: WorkflowStepStatus::Pending,
                child_run_id: None,
                result_text: None,
                context_out: None,
                markers_out: None,
                retry_count: None,
                structured_output: None,
                step_error: None,
            },
        );
        assert!(
            matches!(
                result,
                Err(EngineError::Cancelled(CancellationReason::LeaseLost))
            ),
            "status-only branch should return LeaseLost on stale generation; got {result:?}"
        );
    }

    #[test]
    fn acquire_lease_returns_none_when_held_by_other() {
        let (p, run_id) = make_lease_db();

        // Acquire with token 'x' and a 1-hour TTL.
        let gen = p.acquire_lease(&run_id, "x", 3600).unwrap();
        assert_eq!(gen, Some(1));

        // Attempt to acquire with a different token 'y' — lease is still held.
        let result = p.acquire_lease(&run_id, "y", 3600).unwrap();
        assert_eq!(
            result, None,
            "different-token acquire on held lease should return None"
        );
    }

    fn make_fan_out_db() -> (SqliteWorkflowPersistence, String) {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE workflow_run_step_fan_out_items (
                id TEXT PRIMARY KEY,
                step_run_id TEXT NOT NULL,
                item_type TEXT NOT NULL,
                item_id TEXT NOT NULL,
                item_ref TEXT NOT NULL DEFAULT '',
                child_run_id TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                dispatched_at TEXT,
                completed_at TEXT
            );",
        )
        .unwrap();
        let p = SqliteWorkflowPersistence::from_shared_connection(Arc::new(Mutex::new(conn)));
        (p, "step-1".to_string())
    }

    #[test]
    fn batch_update_fan_out_items_mixed_terminal_statuses() {
        use crate::traits::persistence::{FanOutItemStatus, FanOutItemUpdate};

        let (p, step_id) = make_fan_out_db();

        let id1 = p
            .insert_fan_out_item(&step_id, "ticket", "t-1", "ref-1")
            .unwrap();
        let id2 = p
            .insert_fan_out_item(&step_id, "ticket", "t-2", "ref-2")
            .unwrap();
        let id3 = p
            .insert_fan_out_item(&step_id, "ticket", "t-3", "ref-3")
            .unwrap();

        let updates = vec![
            (
                id1.clone(),
                FanOutItemUpdate::Terminal {
                    status: FanOutItemStatus::Completed,
                },
            ),
            (
                id2.clone(),
                FanOutItemUpdate::Terminal {
                    status: FanOutItemStatus::Failed,
                },
            ),
            (
                id3.clone(),
                FanOutItemUpdate::Terminal {
                    status: FanOutItemStatus::Skipped,
                },
            ),
        ];

        p.batch_update_fan_out_items(&updates).unwrap();

        let items = p.get_fan_out_items(&step_id, None).unwrap();
        let get = |id: &str| items.iter().find(|i| i.id == id).unwrap().clone();

        let item1 = get(&id1);
        assert_eq!(item1.status, "completed");
        assert!(item1.completed_at.is_some(), "completed_at must be set");

        let item2 = get(&id2);
        assert_eq!(item2.status, "failed");
        assert!(item2.completed_at.is_some());

        let item3 = get(&id3);
        assert_eq!(item3.status, "skipped");
        assert!(item3.completed_at.is_some());
    }

    #[test]
    fn batch_update_fan_out_items_empty_is_noop() {
        use crate::traits::persistence::FanOutItemUpdate;

        let (p, step_id) = make_fan_out_db();
        let _id = p
            .insert_fan_out_item(&step_id, "ticket", "t-1", "ref-1")
            .unwrap();

        p.batch_update_fan_out_items(&[] as &[(String, FanOutItemUpdate)])
            .unwrap();

        let items = p.get_fan_out_items(&step_id, None).unwrap();
        assert_eq!(items[0].status, "pending", "empty batch must be a no-op");
    }

    #[test]
    fn batch_update_fan_out_items_running_variant() {
        use crate::traits::persistence::FanOutItemUpdate;

        let (p, step_id) = make_fan_out_db();
        let id1 = p
            .insert_fan_out_item(&step_id, "ticket", "t-1", "ref-1")
            .unwrap();

        let updates = vec![(
            id1.clone(),
            FanOutItemUpdate::Running {
                child_run_id: "run-child-abc".to_string(),
            },
        )];
        p.batch_update_fan_out_items(&updates).unwrap();

        let items = p.get_fan_out_items(&step_id, None).unwrap();
        let item = items.iter().find(|i| i.id == id1).unwrap();
        assert_eq!(item.status, "running");
        assert_eq!(item.child_run_id.as_deref(), Some("run-child-abc"));
        assert!(item.dispatched_at.is_some(), "dispatched_at must be set");
        assert!(item.completed_at.is_none(), "completed_at must not be set");
    }

    #[test]
    fn batch_update_fan_out_items_missing_item_returns_error() {
        use crate::traits::persistence::{FanOutItemStatus, FanOutItemUpdate};

        let (p, _step_id) = make_fan_out_db();

        let updates = vec![(
            "does-not-exist".to_string(),
            FanOutItemUpdate::Terminal {
                status: FanOutItemStatus::Completed,
            },
        )];
        assert!(
            p.batch_update_fan_out_items(&updates).is_err(),
            "should error for non-existent item"
        );
    }

    #[test]
    fn acquire_lease_succeeds_when_expired() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE workflow_runs (
                id TEXT PRIMARY KEY,
                owner_token TEXT,
                lease_until TEXT,
                generation INTEGER NOT NULL DEFAULT 0
            );
            -- Insert a row with an already-expired lease held by 'old-engine'.
            INSERT INTO workflow_runs (id, owner_token, lease_until, generation)
            VALUES ('run-exp', 'old-engine', datetime('now', '-1 second'), 5);",
        )
        .unwrap();
        let p = SqliteWorkflowPersistence::from_shared_connection(Arc::new(Mutex::new(conn)));

        let result = p.acquire_lease("run-exp", "new-engine", 60).unwrap();
        assert!(
            result.is_some(),
            "acquire on expired lease should succeed; got None"
        );
        assert_eq!(
            result,
            Some(6),
            "generation should be incremented from 5 to 6"
        );
    }
}
