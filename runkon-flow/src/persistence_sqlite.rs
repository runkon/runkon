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

use crate::engine_error::EngineError;
use crate::status::{WorkflowRunStatus, WorkflowStepStatus};
use crate::traits::persistence::{
    gate_approval_state_from_fields, FanOutItemRow, FanOutItemStatus, FanOutItemUpdate,
    GateApprovalState, NewRun, NewStep, StepUpdate, WorkflowPersistence,
};
use crate::types::{extract_workflow_title, BlockedOn, WorkflowRun, WorkflowRunStep};

// ---------------------------------------------------------------------------
// Column lists
// ---------------------------------------------------------------------------

/// Column list for `workflow_runs` SELECT queries.
const RUN_COLUMNS: &str =
    "id, workflow_name, worktree_id, parent_run_id, status, dry_run, trigger, \
     started_at, ended_at, result_summary, definition_snapshot, inputs, ticket_id, repo_id, \
     parent_workflow_run_id, target_label, default_bot_name, iteration, blocked_on, \
     total_input_tokens, total_output_tokens, total_cache_read_input_tokens, \
     total_cache_creation_input_tokens, total_turns, total_cost_usd, total_duration_ms, model, \
     error, dismissed";

/// Column list for `workflow_run_steps` SELECT queries (used by `row_to_step`).
const STEP_COLUMNS: &str =
    "id, workflow_run_id, step_name, role, can_commit, condition_expr, status, \
     child_run_id, position, started_at, ended_at, result_text, condition_met, \
     iteration, parallel_group_id, context_out, markers_out, retry_count, \
     gate_type, gate_prompt, gate_timeout, gate_approved_by, gate_approved_at, gate_feedback, \
     structured_output, output_file, gate_options, gate_selections, \
     fan_out_total, fan_out_completed, fan_out_failed, fan_out_skipped, step_error, \
     input_tokens, output_tokens, cache_read_input_tokens, cache_creation_input_tokens, \
     cost_usd, num_turns, duration_ms";

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
    let workflow_title = extract_workflow_title(definition_snapshot.as_deref());
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

        conn.execute(
            "INSERT INTO workflow_runs (id, workflow_name, worktree_id, ticket_id, repo_id, \
             parent_run_id, status, dry_run, trigger, started_at, definition_snapshot, \
             parent_workflow_run_id, target_label) \
             VALUES (:id, :workflow_name, :worktree_id, :ticket_id, :repo_id, :parent_run_id, \
             :status, :dry_run, :trigger, :started_at, :definition_snapshot, \
             :parent_workflow_run_id, :target_label)",
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
            ],
        )
        .map_err(db_err)?;

        let workflow_title = extract_workflow_title(new_run.definition_snapshot.as_deref());
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
        if update.status.is_starting() {
            let now = Utc::now().to_rfc3339();
            conn.execute(
                "UPDATE workflow_run_steps SET status = :status, child_run_id = :child_run_id, \
                 started_at = :started_at WHERE id = :id",
                named_params![
                    ":status": update.status,
                    ":child_run_id": update.child_run_id,
                    ":started_at": now,
                    ":id": step_id,
                ],
            )
            .map_err(db_err)?;
        } else if update.status.is_terminal() {
            let now = Utc::now().to_rfc3339();
            conn.execute(
                "UPDATE workflow_run_steps SET status = :status, \
                 child_run_id = COALESCE(:child_run_id, child_run_id), \
                 ended_at = :ended_at, result_text = :result_text, context_out = :context_out, \
                 markers_out = :markers_out, \
                 retry_count = COALESCE(:retry_count, retry_count), \
                 structured_output = :structured_output, step_error = :step_error \
                 WHERE id = :id",
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
                ],
            )
            .map_err(db_err)?;
        } else {
            conn.execute(
                "UPDATE workflow_run_steps SET status = :status WHERE id = :id",
                named_params![":status": update.status, ":id": step_id],
            )
            .map_err(db_err)?;
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
