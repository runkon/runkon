#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use chrono::Utc;

use crate::cancellation_reason::CancellationReason;
use crate::engine_error::EngineError;
use crate::status::{WorkflowRunStatus, WorkflowStepStatus};
use crate::traits::persistence::{
    gate_approval_state_from_fields, FanOutItemStatus, FanOutItemUpdate, GateApprovalState, NewRun,
    NewStep, StepUpdate, WorkflowPersistence,
};
use crate::types::{FanOutItemRow, WorkflowRun, WorkflowRunStep};

struct InMemoryStore {
    runs: HashMap<String, WorkflowRun>,
    steps: HashMap<String, WorkflowRunStep>,
    fan_out_items: HashMap<String, FanOutItemRow>,
    /// Secondary index: step_run_id → (item_type, item_id) → fan_out_item id for O(1)
    /// idempotency check.
    fan_out_index: HashMap<String, HashMap<(String, String), String>>,
    /// Insertion-order list of fan_out_item ids; used to return items in stable order
    /// (mirrors real SQLite behaviour where rows sort by rowid = insertion order).
    fan_out_order: Vec<String>,
}

/// In-memory implementation of `WorkflowPersistence` for test isolation.
///
/// All state is held in a `Mutex<InMemoryStore>` and discarded when the struct
/// is dropped. No SQLite or filesystem access is required.
pub struct InMemoryWorkflowPersistence {
    store: Mutex<InMemoryStore>,
    /// When `true`, `get_fan_out_items` returns a `Persistence` error.
    fail_get_fan_out_items: AtomicBool,
    /// When `true`, `get_steps` returns a `Workflow` error.
    fail_get_steps: AtomicBool,
    /// When `true`, `acquire_lease` returns a `Persistence` error.
    fail_acquire_lease: AtomicBool,
}

impl InMemoryWorkflowPersistence {
    pub fn new() -> Self {
        Self {
            store: Mutex::new(InMemoryStore {
                runs: HashMap::new(),
                steps: HashMap::new(),
                fan_out_items: HashMap::new(),
                fan_out_index: HashMap::new(),
                fan_out_order: Vec::new(),
            }),
            fail_get_fan_out_items: AtomicBool::new(false),
            fail_get_steps: AtomicBool::new(false),
            fail_acquire_lease: AtomicBool::new(false),
        }
    }
}

impl Default for InMemoryWorkflowPersistence {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryWorkflowPersistence {
    /// Inject a failure into `get_fan_out_items`. When `fail` is `true`, the next
    /// call to `get_fan_out_items` returns `EngineError::Persistence`.
    pub fn set_fail_get_fan_out_items(&self, fail: bool) {
        self.fail_get_fan_out_items
            .store(fail, std::sync::atomic::Ordering::Relaxed);
    }

    /// Inject a failure into `get_steps`. When `fail` is `true`, every call to
    /// `get_steps` returns `EngineError::Workflow`.
    pub fn set_fail_get_steps(&self, fail: bool) {
        self.fail_get_steps
            .store(fail, std::sync::atomic::Ordering::Relaxed);
    }

    /// Inject a failure into `acquire_lease`. When `fail` is `true`, every call
    /// to `acquire_lease` returns `EngineError::Persistence`. Used to test the
    /// refresh-thread DB error path.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn set_fail_acquire_lease(&self, fail: bool) {
        self.fail_acquire_lease
            .store(fail, std::sync::atomic::Ordering::Relaxed);
    }

    /// Test helper: insert a minimal `WorkflowRun` with the given `id` so that
    /// `acquire_lease` can find it. Use this in tests that call `FlowEngine::run`
    /// or `FlowEngine::resume` against an `InMemoryWorkflowPersistence` without
    /// going through `create_run` (which generates its own id).
    #[cfg(test)]
    pub fn seed_run(&self, id: &str) {
        let now = chrono::Utc::now().to_rfc3339();
        let run = crate::types::WorkflowRun {
            id: id.to_string(),
            workflow_name: String::new(),
            worktree_id: None,
            parent_run_id: String::new(),
            status: crate::status::WorkflowRunStatus::Pending,
            dry_run: false,
            trigger: "test".to_string(),
            started_at: now,
            ended_at: None,
            result_summary: None,
            error: None,
            definition_snapshot: None,
            inputs: std::collections::HashMap::new(),
            ticket_id: None,
            repo_id: None,
            parent_workflow_run_id: None,
            target_label: None,
            default_bot_name: None,
            iteration: 0,
            blocked_on: None,
            workflow_title: None,
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
        };
        self.store.lock().unwrap().runs.insert(id.to_string(), run);
    }

    /// Test helper: forcibly expire the current lease for `run_id`, then immediately
    /// steal it with `new_token`. Used to simulate another engine claiming the lease
    /// while the original engine is still running.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn expire_and_steal_lease(&self, run_id: &str, new_token: &str) {
        {
            let mut store = self.store.lock().unwrap();
            if let Some(run) = store.runs.get_mut(run_id) {
                run.lease_until = Some("1970-01-01T00:00:00Z".to_string());
            }
        }
        self.acquire_lease(run_id, new_token, 3600).unwrap();
    }

    /// Test helper: directly set the metric fields on a step so that
    /// `restore_completed_step` can be tested with non-None metric values.
    #[cfg(test)]
    pub fn set_step_metrics_for_test(
        &self,
        step_id: &str,
        cost_usd: Option<f64>,
        num_turns: Option<i64>,
        duration_ms: Option<i64>,
        input_tokens: Option<i64>,
        output_tokens: Option<i64>,
    ) {
        let mut store = self.store.lock().unwrap();
        if let Some(step) = store.steps.get_mut(step_id) {
            step.cost_usd = cost_usd;
            step.num_turns = num_turns;
            step.duration_ms = duration_ms;
            step.input_tokens = input_tokens;
            step.output_tokens = output_tokens;
        }
    }
}

fn lock_err() -> EngineError {
    EngineError::Persistence("InMemoryWorkflowPersistence: mutex poisoned".into())
}

fn format_gate_selection_context(selections: &[String]) -> String {
    let mut s = "User selected the following items:\n".to_string();
    for item in selections {
        s.push_str(&format!("- {item}\n"));
    }
    s
}

impl InMemoryWorkflowPersistence {
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, InMemoryStore>, EngineError> {
        self.store.lock().map_err(|_| lock_err())
    }

    fn with_store<F, T>(&self, f: F) -> Result<T, EngineError>
    where
        F: FnOnce(&mut InMemoryStore) -> Result<T, EngineError>,
    {
        let mut store = self.store.lock().map_err(|_| lock_err())?;
        f(&mut store)
    }
}

impl WorkflowPersistence for InMemoryWorkflowPersistence {
    fn create_run(&self, new_run: NewRun) -> Result<WorkflowRun, EngineError> {
        let id = ulid::Ulid::new().to_string();
        let now = Utc::now().to_rfc3339();
        let run = WorkflowRun {
            id: id.clone(),
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
            workflow_title: None,
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
        };
        let mut store = self.lock()?;
        store.runs.insert(id, run.clone());
        Ok(run)
    }

    fn get_run(&self, run_id: &str) -> Result<Option<WorkflowRun>, EngineError> {
        let store = self.lock()?;
        Ok(store.runs.get(run_id).cloned())
    }

    fn list_active_runs(
        &self,
        statuses: &[WorkflowRunStatus],
    ) -> Result<Vec<WorkflowRun>, EngineError> {
        let store = self.lock()?;
        let mut runs: Vec<WorkflowRun> = store
            .runs
            .values()
            .filter(|r| statuses.contains(&r.status))
            .cloned()
            .collect();
        runs.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        Ok(runs)
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
                "Use set_waiting_blocked_on to transition to Waiting status".into(),
            ));
        }
        let mut store = self.lock()?;
        let run = store
            .runs
            .get_mut(run_id)
            .ok_or_else(|| EngineError::Persistence(format!("run {run_id} not found")))?;
        let now = Utc::now().to_rfc3339();
        let is_terminal = matches!(
            status,
            WorkflowRunStatus::Completed | WorkflowRunStatus::Failed | WorkflowRunStatus::Cancelled
        );
        run.status = status;
        run.result_summary = result_summary.map(String::from);
        run.error = error.map(String::from);
        if is_terminal {
            run.ended_at = Some(now);
        }
        Ok(())
    }

    fn insert_step(&self, new_step: NewStep) -> Result<String, EngineError> {
        let id = ulid::Ulid::new().to_string();
        let now = Utc::now().to_rfc3339();
        let (status, started_at, retry_count) = if let Some(rc) = new_step.retry_count {
            (WorkflowStepStatus::Running, Some(now), rc)
        } else {
            (WorkflowStepStatus::Pending, None, 0)
        };
        let step = WorkflowRunStep {
            id: id.clone(),
            workflow_run_id: new_step.workflow_run_id,
            step_name: new_step.step_name,
            role: new_step.role,
            can_commit: new_step.can_commit,
            condition_expr: None,
            status,
            child_run_id: None,
            position: new_step.position,
            started_at,
            ended_at: None,
            result_text: None,
            condition_met: None,
            iteration: new_step.iteration,
            parallel_group_id: None,
            context_out: None,
            markers_out: None,
            retry_count,
            gate_type: None,
            gate_prompt: None,
            gate_timeout: None,
            gate_approved_by: None,
            gate_approved_at: None,
            gate_feedback: None,
            gate_options: None,
            gate_selections: None,
            input_tokens: None,
            output_tokens: None,
            cache_read_input_tokens: None,
            cache_creation_input_tokens: None,
            cost_usd: None,
            num_turns: None,
            duration_ms: None,
            fan_out_total: None,
            fan_out_completed: 0,
            fan_out_failed: 0,
            fan_out_skipped: 0,
            structured_output: None,
            output_file: None,
            step_error: None,
        };
        let mut store = self.lock()?;
        store.steps.insert(id.clone(), step);
        Ok(id)
    }

    fn update_step(&self, step_id: &str, update: StepUpdate) -> Result<(), EngineError> {
        let mut store = self.lock()?;

        // Check generation before touching the step.
        let run_id = store
            .steps
            .get(step_id)
            .ok_or_else(|| EngineError::Persistence(format!("step {step_id} not found")))?
            .workflow_run_id
            .clone();
        let run_generation = store.runs.get(&run_id).map(|r| r.generation).unwrap_or(0);
        if run_generation != update.generation {
            return Err(EngineError::Cancelled(CancellationReason::LeaseLost));
        }

        let step = store
            .steps
            .get_mut(step_id)
            .expect("step existence verified above when reading workflow_run_id");
        let now = Utc::now().to_rfc3339();
        let is_starting = update.status == WorkflowStepStatus::Running
            || update.status == WorkflowStepStatus::Waiting;
        let is_terminal = matches!(
            update.status,
            WorkflowStepStatus::Completed
                | WorkflowStepStatus::Failed
                | WorkflowStepStatus::Skipped
                | WorkflowStepStatus::TimedOut
        );
        step.status = update.status;
        step.child_run_id = update.child_run_id;
        if is_starting {
            step.started_at = Some(now);
        } else if is_terminal {
            step.ended_at = Some(now);
            step.result_text = update.result_text;
            step.context_out = update.context_out;
            step.markers_out = update.markers_out;
            if let Some(rc) = update.retry_count {
                step.retry_count = rc;
            }
            step.structured_output = update.structured_output;
            step.step_error = update.step_error;
        }
        Ok(())
    }

    fn get_steps(&self, run_id: &str) -> Result<Vec<WorkflowRunStep>, EngineError> {
        if self.fail_get_steps.load(Ordering::Relaxed) {
            return Err(EngineError::Workflow("injected get_steps failure".into()));
        }
        let store = self.lock()?;
        let mut steps: Vec<WorkflowRunStep> = store
            .steps
            .values()
            .filter(|s| s.workflow_run_id == run_id)
            .cloned()
            .collect();
        steps.sort_by_key(|s| s.position);
        Ok(steps)
    }

    fn insert_fan_out_item(
        &self,
        step_run_id: &str,
        item_type: &str,
        item_id: &str,
        item_ref: &str,
    ) -> Result<String, EngineError> {
        let mut store = self.lock()?;
        let dedup_key = (item_type.to_string(), item_id.to_string());
        // Idempotent: O(1) lookup via nested index.
        if let Some(existing_id) = store
            .fan_out_index
            .get(step_run_id)
            .and_then(|m| m.get(&dedup_key))
        {
            return Ok(existing_id.clone());
        }
        let id = ulid::Ulid::new().to_string();
        store
            .fan_out_index
            .entry(step_run_id.to_string())
            .or_default()
            .insert(dedup_key, id.clone());
        store.fan_out_order.push(id.clone());
        store.fan_out_items.insert(
            id.clone(),
            FanOutItemRow {
                id: id.clone(),
                step_run_id: step_run_id.to_string(),
                item_type: item_type.to_string(),
                item_id: item_id.to_string(),
                item_ref: item_ref.to_string(),
                child_run_id: None,
                status: "pending".to_string(),
                dispatched_at: None,
                completed_at: None,
            },
        );
        Ok(id)
    }

    fn update_fan_out_item(
        &self,
        item_id: &str,
        update: FanOutItemUpdate,
    ) -> Result<(), EngineError> {
        let mut store = self.lock()?;
        let item = store
            .fan_out_items
            .get_mut(item_id)
            .ok_or_else(|| EngineError::Persistence(format!("fan-out item {item_id} not found")))?;
        let now = Utc::now().to_rfc3339();
        match update {
            FanOutItemUpdate::Running { child_run_id } => {
                item.status = "running".to_string();
                item.child_run_id = Some(child_run_id);
                item.dispatched_at = Some(now);
            }
            FanOutItemUpdate::Terminal { status } => {
                item.status = status.as_str().to_string();
                item.completed_at = Some(now);
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
        let mut store = self.lock()?;
        let now = Utc::now().to_rfc3339();
        for (item_id, update) in updates {
            let item = store.fan_out_items.get_mut(item_id).ok_or_else(|| {
                EngineError::Persistence(format!("fan-out item {item_id} not found"))
            })?;
            match update {
                FanOutItemUpdate::Running { child_run_id } => {
                    item.status = "running".to_string();
                    item.child_run_id = Some(child_run_id.clone());
                    item.dispatched_at = Some(now.clone());
                }
                FanOutItemUpdate::Terminal { status } => {
                    item.status = status.as_str().to_string();
                    item.completed_at = Some(now.clone());
                }
            }
        }
        Ok(())
    }

    fn get_fan_out_items(
        &self,
        step_run_id: &str,
        status_filter: Option<FanOutItemStatus>,
    ) -> Result<Vec<FanOutItemRow>, EngineError> {
        if self.fail_get_fan_out_items.load(Ordering::Relaxed) {
            return Err(EngineError::Persistence(
                "injected get_fan_out_items failure".into(),
            ));
        }
        let store = self.lock()?;
        // Iterate in insertion order (mirrors SQLite rowid order) so callers get a
        // stable, deterministic sequence regardless of ULID timestamp collisions.
        let items: Vec<FanOutItemRow> = store
            .fan_out_order
            .iter()
            .filter_map(|id| store.fan_out_items.get(id))
            .filter(|i| {
                i.step_run_id == step_run_id
                    && status_filter
                        .as_ref()
                        .is_none_or(|s| i.status == s.as_str())
            })
            .cloned()
            .collect();
        Ok(items)
    }

    fn get_gate_approval(&self, step_id: &str) -> Result<GateApprovalState, EngineError> {
        let store = self.lock()?;
        let Some(step) = store.steps.get(step_id) else {
            return Ok(GateApprovalState::Pending);
        };
        let selections = step.gate_selections.as_deref().and_then(|s| {
            serde_json::from_str::<Vec<String>>(s)
                .map_err(|e| {
                    tracing::warn!(
                        "get_gate_approval: malformed gate_selections JSON for step '{step_id}': {e}"
                    );
                    e
                })
                .ok()
        });
        Ok(gate_approval_state_from_fields(
            step.gate_approved_at.as_deref(),
            step.status.clone(),
            step.gate_feedback.clone(),
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
        let mut store = self.lock()?;
        let now = Utc::now().to_rfc3339();
        let step = store
            .steps
            .get_mut(step_id)
            .ok_or_else(|| EngineError::Persistence(format!("step {step_id} not found")))?;
        step.gate_approved_at = Some(now.clone());
        step.gate_approved_by = Some(approved_by.to_string());
        step.gate_feedback = feedback.map(String::from);
        step.gate_selections = selections
            .map(|s| serde_json::to_string(s).expect("Vec<String> serialization is infallible"));
        if let Some(items) = selections.filter(|s| !s.is_empty()) {
            step.context_out = Some(format_gate_selection_context(items));
        }
        step.status = WorkflowStepStatus::Completed;
        step.ended_at = Some(now);
        Ok(())
    }

    fn reject_gate(
        &self,
        step_id: &str,
        rejected_by: &str,
        feedback: Option<&str>,
    ) -> Result<(), EngineError> {
        let mut store = self.lock()?;
        let now = Utc::now().to_rfc3339();
        let step = store
            .steps
            .get_mut(step_id)
            .ok_or_else(|| EngineError::Persistence(format!("step {step_id} not found")))?;
        step.gate_approved_by = Some(rejected_by.to_string());
        step.gate_feedback = feedback.map(String::from);
        step.status = WorkflowStepStatus::Failed;
        step.ended_at = Some(now);
        Ok(())
    }

    fn is_run_cancelled(&self, run_id: &str) -> Result<bool, EngineError> {
        let store = self.lock()?;
        Ok(store
            .runs
            .get(run_id)
            .map(|r| {
                matches!(
                    r.status,
                    WorkflowRunStatus::Cancelling | WorkflowRunStatus::Cancelled
                )
            })
            .unwrap_or(false))
    }

    fn acquire_lease(
        &self,
        run_id: &str,
        token: &str,
        ttl_seconds: i64,
    ) -> Result<Option<i64>, EngineError> {
        if self.fail_acquire_lease.load(Ordering::Relaxed) {
            return Err(EngineError::Persistence(
                "simulated acquire_lease failure".to_string(),
            ));
        }
        let mut store = self.store.lock().map_err(|_| lock_err())?;
        let now = chrono::Utc::now();

        // If the run doesn't exist, return None (no rows updated) — consistent with the SQLite
        // implementation which returns None when the UPDATE matches 0 rows.
        let Some(run) = store.runs.get_mut(run_id) else {
            return Ok(None);
        };

        let can_claim = match &run.owner_token {
            None => true,
            Some(t) if t == token => true,
            Some(_) => run.lease_until.as_deref().is_some_and(|until| {
                chrono::DateTime::parse_from_rfc3339(until)
                    .map(|exp| exp < now)
                    .unwrap_or(false)
            }),
        };

        if !can_claim {
            return Ok(None);
        }

        // Only increment generation when ownership actually changes.
        if run.owner_token.as_deref() != Some(token) {
            run.generation += 1;
        }
        run.owner_token = Some(token.to_string());
        run.lease_until = Some((now + chrono::Duration::seconds(ttl_seconds)).to_rfc3339());
        Ok(Some(run.generation))
    }

    fn tick_heartbeat(&self, _run_id: &str) -> Result<(), EngineError> {
        Ok(())
    }

    fn persist_metrics(
        &self,
        _run_id: &str,
        _input_tokens: i64,
        _output_tokens: i64,
        _cache_read_input_tokens: i64,
        _cache_creation_input_tokens: i64,
        _cost_usd: f64,
        _num_turns: i64,
        _duration_ms: i64,
    ) -> Result<(), EngineError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::{WorkflowRunStatus, WorkflowStepStatus};
    use crate::traits::persistence::{
        FanOutItemStatus, FanOutItemUpdate, GateApprovalState, NewRun, NewStep, StepUpdate,
        WorkflowPersistence,
    };

    fn make_new_run(name: &str) -> NewRun {
        NewRun {
            workflow_name: name.to_string(),
            worktree_id: None,
            ticket_id: None,
            repo_id: None,
            parent_run_id: "parent-run".to_string(),
            dry_run: false,
            trigger: "test".to_string(),
            definition_snapshot: None,
            parent_workflow_run_id: None,
            target_label: None,
        }
    }

    fn make_new_step(run_id: &str, name: &str) -> NewStep {
        NewStep {
            workflow_run_id: run_id.to_string(),
            step_name: name.to_string(),
            role: "actor".to_string(),
            can_commit: false,
            position: 0,
            iteration: 0,
            retry_count: None,
        }
    }

    #[test]
    fn test_create_and_get_run() {
        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("my-workflow")).unwrap();
        assert_eq!(run.workflow_name, "my-workflow");
        assert_eq!(run.status, WorkflowRunStatus::Pending);

        let got = p.get_run(&run.id).unwrap();
        assert!(got.is_some());
        assert_eq!(got.unwrap().id, run.id);
    }

    #[test]
    fn test_get_run_not_found_returns_none() {
        let p = InMemoryWorkflowPersistence::new();
        let got = p.get_run("nonexistent").unwrap();
        assert!(got.is_none());
    }

    #[test]
    fn test_list_active_runs_by_status() {
        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        p.update_run_status(&run.id, WorkflowRunStatus::Running, None, None)
            .unwrap();

        let running = p.list_active_runs(&[WorkflowRunStatus::Running]).unwrap();
        assert_eq!(running.len(), 1);

        let pending = p.list_active_runs(&[WorkflowRunStatus::Pending]).unwrap();
        assert_eq!(pending.len(), 0);
    }

    #[test]
    fn test_update_run_status_waiting_rejected() {
        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        let err = p
            .update_run_status(&run.id, WorkflowRunStatus::Waiting, None, None)
            .unwrap_err();
        assert!(matches!(err, EngineError::Persistence(_)));
    }

    #[test]
    fn test_insert_pending_step() {
        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        let step_id = p.insert_step(make_new_step(&run.id, "step1")).unwrap();
        assert!(!step_id.is_empty());

        let steps = p.get_steps(&run.id).unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].step_name, "step1");
        assert_eq!(steps[0].status, WorkflowStepStatus::Pending);
        assert!(steps[0].started_at.is_none());
    }

    #[test]
    fn test_insert_running_step() {
        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        let step_id = p
            .insert_step(NewStep {
                workflow_run_id: run.id.clone(),
                step_name: "step1".to_string(),
                role: "actor".to_string(),
                can_commit: false,
                position: 0,
                iteration: 0,
                retry_count: Some(0),
            })
            .unwrap();

        let steps = p.get_steps(&run.id).unwrap();
        assert_eq!(steps[0].id, step_id);
        assert_eq!(steps[0].status, WorkflowStepStatus::Running);
        assert!(steps[0].started_at.is_some());
    }

    #[test]
    fn test_update_step_to_completed() {
        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        let step_id = p.insert_step(make_new_step(&run.id, "s")).unwrap();

        p.update_step(
            &step_id,
            StepUpdate {
                generation: 0,
                status: WorkflowStepStatus::Completed,
                child_run_id: None,
                result_text: Some("done".to_string()),
                context_out: None,
                markers_out: None,
                retry_count: None,
                structured_output: None,
                step_error: None,
            },
        )
        .unwrap();

        let steps = p.get_steps(&run.id).unwrap();
        assert_eq!(steps[0].status, WorkflowStepStatus::Completed);
        assert_eq!(steps[0].result_text.as_deref(), Some("done"));
        assert!(steps[0].ended_at.is_some());
    }

    #[test]
    fn update_step_returns_lease_lost_on_stale_generation() {
        use crate::cancellation_reason::CancellationReason;
        use crate::engine_error::EngineError;

        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        // Acquire lease → generation becomes 1.
        p.acquire_lease(&run.id, "tok", 60).unwrap();
        let step_id = p.insert_step(make_new_step(&run.id, "s")).unwrap();

        // Supply stale generation=0; DB has generation=1.
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
            "stale generation should return LeaseLost; got {result:?}"
        );

        // Correct generation passes.
        p.update_step(
            &step_id,
            StepUpdate {
                generation: 1,
                status: WorkflowStepStatus::Completed,
                child_run_id: None,
                result_text: None,
                context_out: None,
                markers_out: None,
                retry_count: None,
                structured_output: None,
                step_error: None,
            },
        )
        .unwrap();
    }

    #[test]
    fn test_fan_out_item_lifecycle() {
        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        let step_id = p.insert_step(make_new_step(&run.id, "foreach")).unwrap();

        let item_id = p
            .insert_fan_out_item(&step_id, "ticket", "t-1", "ref-1")
            .unwrap();
        assert!(!item_id.is_empty());

        let all = p.get_fan_out_items(&step_id, None).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].status, "pending");

        p.update_fan_out_item(
            &item_id,
            FanOutItemUpdate::Running {
                child_run_id: "child-run-1".to_string(),
            },
        )
        .unwrap();
        let running = p
            .get_fan_out_items(&step_id, Some(FanOutItemStatus::Running))
            .unwrap();
        assert_eq!(running.len(), 1);
        assert_eq!(running[0].child_run_id.as_deref(), Some("child-run-1"));

        p.update_fan_out_item(
            &item_id,
            FanOutItemUpdate::Terminal {
                status: FanOutItemStatus::Completed,
            },
        )
        .unwrap();
        let completed = p
            .get_fan_out_items(&step_id, Some(FanOutItemStatus::Completed))
            .unwrap();
        assert_eq!(completed.len(), 1);
        assert!(completed[0].completed_at.is_some());
    }

    #[test]
    fn test_fan_out_item_idempotent_insert() {
        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        let step_id = p.insert_step(make_new_step(&run.id, "s")).unwrap();

        p.insert_fan_out_item(&step_id, "ticket", "t-1", "ref-1")
            .unwrap();
        p.insert_fan_out_item(&step_id, "ticket", "t-1", "ref-1")
            .unwrap();

        let items = p.get_fan_out_items(&step_id, None).unwrap();
        assert_eq!(items.len(), 1, "duplicate insert should be ignored");
    }

    #[test]
    fn test_fan_out_item_idempotent_respects_item_type() {
        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        let step_id = p.insert_step(make_new_step(&run.id, "s")).unwrap();

        // same step_run_id + item_id, different item_type → two distinct items
        p.insert_fan_out_item(&step_id, "ticket", "t-1", "ref-1")
            .unwrap();
        p.insert_fan_out_item(&step_id, "worktree", "t-1", "ref-2")
            .unwrap();

        let items = p.get_fan_out_items(&step_id, None).unwrap();
        assert_eq!(
            items.len(),
            2,
            "different item_type should create distinct items"
        );

        // idempotency: re-inserting both should not change count
        p.insert_fan_out_item(&step_id, "ticket", "t-1", "ref-1")
            .unwrap();
        p.insert_fan_out_item(&step_id, "worktree", "t-1", "ref-2")
            .unwrap();

        let items = p.get_fan_out_items(&step_id, None).unwrap();
        assert_eq!(items.len(), 2, "re-inserts should be idempotent");
    }

    #[test]
    fn test_gate_pending_by_default() {
        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        let step_id = p.insert_step(make_new_step(&run.id, "gate")).unwrap();

        let state = p.get_gate_approval(&step_id).unwrap();
        assert!(
            matches!(state, GateApprovalState::Pending),
            "newly inserted gate step must be Pending"
        );
    }

    #[test]
    fn test_gate_missing_step_is_pending() {
        let p = InMemoryWorkflowPersistence::new();
        let state = p.get_gate_approval("nonexistent-step").unwrap();
        assert!(matches!(state, GateApprovalState::Pending));
    }

    #[test]
    fn test_approve_gate() {
        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        let step_id = p.insert_step(make_new_step(&run.id, "gate")).unwrap();

        p.approve_gate(&step_id, "alice", Some("looks good"), None)
            .unwrap();

        let state = p.get_gate_approval(&step_id).unwrap();
        match state {
            GateApprovalState::Approved { feedback, .. } => {
                assert_eq!(feedback.as_deref(), Some("looks good"));
            }
            other => panic!("expected Approved, got {other:?}"),
        }
    }

    #[test]
    fn test_approve_gate_with_selections() {
        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        let step_id = p.insert_step(make_new_step(&run.id, "gate")).unwrap();

        let selections = vec!["option-a".to_string(), "option-b".to_string()];
        p.approve_gate(&step_id, "bob", None, Some(&selections))
            .unwrap();

        let state = p.get_gate_approval(&step_id).unwrap();
        match state {
            GateApprovalState::Approved {
                selections: Some(s),
                ..
            } => {
                assert_eq!(s, selections);
            }
            other => panic!("expected Approved with selections, got {other:?}"),
        }
    }

    #[test]
    fn test_reject_gate() {
        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        let step_id = p.insert_step(make_new_step(&run.id, "gate")).unwrap();

        p.reject_gate(&step_id, "carol", Some("not ready")).unwrap();

        let state = p.get_gate_approval(&step_id).unwrap();
        match state {
            GateApprovalState::Rejected { feedback } => {
                assert_eq!(feedback.as_deref(), Some("not ready"));
            }
            other => panic!("expected Rejected, got {other:?}"),
        };

        let steps = p.get_steps(&run.id).unwrap();
        assert_eq!(steps[0].status, WorkflowStepStatus::Failed);
        assert_eq!(steps[0].gate_feedback.as_deref(), Some("not ready"));
    }

    #[test]
    fn test_is_run_cancelled_reflects_status() {
        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();

        // Pending → not cancelled
        assert!(!p.is_run_cancelled(&run.id).unwrap());

        // Cancelling → cancelled
        p.update_run_status(&run.id, WorkflowRunStatus::Cancelling, None, None)
            .unwrap();
        assert!(p.is_run_cancelled(&run.id).unwrap());

        // Cancelled → cancelled
        p.update_run_status(&run.id, WorkflowRunStatus::Cancelled, None, None)
            .unwrap();
        assert!(p.is_run_cancelled(&run.id).unwrap());
    }

    #[test]
    fn test_is_run_cancelled_unknown_run_returns_false() {
        let p = InMemoryWorkflowPersistence::new();
        assert!(!p.is_run_cancelled("nonexistent").unwrap());
    }

    #[test]
    fn test_tick_heartbeat_is_noop() {
        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        assert!(p.tick_heartbeat(&run.id).is_ok());
    }

    #[test]
    fn test_persist_metrics_is_noop() {
        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        assert!(p
            .persist_metrics(&run.id, 100, 200, 50, 25, 0.01, 3, 5000)
            .is_ok());
    }

    #[test]
    fn batch_update_fan_out_items_mixed_terminal_statuses() {
        use crate::traits::persistence::{FanOutItemStatus, FanOutItemUpdate};

        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        let step_id = p.insert_step(make_new_step(&run.id, "foreach")).unwrap();

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

        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        let step_id = p.insert_step(make_new_step(&run.id, "foreach")).unwrap();
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

        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        let step_id = p.insert_step(make_new_step(&run.id, "foreach")).unwrap();
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

        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        let _step_id = p.insert_step(make_new_step(&run.id, "foreach")).unwrap();

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
    fn test_acquire_lease_nonexistent_run_returns_none() {
        let p = InMemoryWorkflowPersistence::new();
        let result = p.acquire_lease("does-not-exist", "token-abc", 30);
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_acquire_lease_existing_run_returns_generation() {
        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        let result = p.acquire_lease(&run.id, "token-abc", 30).unwrap();
        assert_eq!(result, Some(1));
    }

    #[test]
    fn test_acquire_lease_same_token_is_idempotent() {
        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        p.acquire_lease(&run.id, "token-abc", 30).unwrap();
        // Same-token renewal must not increment generation.
        let result = p.acquire_lease(&run.id, "token-abc", 30).unwrap();
        assert_eq!(result, Some(1));
    }

    #[test]
    fn test_acquire_lease_conflict_returns_none() {
        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        p.acquire_lease(&run.id, "token-first", 3600).unwrap();
        let result = p.acquire_lease(&run.id, "token-second", 30).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_acquire_lease_expired_lease_allows_new_token() {
        let p = InMemoryWorkflowPersistence::new();
        let run = p.create_run(make_new_run("test")).unwrap();
        // Acquire with a negative TTL so the lease is already in the past.
        let gen1 = p.acquire_lease(&run.id, "token-first", -1).unwrap();
        assert_eq!(gen1, Some(1));
        // A different token should be able to claim the expired lease.
        let gen2 = p.acquire_lease(&run.id, "token-second", 30).unwrap();
        assert_eq!(gen2, Some(2));
    }
}
