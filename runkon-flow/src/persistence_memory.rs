#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use chrono::Utc;

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
    /// Secondary index: (step_run_id, item_id) → fan_out_item id for O(1) idempotency check.
    fan_out_index: HashMap<(String, String), String>,
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
}

fn lock_err() -> EngineError {
    EngineError::Persistence("InMemoryWorkflowPersistence: mutex poisoned".into())
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
        };
        let mut store = self.store.lock().map_err(|_| lock_err())?;
        store.runs.insert(id, run.clone());
        Ok(run)
    }

    fn get_run(&self, run_id: &str) -> Result<Option<WorkflowRun>, EngineError> {
        let store = self.store.lock().map_err(|_| lock_err())?;
        Ok(store.runs.get(run_id).cloned())
    }

    fn list_active_runs(
        &self,
        statuses: &[WorkflowRunStatus],
    ) -> Result<Vec<WorkflowRun>, EngineError> {
        let store = self.store.lock().map_err(|_| lock_err())?;
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
        let mut store = self.store.lock().map_err(|_| lock_err())?;
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
            fan_out_total: None,
            fan_out_completed: 0,
            fan_out_failed: 0,
            fan_out_skipped: 0,
            structured_output: None,
            output_file: None,
            step_error: None,
        };
        let mut store = self.store.lock().map_err(|_| lock_err())?;
        store.steps.insert(id.clone(), step);
        Ok(id)
    }

    fn update_step(&self, step_id: &str, update: StepUpdate) -> Result<(), EngineError> {
        let mut store = self.store.lock().map_err(|_| lock_err())?;
        let step = store
            .steps
            .get_mut(step_id)
            .ok_or_else(|| EngineError::Persistence(format!("step {step_id} not found")))?;
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
        let store = self.store.lock().map_err(|_| lock_err())?;
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
        let mut store = self.store.lock().map_err(|_| lock_err())?;
        // Idempotent: O(1) lookup via secondary index.
        let index_key = (step_run_id.to_string(), item_id.to_string());
        if let Some(existing_id) = store.fan_out_index.get(&index_key) {
            return Ok(existing_id.clone());
        }
        let id = ulid::Ulid::new().to_string();
        store.fan_out_index.insert(index_key, id.clone());
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
        let mut store = self.store.lock().map_err(|_| lock_err())?;
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
        let store = self.store.lock().map_err(|_| lock_err())?;
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
        let store = self.store.lock().map_err(|_| lock_err())?;
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
        let mut store = self.store.lock().map_err(|_| lock_err())?;
        let now = Utc::now().to_rfc3339();
        let step = store
            .steps
            .get_mut(step_id)
            .ok_or_else(|| EngineError::Persistence(format!("step {step_id} not found")))?;
        step.gate_approved_at = Some(now.clone());
        step.gate_approved_by = Some(approved_by.to_string());
        step.gate_feedback = feedback.map(String::from);
        step.gate_selections = selections.map(|s| serde_json::to_string(s).unwrap_or_default());
        if let Some(items) = selections.filter(|s| !s.is_empty()) {
            let mut out = String::from("User selected the following items:\n");
            for item in items {
                out.push_str(&format!("- {item}\n"));
            }
            step.context_out = Some(out);
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
        let mut store = self.store.lock().map_err(|_| lock_err())?;
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
        let store = self.store.lock().map_err(|_| lock_err())?;
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
}
