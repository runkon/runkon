mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use runkon_flow::cancellation_reason::CancellationReason;
use runkon_flow::engine_error::EngineError;
use runkon_flow::persistence_memory::InMemoryWorkflowPersistence;
use runkon_flow::traits::action_executor::{
    ActionExecutor, ActionOutput, ActionParams, ExecutionContext,
};
use runkon_flow::traits::persistence::{NewRun, WorkflowPersistence};
use runkon_flow::types::WorkflowExecConfig;
use runkon_flow::FlowEngineBuilder;

use common::{call_node, make_def, make_state};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_run(persistence: &Arc<InMemoryWorkflowPersistence>) -> String {
    persistence
        .create_run(NewRun {
            workflow_name: "wf".to_string(),
            worktree_id: None,
            ticket_id: None,
            repo_id: None,
            parent_run_id: String::new(),
            dry_run: false,
            trigger: "test".to_string(),
            definition_snapshot: None,
            parent_workflow_run_id: None,
            target_label: None,
        })
        .unwrap()
        .id
}

/// Executor that sleeps for a fixed duration then returns Ok.
struct SleepExecutor {
    label: String,
    sleep_ms: u64,
}

impl SleepExecutor {
    fn new(name: &str, sleep_ms: u64) -> Self {
        Self {
            label: name.to_string(),
            sleep_ms,
        }
    }
}

impl ActionExecutor for SleepExecutor {
    fn name(&self) -> &str {
        &self.label
    }

    fn execute(
        &self,
        _ectx: &ExecutionContext,
        _params: &ActionParams,
    ) -> Result<ActionOutput, EngineError> {
        std::thread::sleep(Duration::from_millis(self.sleep_ms));
        Ok(ActionOutput::default())
    }
}

/// Executor that counts how many times it was called.
struct CountingExecutor {
    label: String,
    count: Arc<AtomicUsize>,
}

impl CountingExecutor {
    fn new(name: &str, count: Arc<AtomicUsize>) -> Self {
        Self {
            label: name.to_string(),
            count,
        }
    }
}

impl ActionExecutor for CountingExecutor {
    fn name(&self) -> &str {
        &self.label
    }

    fn execute(
        &self,
        _ectx: &ExecutionContext,
        _params: &ActionParams,
    ) -> Result<ActionOutput, EngineError> {
        self.count.fetch_add(1, Ordering::SeqCst);
        Ok(ActionOutput::default())
    }
}

/// Executor that steals the lease mid-execution to simulate another engine claiming it.
///
/// Timeline:
/// 1. Sleeps `steal_after_ms` to let the refresh thread run at least once.
/// 2. Calls `expire_and_steal_lease` so the next refresh sees `Ok(None)`.
/// 3. Sleeps `hold_after_steal_ms` to give the refresh thread time to detect the steal.
/// 4. Returns Ok.
struct LeaseStealingExecutor {
    label: String,
    persistence: Arc<InMemoryWorkflowPersistence>,
    run_id: String,
    steal_after_ms: u64,
    hold_after_steal_ms: u64,
}

impl ActionExecutor for LeaseStealingExecutor {
    fn name(&self) -> &str {
        &self.label
    }

    fn execute(
        &self,
        _ectx: &ExecutionContext,
        _params: &ActionParams,
    ) -> Result<ActionOutput, EngineError> {
        std::thread::sleep(Duration::from_millis(self.steal_after_ms));
        self.persistence
            .expire_and_steal_lease(&self.run_id, "steal-token");
        std::thread::sleep(Duration::from_millis(self.hold_after_steal_ms));
        Ok(ActionOutput::default())
    }
}

// ---------------------------------------------------------------------------
// Test 1: lease is renewed while a slow step executes
// ---------------------------------------------------------------------------

/// Verifies that the refresh thread actually advances `lease_until`.
///
/// A single slow step runs for 3× the refresh interval. After the run, the
/// stored `lease_until` must be strictly later than the time the run started,
/// proving the refresh thread fired and extended the expiry.
#[test]
fn lease_renewed_under_long_running_workflow() {
    // Short intervals so the test finishes quickly.
    // refresh_interval = 50ms, TTL = 1s, step sleeps 200ms (4× refresh interval).
    let refresh_interval = Duration::from_millis(50);
    let lease_ttl_secs: i64 = 1;
    let step_sleep_ms = 200u64;

    let persistence = Arc::new(InMemoryWorkflowPersistence::new());
    let run_id = make_run(&persistence);

    let engine = FlowEngineBuilder::new()
        .action(Box::new(SleepExecutor::new("slow", step_sleep_ms)))
        .build()
        .unwrap();

    let def = make_def("wf", vec![call_node("slow")]);

    let mut state = make_state("wf", Arc::clone(&persistence), {
        let mut m: HashMap<String, Box<dyn ActionExecutor>> = HashMap::new();
        m.insert(
            "slow".to_string(),
            Box::new(SleepExecutor::new("slow", step_sleep_ms)),
        );
        m
    });
    state.workflow_run_id = run_id.clone();
    state.exec_config = WorkflowExecConfig {
        lease_ttl_secs,
        lease_refresh_interval: refresh_interval,
        ..WorkflowExecConfig::default()
    };

    let t_before = chrono::Utc::now();

    let result = engine.run(&def, &mut state);
    assert!(
        result.is_ok(),
        "run should complete successfully: {:?}",
        result
    );

    let run = persistence.get_run(&run_id).unwrap().unwrap();
    let lease_until_str = run
        .lease_until
        .expect("lease_until should be set after a run");
    let lease_until = chrono::DateTime::parse_from_rfc3339(&lease_until_str)
        .expect("lease_until should be valid RFC3339")
        .with_timezone(&chrono::Utc);

    // The initial lease_until was approximately t_before + lease_ttl_secs.
    // After at least 3 refreshes at 50ms each, lease_until ≈ t_last_refresh + lease_ttl_secs.
    // We assert that lease_until > t_before + lease_ttl_secs, meaning it was pushed forward.
    let initial_deadline = t_before + chrono::Duration::seconds(lease_ttl_secs);
    assert!(
        lease_until > initial_deadline,
        "lease_until ({lease_until}) should be strictly after the initial deadline \
         ({initial_deadline}), proving the refresh thread extended it"
    );
}

// ---------------------------------------------------------------------------
// Test 2: forced lease steal aborts the engine cleanly
// ---------------------------------------------------------------------------

/// Verifies the no-duplicate-steps guarantee.
///
/// Step 1 steals the lease mid-execution. The refresh thread detects the theft
/// at its next tick and cancels the engine with `LeaseLost`. Steps 2 and 3 must
/// never be started.
#[test]
fn forced_lease_steal_aborts_engine_cleanly() {
    // refresh_interval = 60ms. The LeaseStealingExecutor:
    //   - sleeps 20ms before stealing (well within first refresh window)
    //   - holds for 120ms after the steal (≥ 2 refresh ticks) so the thread has time to react
    let refresh_interval = Duration::from_millis(60);

    let persistence = Arc::new(InMemoryWorkflowPersistence::new());
    let run_id = make_run(&persistence);

    let step2_count = Arc::new(AtomicUsize::new(0));
    let step3_count = Arc::new(AtomicUsize::new(0));

    let engine = FlowEngineBuilder::new()
        .action(Box::new(LeaseStealingExecutor {
            label: "step1".to_string(),
            persistence: Arc::clone(&persistence),
            run_id: run_id.clone(),
            steal_after_ms: 20,
            hold_after_steal_ms: 120,
        }))
        .action(Box::new(CountingExecutor::new(
            "step2",
            Arc::clone(&step2_count),
        )))
        .action(Box::new(CountingExecutor::new(
            "step3",
            Arc::clone(&step3_count),
        )))
        .build()
        .unwrap();

    let def = make_def(
        "wf",
        vec![call_node("step1"), call_node("step2"), call_node("step3")],
    );

    let mut state = make_state(
        "wf",
        Arc::clone(&persistence),
        // make_state creates an action registry too; we override it via the engine's registry.
        // The engine validates against state.action_registry, so we need all three steps there.
        {
            let mut m: HashMap<String, Box<dyn ActionExecutor>> = HashMap::new();
            m.insert(
                "step1".to_string(),
                Box::new(LeaseStealingExecutor {
                    label: "step1".to_string(),
                    persistence: Arc::clone(&persistence),
                    run_id: run_id.clone(),
                    steal_after_ms: 20,
                    hold_after_steal_ms: 120,
                }),
            );
            m.insert(
                "step2".to_string(),
                Box::new(CountingExecutor::new("step2", Arc::clone(&step2_count))),
            );
            m.insert(
                "step3".to_string(),
                Box::new(CountingExecutor::new("step3", Arc::clone(&step3_count))),
            );
            m
        },
    );
    state.workflow_run_id = run_id.clone();
    state.exec_config = WorkflowExecConfig {
        lease_refresh_interval: refresh_interval,
        ..WorkflowExecConfig::default()
    };

    let result = engine.run(&def, &mut state);

    // The engine must return Err(Cancelled(LeaseLost)).
    assert!(
        matches!(
            result,
            Err(EngineError::Cancelled(CancellationReason::LeaseLost))
        ),
        "expected Err(Cancelled(LeaseLost)), got: {:?}",
        result
    );

    // Steps 2 and 3 must never have executed.
    assert_eq!(
        step2_count.load(Ordering::SeqCst),
        0,
        "step2 must not execute after lease was stolen"
    );
    assert_eq!(
        step3_count.load(Ordering::SeqCst),
        0,
        "step3 must not execute after lease was stolen"
    );
}

// ---------------------------------------------------------------------------
// Test 3: refresh thread terminates on normal completion (no thread leaks)
// ---------------------------------------------------------------------------

/// Runs 20 fast single-step workflows through the same FlowEngine and verifies
/// that all refresh threads exit cleanly (no panics surfaced via join).
#[test]
fn refresh_thread_terminates_on_normal_completion() {
    let refresh_interval = Duration::from_millis(500); // longer than step; thread never fires
    let engine = FlowEngineBuilder::new()
        .action(Box::new(CountingExecutor::new(
            "fast",
            Arc::new(AtomicUsize::new(0)),
        )))
        .build()
        .unwrap();

    let def = make_def("wf", vec![call_node("fast")]);

    for _ in 0..20 {
        let persistence = Arc::new(InMemoryWorkflowPersistence::new());
        let run_id = make_run(&persistence);

        let mut state = make_state("wf", Arc::clone(&persistence), {
            let mut m: HashMap<String, Box<dyn ActionExecutor>> = HashMap::new();
            m.insert(
                "fast".to_string(),
                Box::new(CountingExecutor::new("fast", Arc::new(AtomicUsize::new(0)))),
            );
            m
        });
        state.workflow_run_id = run_id;
        state.exec_config = WorkflowExecConfig {
            lease_refresh_interval: refresh_interval,
            ..WorkflowExecConfig::default()
        };

        // run() internally joins the refresh thread — any thread panic surfaces as Err here.
        let result = engine.run(&def, &mut state);
        assert!(
            result.is_ok(),
            "run should complete successfully: {:?}",
            result
        );
    }
    // If we reach here, all 20 runs completed without deadlock or thread panic.
}
