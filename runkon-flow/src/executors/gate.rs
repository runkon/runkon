use std::collections::HashMap;
use std::thread;
use std::time::Duration;

use crate::dsl::{GateNode, GateOptions, OnFailAction, OnTimeout, QUALITY_GATE_TYPE};
use crate::engine::{emit_event, ExecutionState};
use crate::engine_error::{EngineError, Result};
use crate::events::EngineEvent;
use crate::status::{WorkflowRunStatus, WorkflowStepStatus};
use crate::traits::gate_resolver::{GateParams, GatePoll, GateResolver};
use crate::traits::persistence::{GateApprovalState, GateOption, GateStepInfo, StepUpdate};

fn resume_run_status(state: &ExecutionState, gate_name: &str, context: &str) {
    if let Err(e) = state.persistence.update_run_status(
        &state.workflow_run_id,
        WorkflowRunStatus::Running,
        None,
        None,
    ) {
        tracing::warn!("Gate '{gate_name}': failed to update run status {context}: {e}");
    }
}

fn resolve_gate_options(state: &ExecutionState, node: &GateNode) -> Result<Vec<GateOption>> {
    let map: HashMap<String, String> = if let Some(ref gate_opts) = node.options {
        match gate_opts {
            GateOptions::Static(map) => map.clone(),
            GateOptions::StepRef(dotted) => {
                let dot = dotted.find('.').ok_or_else(|| {
                    EngineError::Workflow(format!(
                        "Gate '{}': options StepRef '{dotted}' must be in 'step.field' format",
                        node.name
                    ))
                })?;
                let step_key = &dotted[..dot];
                let field_key = &dotted[dot + 1..];
                let result = state.step_results.get(step_key).ok_or_else(|| {
                    EngineError::Workflow(format!(
                        "Gate '{}': options StepRef references step '{step_key}' which has no result yet",
                        node.name
                    ))
                })?;
                let json_str = result.structured_output.as_deref().ok_or_else(|| {
                    EngineError::Workflow(format!(
                        "Gate '{}': step '{step_key}' has no structured_output to extract field '{field_key}' from",
                        node.name
                    ))
                })?;
                let val: serde_json::Value = serde_json::from_str(json_str).map_err(|e| {
                    EngineError::Workflow(format!(
                        "Gate '{}': failed to parse structured_output of step '{step_key}': {e}",
                        node.name
                    ))
                })?;
                let obj = val.get(field_key).and_then(|v| v.as_object()).ok_or_else(|| {
                    EngineError::Workflow(format!(
                        "Gate '{}': field '{field_key}' in step '{step_key}' structured_output is not a JSON object",
                        node.name
                    ))
                })?;
                obj.iter()
                    .map(|(k, v)| {
                        v.as_str().map(|s| (k.clone(), s.to_string())).ok_or_else(|| {
                            EngineError::Workflow(format!(
                                "Gate '{}': option '{k}' in field '{field_key}' of step '{step_key}' structured_output is not a string",
                                node.name
                            ))
                        })
                    })
                    .collect::<Result<HashMap<String, String>>>()?
            }
        }
    } else {
        HashMap::new()
    };
    let mut options: Vec<GateOption> = map
        .into_iter()
        .map(|(value, label)| GateOption { value, label })
        .collect();
    options.sort_by(|a, b| a.value.cmp(&b.value));
    Ok(options)
}

/// `approved_by` / `rejected_by` recorded when a `GateResolver` decides a gate:
/// `gate_resolver:<gate_type>`.
pub const GATE_RESOLVER_ACTOR_PREFIX: &str = "gate_resolver";

fn finish_approved(
    state: &mut ExecutionState,
    node: &GateNode,
    feedback: Option<String>,
    selections: Option<Vec<String>>,
) -> Result<()> {
    tracing::info!("Gate '{}' approved", node.name);
    if let Some(ref fb) = feedback {
        state.last_gate_feedback = Some(fb.clone());
    }
    if let Some(sel) = selections {
        if !sel.is_empty() {
            // Store gate selection as feedback
            state.last_gate_feedback = Some(sel.join(", "));
        }
    }
    resume_run_status(state, &node.name, "after approval");
    emit_event(
        state,
        EngineEvent::GateResolved {
            gate_name: node.name.clone(),
            approved: true,
        },
    );
    Ok(())
}

fn finish_rejected(
    state: &mut ExecutionState,
    node: &GateNode,
    feedback: Option<String>,
) -> Result<()> {
    tracing::warn!("Gate '{}' rejected", node.name);
    state.all_succeeded = false;
    resume_run_status(state, &node.name, "after rejection");
    emit_event(
        state,
        EngineEvent::GateResolved {
            gate_name: node.name.clone(),
            approved: false,
        },
    );
    let reason = feedback.unwrap_or_else(|| format!("Gate '{}' rejected", node.name));
    Err(EngineError::Workflow(reason))
}

/// Poll the resolver once. Returns the decision to act on (a manual decision that landed
/// meanwhile, or the resolver's own decision after recording it), or `None` to keep waiting.
fn poll_resolver(
    state: &ExecutionState,
    node: &GateNode,
    step_id: &str,
    resolver: &dyn GateResolver,
    params: &GateParams,
) -> Option<GateApprovalState> {
    let poll = match resolver.poll(&state.workflow_run_id, params, &*state.run_ctx) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(
                "Gate '{}': resolver '{}' poll failed: {e}",
                node.name,
                node.gate_type
            );
            return None;
        }
    };
    if matches!(poll, GatePoll::Pending) {
        return None;
    }
    // A manual decision recorded since the last read wins over the resolver.
    if let Ok(s) = state.persistence.get_gate_approval(step_id) {
        if !matches!(s, GateApprovalState::Pending) {
            return Some(s);
        }
    }
    let actor = format!("{GATE_RESOLVER_ACTOR_PREFIX}:{}", node.gate_type);
    match poll {
        GatePoll::Approved(feedback) => {
            if let Err(e) = state
                .persistence
                .approve_gate(step_id, &actor, feedback.as_deref(), None)
            {
                tracing::warn!("Gate '{}': failed to record approval: {e}", node.name);
                return None;
            }
            Some(GateApprovalState::Approved {
                feedback,
                selections: None,
            })
        }
        GatePoll::Rejected(reason) => {
            if let Err(e) = state
                .persistence
                .reject_gate(step_id, &actor, Some(&reason))
            {
                tracing::warn!("Gate '{}': failed to record rejection: {e}", node.name);
                return None;
            }
            Some(GateApprovalState::Rejected {
                feedback: Some(reason),
            })
        }
        GatePoll::Pending => None,
    }
}

pub fn execute_gate(state: &mut ExecutionState, node: &GateNode, iteration: u32) -> Result<()> {
    let pos = state.position;
    state.position += 1;

    // Skip completed gates on resume — restore feedback for downstream steps
    if super::skip_if_already_completed(state, &node.name, iteration, &node.name) {
        return Ok(());
    }

    // Quality gates evaluate immediately — no blocking/waiting.
    if node.gate_type == QUALITY_GATE_TYPE {
        return execute_quality_gate(state, node, pos, iteration);
    }

    // Dry-run: auto-approve all gates
    if state.exec_config.dry_run {
        tracing::info!("gate '{}': dry-run auto-approved", node.name);
        super::insert_step_with_status(
            state,
            super::StepSpec {
                step_name: &node.name,
                role: "reviewer",
                pos,
                iteration,
                retry_count: None,
                status: WorkflowStepStatus::Completed,
                result_text: Some("dry-run: auto-approved".to_string()),
                gate: None,
            },
        )?;
        return Ok(());
    }

    // Resolve options first so a bad StepRef fails before any row or event exists.
    let options = resolve_gate_options(state, node)?;
    let params_options: HashMap<String, String> = options
        .iter()
        .map(|o| (o.value.clone(), o.label.clone()))
        .collect();
    let gate_info = GateStepInfo {
        gate_type: node.gate_type.clone(),
        prompt: node.prompt.clone(),
        timeout_secs: node.timeout_secs,
        options,
    };

    // Insert step (with gate metadata) and mark as waiting
    let step_id = super::insert_step_with_status(
        state,
        super::StepSpec {
            step_name: &node.name,
            role: "gate",
            pos,
            iteration,
            retry_count: None,
            status: WorkflowStepStatus::Waiting,
            result_text: None,
            gate: Some(gate_info),
        },
    )?;

    emit_event(
        state,
        EngineEvent::GateWaiting {
            gate_name: node.name.clone(),
        },
    );

    tracing::info!("Gate '{}' waiting (type = {})", node.name, node.gate_type);

    let params = GateParams {
        gate_name: node.name.clone(),
        prompt: node.prompt.clone(),
        min_approvals: node.min_approvals,
        approval_mode: node.approval_mode.clone(),
        options: params_options,
        timeout_secs: node.timeout_secs,
        as_identity: node
            .as_identity
            .clone()
            .or_else(|| state.default_as_identity.clone()),
        step_id: step_id.clone(),
    };
    let resolver = state.gate_resolvers.get(&node.gate_type);
    if resolver.is_none() {
        tracing::debug!(
            "Gate '{}': no resolver registered for type '{}'; waiting on manual decisions only",
            node.name,
            node.gate_type
        );
    }
    let mut last_resolver_poll: Option<std::time::Instant> = None;

    // Poll/timeout loop. A recorded manual decision (persistence.get_gate_approval()) always
    // wins; otherwise the node's resolver is polled, throttled by its poll interval.
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(node.timeout_secs) {
            return handle_gate_timeout(state, &step_id, node);
        }

        let mut approval = match state.persistence.get_gate_approval(&step_id) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("Gate '{}': error checking approval state: {e}", node.name);
                GateApprovalState::Pending
            }
        };

        if matches!(approval, GateApprovalState::Pending) {
            if let Some(ref resolver) = resolver {
                let interval = resolver
                    .poll_interval()
                    .unwrap_or(state.exec_config.poll_interval);
                let due = last_resolver_poll.is_none_or(|t| t.elapsed() >= interval);
                if due {
                    last_resolver_poll = Some(std::time::Instant::now());
                    if let Some(decided) =
                        poll_resolver(state, node, &step_id, resolver.as_ref(), &params)
                    {
                        approval = decided;
                    }
                }
            }
        }

        match approval {
            GateApprovalState::Approved {
                feedback,
                selections,
            } => return finish_approved(state, node, feedback, selections),
            GateApprovalState::Rejected { feedback } => {
                return finish_rejected(state, node, feedback)
            }
            GateApprovalState::Pending => thread::sleep(state.exec_config.poll_interval),
        }

        // Check cancellation
        match state.persistence.is_run_cancelled(&state.workflow_run_id) {
            Ok(true) => {
                state
                    .cancellation
                    .cancel(crate::cancellation_reason::CancellationReason::UserRequested(None));
                return Err(EngineError::Cancelled(
                    crate::cancellation_reason::CancellationReason::UserRequested(None),
                ));
            }
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(
                    "Database error during cancellation check for gate '{}': {}",
                    node.name,
                    e
                );
            }
        }
    }
}

/// Evaluate a quality gate by checking a prior step's structured output against a threshold.
pub fn execute_quality_gate(
    state: &mut ExecutionState,
    node: &GateNode,
    pos: i64,
    iteration: u32,
) -> Result<()> {
    let qg = node.quality_gate.as_ref().ok_or_else(|| {
        EngineError::Workflow(format!(
            "Quality gate '{}' is missing required quality_gate configuration (source, threshold)",
            node.name
        ))
    })?;
    let source = qg.source.as_str();
    let threshold = qg.threshold;
    let on_fail_action = qg.on_fail_action.clone();

    let step_id = super::insert_step_record(state, &node.name, "gate", pos, iteration, None)?;
    let generation = state.expect_lease_generation();

    let set_step_status = |status: WorkflowStepStatus, context: &str| -> Result<()> {
        state.persistence.update_step(
            &step_id,
            StepUpdate {
                generation,
                status,
                child_run_id: None,
                result_text: Some(context.to_string()),
                context_out: None,
                markers_out: None,
                retry_count: None,
                structured_output: None,
                step_error: None,
            },
        )
    };

    // Look up the source step's structured output
    let (confidence, degradation_reason): (u32, Option<String>) = match state
        .step_results
        .get(source)
    {
        Some(result) => {
            if let Some(ref json_str) = result.structured_output {
                match serde_json::from_str::<serde_json::Value>(json_str) {
                    Ok(val) => {
                        if let Some(c) = val.get("confidence").and_then(|v| v.as_u64()) {
                            (c.min(100) as u32, None)
                        } else if let Some(f) = val.get("confidence").and_then(|v| v.as_f64()) {
                            ((f as u64).min(100) as u32, None)
                        } else {
                            let reason = format!(
                                "'confidence' key missing or not a number in structured output from '{}'",
                                source
                            );
                            tracing::warn!("quality_gate '{}': {}", node.name, reason);
                            (0, Some(reason))
                        }
                    }
                    Err(e) => {
                        let reason =
                            format!("failed to parse structured output from '{}': {}", source, e);
                        tracing::warn!("quality_gate '{}': {}", node.name, reason);
                        (0, Some(reason))
                    }
                }
            } else {
                let reason = format!("source step '{}' has no structured output", source);
                tracing::warn!("quality_gate '{}': {}", node.name, reason);
                (0, Some(reason))
            }
        }
        None => {
            let msg = format!(
                "Quality gate '{}': source step '{}' not found in step results",
                node.name, source
            );
            set_step_status(WorkflowStepStatus::Failed, &msg)?;
            return Err(EngineError::Workflow(msg));
        }
    };

    let passed = confidence >= threshold;
    let mut context = format!(
        "quality_gate: confidence={}, threshold={}, result={}",
        confidence,
        threshold,
        if passed { "pass" } else { "fail" }
    );
    if let Some(ref reason) = degradation_reason {
        context.push_str(&format!(" (confidence defaulted to 0: {})", reason));
    }

    if passed {
        tracing::info!(
            "quality_gate '{}': passed (confidence {} >= threshold {})",
            node.name,
            confidence,
            threshold
        );
        set_step_status(WorkflowStepStatus::Completed, &context)?;
    } else {
        tracing::warn!(
            "quality_gate '{}': failed (confidence {} < threshold {})",
            node.name,
            confidence,
            threshold
        );
        match on_fail_action {
            OnFailAction::Fail => {
                set_step_status(WorkflowStepStatus::Failed, &context)?;
                return Err(EngineError::Workflow(format!(
                    "Quality gate '{}' failed: confidence {} is below threshold {}",
                    node.name, confidence, threshold
                )));
            }
            OnFailAction::Continue => {
                set_step_status(
                    WorkflowStepStatus::Completed,
                    &format!("{} (on_fail=continue, proceeding)", context),
                )?;
            }
        }
    }

    Ok(())
}

pub fn handle_gate_timeout(
    state: &mut ExecutionState,
    step_id: &str,
    node: &GateNode,
) -> Result<()> {
    tracing::warn!("Gate '{}' timed out", node.name);
    let generation = state.expect_lease_generation();
    match node.on_timeout {
        OnTimeout::Fail => {
            state.persistence.update_step(
                step_id,
                StepUpdate {
                    generation,
                    status: WorkflowStepStatus::Failed,
                    child_run_id: None,
                    result_text: Some("gate timed out".to_string()),
                    context_out: None,
                    markers_out: None,
                    retry_count: None,
                    structured_output: None,
                    step_error: Some(format!("Gate '{}' timed out", node.name)),
                },
            )?;
            state.all_succeeded = false;
            resume_run_status(state, &node.name, "after timeout (fail)");
            Err(EngineError::Workflow(format!(
                "Gate '{}' timed out",
                node.name
            )))
        }
        OnTimeout::Continue => {
            state.persistence.update_step(
                step_id,
                StepUpdate {
                    generation,
                    status: WorkflowStepStatus::TimedOut,
                    child_run_id: None,
                    result_text: Some("gate timed out (continuing)".to_string()),
                    context_out: None,
                    markers_out: None,
                    retry_count: None,
                    structured_output: None,
                    step_error: None,
                },
            )?;
            resume_run_status(state, &node.name, "after timeout (continue)");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::dsl::{ApprovalMode, GateNode, GateOptions, OnTimeout};
    use crate::events::EventSink;
    use crate::test_helpers::{
        make_test_execution_state, CountingPersistence, ForwardSink, VecSink,
    };
    use crate::traits::persistence::{NewRun, WorkflowPersistence};
    use crate::types::StepResult;

    fn gate_node(options: Option<GateOptions>, timeout_secs: u64) -> GateNode {
        GateNode {
            name: "approve".into(),
            gate_type: "human_approval".into(),
            prompt: Some("ship it?".into()),
            min_approvals: 1,
            approval_mode: ApprovalMode::default(),
            timeout_secs,
            on_timeout: OnTimeout::Fail,
            as_identity: None,
            quality_gate: None,
            options,
        }
    }

    fn make_state(sink: Arc<VecSink>) -> (ExecutionState, Arc<CountingPersistence>, String) {
        let cp = Arc::new(CountingPersistence::new());
        let run_id = cp
            .create_run(NewRun {
                workflow_name: "wf".to_string(),
                parent_run_id: String::new(),
                dry_run: false,
                trigger: "manual".to_string(),
                definition_snapshot: None,
                parent_workflow_run_id: None,
            })
            .unwrap()
            .id;
        let p: Arc<dyn WorkflowPersistence> = Arc::clone(&cp) as _;
        let mut state = make_test_execution_state(p, run_id.clone());
        state.event_sinks = Arc::from(vec![Arc::new(ForwardSink(sink)) as Arc<dyn EventSink>]);
        (state, cp, run_id)
    }

    fn with_structured(state: &mut ExecutionState, json: &str) {
        state.step_results.insert(
            "prev".into(),
            StepResult {
                step_name: "prev".into(),
                structured_output: Some(json.into()),
                ..StepResult::default()
            },
        );
    }

    fn step_ref(s: &str) -> Option<GateOptions> {
        Some(GateOptions::StepRef(s.into()))
    }

    #[test]
    fn execute_gate_persists_gate_metadata_at_insert() {
        let (mut state, cp, run_id) = make_state(VecSink::new());
        let mut opts = HashMap::new();
        opts.insert("b".to_string(), "Beta".to_string());
        opts.insert("a".to_string(), "Alpha".to_string());
        let node = gate_node(Some(GateOptions::Static(opts)), 0);
        // timeout_secs = 0 → the poll loop times out immediately; we only care about the row.
        let _ = execute_gate(&mut state, &node, 0);
        let steps = cp.get_steps(&run_id).unwrap();
        assert_eq!(steps.len(), 1);
        let s = &steps[0];
        assert_eq!(s.gate_type.as_deref(), Some("human_approval"));
        assert_eq!(s.gate_prompt.as_deref(), Some("ship it?"));
        assert_eq!(s.gate_timeout.as_deref(), Some("0s"));
        assert_eq!(
            s.gate_options.as_deref(),
            Some(r#"[{"label":"Alpha","value":"a"},{"label":"Beta","value":"b"}]"#)
        );
    }

    #[test]
    fn unresolvable_step_ref_fails_before_any_row_or_event() {
        let cases: [(&str, Option<&str>); 5] = [
            ("nodot", None),
            ("missing.field", None),
            ("prev.field", None), // no structured_output
            ("prev.field", Some("not json")),
            ("prev.field", Some(r#"{"field": "scalar"}"#)),
        ];
        for (dotted, structured) in cases {
            let sink = VecSink::new();
            let (mut state, cp, run_id) = make_state(Arc::clone(&sink));
            if dotted.starts_with("prev") {
                state.step_results.insert(
                    "prev".into(),
                    StepResult {
                        step_name: "prev".into(),
                        structured_output: structured.map(String::from),
                        ..StepResult::default()
                    },
                );
            }
            let node = gate_node(step_ref(dotted), 0);
            let err = execute_gate(&mut state, &node, 0);
            assert!(err.is_err(), "{dotted} / {structured:?} should fail");
            assert!(cp.get_steps(&run_id).unwrap().is_empty());
            assert!(!sink
                .collected()
                .iter()
                .any(|e| matches!(e.event, EngineEvent::GateWaiting { .. })));
        }
    }

    #[test]
    fn step_ref_options_are_resolved_and_sorted() {
        let (mut state, _cp, _id) = make_state(VecSink::new());
        with_structured(&mut state, r#"{"field": {"z": "Zed", "a": "Alpha"}}"#);
        let opts = resolve_gate_options(&state, &gate_node(step_ref("prev.field"), 0)).unwrap();
        let got: Vec<(&str, &str)> = opts
            .iter()
            .map(|o| (o.value.as_str(), o.label.as_str()))
            .collect();
        assert_eq!(got, vec![("a", "Alpha"), ("z", "Zed")]);
    }

    #[test]
    fn step_ref_non_string_option_value_is_an_error() {
        let (mut state, _cp, _id) = make_state(VecSink::new());
        with_structured(&mut state, r#"{"field": {"a": "Alpha", "bad": 3}}"#);
        let err = resolve_gate_options(&state, &gate_node(step_ref("prev.field"), 0)).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("'bad'") && msg.contains("not a string"),
            "{msg}"
        );
    }
}
