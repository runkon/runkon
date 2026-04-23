use std::thread;
use std::time::Duration;

use crate::dsl::{GateNode, GateOptions, GateType, OnFailAction, OnTimeout};
use crate::engine::{emit_event, restore_step, should_skip, ExecutionState};
use crate::engine_error::{EngineError, Result};
use crate::events::EngineEvent;
use crate::status::{WorkflowRunStatus, WorkflowStepStatus};
use crate::traits::persistence::{GateApprovalState, NewStep, StepUpdate};

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

pub fn execute_gate(state: &mut ExecutionState, node: &GateNode, iteration: u32) -> Result<()> {
    let pos = state.position;
    state.position += 1;

    // Skip completed gates on resume — restore feedback for downstream steps
    if should_skip(state, &node.name, iteration) {
        tracing::info!("Skipping completed gate '{}'", node.name);
        restore_step(state, &node.name, iteration);
        return Ok(());
    }

    // Quality gates evaluate immediately — no blocking/waiting.
    if node.gate_type == GateType::QualityGate {
        return execute_quality_gate(state, node, pos, iteration);
    }

    // Dry-run: auto-approve all gates
    if state.exec_config.dry_run {
        tracing::info!("gate '{}': dry-run auto-approved", node.name);
        let step_id = state
            .persistence
            .insert_step(NewStep {
                workflow_run_id: state.workflow_run_id.clone(),
                step_name: node.name.clone(),
                role: "reviewer".to_string(),
                can_commit: false,
                position: pos,
                iteration: iteration as i64,
                retry_count: None,
            })
            .map_err(|e| EngineError::Persistence(e.to_string()))?;
        state
            .persistence
            .update_step(
                &step_id,
                StepUpdate {
                    status: WorkflowStepStatus::Completed,
                    child_run_id: None,
                    result_text: Some("dry-run: auto-approved".to_string()),
                    context_out: None,
                    markers_out: None,
                    retry_count: None,
                    structured_output: None,
                    step_error: None,
                },
            )
            .map_err(|e| EngineError::Persistence(e.to_string()))?;
        return Ok(());
    }

    let step_id = state
        .persistence
        .insert_step(NewStep {
            workflow_run_id: state.workflow_run_id.clone(),
            step_name: node.name.clone(),
            role: "gate".to_string(),
            can_commit: false,
            position: pos,
            iteration: iteration as i64,
            retry_count: None,
        })
        .map_err(|e| EngineError::Persistence(e.to_string()))?;

    // Mark as waiting
    state
        .persistence
        .update_step(
            &step_id,
            StepUpdate {
                status: WorkflowStepStatus::Waiting,
                child_run_id: None,
                result_text: None,
                context_out: None,
                markers_out: None,
                retry_count: None,
                structured_output: None,
                step_error: None,
            },
        )
        .map_err(|e| EngineError::Persistence(e.to_string()))?;

    emit_event(
        state,
        EngineEvent::GateWaiting {
            gate_name: node.name.clone(),
        },
    );

    // Resolve gate options (if any) — stored for future use by gate resolvers
    let _resolved_options: Vec<String> = if let Some(ref gate_opts) = node.options {
        match gate_opts {
            GateOptions::Static(items) => items.clone(),
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
                let arr = val.get(field_key).and_then(|v| v.as_array()).ok_or_else(|| {
                    EngineError::Workflow(format!(
                        "Gate '{}': field '{field_key}' in step '{step_key}' structured_output is not a JSON array",
                        node.name
                    ))
                })?;
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            }
        }
    } else {
        Vec::new()
    };

    // Log human gate instructions before entering the poll loop.
    if matches!(
        node.gate_type,
        GateType::HumanApproval | GateType::HumanReview
    ) {
        tracing::info!("Gate '{}' waiting for human action:", node.name);
        if let Some(ref p) = node.prompt {
            tracing::info!("  Prompt: {p}");
        }
        tracing::info!(
            "  Approve:  conductor workflow gate-approve {}",
            state.workflow_run_id
        );
        tracing::info!(
            "  Reject:   conductor workflow gate-reject {}",
            state.workflow_run_id
        );
        if node.gate_type == GateType::HumanReview {
            tracing::info!(
                "  Feedback: conductor workflow gate-feedback {} \"<text>\"",
                state.workflow_run_id
            );
        }
    } else if node.gate_type == GateType::PrApproval {
        tracing::info!("Gate '{}' polling for PR approvals...", node.name);
    } else if node.gate_type == GateType::PrChecks {
        tracing::info!("Gate '{}' polling for PR checks...", node.name);
    }

    // Poll/timeout loop — poll via persistence.get_gate_approval()
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(node.timeout_secs) {
            return handle_gate_timeout(state, &step_id, node);
        }

        match state.persistence.get_gate_approval(&step_id) {
            Ok(GateApprovalState::Approved {
                feedback,
                selections,
            }) => {
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
                return Ok(());
            }
            Ok(GateApprovalState::Rejected { feedback }) => {
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
                return Err(EngineError::Workflow(reason));
            }
            Ok(GateApprovalState::Pending) => {
                thread::sleep(state.exec_config.poll_interval);
            }
            Err(e) => {
                tracing::warn!("Gate '{}': error checking approval state: {e}", node.name);
                thread::sleep(state.exec_config.poll_interval);
            }
        }

        // Check cancellation
        if let Ok(true) = state.persistence.is_run_cancelled(&state.workflow_run_id) {
            return Err(EngineError::Workflow("Workflow run cancelled".to_string()));
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

    let step_id = state
        .persistence
        .insert_step(NewStep {
            workflow_run_id: state.workflow_run_id.clone(),
            step_name: node.name.clone(),
            role: "gate".to_string(),
            can_commit: false,
            position: pos,
            iteration: iteration as i64,
            retry_count: None,
        })
        .map_err(|e| EngineError::Persistence(e.to_string()))?;

    let set_step_status = |status: WorkflowStepStatus, context: &str| -> Result<()> {
        state
            .persistence
            .update_step(
                &step_id,
                StepUpdate {
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
            .map_err(|e| EngineError::Persistence(e.to_string()))
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
    match node.on_timeout {
        OnTimeout::Fail => {
            state
                .persistence
                .update_step(
                    step_id,
                    StepUpdate {
                        status: WorkflowStepStatus::Failed,
                        child_run_id: None,
                        result_text: Some("gate timed out".to_string()),
                        context_out: None,
                        markers_out: None,
                        retry_count: None,
                        structured_output: None,
                        step_error: None,
                    },
                )
                .map_err(|e| EngineError::Persistence(e.to_string()))?;
            state.all_succeeded = false;
            resume_run_status(state, &node.name, "after timeout (fail)");
            Err(EngineError::Workflow(format!(
                "Gate '{}' timed out",
                node.name
            )))
        }
        OnTimeout::Continue => {
            state
                .persistence
                .update_step(
                    step_id,
                    StepUpdate {
                        status: WorkflowStepStatus::TimedOut,
                        child_run_id: None,
                        result_text: Some("gate timed out (continuing)".to_string()),
                        context_out: None,
                        markers_out: None,
                        retry_count: None,
                        structured_output: None,
                        step_error: None,
                    },
                )
                .map_err(|e| EngineError::Persistence(e.to_string()))?;
            resume_run_status(state, &node.name, "after timeout (continue)");
            Ok(())
        }
    }
}
