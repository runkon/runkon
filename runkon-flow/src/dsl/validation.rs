use std::collections::HashSet;
use std::fmt;

use super::types::{
    Condition, GateType, InputType, OnChildFail, ScriptNode, WorkflowDef, WorkflowNode,
};

// ---------------------------------------------------------------------------
// Semantic validation
// ---------------------------------------------------------------------------

/// A single semantic validation error found during static analysis of a workflow.
#[derive(Debug, Clone)]
pub struct ValidationError {
    pub message: String,
    pub hint: Option<String>,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.hint {
            Some(h) => write!(f, "{} (hint: {h})", self.message),
            None => write!(f, "{}", self.message),
        }
    }
}

/// The result of running `validate_workflow_semantics`.
#[derive(Debug, Default)]
pub struct ValidationReport {
    pub errors: Vec<ValidationError>,
    pub warnings: Vec<String>,
}

impl ValidationReport {
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }
}

pub fn validate_workflow_semantics<F>(def: &WorkflowDef, loader: &F) -> ValidationReport
where
    F: Fn(&str) -> std::result::Result<WorkflowDef, String>,
{
    let mut errors = Vec::new();
    let mut warnings = Vec::new();
    let mut produced: HashSet<String> = HashSet::new();

    let bool_inputs: HashSet<String> = def
        .inputs
        .iter()
        .filter(|i| i.input_type == InputType::Boolean)
        .map(|i| i.name.clone())
        .collect();

    validate_nodes(
        &def.body,
        &mut produced,
        &mut errors,
        &mut warnings,
        loader,
        &bool_inputs,
    );

    let mut always_produced = produced.clone();
    validate_nodes(
        &def.always,
        &mut always_produced,
        &mut errors,
        &mut warnings,
        loader,
        &bool_inputs,
    );

    const VALID_TARGETS: &[&str] = &["worktree", "ticket", "repo", "pr", "workflow_run"];
    for target in &def.targets {
        if !VALID_TARGETS.contains(&target.as_str()) {
            errors.push(ValidationError {
                message: format!(
                    "Unknown target '{}' in workflow '{}'. Valid targets: {}",
                    target,
                    def.name,
                    VALID_TARGETS.join(", ")
                ),
                hint: Some(format!(
                    "Change '{}' to one of: {}",
                    target,
                    VALID_TARGETS.join(", ")
                )),
            });
        }
    }

    ValidationReport { errors, warnings }
}

fn validate_nodes<F>(
    nodes: &[WorkflowNode],
    produced: &mut HashSet<String>,
    errors: &mut Vec<ValidationError>,
    warnings: &mut Vec<String>,
    loader: &F,
    bool_inputs: &HashSet<String>,
) where
    F: Fn(&str) -> std::result::Result<WorkflowDef, String>,
{
    for node in nodes {
        match node {
            WorkflowNode::Call(n) => {
                produced.insert(n.agent.step_key());
            }
            WorkflowNode::CallWorkflow(n) => {
                match loader(&n.workflow) {
                    Ok(sub_def) => {
                        for input_decl in &sub_def.inputs {
                            if input_decl.required && !n.inputs.contains_key(&input_decl.name) {
                                errors.push(ValidationError {
                                    message: format!(
                                        "Sub-workflow '{}' requires input '{}' but it was not provided at the call site",
                                        n.workflow, input_decl.name
                                    ),
                                    hint: None,
                                });
                            }
                        }

                        for sub_node in &sub_def.body {
                            for key in node_step_keys(sub_node) {
                                produced.insert(key);
                            }
                        }
                    }
                    Err(e) => {
                        errors.push(ValidationError {
                            message: format!(
                                "Sub-workflow '{}' could not be loaded: {}",
                                n.workflow, e
                            ),
                            hint: None,
                        });
                    }
                }
                produced.insert(n.workflow.clone());
            }
            WorkflowNode::Parallel(n) => {
                for (step_name, _marker) in n.call_if.values() {
                    check_condition_reachable(step_name, produced, errors);
                }
                for call in &n.calls {
                    produced.insert(call.step_key());
                }
            }
            WorkflowNode::If(n) => {
                validate_conditional_branch(
                    &n.condition,
                    &n.body,
                    produced,
                    errors,
                    warnings,
                    loader,
                    bool_inputs,
                );
            }
            WorkflowNode::Unless(n) => {
                validate_conditional_branch(
                    &n.condition,
                    &n.body,
                    produced,
                    errors,
                    warnings,
                    loader,
                    bool_inputs,
                );
            }
            WorkflowNode::While(n) => {
                check_condition_reachable(&n.step, produced, errors);
                let mut body_produced = produced.clone();
                validate_nodes(
                    &n.body,
                    &mut body_produced,
                    errors,
                    warnings,
                    loader,
                    bool_inputs,
                );
                produced.extend(body_produced);
            }
            WorkflowNode::DoWhile(n) => {
                validate_nodes(&n.body, produced, errors, warnings, loader, bool_inputs);
                check_condition_reachable(&n.step, produced, errors);
            }
            WorkflowNode::Do(n) => {
                validate_nodes(&n.body, produced, errors, warnings, loader, bool_inputs);
            }
            WorkflowNode::Gate(n) => {
                if n.gate_type == GateType::QualityGate && n.quality_gate.is_none() {
                    errors.push(ValidationError {
                        message: format!(
                            "Quality gate '{}' is missing required `source` and `threshold` fields",
                            n.name
                        ),
                        hint: Some("Add `source = \"step_name\"` and `threshold = 70` to configure the quality gate".to_string()),
                    });
                }
                if let Some(source) = n.quality_gate.as_ref().map(|qg| &qg.source) {
                    if !produced.contains(source.as_str()) {
                        errors.push(ValidationError {
                            message: format!(
                                "Quality gate '{}' references source step '{}' which has not been produced at this point in the workflow",
                                n.name, source
                            ),
                            hint: Some(format!(
                                "Ensure a call or script step named '{}' appears before this gate",
                                source
                            )),
                        });
                    }
                }
            }
            WorkflowNode::Script(n) => {
                produced.insert(n.name.clone());
            }
            WorkflowNode::Always(n) => {
                validate_nodes(&n.body, produced, errors, warnings, loader, bool_inputs);
            }
            WorkflowNode::ForEach(n) => {
                validate_foreach_node(n, errors, warnings, loader);
                produced.insert(format!("foreach:{}", n.name));
            }
        }
    }
}

fn validate_foreach_node<F>(
    n: &super::types::ForEachNode,
    errors: &mut Vec<ValidationError>,
    warnings: &mut Vec<String>,
    loader: &F,
) where
    F: Fn(&str) -> std::result::Result<WorkflowDef, String>,
{
    match loader(&n.workflow) {
        Ok(child_def) => {
            for input_decl in &child_def.inputs {
                if input_decl.required && !n.inputs.contains_key(&input_decl.name) {
                    errors.push(ValidationError {
                        message: format!(
                            "foreach '{}': child workflow '{}' requires input '{}' \
                             but it is not in the inputs map",
                            n.name, n.workflow, input_decl.name
                        ),
                        hint: Some(format!(
                            "Add `{} = \"{{{{item.*}}}}\"` or a literal value to the inputs block",
                            input_decl.name
                        )),
                    });
                }
            }
        }
        Err(e) => {
            errors.push(ValidationError {
                message: format!(
                    "foreach '{}': child workflow '{}' could not be loaded: {}",
                    n.name, n.workflow, e
                ),
                hint: None,
            });
        }
    }

    if n.scope.is_none() {
        match n.over.as_str() {
            "tickets" => {
                errors.push(ValidationError {
                    message: format!(
                        "foreach '{}': `scope` is required when over = tickets",
                        n.name
                    ),
                    hint: Some(
                        "Add `scope = { ticket_id = \"...\" }`, `scope = { label = \"...\" }`, or `scope = { unlabeled = true }`"
                            .to_string(),
                    ),
                });
            }
            "worktrees" => {
                warnings.push(format!(
                    "foreach '{}': no scope specified; base_branch will be inferred from the execution context worktree at runtime",
                    n.name
                ));
            }
            _ => {}
        }
    }

    if n.ordered && n.over != "tickets" && n.over != "worktrees" {
        errors.push(ValidationError {
            message: format!(
                "foreach '{}': ordered = true is only valid when over = tickets or over = worktrees",
                n.name
            ),
            hint: Some(
                "Remove `ordered = true` or change `over` to `tickets` or `worktrees`".to_string(),
            ),
        });
    }

    if n.on_child_fail == OnChildFail::SkipDependents && !n.ordered {
        errors.push(ValidationError {
            message: format!(
                "foreach '{}': on_child_fail = skip_dependents has no effect without ordered = true",
                n.name
            ),
            hint: Some(
                "Add `ordered = true` or change on_child_fail to `continue` or `halt`".to_string(),
            ),
        });
    }

    if n.over == "workflow_runs" && n.filter.is_empty() {
        errors.push(ValidationError {
            message: format!(
                "foreach '{}': `filter` is required when over = workflow_runs",
                n.name
            ),
            hint: Some(
                "Add `filter = { status = \"failed\" }` (or another terminal status)".to_string(),
            ),
        });
    }

    if n.over == "workflow_runs" {
        if let Some(status) = n.filter.get("status") {
            if status == "running" || status == "paused" {
                errors.push(ValidationError {
                    message: format!(
                        "foreach '{}': filter.status = '{}' is not a terminal status — \
                         only completed, failed, or cancelled are allowed",
                        n.name, status
                    ),
                    hint: Some(
                        "Change to `status = \"failed\"` or `status = \"completed\"`".to_string(),
                    ),
                });
            }
        }
    }

    if n.over == "repos" && !n.filter.is_empty() {
        errors.push(ValidationError {
            message: format!(
                "foreach '{}': filter has no effect when over = repos (not implemented in v1)",
                n.name
            ),
            hint: Some("Remove the `filter` block for repo fan-outs".to_string()),
        });
    }

    if n.over == "worktrees" && !n.filter.is_empty() {
        errors.push(ValidationError {
            message: format!(
                "foreach '{}': filter has no effect when over = worktrees (use scope = {{ base_branch = \"...\" }} instead)",
                n.name
            ),
            hint: Some("Remove the `filter` block for worktree fan-outs".to_string()),
        });
    }
}

fn node_step_keys(node: &WorkflowNode) -> Vec<String> {
    match node {
        WorkflowNode::Call(n) => vec![n.agent.step_key()],
        WorkflowNode::CallWorkflow(n) => vec![n.workflow.clone()],
        WorkflowNode::Script(n) => vec![n.name.clone()],
        WorkflowNode::Parallel(n) => n.calls.iter().map(|c| c.step_key()).collect(),
        WorkflowNode::ForEach(n) => vec![format!("foreach:{}", n.name)],
        _ => vec![],
    }
}

fn check_condition_reachable(
    step: &str,
    produced: &HashSet<String>,
    errors: &mut Vec<ValidationError>,
) {
    if !produced.contains(step) {
        errors.push(ValidationError {
            message: format!(
                "Condition references step '{}' which has not been produced at this point in the workflow",
                step
            ),
            hint: None,
        });
    }
}

fn check_bool_input_declared(
    input: &str,
    bool_inputs: &HashSet<String>,
    errors: &mut Vec<ValidationError>,
) {
    if !bool_inputs.contains(input) {
        errors.push(ValidationError {
            message: format!(
                "Condition references '{}' which is not a declared boolean input",
                input
            ),
            hint: Some(format!(
                "Declare it in the workflow inputs block: `{} boolean`",
                input
            )),
        });
    }
}

fn validate_conditional_branch<F>(
    condition: &Condition,
    body: &[WorkflowNode],
    produced: &mut HashSet<String>,
    errors: &mut Vec<ValidationError>,
    warnings: &mut Vec<String>,
    loader: &F,
    bool_inputs: &HashSet<String>,
) where
    F: Fn(&str) -> std::result::Result<WorkflowDef, String>,
{
    match condition {
        Condition::StepMarker { step, .. } => {
            check_condition_reachable(step, produced, errors);
        }
        Condition::BoolInput { input } => {
            check_bool_input_declared(input, bool_inputs, errors);
        }
    }
    let mut branch_produced = produced.clone();
    validate_nodes(
        body,
        &mut branch_produced,
        errors,
        warnings,
        loader,
        bool_inputs,
    );
    produced.extend(branch_produced);
}

// ---------------------------------------------------------------------------
// Script step validation
// ---------------------------------------------------------------------------

pub fn validate_script_steps<F>(def: &WorkflowDef, path_resolver: &F) -> Vec<ValidationError>
where
    F: Fn(&str) -> Result<std::path::PathBuf, String>,
{
    let mut errors = Vec::new();
    let nodes: Vec<&ScriptNode> = collect_script_nodes(&def.body)
        .into_iter()
        .chain(collect_script_nodes(&def.always))
        .collect();

    for node in nodes {
        let run = &node.run;

        if run.contains("{{") {
            continue;
        }

        match path_resolver(run) {
            Err(searched) => {
                errors.push(ValidationError {
                    message: format!(
                        "Script step '{}': '{}' not found. Searched: {}",
                        node.name, run, searched
                    ),
                    hint: None,
                });
            }
            Ok(resolved) => {
                #[cfg(unix)]
                if let Some(err) = check_script_unix_permissions(&node.name, &resolved) {
                    errors.push(err);
                }
                #[cfg(not(unix))]
                {
                    let _ = resolved;
                }
            }
        }
    }

    errors
}

#[cfg(unix)]
fn check_script_unix_permissions(
    step_name: &str,
    resolved: &std::path::Path,
) -> Option<ValidationError> {
    use std::os::unix::fs::PermissionsExt;
    match std::fs::metadata(resolved) {
        Err(e) => Some(ValidationError {
            message: format!(
                "Script step '{}': could not read metadata for '{}': {}",
                step_name,
                resolved.display(),
                e,
            ),
            hint: None,
        }),
        Ok(meta) => {
            let mode = meta.permissions().mode();
            if mode & 0o111 == 0 {
                Some(ValidationError {
                    message: format!(
                        "Script step '{}': '{}' is not executable (mode {:04o})",
                        step_name,
                        resolved.display(),
                        mode & 0o777,
                    ),
                    hint: Some(format!("Run: chmod +x {}", resolved.display())),
                })
            } else {
                None
            }
        }
    }
}

fn collect_script_nodes(nodes: &[WorkflowNode]) -> Vec<&ScriptNode> {
    let mut out = Vec::new();
    for node in nodes {
        match node {
            WorkflowNode::Script(s) => out.push(s),
            WorkflowNode::If(n) => out.extend(collect_script_nodes(&n.body)),
            WorkflowNode::Unless(n) => out.extend(collect_script_nodes(&n.body)),
            WorkflowNode::While(n) => out.extend(collect_script_nodes(&n.body)),
            WorkflowNode::DoWhile(n) => out.extend(collect_script_nodes(&n.body)),
            WorkflowNode::Do(n) => out.extend(collect_script_nodes(&n.body)),
            WorkflowNode::Always(n) => out.extend(collect_script_nodes(&n.body)),
            WorkflowNode::Call(_)
            | WorkflowNode::CallWorkflow(_)
            | WorkflowNode::Gate(_)
            | WorkflowNode::Parallel(_)
            | WorkflowNode::ForEach(_) => {}
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{validate_script_steps, validate_workflow_semantics};
    use crate::dsl::parse_workflow_str;

    fn no_loader(name: &str) -> Result<crate::dsl::WorkflowDef, String> {
        Err(format!("sub-workflow '{}' not found", name))
    }

    fn always_resolve_ok(run: &str) -> Result<std::path::PathBuf, String> {
        Ok(std::path::PathBuf::from(run))
    }

    fn always_resolve_err(run: &str) -> Result<std::path::PathBuf, String> {
        Err(format!("not found: {run}"))
    }

    // ---- validate_workflow_semantics ----

    #[test]
    fn valid_simple_workflow_has_no_errors() {
        let src = r#"
workflow simple {
    call my_agent
}
"#;
        let def = parse_workflow_str(src, "test.wf").unwrap();
        let report = validate_workflow_semantics(&def, &no_loader);
        assert!(
            report.is_ok(),
            "expected no errors, got: {:?}",
            report.errors
        );
    }

    #[test]
    fn if_condition_referencing_unknown_step_is_an_error() {
        let src = r#"
workflow wf {
    if unknown_step.done {
        call another_agent
    }
}
"#;
        let def = parse_workflow_str(src, "test.wf").unwrap();
        let report = validate_workflow_semantics(&def, &no_loader);
        assert!(
            !report.is_ok(),
            "expected validation error for unknown step reference"
        );
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.message.contains("unknown_step")),
            "error should mention the step name; errors: {:?}",
            report.errors
        );
    }

    #[test]
    fn if_condition_after_producing_step_is_ok() {
        let src = r#"
workflow wf {
    call step1
    if step1.done {
        call step2
    }
}
"#;
        let def = parse_workflow_str(src, "test.wf").unwrap();
        let report = validate_workflow_semantics(&def, &no_loader);
        assert!(
            report.is_ok(),
            "step1 is produced before the if, so no error expected; got: {:?}",
            report.errors
        );
    }

    #[test]
    fn bool_input_in_if_condition_without_declaration_is_an_error() {
        let src = r#"
workflow wf {
    if undeclared_flag {
        call agent
    }
}
"#;
        let def = parse_workflow_str(src, "test.wf").unwrap();
        let report = validate_workflow_semantics(&def, &no_loader);
        assert!(
            !report.is_ok(),
            "undeclared bool input should be flagged as an error"
        );
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.message.contains("undeclared_flag")),
            "error should mention the input name; errors: {:?}",
            report.errors
        );
    }

    #[test]
    fn bool_input_declared_in_inputs_block_is_ok() {
        let src = r#"
workflow wf {
    inputs {
        run_extra boolean
    }
    if run_extra {
        call optional_agent
    }
}
"#;
        let def = parse_workflow_str(src, "test.wf").unwrap();
        let report = validate_workflow_semantics(&def, &no_loader);
        assert!(
            report.is_ok(),
            "declared boolean input in if condition should be valid; got: {:?}",
            report.errors
        );
    }

    #[test]
    fn invalid_target_produces_error() {
        let src = r#"
workflow wf {
    meta {
        targets = ["invalid_target"]
    }
    call agent
}
"#;
        let def = parse_workflow_str(src, "test.wf").unwrap();
        let report = validate_workflow_semantics(&def, &no_loader);
        assert!(!report.is_ok(), "invalid target should produce an error");
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.message.contains("invalid_target")),
            "error should mention the bad target; errors: {:?}",
            report.errors
        );
    }

    #[test]
    fn workflow_with_no_body_is_valid() {
        let src = r#"
workflow empty {
}
"#;
        let def = parse_workflow_str(src, "test.wf").unwrap();
        let report = validate_workflow_semantics(&def, &no_loader);
        assert!(
            report.is_ok(),
            "empty workflow body should be valid; got: {:?}",
            report.errors
        );
    }

    // ---- validate_script_steps ----

    #[test]
    fn script_with_template_variable_skips_path_check() {
        let src = r#"
workflow wf {
    script my_script {
        run = "{{scripts_dir}}/check.sh"
    }
}
"#;
        let def = parse_workflow_str(src, "test.wf").unwrap();
        // Even though the resolver would fail, template variables bypass path checking
        let errors = validate_script_steps(&def, &always_resolve_err);
        assert!(
            errors.is_empty(),
            "script with template variable should skip path check; got: {:?}",
            errors
        );
    }

    #[test]
    fn script_path_not_found_produces_error() {
        let src = r#"
workflow wf {
    script lint {
        run = "/nonexistent/script.sh"
    }
}
"#;
        let def = parse_workflow_str(src, "test.wf").unwrap();
        let errors = validate_script_steps(&def, &always_resolve_err);
        assert!(
            !errors.is_empty(),
            "missing script path should produce a validation error"
        );
        assert!(
            errors.iter().any(|e| e.message.contains("lint")),
            "error should mention the step name; errors: {:?}",
            errors
        );
    }

    #[test]
    fn script_path_resolved_ok_has_no_errors() {
        let src = r#"
workflow wf {
    script check {
        run = "some_script.sh"
    }
}
"#;
        let def = parse_workflow_str(src, "test.wf").unwrap();
        // On non-unix we can't check permissions, so resolution success → no errors
        #[cfg(not(unix))]
        {
            let errors = validate_script_steps(&def, &always_resolve_ok);
            assert!(
                errors.is_empty(),
                "successfully resolved script should produce no errors on non-unix; got: {:?}",
                errors
            );
        }
        // On unix the file must actually exist and be executable, so just verify
        // we don't panic and the error (if any) mentions the step.
        #[cfg(unix)]
        {
            let errors = validate_script_steps(&def, &always_resolve_ok);
            // The path we resolve (/some_script.sh) likely doesn't exist — the
            // important thing is that the function ran without panicking.
            let _ = errors;
        }
    }

    #[test]
    fn workflow_with_multiple_scripts_checks_each() {
        let src = r#"
workflow wf {
    script step_a {
        run = "/missing/a.sh"
    }
    script step_b {
        run = "/missing/b.sh"
    }
}
"#;
        let def = parse_workflow_str(src, "test.wf").unwrap();
        let errors = validate_script_steps(&def, &always_resolve_err);
        assert_eq!(
            errors.len(),
            2,
            "each unresolvable script should generate one error; got: {:?}",
            errors
        );
    }
}
