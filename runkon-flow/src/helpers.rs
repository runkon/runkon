use std::collections::HashMap;

use crate::dsl::{WhileNode, WorkflowNode};
use crate::status::WorkflowStepStatus;
use serde::{Deserialize, Serialize};

// Forward declaration for ExecutionState — defined in engine.rs
use crate::engine::ExecutionState;

// ---------------------------------------------------------------------------
// Flow output block parsing
// ---------------------------------------------------------------------------

/// Parsed `<<<FLOW_OUTPUT>>>` block.
///
/// `markers` and `context` are core engine-recognized fields. Any other
/// top-level JSON fields are preserved in `extras` so they round-trip through
/// re-serialization. Engine plumbing (e.g. `prompt_builder` exposing
/// `{{base_branch}}` from a `resolve-pr-base.sh` step — see #2736) reads from
/// `extras` to inject typed values as template variables for subsequent steps.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FlowOutput {
    #[serde(default)]
    pub markers: Vec<String>,
    #[serde(default)]
    pub context: String,
    /// Extra top-level fields preserved on parse and re-emitted on serialize.
    /// Allows scripts to expose typed values to downstream steps via the
    /// engine's variable substitution layer.
    #[serde(flatten, default)]
    pub extras: HashMap<String, serde_json::Value>,
}

/// Extract markers and context from a `<<<FLOW_OUTPUT>>> … <<<END_FLOW_OUTPUT>>>`
/// block embedded in `text`. Returns `None` when no valid block is found.
pub fn parse_flow_output(text: &str) -> Option<FlowOutput> {
    const START: &str = "<<<FLOW_OUTPUT>>>";
    const END: &str = "<<<END_FLOW_OUTPUT>>>";

    // Find the last START marker whose content starts with `{`, `[`, or a markdown
    // code fence (``` …) — guards against occurrences inside JSON string values or
    // plain prose where the marker appears as a literal string.
    let start_pos = text
        .rmatch_indices(START)
        .find(|(pos, _)| {
            let after = text[pos + START.len()..].trim_start();
            after.starts_with('{') || after.starts_with('[') || after.starts_with('`')
        })
        .map(|(pos, _)| pos)?;

    let after_start = &text[start_pos + START.len()..];
    let end_pos = after_start.find(END)?;
    let raw = after_start[..end_pos].trim();

    // Strip optional markdown code fences (```json … ``` or ``` … ```)
    let raw = if let Some(stripped) = raw.strip_prefix("```") {
        let stripped = stripped.trim_start_matches(|c: char| c.is_alphanumeric());
        stripped
            .trim_start_matches('\n')
            .trim_end_matches("```")
            .trim()
    } else {
        raw
    };

    // Best-effort cleanup common in LLM output: trailing commas and bad escapes.
    let cleaned = strip_trailing_commas(raw);
    let cleaned = fix_backslash_escapes(&cleaned);

    serde_json::from_str::<FlowOutput>(&cleaned)
        .map_err(|e| {
            let snippet: String = cleaned.chars().take(200).collect();
            tracing::warn!(
                "parse_flow_output: invalid JSON in FLOW_OUTPUT block: {e}\n  snippet: {snippet}"
            );
        })
        .ok()
}

/// Remove trailing commas before `}` or `]` (common LLM JSON artifact).
///
/// Preserves whitespace between the comma and the closing bracket so that
/// re-parsing produces the same layout.
pub fn strip_trailing_commas(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    // Reuse a single buffer across all commas to avoid repeated small heap
    // allocations (each comma would otherwise allocate a fresh String).
    let mut ws_buf = String::with_capacity(16);
    while let Some(c) = chars.next() {
        if c == ',' {
            ws_buf.clear();
            while chars.peek().is_some_and(|p| p.is_whitespace()) {
                ws_buf.push(chars.next().unwrap());
            }
            if chars.peek().is_some_and(|p| *p == '}' || *p == ']') {
                result.push_str(&ws_buf);
                continue;
            }
            result.push(c);
            result.push_str(&ws_buf);
        } else {
            result.push(c);
        }
    }
    result
}

/// Fix invalid backslash escapes inside JSON string literals.
///
/// Walks the input character-by-character, tracking JSON string boundaries.
/// When inside a string, a `\` followed by an invalid JSON escape character
/// is doubled to `\\`, making it a valid JSON escaped backslash. Valid escape
/// sequences (including `\\`, `\"`, `\uXXXX`) are emitted verbatim.
/// Backslashes outside string literals are passed through unchanged.
pub fn fix_backslash_escapes(s: &str) -> String {
    const VALID_ESCAPE: &[char] = &['"', '\\', '/', 'b', 'f', 'n', 'r', 't', 'u'];

    let mut chars = s.chars().peekable();
    let mut result = String::with_capacity(s.len() + 16);
    let mut in_string = false;

    while let Some(c) = chars.next() {
        if !in_string {
            result.push(c);
            if c == '"' {
                in_string = true;
            }
        } else {
            match c {
                '"' => {
                    result.push(c);
                    in_string = false;
                }
                '\\' => {
                    if chars.peek().is_some_and(|nc| VALID_ESCAPE.contains(nc)) {
                        result.push('\\');
                        result.push(chars.next().unwrap());
                    } else {
                        result.push('\\');
                        result.push('\\');
                    }
                }
                _ => result.push(c),
            }
        }
    }
    result
}

/// Parse a human-readable duration string into a `std::time::Duration`.
///
/// Supported suffixes: `ms` (milliseconds), `h` (hours), `m` (minutes), `s` (seconds).
/// A bare integer is treated as seconds.
pub(crate) fn parse_duration(s: &str) -> std::result::Result<std::time::Duration, String> {
    // "ms" must precede "s" so "3ms" is not partially matched by the "s" entry.
    for (suffix, millis_per_unit) in &[("ms", 1u64), ("h", 3_600_000), ("m", 60_000), ("s", 1_000)]
    {
        if let Some(n) = s.strip_suffix(suffix) {
            let count = n
                .parse::<u64>()
                .map_err(|e| format!("invalid timeout '{s}': {e}"))?;
            return Ok(std::time::Duration::from_millis(count * millis_per_unit));
        }
    }
    let sec = s
        .parse::<u64>()
        .map_err(|e| format!("invalid timeout '{s}': {e}"))?;
    Ok(std::time::Duration::from_secs(sec))
}

/// Serialize `v` to a JSON string; on failure log a warning with `ctx` and return `"[]"`.
pub fn serialize_or_empty_array<T: serde::Serialize>(v: &T, ctx: &str) -> String {
    serde_json::to_string(v).unwrap_or_else(|e| {
        tracing::warn!("{ctx}: failed to serialize to JSON array: {e}");
        "[]".to_string()
    })
}

/// Build a human-readable summary of a workflow execution.
pub fn build_workflow_summary(state: &ExecutionState) -> String {
    let steps = match state.persistence.get_steps(&state.workflow_run_id) {
        Ok(steps) => steps,
        Err(e) => {
            tracing::warn!(
                "build_workflow_summary: failed to fetch steps for run {}: {e}",
                state.workflow_run_id
            );
            vec![]
        }
    };

    let total = steps.len();
    let (completed, failed, skipped, timed_out) = steps.iter().fold(
        (0usize, 0usize, 0usize, 0usize),
        |(c, f, sk, to), s| match s.status {
            WorkflowStepStatus::Completed => (c + 1, f, sk, to),
            WorkflowStepStatus::Failed => (c, f + 1, sk, to),
            WorkflowStepStatus::Skipped => (c, f, sk + 1, to),
            WorkflowStepStatus::TimedOut => (c, f, sk, to + 1),
            _ => (c, f, sk, to),
        },
    );

    let mut lines = Vec::new();
    lines.push(format!(
        "Workflow '{}': {completed}/{total} steps completed{}{}{}",
        state.workflow_name,
        if failed > 0 {
            format!(", {failed} failed")
        } else {
            String::new()
        },
        if skipped > 0 {
            format!(", {skipped} skipped")
        } else {
            String::new()
        },
        if timed_out > 0 {
            format!(", {timed_out} timed out")
        } else {
            String::new()
        },
    ));

    for step in &steps {
        let marker = step.status.short_label();
        let iter_label = if step.iteration > 0 {
            format!(" (iter {})", step.iteration)
        } else {
            String::new()
        };
        let never_executed = step.status == WorkflowStepStatus::Failed && step.started_at.is_none();
        let step_note = if never_executed {
            " (never executed)"
        } else {
            ""
        };
        lines.push(format!(
            "  [{marker}] {}{iter_label}{step_note}",
            step.step_name
        ));
    }

    if state.all_succeeded {
        lines.push("Status: SUCCESS".to_string());
    } else {
        lines.push("Status: FAILED".to_string());
    }

    lines.join("\n")
}

/// Extract all leaf-node step keys from a workflow node.
///
/// Recurses into control-flow nodes (if/unless/while/always) and collects
/// keys from all trackable leaves: `Call`, `Parallel` agents, `Gate`, and
/// `CallWorkflow`.
pub fn collect_leaf_step_keys(node: &WorkflowNode) -> Vec<String> {
    match node {
        WorkflowNode::Call(c) => vec![c.agent.step_key()],
        WorkflowNode::Parallel(p) => p.calls.iter().map(|a| a.step_key()).collect(),
        WorkflowNode::Gate(g) => vec![g.name.clone()],
        WorkflowNode::CallWorkflow(cw) => vec![format!("workflow:{}", cw.workflow)],
        WorkflowNode::Script(s) => vec![s.name.clone()],
        WorkflowNode::If(n) => n.body.iter().flat_map(collect_leaf_step_keys).collect(),
        WorkflowNode::Unless(n) => n.body.iter().flat_map(collect_leaf_step_keys).collect(),
        WorkflowNode::While(n) => n.body.iter().flat_map(collect_leaf_step_keys).collect(),
        WorkflowNode::DoWhile(n) => n.body.iter().flat_map(collect_leaf_step_keys).collect(),
        WorkflowNode::Do(n) => n.body.iter().flat_map(collect_leaf_step_keys).collect(),
        WorkflowNode::Always(n) => n.body.iter().flat_map(collect_leaf_step_keys).collect(),
        WorkflowNode::ForEach(n) => vec![format!("foreach:{}", n.name)],
    }
}

/// Find the starting iteration for a while loop on resume.
///
/// Looks at the skip_completed set for step keys that match body nodes of the
/// while loop. Returns the max iteration that has all body nodes completed,
/// so the loop resumes from the iteration where it failed.
pub fn find_max_completed_while_iteration(state: &ExecutionState, node: &WhileNode) -> u32 {
    let step_map = match state.resume_ctx {
        Some(ref ctx) => &ctx.step_map,
        None => return 0,
    };

    // Collect step keys from all trackable body nodes
    let body_keys: Vec<String> = node.body.iter().flat_map(collect_leaf_step_keys).collect();

    if body_keys.is_empty() {
        return 0;
    }

    let body_key_set: std::collections::HashSet<&str> =
        body_keys.iter().map(String::as_str).collect();

    // Build a map from iteration -> set of completed step names, restricted to body
    // keys only — non-body steps from other parts of the workflow are irrelevant here.
    let mut completed_by_iter: std::collections::HashMap<u32, std::collections::HashSet<&str>> =
        std::collections::HashMap::new();
    for (name, iter) in step_map.keys() {
        if body_key_set.contains(name.as_str()) {
            completed_by_iter
                .entry(*iter)
                .or_default()
                .insert(name.as_str());
        }
    }

    // Find the highest iteration where all body nodes are completed.
    // Since completed_by_iter only contains body keys, a simple count check
    // avoids the O(body_keys) all() scan on every iteration.
    let body_len = body_keys.len();
    let mut iter = 0u32;
    while completed_by_iter.get(&iter).map(|s| s.len()).unwrap_or(0) == body_len {
        iter += 1;
    }
    iter
}

#[cfg(test)]
mod parse_tests {
    use super::parse_flow_output;

    #[test]
    fn parses_well_formed_block_with_markers_and_context() {
        let text = concat!(
            "some preamble\n",
            "<<<FLOW_OUTPUT>>>\n",
            r#"{"markers":["a","b"],"context":"hello world"}"#,
            "\n",
            "<<<END_FLOW_OUTPUT>>>\n",
            "some suffix"
        );
        let out = parse_flow_output(text).unwrap();
        assert_eq!(out.markers, vec!["a", "b"]);
        assert_eq!(out.context, "hello world");
    }

    #[test]
    fn strips_trailing_commas_before_braces() {
        let text = concat!(
            "<<<FLOW_OUTPUT>>>\n",
            r#"{"markers":["x",],"context":"ctx",}"#,
            "\n",
            "<<<END_FLOW_OUTPUT>>>"
        );
        let out = parse_flow_output(text).unwrap();
        assert_eq!(out.markers, vec!["x"]);
        assert_eq!(out.context, "ctx");
    }

    #[test]
    fn fixes_invalid_backslash_escapes() {
        // \p is not a valid JSON escape sequence; fix_backslash_escapes doubles it
        // so the JSON becomes parseable.
        let text = concat!(
            "<<<FLOW_OUTPUT>>>\n",
            r#"{"markers":[],"context":"C:\path\to\file"}"#,
            "\n",
            "<<<END_FLOW_OUTPUT>>>"
        );
        let out = parse_flow_output(text).unwrap();
        // Successfully parsed despite the invalid backslash sequences.
        assert!(out.context.contains("path"));
    }

    #[test]
    fn preserves_valid_backslash_escapes() {
        // \\ is a valid JSON escape for a literal backslash; fix_backslash_escapes
        // must not corrupt it into an unparseable sequence.
        let text = concat!(
            "<<<FLOW_OUTPUT>>>\n",
            r#"{"markers":[],"context":"C:\\Users\\dev"}"#,
            "\n",
            "<<<END_FLOW_OUTPUT>>>"
        );
        let out = parse_flow_output(text).unwrap();
        assert_eq!(out.context, r"C:\Users\dev");
    }

    #[test]
    fn strips_markdown_code_fence() {
        let text = concat!(
            "<<<FLOW_OUTPUT>>>\n",
            "```json\n",
            r#"{"markers":["fenced"],"context":"fenced context"}"#,
            "\n",
            "```\n",
            "<<<END_FLOW_OUTPUT>>>"
        );
        let out = parse_flow_output(text).unwrap();
        assert_eq!(out.markers, vec!["fenced"]);
        assert_eq!(out.context, "fenced context");
    }

    #[test]
    fn returns_none_when_no_block_present() {
        assert!(parse_flow_output("no flow output block here").is_none());
        assert!(parse_flow_output("").is_none());
    }

    /// `extras` captures any top-level fields beyond `markers` and `context`,
    /// and re-serialization round-trips them so downstream readers
    /// (e.g. `prompt_builder::build_variable_map` looking for `base_branch`)
    /// can pick them up. #2736.
    #[test]
    fn extras_fields_are_preserved_on_parse_and_serialize() {
        let text = concat!(
            "<<<FLOW_OUTPUT>>>\n",
            r#"{"markers":["base_branch_resolved"],"context":"release/0.10.0","base_branch":"release/0.10.0"}"#,
            "\n",
            "<<<END_FLOW_OUTPUT>>>"
        );
        let out = parse_flow_output(text).expect("parse must succeed");

        // Core fields still extracted correctly.
        assert_eq!(out.markers, vec!["base_branch_resolved".to_string()]);
        assert_eq!(out.context, "release/0.10.0");

        // Extra fields land in `extras`.
        assert_eq!(
            out.extras.get("base_branch").and_then(|v| v.as_str()),
            Some("release/0.10.0"),
            "base_branch should round-trip into extras"
        );

        // Re-serialization must include the extras at the top level so
        // structured_output JSON readers find them.
        let json = serde_json::to_string(&out).expect("serialize must succeed");
        let reparsed: serde_json::Value =
            serde_json::from_str(&json).expect("reparse must succeed");
        assert_eq!(
            reparsed.get("base_branch").and_then(|v| v.as_str()),
            Some("release/0.10.0")
        );
    }

    #[test]
    fn markers_field_missing_defaults_to_empty_vec() {
        let text = concat!(
            "<<<FLOW_OUTPUT>>>\n",
            r#"{"context":"only context, no markers"}"#,
            "\n",
            "<<<END_FLOW_OUTPUT>>>"
        );
        let out = parse_flow_output(text).unwrap();
        assert!(out.markers.is_empty());
        assert_eq!(out.context, "only context, no markers");
    }

    #[test]
    fn context_field_missing_defaults_to_empty_string() {
        let text = concat!(
            "<<<FLOW_OUTPUT>>>\n",
            r#"{"markers":["m1"]}"#,
            "\n",
            "<<<END_FLOW_OUTPUT>>>"
        );
        let out = parse_flow_output(text).unwrap();
        assert_eq!(out.markers, vec!["m1"]);
        assert_eq!(out.context, "");
    }

    #[test]
    fn marker_in_field_value_finds_real_block() {
        let text = r#"Some agent output.
<<<FLOW_OUTPUT>>>
{
  "markers": ["done"],
  "context": "saw <<<FLOW_OUTPUT>>> in the log and handled it"
}
<<<END_FLOW_OUTPUT>>>
"#;
        let out = parse_flow_output(text).unwrap();
        assert_eq!(out.markers, vec!["done"]);
        assert!(out.context.contains("<<<FLOW_OUTPUT>>>"));
    }

    #[test]
    fn skips_code_examples_finds_real_block() {
        let text = r#"Here is how to emit output:
```bash
echo '<<<FLOW_OUTPUT>>>'
echo '{"markers": ["fake"], "context": "example"}'
echo '<<<END_FLOW_OUTPUT>>>'
```

Actual output:
<<<FLOW_OUTPUT>>>
{"markers": ["real"], "context": "this is the real result"}
<<<END_FLOW_OUTPUT>>>
"#;
        let out = parse_flow_output(text).unwrap();
        assert_eq!(out.markers, vec!["real"]);
        assert_eq!(out.context, "this is the real result");
    }

    #[test]
    fn multiple_complete_blocks_returns_last() {
        let text = r#"Example 1:
<<<FLOW_OUTPUT>>>
{"markers": ["example1"], "context": "first example"}
<<<END_FLOW_OUTPUT>>>

Example 2:
<<<FLOW_OUTPUT>>>
{"markers": ["example2"], "context": "second example"}
<<<END_FLOW_OUTPUT>>>

Real output:
<<<FLOW_OUTPUT>>>
{"markers": ["real"], "context": "the actual result"}
<<<END_FLOW_OUTPUT>>>
"#;
        let out = parse_flow_output(text).unwrap();
        assert_eq!(out.markers, vec!["real"]);
        assert_eq!(out.context, "the actual result");
    }

    #[test]
    fn malformed_json_returns_none() {
        let text = concat!(
            "<<<FLOW_OUTPUT>>>\n",
            "{markers: [\"done\"]}\n",
            "<<<END_FLOW_OUTPUT>>>\n"
        );
        assert!(parse_flow_output(text).is_none());
    }

    #[test]
    fn markers_field_with_wrong_type_returns_none() {
        // Direct deserialization is stricter than the old manual extraction:
        // a "markers" field that is a string instead of an array must fail.
        let text = concat!(
            "<<<FLOW_OUTPUT>>>\n",
            r#"{"markers":"not-an-array","context":"ok"}"#,
            "\n",
            "<<<END_FLOW_OUTPUT>>>\n"
        );
        assert!(
            parse_flow_output(text).is_none(),
            "markers field with non-array type should cause parse failure"
        );
    }

    #[test]
    fn context_field_with_wrong_type_returns_none() {
        // A "context" field that is a number instead of a string must fail.
        let text = concat!(
            "<<<FLOW_OUTPUT>>>\n",
            r#"{"markers":["m1"],"context":42}"#,
            "\n",
            "<<<END_FLOW_OUTPUT>>>\n"
        );
        assert!(
            parse_flow_output(text).is_none(),
            "context field with non-string type should cause parse failure"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{collect_leaf_step_keys, parse_duration, serialize_or_empty_array};
    use crate::dsl::{
        AgentRef, AlwaysNode, CallNode, CallWorkflowNode, Condition, DoNode, ForEachNode, GateNode,
        GateType, IfNode, OnMaxIter, ParallelNode, ScriptNode, UnlessNode, WhileNode, WorkflowNode,
    };
    use crate::test_helpers::call_node;

    fn script_node(name: &str) -> WorkflowNode {
        WorkflowNode::Script(ScriptNode {
            name: name.to_string(),
            run: "echo hello".to_string(),
            env: Default::default(),
            timeout: None,
            retries: 0,
            on_fail: None,
            bot_name: None,
        })
    }

    // ---- collect_leaf_step_keys ----

    #[test]
    fn leaf_keys_from_call_node() {
        let node = call_node("plan");
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["plan".to_string()]);
    }

    #[test]
    fn leaf_keys_from_script_node() {
        let node = script_node("lint");
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["lint".to_string()]);
    }

    #[test]
    fn leaf_keys_from_call_workflow_node() {
        let node = WorkflowNode::CallWorkflow(CallWorkflowNode {
            workflow: "child-wf".to_string(),
            inputs: Default::default(),
            retries: 0,
            on_fail: None,
            bot_name: None,
        });
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["workflow:child-wf".to_string()]);
    }

    #[test]
    fn leaf_keys_from_parallel_node() {
        let node = WorkflowNode::Parallel(ParallelNode {
            fail_fast: true,
            min_success: None,
            calls: vec![
                AgentRef::Name("agent_a".to_string()),
                AgentRef::Name("agent_b".to_string()),
            ],
            output: None,
            call_outputs: Default::default(),
            with: vec![],
            call_with: Default::default(),
            call_if: Default::default(),
            call_retries: Default::default(),
        });
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["agent_a".to_string(), "agent_b".to_string()]);
    }

    #[test]
    fn leaf_keys_from_gate_node() {
        let node = WorkflowNode::Gate(GateNode {
            name: "human_approval".to_string(),
            gate_type: GateType::HumanApproval,
            prompt: None,
            min_approvals: 1,
            approval_mode: Default::default(),
            timeout_secs: 0,
            on_timeout: crate::dsl::OnTimeout::Fail,
            bot_name: None,
            quality_gate: None,
            options: None,
        });
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["human_approval".to_string()]);
    }

    #[test]
    fn leaf_keys_from_foreach_node() {
        let node = WorkflowNode::ForEach(ForEachNode {
            name: "fan".to_string(),
            over: "tickets".to_string(),
            scope: None,
            filter: Default::default(),
            ordered: false,
            on_cycle: crate::dsl::OnCycle::Fail,
            max_parallel: 4,
            workflow: "child".to_string(),
            inputs: Default::default(),
            on_child_fail: crate::dsl::OnChildFail::Continue,
        });
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["foreach:fan".to_string()]);
    }

    #[test]
    fn leaf_keys_from_if_node_recurses_into_body() {
        let node = WorkflowNode::If(IfNode {
            condition: Condition::BoolInput {
                input: "flag".to_string(),
            },
            body: vec![call_node("inner_agent")],
        });
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["inner_agent".to_string()]);
    }

    #[test]
    fn leaf_keys_from_unless_node_recurses_into_body() {
        let node = WorkflowNode::Unless(UnlessNode {
            condition: Condition::StepMarker {
                step: "s".to_string(),
                marker: "m".to_string(),
            },
            body: vec![call_node("fallback")],
        });
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["fallback".to_string()]);
    }

    #[test]
    fn leaf_keys_from_while_node_recurses_into_body() {
        let node = WorkflowNode::While(WhileNode {
            step: "s".to_string(),
            marker: "m".to_string(),
            max_iterations: 5,
            stuck_after: None,
            on_max_iter: OnMaxIter::Fail,
            body: vec![call_node("loop_agent")],
        });
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["loop_agent".to_string()]);
    }

    #[test]
    fn leaf_keys_from_do_node_recurses_into_body() {
        let node = WorkflowNode::Do(DoNode {
            output: None,
            with: vec![],
            body: vec![call_node("step_a"), script_node("step_b")],
        });
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["step_a".to_string(), "step_b".to_string()]);
    }

    #[test]
    fn leaf_keys_from_always_node_recurses_into_body() {
        let node = WorkflowNode::Always(AlwaysNode {
            body: vec![call_node("cleanup")],
        });
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["cleanup".to_string()]);
    }

    #[test]
    fn leaf_keys_empty_body_returns_empty() {
        let node = WorkflowNode::If(IfNode {
            condition: Condition::BoolInput {
                input: "x".to_string(),
            },
            body: vec![],
        });
        let keys = collect_leaf_step_keys(&node);
        assert!(keys.is_empty());
    }

    #[test]
    fn leaf_keys_path_agent_uses_file_stem() {
        let node = WorkflowNode::Call(CallNode {
            agent: AgentRef::Path(".claude/agents/plan.md".to_string()),
            retries: 0,
            on_fail: None,
            output: None,
            with: vec![],
            bot_name: None,
            plugin_dirs: vec![],
            timeout: None,
        });
        let keys = collect_leaf_step_keys(&node);
        assert_eq!(keys, vec!["plan".to_string()]);
    }

    // ---------------------------------------------------------------------------
    // parse_duration — all suffix branches and bare-integer fallback
    // ---------------------------------------------------------------------------

    #[test]
    fn parse_duration_milliseconds() {
        assert_eq!(
            parse_duration("250ms").unwrap(),
            std::time::Duration::from_millis(250)
        );
    }

    #[test]
    fn parse_duration_hours() {
        assert_eq!(
            parse_duration("2h").unwrap(),
            std::time::Duration::from_secs(7200)
        );
    }

    #[test]
    fn parse_duration_minutes() {
        assert_eq!(
            parse_duration("30m").unwrap(),
            std::time::Duration::from_secs(1800)
        );
    }

    #[test]
    fn parse_duration_seconds() {
        assert_eq!(
            parse_duration("10s").unwrap(),
            std::time::Duration::from_secs(10)
        );
    }

    #[test]
    fn parse_duration_bare_integer_treated_as_seconds() {
        assert_eq!(
            parse_duration("5").unwrap(),
            std::time::Duration::from_secs(5)
        );
    }

    #[test]
    fn parse_duration_ms_not_matched_by_s_suffix() {
        // "3ms" must not be misinterpreted as "3m" + trailing "s"
        assert_eq!(
            parse_duration("3ms").unwrap(),
            std::time::Duration::from_millis(3)
        );
    }

    #[test]
    fn parse_duration_invalid_returns_err() {
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("1x").is_err());
    }

    // ---------------------------------------------------------------------------
    // serialize_or_empty_array — happy path and fallback
    // ---------------------------------------------------------------------------

    #[test]
    fn serialize_or_empty_array_serializes_vec() {
        let result = serialize_or_empty_array(&vec!["a", "b"], "test");
        assert_eq!(result, r#"["a","b"]"#);
    }

    #[test]
    fn serialize_or_empty_array_returns_bracket_pair_on_failure() {
        struct AlwaysFails;
        impl serde::Serialize for AlwaysFails {
            fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("intentional failure"))
            }
        }
        let result = serialize_or_empty_array(&AlwaysFails, "test");
        assert_eq!(result, "[]");
    }
}
