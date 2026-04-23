use std::collections::HashMap;

use crate::engine::ExecutionState;
use crate::engine::ENGINE_INJECTED_KEYS;

fn substitute_variables_impl(
    template: &str,
    vars: &HashMap<&str, String>,
    strip_unresolved: bool,
) -> String {
    // Single-pass tokeniser: scan the original template once, emitting each
    // {{key}} replacement exactly once.  This prevents double-substitution —
    // a replaced value containing {{other}} is written verbatim and never
    // re-scanned, so injected placeholder text cannot escape shell quoting.
    let mut out = String::with_capacity(template.len());
    let mut pos = 0;
    let bytes = template.as_bytes();
    while pos < bytes.len() {
        if bytes[pos..].starts_with(b"{{") {
            if let Some(end_rel) = template[pos + 2..].find("}}") {
                let key = &template[pos + 2..pos + 2 + end_rel];
                if let Some(value) = vars.get(key) {
                    out.push_str(value);
                } else if !strip_unresolved {
                    // Preserve unresolved placeholders literally.
                    out.push_str(&template[pos..pos + 2 + end_rel + 2]);
                }
                pos += 2 + end_rel + 2;
            } else {
                // No closing `}}` — copy the rest verbatim.
                out.push_str(&template[pos..]);
                pos = bytes.len();
            }
        } else {
            // Find the next `{{` and copy everything before it.
            let next = template[pos..]
                .find("{{")
                .map(|i| pos + i)
                .unwrap_or(bytes.len());
            out.push_str(&template[pos..next]);
            pos = next;
        }
    }
    out
}

/// For agent prompts: substitutes variables AND strips unresolved `{{…}}` placeholders.
pub fn substitute_variables(prompt: &str, vars: &HashMap<&str, String>) -> String {
    substitute_variables_impl(prompt, vars, true)
}

/// For data contexts (env vars, sub-workflow inputs): substitutes variables but
/// preserves any `{{…}}` text that was not a template variable.
pub fn substitute_variables_keep_literal(template: &str, vars: &HashMap<&str, String>) -> String {
    substitute_variables_impl(template, vars, false)
}

/// POSIX sh single-quote escape a value so it cannot break out of a shell command.
///
/// Wraps `s` in single quotes and replaces embedded `'` with `'\''`.
pub fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Build the variable map from execution state (used for substitution in sub-workflow inputs).
pub fn build_variable_map(state: &ExecutionState) -> HashMap<&str, String> {
    let mut vars: HashMap<&str, String> = HashMap::new();

    // Non-injected user-defined inputs
    for (k, v) in &state.inputs {
        if !ENGINE_INJECTED_KEYS.contains(&k.as_str()) {
            vars.insert(k.as_str(), v.clone());
        }
    }

    // Engine-injected variables from the worktree context
    let wt = &state.worktree_ctx;
    if let Some(ref tid) = wt.ticket_id {
        vars.insert("ticket_id", tid.clone());
    }
    if let Some(ref rid) = wt.repo_id {
        vars.insert("repo_id", rid.clone());
    }
    vars.insert("repo_path", wt.repo_path.clone());
    vars.insert("workflow_run_id", state.workflow_run_id.clone());

    let prior_context = state
        .contexts
        .last()
        .map(|c| c.context.clone())
        .unwrap_or_default();
    vars.insert("prior_context", prior_context);
    let prior_contexts_json = if state.contexts.is_empty() {
        "[]".to_string()
    } else {
        serde_json::to_string(&state.contexts).unwrap_or_default()
    };
    vars.insert("prior_contexts", prior_contexts_json);
    if let Some(ref gf) = state.last_gate_feedback {
        vars.insert("gate_feedback", gf.clone());
    }
    // prior_output: raw JSON from the last step's structured output (if any)
    if let Some(last_output) = state
        .contexts
        .iter()
        .rev()
        .find_map(|c| c.structured_output.as_ref())
    {
        vars.insert("prior_output", last_output.clone());
    }
    // prior_output_file: path to the last script step's stdout temp file (if any)
    if let Some(path) = state
        .contexts
        .iter()
        .rev()
        .find_map(|c| c.output_file.as_ref())
    {
        vars.insert("prior_output_file", path.clone());
    }
    // dry_run: "true" or "false"
    vars.insert("dry_run", state.exec_config.dry_run.to_string());
    vars
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitute_strips_unresolved() {
        let vars = HashMap::new();
        let result = substitute_variables("hello {{unknown}}", &vars);
        assert_eq!(result, "hello ");
    }

    #[test]
    fn substitute_resolves_known_strips_unknown() {
        let mut vars = HashMap::new();
        vars.insert("name", "world".to_string());
        let result = substitute_variables("hello {{name}} and {{unknown}}", &vars);
        assert_eq!(result, "hello world and ");
    }

    #[test]
    fn substitute_keep_literal_preserves_unresolved() {
        let mut vars = HashMap::new();
        vars.insert("name", "world".to_string());
        let result = substitute_variables_keep_literal("hello {{name}} and {{unknown}}", &vars);
        assert_eq!(result, "hello world and {{unknown}}");
    }

    #[test]
    fn substitute_keep_literal_preserves_embedded_json() {
        let json_value = r#"{"risks":["{{deterministic-review.score}}","other"]}"#.to_string();
        let mut vars = HashMap::new();
        vars.insert("prior_output", json_value);
        let result = substitute_variables_keep_literal("{{prior_output}}", &vars);
        assert_eq!(
            result,
            r#"{"risks":["{{deterministic-review.score}}","other"]}"#
        );
    }

    #[test]
    fn substitute_no_double_substitution() {
        // If variable A's value contains {{B}}, B must not be expanded in the output.
        let mut vars = HashMap::new();
        vars.insert("a", "{{b}}".to_string());
        vars.insert("b", "injected".to_string());
        let result = substitute_variables_keep_literal("{{a}}", &vars);
        // Should emit the literal value of a, not expand {{b}} inside it.
        assert_eq!(result, "{{b}}");
    }

    #[test]
    fn shell_quote_no_double_substitution() {
        // Simulates the shell-quoting path used in script execution:
        // a shell-safe var map is built then substituted into the run template.
        let mut vars = HashMap::new();
        vars.insert("cmd", "'{{evil}}'".to_string()); // already shell-quoted value
        vars.insert("evil", ";rm -rf /".to_string());
        // The run template only references {{cmd}}; {{evil}} should not be expanded.
        let result = substitute_variables("run {{cmd}}", &vars);
        assert_eq!(result, "run '{{evil}}'");
    }
}
