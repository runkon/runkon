use std::collections::{HashMap, HashSet};

use crate::engine::ExecutionState;

/// What to do with a well-formed `{{name}}` that has no value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unresolved {
    /// Drop the placeholder from the output.
    Strip,
    /// Keep the placeholder's original text.
    KeepLiteral,
}

/// True when `s` is a variable name: `[A-Za-z_][A-Za-z0-9_.-]*`.
///
/// This is the only kind of text the substitution functions treat as a placeholder
/// key (after trimming spaces and tabs). Hosts' validators should use it to decide
/// whether a `{{…}}` in a prompt is a placeholder or literal text.
///
/// Every engine- and host-injected name fits this grammar: input names, `ticket_*`,
/// `repo_*`, `<gate>.feedback`, `item.*` and `gate_feedback`.
pub fn is_variable_name(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
}

#[derive(Debug, Clone, Copy)]
enum Tag<'a> {
    Var(&'a str),
    If(&'a str),
    Else,
    EndIf,
}

fn skip_blank(s: &str) -> &str {
    s.trim_start_matches([' ', '\t'])
}

/// Split a leading variable name off `s`, if there is one.
fn take_name(s: &str) -> Option<(&str, &str)> {
    let end = s
        .find(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-')))
        .unwrap_or(s.len());
    let name = &s[..end];
    if is_variable_name(name) {
        Some((name, &s[end..]))
    } else {
        None
    }
}

/// Parse a tag at the start of `s` (which begins with `{{`). Returns the tag and its
/// byte length, or `None` when the text is not a well-formed tag. Scanning stops at
/// the first character that cannot belong to a tag, so it never reads past a newline
/// or an unrelated `}}`.
fn parse_tag(s: &str) -> Option<(Tag<'_>, usize)> {
    let rest = skip_blank(&s[2..]);
    let (tag, rest) = if let Some(after) = rest.strip_prefix("#if") {
        // Require at least one blank between `#if` and the name.
        let named = skip_blank(after);
        if named.len() == after.len() {
            return None;
        }
        let (name, rest) = take_name(named)?;
        (Tag::If(name), rest)
    } else if let Some(after) = rest.strip_prefix("/if") {
        (Tag::EndIf, after)
    } else {
        let (name, rest) = take_name(rest)?;
        if name == "else" {
            (Tag::Else, rest)
        } else {
            (Tag::Var(name), rest)
        }
    };
    let rest = skip_blank(rest).strip_prefix("}}")?;
    Some((tag, s.len() - rest.len()))
}

enum Tok<'a> {
    Text(&'a str),
    Tag(Tag<'a>, &'a str),
}

fn tokenize(template: &str) -> Vec<Tok<'_>> {
    let mut toks = Vec::new();
    let mut pos = 0;
    while pos < template.len() {
        let Some(rel) = template[pos..].find("{{") else {
            toks.push(Tok::Text(&template[pos..]));
            break;
        };
        let open = pos + rel;
        if open > pos {
            toks.push(Tok::Text(&template[pos..open]));
        }
        match parse_tag(&template[open..]) {
            Some((tag, len)) => {
                toks.push(Tok::Tag(tag, &template[open..open + len]));
                pos = open + len;
            }
            None => {
                // Not a placeholder: emit one `{` literally and resume one byte later,
                // so `{{{x}}}` still finds `{{x}}` and a stray `{{` swallows nothing.
                toks.push(Tok::Text(&template[open..open + 1]));
                pos = open + 1;
            }
        }
    }
    toks
}

/// Single-pass substitution of `{{name}}` placeholders and `{{#if name}}` blocks.
///
/// * `lookup` returns the raw value of a variable. `{{#if name}}` is true when the
///   value is set and non-empty.
/// * `render` writes a value into the output. Use `|v, out| out.push_str(v)` for
///   verbatim output, or a quoting function for shell contexts.
/// * `unresolved` decides what happens to a well-formed `{{name}}` with no value.
///
/// A `{{` is a placeholder only when the text up to the next `}}`, with spaces and
/// tabs trimmed, is a variable name (see [`is_variable_name`]) or a conditional tag.
/// Anything else (`{{...}}`, `{{<gate>.feedback}}`, a lone `{{`) is kept literally.
/// Unbalanced conditional tags are also kept literally. The template is scanned once
/// and substituted values are never re-scanned.
pub fn substitute_with<'v>(
    template: &str,
    lookup: impl Fn(&str) -> Option<&'v str>,
    render: impl Fn(&str, &mut String),
    unresolved: Unresolved,
) -> String {
    let toks = tokenize(template);

    // Pair up conditional tags; unbalanced ones are rendered literally.
    let mut literal = vec![false; toks.len()];
    let mut stack: Vec<(usize, Option<usize>)> = Vec::new();
    for (i, tok) in toks.iter().enumerate() {
        match tok {
            Tok::Tag(Tag::If(_), _) => stack.push((i, None)),
            Tok::Tag(Tag::Else, _) => match stack.last_mut() {
                Some((_, e @ None)) => *e = Some(i),
                _ => literal[i] = true,
            },
            Tok::Tag(Tag::EndIf, _) => literal[i] = stack.pop().is_none(),
            _ => {}
        }
    }
    for (if_idx, else_idx) in stack {
        literal[if_idx] = true;
        if let Some(e) = else_idx {
            literal[e] = true;
        }
    }

    let mut out = String::with_capacity(template.len());
    let mut stripped: Vec<&str> = Vec::new();
    // (emitting before the block, condition value)
    let mut blocks: Vec<(bool, bool)> = Vec::new();
    let mut emit = true;
    for (i, tok) in toks.iter().enumerate() {
        match tok {
            Tok::Text(t) => {
                if emit {
                    out.push_str(t);
                }
            }
            Tok::Tag(_, raw) if literal[i] => {
                if emit {
                    out.push_str(raw);
                }
            }
            Tok::Tag(Tag::If(name), _) => {
                let cond = lookup(name).is_some_and(|v| !v.is_empty());
                blocks.push((emit, cond));
                emit = emit && cond;
            }
            Tok::Tag(Tag::Else, _) => {
                if let Some(&(parent, cond)) = blocks.last() {
                    emit = parent && !cond;
                }
            }
            Tok::Tag(Tag::EndIf, _) => {
                if let Some((parent, _)) = blocks.pop() {
                    emit = parent;
                }
            }
            Tok::Tag(Tag::Var(name), raw) => {
                if !emit {
                    continue;
                }
                match lookup(name) {
                    Some(v) => render(v, &mut out),
                    None => match unresolved {
                        Unresolved::Strip => stripped.push(name),
                        Unresolved::KeepLiteral => out.push_str(raw),
                    },
                }
            }
        }
    }
    if !stripped.is_empty() {
        tracing::debug!(names = ?stripped, "stripped unresolved placeholders");
    }
    out
}

fn substitute_variables_impl(
    template: &str,
    vars: &HashMap<String, String>,
    unresolved: Unresolved,
) -> String {
    substitute_with(
        template,
        |k| vars.get(k).map(String::as_str),
        |v, out| out.push_str(v),
        unresolved,
    )
}

/// For agent prompts: substitutes variables AND strips unresolved `{{name}}`
/// placeholders.
///
/// Only `{{ name }}` (a variable name, spaces trimmed) is a placeholder; other `{{`
/// text such as `{{...}}` is kept literally. `{{#if name}} … {{else}} … {{/if}}`
/// blocks are supported. See [`substitute_with`].
pub fn substitute_variables(prompt: &str, vars: &HashMap<String, String>) -> String {
    substitute_variables_impl(prompt, vars, Unresolved::Strip)
}

/// For data contexts (env vars, sub-workflow inputs): substitutes variables but
/// preserves any `{{…}}` text that was not a resolvable template variable.
///
/// Follows the same name rule and `{{#if}}` support as [`substitute_variables`]; an
/// unknown `{{name}}` is kept verbatim, and counts as false in `{{#if name}}`.
pub fn substitute_variables_keep_literal(template: &str, vars: &HashMap<String, String>) -> String {
    substitute_variables_impl(template, vars, Unresolved::KeepLiteral)
}

/// The names of every `{{name}}` variable placeholder in `template`, in order.
///
/// Uses the same placeholder rule as [`substitute_variables`] (`{{...}}` and a lone
/// `{{` are not placeholders). `{{#if}}`, `{{else}}` and `{{/if}}` tags are not
/// included; a variable inside an `{{#if}}` body is.
pub fn placeholder_names(template: &str) -> Vec<&str> {
    tokenize(template)
        .into_iter()
        .filter_map(|t| match t {
            Tok::Tag(Tag::Var(name), _) => Some(name),
            _ => None,
        })
        .collect()
}

/// POSIX sh single-quote escape a value so it cannot break out of a shell command.
///
/// Wraps `s` in single quotes and replaces embedded `'` with `'\''`.
pub fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Build the variable map from execution state (used for substitution in sub-workflow inputs).
pub fn build_variable_map(state: &ExecutionState) -> HashMap<String, String> {
    let mut vars: HashMap<String, String> = HashMap::new();

    // Copy all of `state.inputs` through. This includes both user-supplied
    // inputs and engine-populated values that conductor-core injects via
    // `inject_ticket_variables` / `inject_repo_variables` (`ticket_url`,
    // `ticket_title`, `ticket_body`, `ticket_source_id`, `ticket_source_type`,
    // `ticket_raw_json`, `repo_name`).
    //
    // Earlier this loop filtered out every key in `ENGINE_INJECTED_KEYS` to
    // prevent user shadowing of engine values, but the engine only re-injects
    // 4 of the 11 names (`ticket_id`, `repo_id`, `repo_path`, `workflow_run_id`)
    // explicitly below — so the other 7 ended up empty in the variable map.
    // See #2636. Those keys get re-asserted below from `run_ctx`,
    // which preserves the "engine wins on conflict" property for the keys
    // the engine actually owns.
    for (k, v) in &state.inputs {
        vars.insert(k.clone(), v.clone());
    }

    // Engine-injected variables from the run context
    for (k, v) in state.run_ctx.injected_variables() {
        vars.insert(k.to_string(), v);
    }
    vars.insert("workflow_run_id".into(), state.workflow_run_id.clone());

    let prior_context = state
        .contexts
        .last()
        .map(|c| c.context.clone())
        .unwrap_or_default();
    vars.insert("prior_context".into(), prior_context);
    let prior_contexts_json = if state.contexts.is_empty() {
        "[]".to_string()
    } else {
        crate::helpers::serialize_or_empty_array(
            &state.contexts,
            "build_variable_map:prior_contexts",
        )
    };
    vars.insert("prior_contexts".into(), prior_contexts_json);
    if let Some(ref gf) = state.last_gate_feedback {
        vars.insert("gate_feedback".into(), gf.clone());
    }
    // prior_output: raw JSON from the last step's structured output (if any)
    if let Some(last_output) = state
        .contexts
        .iter()
        .rev()
        .find_map(|c| c.structured_output.as_ref())
    {
        vars.insert("prior_output".into(), last_output.clone());
    }
    // prior_output_file: path to the last script step's stdout temp file (if any)
    if let Some(path) = state
        .contexts
        .iter()
        .rev()
        .find_map(|c| c.output_file.as_ref())
    {
        vars.insert("prior_output_file".into(), path.clone());
    }
    // dry_run: "true" or "false"
    vars.insert("dry_run".into(), state.exec_config.dry_run.to_string());

    // Script-exported variables from prior steps' FLOW_OUTPUT extras (#2736).
    //
    // Any string-valued top-level field other than the FlowOutput-recognized
    // ones (`markers`, `context`) is exposed as `{{name}}` to subsequent
    // steps. Used by `resolve-pr-base.sh` to plumb `{{base_branch}}` to all
    // downstream consumers; future scripts can export additional values
    // without engine-side code changes.
    //
    // Parse as `FlowOutput` (not raw `serde_json::Value`) so the named fields
    // are stripped and we iterate only `.extras`. This couples the skip-list
    // to FlowOutput's serde schema — if FlowOutput gains a new named field,
    // it lands in named-field land automatically rather than leaking through
    // a manual `markers`/`context` guard.
    //
    // Shadowing guard: keys present in the host's injected-variables map (plus
    // the engine-owned `workflow_run_id`) cannot be overwritten by a script —
    // those are reserved for engine/host-controlled state. A script that
    // tries to export one of these gets a warning and is ignored.
    //
    // Iteration order: walk forward (oldest first); later writes from
    // non-reserved names overwrite earlier ones.
    let reserved: HashSet<&str> = state.run_ctx.injected_variables().into_keys().collect();
    for c in &state.contexts {
        let json = match &c.structured_output {
            Some(j) => j,
            None => continue,
        };
        let flow_output = match serde_json::from_str::<crate::helpers::FlowOutput>(json) {
            Ok(out) => out,
            Err(e) => {
                // structured_output is always written by script.rs via
                // serde_json, so a parse failure here is a real bug — but
                // not fatal to the run. Log and skip so other steps' exports
                // still flow.  Same path catches non-object JSON since
                // FlowOutput requires the input be a JSON object.
                tracing::warn!(
                    step = %c.step,
                    error = %e,
                    "build_variable_map: structured_output is not a valid FlowOutput JSON — \
                     {{name}} variable exports from this step will be unavailable",
                );
                continue;
            }
        };
        for (key, value) in &flow_output.extras {
            if reserved.contains(key.as_str()) || key == "workflow_run_id" {
                tracing::warn!(
                    step = %c.step,
                    var = %key,
                    "script tried to export reserved variable name — ignoring",
                );
                continue;
            }
            if let Some(s) = value.as_str() {
                vars.insert(key.clone(), s.to_string());
            }
        }
    }

    // Per-gate feedback, inserted last so the engine's value wins over a script extra of
    // the same name. A decided gate without feedback is an empty string; a gate that never
    // ran is absent.
    for (gate, value) in &state.gate_feedback_by_gate {
        vars.insert(
            format!("{gate}.feedback"),
            value.clone().unwrap_or_default(),
        );
    }

    vars
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder_names_lists_variables_only() {
        assert_eq!(placeholder_names("a {{x}} b {{ y }}"), vec!["x", "y"]);
        assert_eq!(placeholder_names("{{...}} {{ and {{a.b}}"), vec!["a.b"]);
        assert_eq!(
            placeholder_names("{{#if x}}{{y}}{{else}}{{z}}{{/if}}"),
            vec!["y", "z"]
        );
        assert!(placeholder_names("plain").is_empty());
    }

    #[test]
    fn build_variable_map_exposes_per_gate_feedback() {
        let cp = std::sync::Arc::new(crate::test_helpers::CountingPersistence::new());
        let mut state = crate::test_helpers::make_test_execution_state(
            cp as std::sync::Arc<dyn crate::traits::persistence::WorkflowPersistence>,
            "run-1".into(),
        );
        state
            .gate_feedback_by_gate
            .insert("first".into(), Some("edited draft".into()));
        state.gate_feedback_by_gate.insert("second".into(), None);
        let vars = build_variable_map(&state);
        assert_eq!(vars["first.feedback"], "edited draft");
        assert_eq!(vars["second.feedback"], "");
        assert!(!vars.contains_key("third.feedback"));
    }

    #[test]
    fn substitute_strips_unresolved() {
        let vars = HashMap::new();
        let result = substitute_variables("hello {{unknown}}", &vars);
        assert_eq!(result, "hello ");
    }

    #[test]
    fn substitute_resolves_known_strips_unknown() {
        let mut vars: HashMap<String, String> = HashMap::new();
        vars.insert("name".into(), "world".to_string());
        let result = substitute_variables("hello {{name}} and {{unknown}}", &vars);
        assert_eq!(result, "hello world and ");
    }

    #[test]
    fn substitute_keep_literal_preserves_unresolved() {
        let mut vars: HashMap<String, String> = HashMap::new();
        vars.insert("name".into(), "world".to_string());
        let result = substitute_variables_keep_literal("hello {{name}} and {{unknown}}", &vars);
        assert_eq!(result, "hello world and {{unknown}}");
    }

    #[test]
    fn substitute_keep_literal_preserves_embedded_json() {
        let json_value = r#"{"risks":["{{deterministic-review.score}}","other"]}"#.to_string();
        let mut vars: HashMap<String, String> = HashMap::new();
        vars.insert("prior_output".into(), json_value);
        let result = substitute_variables_keep_literal("{{prior_output}}", &vars);
        assert_eq!(
            result,
            r#"{"risks":["{{deterministic-review.score}}","other"]}"#
        );
    }

    #[test]
    fn substitute_no_double_substitution() {
        // If variable A's value contains {{B}}, B must not be expanded in the output.
        let mut vars: HashMap<String, String> = HashMap::new();
        vars.insert("a".into(), "{{b}}".to_string());
        vars.insert("b".into(), "injected".to_string());
        let result = substitute_variables_keep_literal("{{a}}", &vars);
        // Should emit the literal value of a, not expand {{b}} inside it.
        assert_eq!(result, "{{b}}");
    }

    fn vmap(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn literal_open_braces_do_not_swallow_text_in_either_mode() {
        let vars = vmap(&[("x", "1")]);
        let t = "a literal {{ here\nthen {{x}}";
        let want = "a literal {{ here\nthen 1";
        assert_eq!(substitute_variables(t, &vars), want);
        assert_eq!(substitute_variables_keep_literal(t, &vars), want);
    }

    #[test]
    fn non_name_braces_stay_literal() {
        let vars = vmap(&[("x", "1")]);
        for lit in [
            "{{...}}",
            "{{<gate>.feedback}}",
            "{{item.*}}",
            "{{}}",
            "{{ }}",
        ] {
            assert_eq!(substitute_variables(lit, &vars), lit);
            let before = format!("{lit} then {{{{x}}}}");
            assert_eq!(
                substitute_variables(&before, &vars),
                format!("{lit} then 1")
            );
            assert_eq!(
                substitute_variables_keep_literal(&before, &vars),
                format!("{lit} then 1")
            );
        }
    }

    #[test]
    fn open_braces_at_end_or_unclosed() {
        let vars = vmap(&[("x", "1")]);
        assert_eq!(substitute_variables("end {{", &vars), "end {{");
        assert_eq!(substitute_variables("{{x", &vars), "{{x");
        assert_eq!(substitute_variables("{{x}", &vars), "{{x}");
    }

    #[test]
    fn literal_open_braces_several_lines_before_placeholder() {
        let vars = vmap(&[("feature_base_branch", "release/1")]);
        let t = "line 1 {{ literal\nline 2\nline 3\nbase {{feature_base_branch}}";
        assert_eq!(
            substitute_variables(t, &vars),
            "line 1 {{ literal\nline 2\nline 3\nbase release/1"
        );
    }

    #[test]
    fn spaces_and_brace_runs() {
        let vars = vmap(&[("x", "1"), ("a.b-c_d", "2")]);
        assert_eq!(substitute_variables("{{ x }}", &vars), "1");
        assert_eq!(substitute_variables("{{\tx\t}}", &vars), "1");
        assert_eq!(substitute_variables("{{{{x}}", &vars), "{{1");
        assert_eq!(substitute_variables("{{{x}}}", &vars), "{1}");
        assert_eq!(substitute_variables("{{a.b-c_d}}", &vars), "2");
        assert_eq!(
            substitute_variables_keep_literal("{{ nope }}", &vars),
            "{{ nope }}"
        );
    }

    #[test]
    fn variable_name_grammar() {
        for ok in ["a", "_a", "a1", "a.b", "a-b", "gate.feedback"] {
            assert!(is_variable_name(ok), "{ok}");
        }
        for bad in ["", "1a", ".a", "-a", "a b", "a*", "<g>.f", "..."] {
            assert!(!is_variable_name(bad), "{bad}");
        }
    }

    #[test]
    fn if_blocks_set_empty_missing() {
        let set = vmap(&[("x", "v")]);
        let empty = vmap(&[("x", "")]);
        let none = vmap(&[]);
        let t = "[{{#if x}}A{{/if}}]";
        for f in [substitute_variables, substitute_variables_keep_literal] {
            assert_eq!(f(t, &set), "[A]");
            assert_eq!(f(t, &empty), "[]");
            assert_eq!(f(t, &none), "[]");
        }
    }

    #[test]
    fn if_else_nesting_and_body_placeholders() {
        let vars = vmap(&[("x", "v"), ("y", "")]);
        let t = "{{#if x}}x={{x}}{{#if y}}Y{{else}}noY{{/if}}{{else}}no x{{/if}}";
        assert_eq!(substitute_variables(t, &vars), "x=vnoY");
        let t2 = "{{ #if y }}a{{ else }}b{{ /if }}";
        // `#if` needs no space handling between `{{` and `#`: spaces are trimmed.
        assert_eq!(substitute_variables(t2, &vars), "b");
        // Unknown placeholders in a skipped branch are dropped, not kept.
        assert_eq!(
            substitute_variables_keep_literal("{{#if y}}{{zz}}{{/if}}", &vars),
            ""
        );
    }

    #[test]
    fn else_nested_in_false_parent_stays_suppressed() {
        let t = "{{#if y}}{{#if x}}A{{else}}B{{/if}}{{else}}C{{/if}}";
        // Outer false: neither inner arm may leak, only the outer else shows.
        let outer_false = vmap(&[("x", "v"), ("y", "")]);
        assert_eq!(substitute_variables(t, &outer_false), "C");
        let both_false = vmap(&[("x", ""), ("y", "")]);
        assert_eq!(substitute_variables(t, &both_false), "C");
        // Outer true: the inner conditional picks its own arm.
        let inner_false = vmap(&[("x", ""), ("y", "v")]);
        assert_eq!(substitute_variables(t, &inner_false), "B");
        let both_true = vmap(&[("x", "v"), ("y", "v")]);
        assert_eq!(substitute_variables(t, &both_true), "A");
    }

    #[test]
    fn unbalanced_conditionals_stay_literal() {
        let vars = vmap(&[("x", "v")]);
        assert_eq!(substitute_variables("{{#if x}}A", &vars), "{{#if x}}A");
        assert_eq!(substitute_variables("A{{/if}}", &vars), "A{{/if}}");
        assert_eq!(substitute_variables("A{{else}}B", &vars), "A{{else}}B");
        assert_eq!(
            substitute_variables("{{#if}}A{{/if}}", &vars),
            "{{#if}}A{{/if}}"
        );
        assert_eq!(
            substitute_variables("{{#each x}}{{x}}{{/each}}", &vars),
            "{{#each x}}v{{/each}}"
        );
        // A second `{{else}}` in one block is literal text, here inside the skipped branch.
        assert_eq!(
            substitute_variables("{{#if x}}A{{else}}B{{else}}C{{/if}}", &vars),
            "A"
        );
        assert_eq!(
            substitute_variables("{{#if y}}A{{else}}B{{else}}C{{/if}}", &vars),
            "B{{else}}C"
        );
    }

    #[test]
    fn if_does_not_rescan_values() {
        let vars = vmap(&[("a", "{{#if b}}X{{/if}}{{b}}"), ("b", "B")]);
        assert_eq!(
            substitute_variables("{{a}}", &vars),
            "{{#if b}}X{{/if}}{{b}}"
        );
    }

    #[test]
    fn render_hook_quotes_but_if_sees_raw_value() {
        let vars = vmap(&[("e", ""), ("v", "it's")]);
        let out = substitute_with(
            "{{#if e}}empty-set{{else}}empty-falsy{{/if}} {{e}} {{v}}",
            |k| vars.get(k).map(String::as_str),
            |v, out| out.push_str(&shell_quote(v)),
            Unresolved::Strip,
        );
        assert_eq!(out, "empty-falsy '' 'it'\\''s'");
    }

    #[test]
    fn shell_quote_no_double_substitution() {
        // Simulates the shell-quoting path used in script execution:
        // a shell-safe var map is built then substituted into the run template.
        let mut vars: HashMap<String, String> = HashMap::new();
        vars.insert("cmd".into(), "'{{evil}}'".to_string()); // already shell-quoted value
        vars.insert("evil".into(), ";rm -rf /".to_string());
        // The run template only references {{cmd}}; {{evil}} should not be expanded.
        let result = substitute_variables("run {{cmd}}", &vars);
        assert_eq!(result, "run '{{evil}}'");
    }

    /// `build_variable_map` exposes `{{base_branch}}` from any prior step's
    /// structured_output JSON containing a top-level `base_branch` string.
    /// #2736 — `resolve-pr-base.sh` writes this once at the start of
    /// review-pr.wf and downstream consumers read it without re-running gh.
    #[test]
    fn build_variable_map_exposes_base_branch_from_prior_context() {
        use crate::test_helpers::CountingPersistence;
        use std::sync::Arc;

        let cp = Arc::new(CountingPersistence::new());
        let mut state = crate::test_helpers::make_test_execution_state(
            cp as Arc<dyn crate::traits::persistence::WorkflowPersistence>,
            "run-1".into(),
        );

        // No prior context → {{base_branch}} should be unset.
        let vars = build_variable_map(&state);
        assert!(
            !vars.contains_key("base_branch"),
            "no prior step → no base_branch variable"
        );

        // A prior step with structured_output carrying base_branch → exposed.
        state.contexts.push(crate::types::ContextEntry {
            step: "resolve-pr-base".into(),
            iteration: 0,
            context: "release/0.10.0".into(),
            markers: vec!["base_branch_resolved".into()],
            structured_output: Some(
                r#"{"markers":["base_branch_resolved"],"context":"release/0.10.0","base_branch":"release/0.10.0"}"#
                    .into(),
            ),
            output_file: None,
        });
        let vars = build_variable_map(&state);
        assert_eq!(
            vars.get("base_branch").map(String::as_str),
            Some("release/0.10.0"),
            "base_branch must be exposed from prior structured_output"
        );

        // A later step with no base_branch → previous value persists.
        state.contexts.push(crate::types::ContextEntry {
            step: "detect-file-types".into(),
            iteration: 0,
            context: "code changes".into(),
            markers: vec![],
            structured_output: Some(r#"{"markers":[],"context":"Found 2 files"}"#.into()),
            output_file: None,
        });
        let vars = build_variable_map(&state);
        assert_eq!(
            vars.get("base_branch").map(String::as_str),
            Some("release/0.10.0"),
            "later step without base_branch must not clobber the value"
        );

        // A later step that overwrites base_branch → wins.
        state.contexts.push(crate::types::ContextEntry {
            step: "override".into(),
            iteration: 0,
            context: "main".into(),
            markers: vec![],
            structured_output: Some(
                r#"{"markers":[],"context":"main","base_branch":"main"}"#.into(),
            ),
            output_file: None,
        });
        let vars = build_variable_map(&state);
        assert_eq!(
            vars.get("base_branch").map(String::as_str),
            Some("main"),
            "later step with base_branch must overwrite earlier value"
        );
    }

    /// Substitution: a template referencing {{base_branch}} resolves to the
    /// value exposed by `build_variable_map`. End-to-end verification that the
    /// engine variable injection works for the new variable.
    #[test]
    fn substitute_uses_base_branch_from_variable_map() {
        use crate::test_helpers::CountingPersistence;
        use std::sync::Arc;

        let cp = Arc::new(CountingPersistence::new());
        let mut state = crate::test_helpers::make_test_execution_state(
            cp as Arc<dyn crate::traits::persistence::WorkflowPersistence>,
            "run-1".into(),
        );
        state.contexts.push(crate::types::ContextEntry {
            step: "resolve-pr-base".into(),
            iteration: 0,
            context: "release/0.10.0".into(),
            markers: vec![],
            structured_output: Some(
                r#"{"markers":[],"context":"release/0.10.0","base_branch":"release/0.10.0"}"#
                    .into(),
            ),
            output_file: None,
        });

        let vars = build_variable_map(&state);
        let rendered = substitute_variables("git diff origin/{{base_branch}}...HEAD", &vars);
        assert_eq!(rendered, "git diff origin/release/0.10.0...HEAD");
    }

    /// Generic exports: any string-valued top-level field beyond
    /// `markers`/`context` becomes a `{{name}}` variable. Used by future
    /// scripts that want to plumb their own values without engine-side
    /// code changes — see #2736 review round 2.
    #[test]
    fn build_variable_map_exposes_arbitrary_string_extras() {
        use crate::test_helpers::CountingPersistence;
        use std::sync::Arc;

        let cp = Arc::new(CountingPersistence::new());
        let mut state = crate::test_helpers::make_test_execution_state(
            cp as Arc<dyn crate::traits::persistence::WorkflowPersistence>,
            "run-1".into(),
        );
        state.contexts.push(crate::types::ContextEntry {
            step: "some-script".into(),
            iteration: 0,
            context: "ok".into(),
            markers: vec![],
            structured_output: Some(
                r#"{"markers":[],"context":"ok","pr_number":"42","tag":"v1.2.3"}"#.into(),
            ),
            output_file: None,
        });

        let vars = build_variable_map(&state);
        assert_eq!(vars.get("pr_number").map(String::as_str), Some("42"));
        assert_eq!(vars.get("tag").map(String::as_str), Some("v1.2.3"));
    }

    /// Shadowing guard: a script that tries to export a key already injected by
    /// the engine (workflow_run_id, ticket_id, repo_path, …) is ignored. The
    /// engine-injected value wins. Prevents a malicious or careless script
    /// from overriding load-bearing engine state.
    #[test]
    fn build_variable_map_blocks_engine_injected_key_shadowing() {
        use crate::test_helpers::CountingPersistence;
        use std::sync::Arc;

        let cp = Arc::new(CountingPersistence::new());
        let mut state = crate::test_helpers::make_test_execution_state(
            cp as Arc<dyn crate::traits::persistence::WorkflowPersistence>,
            "run-real".into(),
        );
        {
            let mut vars = std::collections::HashMap::new();
            vars.insert("repo_path", "/repo/real".to_string());
            vars.insert("ticket_id", "TICK-real".to_string());
            state.run_ctx = Arc::new(crate::traits::run_context::NoopRunContext::with_vars(vars))
                as Arc<dyn crate::traits::run_context::RunContext>;
        }

        // Script tries to override every host-injected key plus the engine-owned
        // workflow_run_id. The reserved set mirrors exactly what production code
        // guards: run_ctx.injected_variables().keys() + "workflow_run_id".
        let injected_keys: Vec<&'static str> =
            state.run_ctx.injected_variables().into_keys().collect();
        let mut malicious = serde_json::Map::new();
        malicious.insert("markers".into(), serde_json::Value::Array(vec![]));
        malicious.insert("context".into(), serde_json::Value::String("evil".into()));
        for key in injected_keys
            .iter()
            .copied()
            .chain(std::iter::once("workflow_run_id"))
        {
            malicious.insert(
                key.into(),
                serde_json::Value::String(format!("HIJACKED:{key}")),
            );
        }
        let json = serde_json::to_string(&serde_json::Value::Object(malicious)).unwrap();
        state.contexts.push(crate::types::ContextEntry {
            step: "evil-script".into(),
            iteration: 0,
            context: "evil".into(),
            markers: vec![],
            structured_output: Some(json),
            output_file: None,
        });

        let vars = build_variable_map(&state);

        // Engine-injected values should NOT be overridden.
        assert_eq!(
            vars.get("workflow_run_id").map(String::as_str),
            Some("run-real"),
            "workflow_run_id must not be hijacked"
        );
        assert_eq!(
            vars.get("repo_path").map(String::as_str),
            Some("/repo/real"),
            "repo_path must not be hijacked"
        );
        assert_eq!(
            vars.get("ticket_id").map(String::as_str),
            Some("TICK-real"),
            "ticket_id must not be hijacked"
        );

        // Sanity: every host-injected key and workflow_run_id is non-hijacked.
        let injected_keys: Vec<&'static str> =
            state.run_ctx.injected_variables().into_keys().collect();
        for key in injected_keys
            .iter()
            .copied()
            .chain(std::iter::once("workflow_run_id"))
        {
            if let Some(v) = vars.get(key) {
                assert!(
                    !v.starts_with("HIJACKED:"),
                    "engine-injected key '{key}' was overridden by script export"
                );
            }
        }
    }

    /// Malformed JSON in a step's structured_output must not abort the loop —
    /// other steps' exports should still flow. Covers the parse-failure log
    /// path in `build_variable_map`.
    #[test]
    fn build_variable_map_skips_steps_with_invalid_structured_output() {
        use crate::test_helpers::CountingPersistence;
        use std::sync::Arc;

        let cp = Arc::new(CountingPersistence::new());
        let mut state = crate::test_helpers::make_test_execution_state(
            cp as Arc<dyn crate::traits::persistence::WorkflowPersistence>,
            "run-1".into(),
        );

        // Step 1: malformed JSON. Should be skipped with a warning.
        state.contexts.push(crate::types::ContextEntry {
            step: "broken-step".into(),
            iteration: 0,
            context: String::new(),
            markers: vec![],
            structured_output: Some("this is not json".into()),
            output_file: None,
        });

        // Step 2: also "not an object" (a JSON array). FlowOutput parse
        // requires an object, so this should also be skipped — same path.
        state.contexts.push(crate::types::ContextEntry {
            step: "array-step".into(),
            iteration: 0,
            context: String::new(),
            markers: vec![],
            structured_output: Some(r#"["nope", "not an object"]"#.into()),
            output_file: None,
        });

        // Step 3: valid extras. Should flow.
        state.contexts.push(crate::types::ContextEntry {
            step: "good-step".into(),
            iteration: 0,
            context: "ok".into(),
            markers: vec![],
            structured_output: Some(r#"{"markers":[],"context":"ok","payload":"survived"}"#.into()),
            output_file: None,
        });

        let vars = build_variable_map(&state);
        // Bad steps contribute nothing.
        assert!(!vars.contains_key("broken-step"));
        // Good step's extras still appear — proves the loop continues past failures.
        assert_eq!(vars.get("payload").map(String::as_str), Some("survived"));
    }

    /// Regression for #2636: engine-populated values from `state.inputs`
    /// (`ticket_url` etc., set by conductor-core's `inject_ticket_variables`)
    /// must reach the variable map. Previously the loop filtered every key in
    /// `ENGINE_INJECTED_KEYS` and only 4 of the 11 names were re-injected from
    /// `run_ctx`, so the other 7 ended up empty.
    #[test]
    fn build_variable_map_injects_ticket_url_and_friends_from_state_inputs() {
        use crate::test_helpers::CountingPersistence;
        use std::sync::Arc;

        let cp = Arc::new(CountingPersistence::new());
        let mut state = crate::test_helpers::make_test_execution_state(
            cp as Arc<dyn crate::traits::persistence::WorkflowPersistence>,
            "run-1".into(),
        );
        // Simulate conductor-core's inject_ticket_variables / inject_repo_variables.
        state.inputs.insert(
            "ticket_url".into(),
            "https://github.com/owner/repo/issues/42".into(),
        );
        state
            .inputs
            .insert("ticket_title".into(), "Fix something".into());
        state
            .inputs
            .insert("ticket_body".into(), "body text".into());
        state.inputs.insert("ticket_source_id".into(), "42".into());
        state
            .inputs
            .insert("ticket_source_type".into(), "github".into());
        state.inputs.insert("ticket_raw_json".into(), "{}".into());
        state.inputs.insert("repo_name".into(), "owner/repo".into());
        // And one regular user input for sanity.
        state
            .inputs
            .insert("user_var".into(), "user-supplied".into());

        let vars = build_variable_map(&state);

        assert_eq!(
            vars.get("ticket_url").map(String::as_str),
            Some("https://github.com/owner/repo/issues/42"),
            "ticket_url must be exposed — this is the #2636 bug"
        );
        assert_eq!(
            vars.get("ticket_title").map(String::as_str),
            Some("Fix something"),
        );
        assert_eq!(
            vars.get("ticket_body").map(String::as_str),
            Some("body text")
        );
        assert_eq!(vars.get("ticket_source_id").map(String::as_str), Some("42"));
        assert_eq!(
            vars.get("ticket_source_type").map(String::as_str),
            Some("github"),
        );
        assert_eq!(vars.get("ticket_raw_json").map(String::as_str), Some("{}"));
        assert_eq!(
            vars.get("repo_name").map(String::as_str),
            Some("owner/repo")
        );
        // User inputs still flow.
        assert_eq!(
            vars.get("user_var").map(String::as_str),
            Some("user-supplied"),
        );
    }

    /// `run_ctx` values still win for the 4 keys the engine injects
    /// explicitly — protects against stale `state.inputs` values diverging
    /// from the actual run's context (e.g. on resume).
    #[test]
    fn build_variable_map_worktree_ctx_overrides_state_inputs_for_owned_keys() {
        use crate::test_helpers::CountingPersistence;
        use std::sync::Arc;

        let cp = Arc::new(CountingPersistence::new());
        let mut state = crate::test_helpers::make_test_execution_state(
            cp as Arc<dyn crate::traits::persistence::WorkflowPersistence>,
            "run-real".into(),
        );
        // run_ctx is the source of truth for these.
        {
            let mut vars = std::collections::HashMap::new();
            vars.insert("ticket_id", "TICK-real".to_string());
            vars.insert("repo_id", "repo-real".to_string());
            vars.insert("repo_path", "/real".to_string());
            state.run_ctx = Arc::new(crate::traits::run_context::NoopRunContext::with_vars(vars))
                as Arc<dyn crate::traits::run_context::RunContext>;
        }
        // state.inputs has stale values that would otherwise win after the
        // filter was removed in #2636.
        state.inputs.insert("ticket_id".into(), "TICK-stale".into());
        state.inputs.insert("repo_id".into(), "repo-stale".into());
        state.inputs.insert("repo_path".into(), "/stale".into());

        let vars = build_variable_map(&state);

        assert_eq!(vars.get("ticket_id").map(String::as_str), Some("TICK-real"));
        assert_eq!(vars.get("repo_id").map(String::as_str), Some("repo-real"));
        assert_eq!(vars.get("repo_path").map(String::as_str), Some("/real"));
        assert_eq!(
            vars.get("workflow_run_id").map(String::as_str),
            Some("run-real"),
        );
    }
}
