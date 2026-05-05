use std::collections::HashMap;

use crate::engine::{ExecutionState, ENGINE_INJECTED_KEYS};

fn substitute_variables_impl(
    template: &str,
    vars: &HashMap<String, String>,
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
pub fn substitute_variables(prompt: &str, vars: &HashMap<String, String>) -> String {
    substitute_variables_impl(prompt, vars, true)
}

/// For data contexts (env vars, sub-workflow inputs): substitutes variables but
/// preserves any `{{…}}` text that was not a template variable.
pub fn substitute_variables_keep_literal(template: &str, vars: &HashMap<String, String>) -> String {
    substitute_variables_impl(template, vars, false)
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
    // Shadowing guard: keys present in `ENGINE_INJECTED_KEYS` cannot be
    // overwritten by a script — those are reserved for engine-controlled
    // state (workflow_run_id, ticket_id, repo_path, etc.). A script that
    // tries to export one of these gets a warning and is ignored.
    //
    // Iteration order: walk forward (oldest first); later writes from
    // non-reserved names overwrite earlier ones.
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
            if ENGINE_INJECTED_KEYS.contains(&key.as_str()) {
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
            vars.insert(
                crate::traits::run_context::keys::REPO_PATH,
                "/repo/real".to_string(),
            );
            vars.insert(
                crate::traits::run_context::keys::TICKET_ID,
                "TICK-real".to_string(),
            );
            state.run_ctx = Arc::new(crate::traits::run_context::NoopRunContext::with_vars(vars))
                as Arc<dyn crate::traits::run_context::RunContext>;
        }

        // Script tries to override every engine-injected key it can find.
        // Prefer ENGINE_INJECTED_KEYS as the source of truth so this test
        // stays correct as that list evolves.
        let mut malicious = serde_json::Map::new();
        malicious.insert("markers".into(), serde_json::Value::Array(vec![]));
        malicious.insert("context".into(), serde_json::Value::String("evil".into()));
        for key in ENGINE_INJECTED_KEYS {
            malicious.insert(
                (*key).into(),
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

        // Sanity: every engine-injected key is non-hijacked, regardless of
        // whether it was set on the state we constructed.
        for key in ENGINE_INJECTED_KEYS {
            if let Some(v) = vars.get(*key) {
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
            vars.insert(
                crate::traits::run_context::keys::TICKET_ID,
                "TICK-real".to_string(),
            );
            vars.insert(
                crate::traits::run_context::keys::REPO_ID,
                "repo-real".to_string(),
            );
            vars.insert(
                crate::traits::run_context::keys::REPO_PATH,
                "/real".to_string(),
            );
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
