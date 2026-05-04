pub const STEP_ROLE_FOREACH: &str = "foreach";
pub const STEP_ROLE_WORKFLOW: &str = "workflow";
pub const STEP_ROLE_GATE: &str = "gate";
pub const STEP_ROLE_AGENT: &str = "agent";

/// Column list for `workflow_runs` SELECT queries (used by row mappers in both
/// conductor-core and runkon-flow). Single source of truth — importers use
/// `runkon_flow::constants::RUN_COLUMNS` or `pub use` re-export.
pub const RUN_COLUMNS: &str =
    "id, workflow_name, worktree_id, parent_run_id, status, dry_run, trigger, \
     started_at, ended_at, result_summary, definition_snapshot, inputs, ticket_id, repo_id, \
     parent_workflow_run_id, target_label, default_bot_name, iteration, blocked_on, \
     total_input_tokens, total_output_tokens, total_cache_read_input_tokens, \
     total_cache_creation_input_tokens, total_turns, total_cost_usd, total_duration_ms, model, \
     error, dismissed, workflow_title, owner_token, lease_until, generation";

/// SQL fragment listing every terminal step status, for use in `IN`/`NOT IN` clauses.
pub const TERMINAL_STATUSES_SQL: &str = "'completed','failed','skipped','timed_out'";

/// Column list for `workflow_run_steps` SELECT queries (used by row mappers in both
/// conductor-core and runkon-flow). Single source of truth — importers use
/// `runkon_flow::constants::STEP_COLUMNS` or `pub use` re-export.
pub const STEP_COLUMNS: &str =
    "id, workflow_run_id, step_name, role, can_commit, condition_expr, status, \
     child_run_id, position, started_at, ended_at, result_text, condition_met, \
     iteration, parallel_group_id, context_out, markers_out, retry_count, \
     gate_type, gate_prompt, gate_timeout, gate_approved_by, gate_approved_at, gate_feedback, \
     structured_output, output_file, gate_options, gate_selections, \
     fan_out_total, fan_out_completed, fan_out_failed, fan_out_skipped, step_error, \
     input_tokens, output_tokens, cache_read_input_tokens, cache_creation_input_tokens, \
     cost_usd, num_turns, duration_ms";

pub const FLOW_OUTPUT_INSTRUCTION: &str = r#"
When you have finished your work, output the following block exactly as the
last thing in your response. Do not include this block in code examples or
anywhere else — only as the final output.

<<<FLOW_OUTPUT>>>
{"markers": [], "context": ""}
<<<END_FLOW_OUTPUT>>>

markers: array of string signals consumed by the workflow engine
         (e.g. ["has_review_issues", "has_critical_issues"])
context: one or two sentence summary of what you did or found,
         passed to the next step as {{prior_context}}
"#;
