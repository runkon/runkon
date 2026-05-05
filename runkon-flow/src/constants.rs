pub const STEP_ROLE_FOREACH: &str = "foreach";
pub const STEP_ROLE_WORKFLOW: &str = "workflow";
pub const STEP_ROLE_GATE: &str = "gate";
pub const STEP_ROLE_AGENT: &str = "agent";

/// Column list for `workflow_runs` SELECT queries (used by row mappers in both
/// conductor-core and runkon-flow). Single source of truth — importers use
/// `runkon_flow::constants::RUN_COLUMNS` or `pub use` re-export.
pub const RUN_COLUMNS: &str = "id, workflow_name, parent_run_id, status, dry_run, trigger, \
     started_at, ended_at, result_summary, definition_snapshot, inputs, \
     parent_workflow_run_id, iteration, blocked_on, \
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
     fan_out_total, fan_out_completed, fan_out_failed, fan_out_skipped, step_error";

/// Canonical metadata key strings for the seven Claude-SDK metric fields.
/// Writers (`ClaudeAgentExecutor`) and readers (`record_step_success`) must
/// use these constants so key names never diverge silently.
pub mod metadata_keys {
    pub const COST_USD: &str = "cost_usd";
    pub const NUM_TURNS: &str = "num_turns";
    pub const DURATION_MS: &str = "duration_ms";
    pub const INPUT_TOKENS: &str = "input_tokens";
    pub const OUTPUT_TOKENS: &str = "output_tokens";
    pub const CACHE_READ_INPUT_TOKENS: &str = "cache_read_input_tokens";
    pub const CACHE_CREATION_INPUT_TOKENS: &str = "cache_creation_input_tokens";
}

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
