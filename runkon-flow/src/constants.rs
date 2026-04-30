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
     error, dismissed, workflow_title";

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
