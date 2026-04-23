pub const STEP_ROLE_FOREACH: &str = "foreach";
pub const STEP_ROLE_WORKFLOW: &str = "workflow";
pub const STEP_ROLE_GATE: &str = "gate";
pub const STEP_ROLE_AGENT: &str = "agent";

pub const CONDUCTOR_OUTPUT_INSTRUCTION: &str = r#"
When you have finished your work, output the following block exactly as the
last thing in your response. Do not include this block in code examples or
anywhere else — only as the final output.

<<<CONDUCTOR_OUTPUT>>>
{"markers": [], "context": ""}
<<<END_CONDUCTOR_OUTPUT>>>

markers: array of string signals consumed by the workflow engine
         (e.g. ["has_review_issues", "has_critical_issues"])
context: one or two sentence summary of what you did or found,
         passed to the next step as {{prior_context}}
"#;
