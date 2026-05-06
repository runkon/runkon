use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use runkon_flow::constants::FLOW_OUTPUT_INSTRUCTION;
use runkon_flow::output_schema::{ArrayItems, FieldDef, FieldType, OutputSchema};
use runkon_runtimes::{AgentDef, AgentRole};
use serde::Deserialize;

// ---------------------------------------------------------------------------
// Frontmatter parsing
// ---------------------------------------------------------------------------

/// Split a file's content into `(frontmatter_yaml, body)`.
///
/// Returns `None` if the content doesn't start with `---` or has no closing `---`.
fn parse_frontmatter(content: &str) -> Option<(&str, &str)> {
    let trimmed = content.trim_start();
    if !trimmed.starts_with("---") {
        return None;
    }
    let after_open = &trimmed[3..];
    let after_open = after_open.strip_prefix('\n').unwrap_or(after_open);
    let close_pos = after_open.find("\n---")?;
    let yaml = &after_open[..close_pos];
    let rest = &after_open[close_pos + 4..]; // skip "\n---"
    let body = rest.strip_prefix('\n').unwrap_or(rest);
    Some((yaml, body))
}

#[derive(Debug, Clone, Deserialize)]
struct AgentFrontmatter {
    #[serde(default = "default_role")]
    role: String,
    #[serde(default)]
    can_commit: bool,
    #[serde(default)]
    model: Option<String>,
    #[serde(default = "default_runtime")]
    runtime: String,
}

fn default_role() -> String {
    "reviewer".to_string()
}

fn default_runtime() -> String {
    "claude".to_string()
}

fn parse_agent_file(path: &Path) -> Result<AgentDef, String> {
    let content = fs::read_to_string(path)
        .map_err(|e| format!("Failed to read agent file {}: {e}", path.display()))?;

    let file_stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string();

    let (frontmatter, body) = match parse_frontmatter(&content) {
        Some(pair) => pair,
        None => {
            return Ok(AgentDef {
                name: file_stem,
                role: AgentRole::Reviewer,
                can_commit: false,
                model: None,
                runtime: default_runtime(),
                prompt: content.trim().to_string(),
            });
        }
    };

    let fm: AgentFrontmatter = serde_yml::from_str(frontmatter)
        .map_err(|e| format!("Invalid YAML frontmatter in {}: {e}", path.display()))?;

    let role: AgentRole = fm
        .role
        .parse()
        .map_err(|e: String| format!("In {}: {e}", path.display()))?;

    if fm.can_commit && role != AgentRole::Actor {
        return Err(format!(
            "In {}: can_commit requires role: actor",
            path.display()
        ));
    }

    Ok(AgentDef {
        name: file_stem,
        role,
        can_commit: fm.can_commit,
        model: fm.model,
        runtime: fm.runtime,
        prompt: body.trim().to_string(),
    })
}

// ---------------------------------------------------------------------------
// Path safety helpers
// ---------------------------------------------------------------------------

fn validate_path_within_base(path: &Path, base: &str) -> Result<PathBuf, String> {
    let canonical = path
        .canonicalize()
        .map_err(|_| format!("Agent file not found: '{}'", path.display()))?;
    let canonical_base = PathBuf::from(base)
        .canonicalize()
        .map_err(|e| format!("Failed to canonicalize base '{base}': {e}"))?;
    if !canonical.starts_with(&canonical_base) {
        return Err(format!(
            "Agent path '{}' escapes the base directory — path traversal is not allowed",
            path.display()
        ));
    }
    Ok(canonical)
}

fn validate_path_within_either_base(path: &Path, base1: &str, base2: &str) -> Result<(), String> {
    validate_path_within_base(path, base1)
        .or_else(|_| validate_path_within_base(path, base2))
        .map(|_| ())
}

fn find_agent_path(bases: &[&str], subdir: &Path, filename: &str) -> Option<PathBuf> {
    bases.iter().find_map(|base| {
        let path = PathBuf::from(base).join(subdir).join(filename);
        path.is_file().then_some(path)
    })
}

// ---------------------------------------------------------------------------
// Agent loading
// ---------------------------------------------------------------------------

/// Load an agent definition by name from the standard search order.
///
/// Resolution order (first match wins):
/// 1. `.conductor/workflows/<workflow_name>/agents/<name>.md` — workflow-local override
/// 2. `.conductor/agents/<name>.md` — shared conductor agents
/// 3. `.claude/agents/<name>.md` — Claude Code agents fallback
/// 4. `<plugin_dir>/agents/<name>.md` — extra plugin directories
pub fn load_agent(
    working_dir: &str,
    repo_path: &str,
    name: &str,
    workflow_name: Option<&str>,
    plugin_dirs: &[String],
) -> Result<AgentDef, String> {
    let filename = format!("{name}.md");
    let bases = [working_dir, repo_path];

    // 1. Workflow-local override (worktree, then repo)
    if let Some(wf_name) = workflow_name {
        let subdir = Path::new(".conductor")
            .join("workflows")
            .join(wf_name)
            .join("agents");
        if let Some(path) = find_agent_path(&bases, &subdir, &filename) {
            validate_path_within_either_base(&path, working_dir, repo_path)?;
            return parse_agent_file(&path);
        }
    }

    // 2. Shared conductor agents (worktree, then repo)
    if let Some(path) = find_agent_path(&bases, Path::new(".conductor/agents"), &filename) {
        validate_path_within_either_base(&path, working_dir, repo_path)?;
        return parse_agent_file(&path);
    }

    // 3. Claude Code agents fallback (worktree, then repo)
    if let Some(path) = find_agent_path(&bases, Path::new(".claude/agents"), &filename) {
        validate_path_within_either_base(&path, working_dir, repo_path)?;
        return parse_agent_file(&path);
    }

    // 4. Extra plugin directories (lowest priority)
    for dir in plugin_dirs {
        let path = Path::new(dir).join("agents").join(&filename);
        if path.is_file() {
            validate_path_within_base(&path, dir)?;
            return parse_agent_file(&path);
        }
    }

    let mut searched = String::new();
    if let Some(wf) = workflow_name {
        searched.push_str(&format!("  .conductor/workflows/{wf}/agents/{filename}\n"));
    }
    searched.push_str(&format!("  .conductor/agents/{filename}\n"));
    searched.push_str(&format!("  .claude/agents/{filename}"));
    for dir in plugin_dirs {
        searched.push_str(&format!("\n  {dir}/agents/{filename}"));
    }

    Err(format!("Agent '{name}' not found. Searched:\n{searched}"))
}

// ---------------------------------------------------------------------------
// Snippet loading
// ---------------------------------------------------------------------------

fn validate_name_segment(name: &str) -> Result<(), String> {
    if name.contains("..") || name.contains('/') || name.contains('\\') || name.contains('\0') {
        return Err(format!(
            "Snippet name '{name}' contains invalid characters (path separators or '..' are not allowed)"
        ));
    }
    Ok(())
}

fn find_snippet_path(bases: &[&str], subdir: &Path, filename: &str) -> Option<PathBuf> {
    bases.iter().find_map(|base| {
        let path = PathBuf::from(base).join(subdir).join(filename);
        path.is_file().then_some(path)
    })
}

fn load_snippet_by_name(
    working_dir: &str,
    repo_path: &str,
    name: &str,
    workflow_name: Option<&str>,
) -> Result<String, String> {
    validate_name_segment(name)?;
    if let Some(wf) = workflow_name {
        validate_name_segment(wf)?;
    }

    let filename = format!("{name}.md");
    let bases = [working_dir, repo_path];

    // 1. Workflow-local override
    if let Some(wf_name) = workflow_name {
        let subdir = Path::new(".conductor")
            .join("workflows")
            .join(wf_name)
            .join("prompts");
        if let Some(path) = find_snippet_path(&bases, &subdir, &filename) {
            return fs::read_to_string(&path)
                .map(|s| s.trim().to_string())
                .map_err(|e| format!("Failed to read snippet {}: {e}", path.display()));
        }
    }

    // 2. Shared conductor prompts
    if let Some(path) = find_snippet_path(&bases, Path::new(".conductor/prompts"), &filename) {
        return fs::read_to_string(&path)
            .map(|s| s.trim().to_string())
            .map_err(|e| format!("Failed to read snippet {}: {e}", path.display()));
    }

    let wf_hint = workflow_name
        .map(|wf| format!("  .conductor/workflows/{wf}/prompts/{filename}\n"))
        .unwrap_or_default();
    Err(format!(
        "Prompt snippet '{name}' not found. Searched:\n{wf_hint}  .conductor/prompts/{filename}"
    ))
}

fn load_snippet_by_path(repo_path: &str, rel_path: &str) -> Result<String, String> {
    if Path::new(rel_path).is_absolute() {
        return Err(format!(
            "Explicit prompt snippet path '{rel_path}' must be relative, not absolute"
        ));
    }

    let joined = PathBuf::from(repo_path).join(rel_path);
    let Ok(canonical) = joined.canonicalize() else {
        return Err(format!("Prompt snippet file not found: '{rel_path}'"));
    };

    let canonical_repo = PathBuf::from(repo_path)
        .canonicalize()
        .map_err(|e| format!("Failed to canonicalize repo root '{repo_path}': {e}"))?;

    if !canonical.starts_with(&canonical_repo) {
        return Err(format!(
            "Prompt snippet path '{rel_path}' escapes the repository root — path traversal is not allowed"
        ));
    }

    if !canonical.is_file() {
        return Err(format!("Prompt snippet file not found: '{rel_path}'"));
    }

    fs::read_to_string(&canonical)
        .map(|s| s.trim().to_string())
        .map_err(|e| format!("Failed to read snippet file {}: {e}", canonical.display()))
}

/// Load and concatenate multiple prompt snippets into a single string.
///
/// Snippet refs containing `/` or `\` are treated as explicit paths relative to
/// `repo_path`; all others are resolved by name via the standard search order.
pub fn load_and_concat_snippets(
    working_dir: &str,
    repo_path: &str,
    refs: &[String],
    workflow_name: Option<&str>,
) -> Result<String, String> {
    if refs.is_empty() {
        return Ok(String::new());
    }

    let mut parts = Vec::with_capacity(refs.len());
    for name in refs {
        let content = if name.contains('/') || name.contains('\\') {
            load_snippet_by_path(repo_path, name)?
        } else {
            load_snippet_by_name(working_dir, repo_path, name, workflow_name)?
        };
        parts.push(content);
    }
    Ok(parts.join("\n\n"))
}

// ---------------------------------------------------------------------------
// Prompt generation
// ---------------------------------------------------------------------------

fn substitute_variables(template: &str, vars: &HashMap<&str, &str>) -> String {
    let mut result = String::with_capacity(template.len());
    let mut remaining = template;
    while let Some(open) = remaining.find("{{") {
        result.push_str(&remaining[..open]);
        remaining = &remaining[open + 2..];
        if let Some(close) = remaining.find("}}") {
            let key = &remaining[..close];
            remaining = &remaining[close + 2..];
            if let Some(val) = vars.get(key) {
                result.push_str(val);
            }
            // unresolved → strip (drop it)
        } else {
            result.push_str("{{");
            break;
        }
    }
    result.push_str(remaining);
    result
}

/// Generate schema-specific output instructions to append to an agent prompt.
pub fn generate_prompt_instructions(schema: &OutputSchema) -> String {
    let mut out = String::new();
    out.push_str(
        "When you have finished your work, output the following block exactly as the\n\
         last thing in your response. Do not include this block in code examples or\n\
         anywhere else — only as the final output.\n\n\
         <<<FLOW_OUTPUT>>>\n",
    );

    let json_example = generate_json_example(&schema.fields, 0);
    out.push_str(&json_example);
    out.push_str("\n<<<END_FLOW_OUTPUT>>>\n");

    let hints = generate_field_hints(&schema.fields, "");
    if !hints.is_empty() {
        out.push('\n');
        out.push_str(&hints);
    }

    out
}

fn generate_json_example(fields: &[FieldDef], indent: usize) -> String {
    let pad = "  ".repeat(indent);
    let inner_pad = "  ".repeat(indent + 1);
    let mut lines = Vec::new();

    lines.push(format!("{pad}{{"));
    for (i, field) in fields.iter().enumerate() {
        let comma = if i + 1 < fields.len() { "," } else { "" };
        let value = generate_field_example_value(field, indent + 1);
        lines.push(format!("{inner_pad}\"{}\": {value}{comma}", field.name));
    }
    lines.push(format!("{pad}}}"));

    lines.join("\n")
}

fn generate_field_example_value(field: &FieldDef, indent: usize) -> String {
    let inner_pad = "  ".repeat(indent + 1);
    match &field.field_type {
        FieldType::String => {
            if let Some(ref desc) = field.desc {
                format!("\"{}\"", desc)
            } else {
                "\"...\"".to_string()
            }
        }
        FieldType::Number => "0".to_string(),
        FieldType::Boolean => "true".to_string(),
        FieldType::Enum(variants) => {
            let joined = variants.join("|");
            format!("\"{joined}\"")
        }
        FieldType::Array {
            items: ArrayItems::Scalar(ft),
        } => {
            let example = match ft.as_ref() {
                FieldType::String => "\"...\", \"...\"",
                FieldType::Number => "0, 0",
                FieldType::Boolean => "true, false",
                FieldType::Enum(variants) => {
                    let joined = variants.join("|");
                    return format!("[\"{joined}\"]");
                }
                _ => return "[]".to_string(),
            };
            format!("[{example}]")
        }
        FieldType::Array {
            items: ArrayItems::Object(fields),
        } if fields.is_empty() => "[]".to_string(),
        FieldType::Array {
            items: ArrayItems::Object(fields),
        } => {
            let item_json = generate_json_example(fields, indent + 1);
            format!("[\n{item_json}\n{inner_pad}]")
        }
        FieldType::Array {
            items: ArrayItems::Untyped,
        } => "[]".to_string(),
        FieldType::Object { fields } if fields.is_empty() => "{}".to_string(),
        FieldType::Object { fields } => generate_json_example(fields, indent),
    }
}

fn generate_field_hints(fields: &[FieldDef], prefix: &str) -> String {
    let mut hints = Vec::new();
    for field in fields {
        let full_name = if prefix.is_empty() {
            field.name.clone()
        } else {
            format!("{prefix}.{}", field.name)
        };

        let optional_tag = if !field.required { " (optional)" } else { "" };

        let push_examples = |hints: &mut Vec<String>, field: &FieldDef| {
            if let Some(ref examples) = field.examples {
                let examples_str = examples
                    .iter()
                    .map(|e| format!("\"{e}\""))
                    .collect::<Vec<_>>()
                    .join(", ");
                hints.push(format!("  examples: [{examples_str}]"));
            }
        };

        match &field.field_type {
            FieldType::Array {
                items: ArrayItems::Scalar(ft),
            } => {
                let type_label = match ft.as_ref() {
                    FieldType::String => "string".to_owned(),
                    FieldType::Number => "number".to_owned(),
                    FieldType::Boolean => "boolean".to_owned(),
                    FieldType::Enum(v) => {
                        let joined = v.join(", ");
                        format!("enum({joined})")
                    }
                    _ => "unknown".to_owned(),
                };
                if let Some(ref desc) = field.desc {
                    hints.push(format!(
                        "\"{full_name}\"{optional_tag}: {desc} (array of {type_label})"
                    ));
                } else {
                    hints.push(format!(
                        "\"{full_name}\"{optional_tag}: array of {type_label}"
                    ));
                }
                push_examples(&mut hints, field);
            }
            FieldType::Array {
                items: ArrayItems::Object(sub_fields),
            } if !sub_fields.is_empty() => {
                if let Some(ref desc) = field.desc {
                    hints.push(format!("\"{full_name}\"{optional_tag}: {desc}"));
                }
                let sub_hints = generate_field_hints(sub_fields, &format!("{full_name}[]"));
                if !sub_hints.is_empty() {
                    hints.push(sub_hints);
                }
            }
            FieldType::Object { fields: sub } if !sub.is_empty() => {
                if let Some(ref desc) = field.desc {
                    hints.push(format!("\"{full_name}\"{optional_tag}: {desc}"));
                }
                let sub_hints = generate_field_hints(sub, &full_name);
                if !sub_hints.is_empty() {
                    hints.push(sub_hints);
                }
            }
            _ => {
                if let Some(ref desc) = field.desc {
                    hints.push(format!("\"{full_name}\"{optional_tag}: {desc}"));
                }
                push_examples(&mut hints, field);
                if field.desc.is_none() && !field.required {
                    hints.push(format!("\"{full_name}\" is optional and may be omitted."));
                }
            }
        }
    }
    hints.join("\n")
}

fn build_prompt_core(
    agent_def: &AgentDef,
    vars: &HashMap<&str, &str>,
    schema: Option<&OutputSchema>,
    snippets: &[&str],
    retry_error: Option<&str>,
    dry_run: bool,
) -> String {
    let substituted = substitute_variables(&agent_def.prompt, vars);
    let mut prompt = String::with_capacity(substituted.len() * 2);

    if dry_run {
        prompt.push_str("DO NOT commit or push any changes. This is a dry run.\n\n");
    }
    if let Some(msg) = retry_error {
        prompt.push_str("[Previous attempt failed]\nError: ");
        prompt.push_str(msg);
        prompt.push_str("\nPlease re-read the instructions below and correct your output.\n\n");
    }
    prompt.push_str(
        "Your task below is your ONLY priority. Complete it fully before considering anything else.\n\n",
    );
    prompt.push_str(&substituted);

    if let Some(fsm_path) = vars.get("fsm_path") {
        if !fsm_path.is_empty() {
            prompt.push_str("\n\n## Mandatory First Action\n\n");
            prompt.push_str("Before doing ANYTHING else, read the FSM definition file at:\n");
            prompt.push('`');
            prompt.push_str(fsm_path);
            prompt.push_str("`\n\n");
            prompt.push_str(
                "This file defines the state machine that governs your behavior in this workflow. ",
            );
            prompt
                .push_str("You MUST read and understand it before proceeding with any other work.");
        }
    }

    if !vars.is_empty() {
        prompt.push_str("\n\n## Template Variables\n\n");
        prompt.push_str(
            "The following template placeholders are available and have been substituted in this prompt:\n\n",
        );
        for (key, value) in vars {
            prompt.push_str("- `{{");
            prompt.push_str(key);
            prompt.push_str("}}` = `");
            prompt.push_str(value);
            prompt.push_str("`\n");
        }
    }

    for snippet in snippets {
        if !snippet.is_empty() {
            let substituted = substitute_variables(snippet, vars);
            prompt.push_str("\n\n");
            prompt.push_str(&substituted);
        }
    }

    match schema {
        Some(s) => {
            prompt.push('\n');
            prompt.push_str(&generate_prompt_instructions(s));
        }
        None => {
            prompt.push_str(FLOW_OUTPUT_INSTRUCTION);
        }
    }
    prompt
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Prompt-building parameters for [`load_agent_and_build_prompt`].
///
/// Groups the prompt-building inputs to keep `load_agent_and_build_prompt`'s
/// parameter count under clippy's limit.
pub struct BuildPromptParams<'a> {
    /// Resolved template variable map.
    pub inputs: &'a HashMap<String, String>,
    /// Raw snippet names/paths from the DSL `with` field (unresolved).
    pub snippet_refs: &'a [String],
    /// Error from the previous failed attempt, if any.
    pub retry_error: Option<&'a str>,
    /// If true and the agent has `can_commit: true`, prefix with dry-run notice.
    pub dry_run: bool,
    /// Optional output schema for structured output enforcement.
    pub schema: Option<&'a OutputSchema>,
}

/// Load an agent and build the fully-substituted prompt.
///
/// - `working_dir` — worktree root path (used for agent file search)
/// - `repo_path` — repo root path (used for agent file search and snippet resolution)
/// - `plugin_dirs` — extra directories to search for agent definitions
/// - `workflow_name` — parent workflow name (for workflow-local agent/snippet overrides)
/// - `agent_name` — short agent name (e.g. `"plan"`)
/// - `params` — prompt-building parameters (inputs, snippets, schema, flags)
pub fn load_agent_and_build_prompt(
    working_dir: &str,
    repo_path: &str,
    plugin_dirs: &[String],
    workflow_name: &str,
    agent_name: &str,
    params: &BuildPromptParams<'_>,
) -> Result<(AgentDef, String), String> {
    let agent_def = load_agent(
        working_dir,
        repo_path,
        agent_name,
        Some(workflow_name),
        plugin_dirs,
    )?;

    let resolved_snippets = if !params.snippet_refs.is_empty() {
        let text = load_and_concat_snippets(
            working_dir,
            repo_path,
            params.snippet_refs,
            Some(workflow_name),
        )?;
        if text.is_empty() {
            vec![]
        } else {
            vec![text]
        }
    } else {
        vec![]
    };

    let vars: HashMap<&str, &str> = params
        .inputs
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    let snippet_refs_str: Vec<&str> = resolved_snippets.iter().map(String::as_str).collect();

    let effective_dry_run = agent_def.can_commit && params.dry_run;

    let prompt = build_prompt_core(
        &agent_def,
        &vars,
        params.schema,
        &snippet_refs_str,
        params.retry_error,
        effective_dry_run,
    );

    Ok((agent_def, prompt))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use runkon_flow::output_schema::{ArrayItems, FieldDef, FieldType, OutputSchema};
    use tempfile::TempDir;

    fn make_field(name: &str, required: bool, field_type: FieldType) -> FieldDef {
        FieldDef {
            name: name.to_string(),
            required,
            field_type,
            desc: None,
            examples: None,
        }
    }

    fn make_schema(name: &str, fields: Vec<FieldDef>) -> OutputSchema {
        OutputSchema {
            name: name.to_string(),
            fields,
            markers: None,
        }
    }

    fn write_file(dir: &TempDir, rel: &str, content: &str) {
        let path = dir.path().join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    // ── load_agent ──────────────────────────────────────────────────────────

    #[test]
    fn load_agent_from_conductor_agents() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();

        write_file(&tmp, ".conductor/agents/worker.md", "Do the work.");

        let agent = load_agent(&dir, &dir, "worker", None, &[]).unwrap();
        assert_eq!(agent.name, "worker");
        assert_eq!(agent.prompt, "Do the work.");
        assert_eq!(agent.role, runkon_runtimes::AgentRole::Reviewer);
    }

    #[test]
    fn load_agent_with_frontmatter_actor_role() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();

        write_file(
            &tmp,
            ".conductor/agents/committer.md",
            "---\nrole: actor\ncan_commit: true\n---\nMake changes.",
        );

        let agent = load_agent(&dir, &dir, "committer", None, &[]).unwrap();
        assert_eq!(agent.role, runkon_runtimes::AgentRole::Actor);
        assert!(agent.can_commit);
        assert_eq!(agent.prompt, "Make changes.");
    }

    #[test]
    fn load_agent_workflow_local_takes_priority_over_shared() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();

        write_file(&tmp, ".conductor/agents/worker.md", "Shared agent.");
        write_file(
            &tmp,
            ".conductor/workflows/my-wf/agents/worker.md",
            "Workflow-local agent.",
        );

        let agent = load_agent(&dir, &dir, "worker", Some("my-wf"), &[]).unwrap();
        assert_eq!(agent.prompt, "Workflow-local agent.");
    }

    #[test]
    fn load_agent_falls_back_to_claude_agents() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();

        write_file(&tmp, ".claude/agents/fallback.md", "Claude fallback.");

        let agent = load_agent(&dir, &dir, "fallback", None, &[]).unwrap();
        assert_eq!(agent.prompt, "Claude fallback.");
    }

    #[test]
    fn load_agent_missing_returns_descriptive_error() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();

        let err = load_agent(&dir, &dir, "ghost", None, &[]).unwrap_err();
        assert!(err.contains("ghost"), "got: {err}");
        assert!(err.contains("not found"), "got: {err}");
    }

    #[test]
    fn load_agent_malformed_frontmatter_returns_error() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();

        // Unclosed bracket → invalid YAML
        write_file(
            &tmp,
            ".conductor/agents/bad.md",
            "---\nrole: [unclosed\n---\nPrompt.",
        );

        let err = load_agent(&dir, &dir, "bad", None, &[]).unwrap_err();
        assert!(
            err.contains("Invalid YAML") || err.contains("frontmatter"),
            "got: {err}"
        );
    }

    #[test]
    fn load_agent_can_commit_without_actor_role_is_error() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();

        write_file(
            &tmp,
            ".conductor/agents/bad.md",
            "---\nrole: reviewer\ncan_commit: true\n---\nPrompt.",
        );

        let err = load_agent(&dir, &dir, "bad", None, &[]).unwrap_err();
        assert!(err.contains("can_commit"), "got: {err}");
    }

    // ── load_and_concat_snippets ─────────────────────────────────────────────

    #[test]
    fn load_and_concat_snippets_empty_list_returns_empty() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();

        let result = load_and_concat_snippets(&dir, &dir, &[], None).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn load_and_concat_snippets_single_snippet() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();

        write_file(&tmp, ".conductor/prompts/intro.md", "Hello world.");

        let result = load_and_concat_snippets(&dir, &dir, &["intro".to_string()], None).unwrap();
        assert_eq!(result, "Hello world.");
    }

    #[test]
    fn load_and_concat_snippets_preserves_order() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();

        write_file(&tmp, ".conductor/prompts/first.md", "First.");
        write_file(&tmp, ".conductor/prompts/second.md", "Second.");

        let result = load_and_concat_snippets(
            &dir,
            &dir,
            &["first".to_string(), "second".to_string()],
            None,
        )
        .unwrap();
        assert_eq!(result, "First.\n\nSecond.");
    }

    #[test]
    fn load_and_concat_snippets_missing_returns_error() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();

        let err = load_and_concat_snippets(&dir, &dir, &["missing".to_string()], None).unwrap_err();
        assert!(err.contains("missing"), "got: {err}");
    }

    #[test]
    fn load_and_concat_snippets_workflow_local_overrides_shared() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();

        write_file(&tmp, ".conductor/prompts/ctx.md", "Shared.");
        write_file(
            &tmp,
            ".conductor/workflows/my-wf/prompts/ctx.md",
            "Workflow-local.",
        );

        let result =
            load_and_concat_snippets(&dir, &dir, &["ctx".to_string()], Some("my-wf")).unwrap();
        assert_eq!(result, "Workflow-local.");
    }

    // ── generate_prompt_instructions ─────────────────────────────────────────

    #[test]
    fn generate_prompt_instructions_contains_flow_output_markers() {
        let schema = make_schema("test", vec![make_field("status", true, FieldType::String)]);
        let out = generate_prompt_instructions(&schema);
        assert!(out.contains("<<<FLOW_OUTPUT>>>"), "got: {out}");
        assert!(out.contains("<<<END_FLOW_OUTPUT>>>"), "got: {out}");
        assert!(out.contains("\"status\""), "got: {out}");
    }

    #[test]
    fn generate_prompt_instructions_optional_field_is_labeled() {
        let schema = make_schema(
            "test",
            vec![
                make_field("required_field", true, FieldType::String),
                make_field("opt_field", false, FieldType::String),
            ],
        );
        let out = generate_prompt_instructions(&schema);
        assert!(out.contains("opt_field"), "got: {out}");
        assert!(out.contains("optional"), "got: {out}");
    }

    #[test]
    fn generate_prompt_instructions_array_field() {
        let schema = make_schema(
            "test",
            vec![make_field(
                "tags",
                true,
                FieldType::Array {
                    items: ArrayItems::Scalar(Box::new(FieldType::String)),
                },
            )],
        );
        let out = generate_prompt_instructions(&schema);
        assert!(out.contains("\"tags\""), "got: {out}");
        assert!(out.contains("["), "got: {out}");
    }

    #[test]
    fn generate_prompt_instructions_nested_object() {
        let schema = make_schema(
            "test",
            vec![FieldDef {
                name: "meta".to_string(),
                required: true,
                field_type: FieldType::Object {
                    fields: vec![make_field("count", true, FieldType::Number)],
                },
                desc: None,
                examples: None,
            }],
        );
        let out = generate_prompt_instructions(&schema);
        assert!(out.contains("\"meta\""), "got: {out}");
        assert!(out.contains("\"count\""), "got: {out}");
    }

    #[test]
    fn generate_prompt_instructions_enum_field_shows_variants() {
        let schema = make_schema(
            "test",
            vec![make_field(
                "status",
                true,
                FieldType::Enum(vec!["open".to_string(), "closed".to_string()]),
            )],
        );
        let out = generate_prompt_instructions(&schema);
        assert!(out.contains("open|closed"), "got: {out}");
    }

    // ── load_agent_and_build_prompt ──────────────────────────────────────────

    #[test]
    fn load_agent_and_build_prompt_substitutes_template_vars() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();

        write_file(&tmp, ".conductor/agents/worker.md", "Process {{task}}.");

        let mut inputs = HashMap::new();
        inputs.insert("task".to_string(), "build".to_string());

        let params = BuildPromptParams {
            inputs: &inputs,
            snippet_refs: &[],
            retry_error: None,
            dry_run: false,
            schema: None,
        };

        let (agent_def, prompt) =
            load_agent_and_build_prompt(&dir, &dir, &[], "my-wf", "worker", &params).unwrap();

        assert_eq!(agent_def.name, "worker");
        assert!(prompt.contains("Process build."), "got: {prompt}");
    }

    #[test]
    fn load_agent_and_build_prompt_appends_snippet() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();

        write_file(&tmp, ".conductor/agents/worker.md", "Main prompt.");
        write_file(&tmp, ".conductor/prompts/extra.md", "Extra context.");

        let inputs = HashMap::new();
        let snippet_refs = vec!["extra".to_string()];
        let params = BuildPromptParams {
            inputs: &inputs,
            snippet_refs: &snippet_refs,
            retry_error: None,
            dry_run: false,
            schema: None,
        };

        let (_, prompt) =
            load_agent_and_build_prompt(&dir, &dir, &[], "my-wf", "worker", &params).unwrap();

        assert!(prompt.contains("Main prompt."), "got: {prompt}");
        assert!(prompt.contains("Extra context."), "got: {prompt}");
    }

    #[test]
    fn load_agent_and_build_prompt_with_schema_adds_flow_output_block() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();

        write_file(&tmp, ".conductor/agents/reviewer.md", "Review the code.");

        let schema = make_schema(
            "review",
            vec![make_field("approved", true, FieldType::Boolean)],
        );
        let inputs = HashMap::new();
        let params = BuildPromptParams {
            inputs: &inputs,
            snippet_refs: &[],
            retry_error: None,
            dry_run: false,
            schema: Some(&schema),
        };

        let (_, prompt) =
            load_agent_and_build_prompt(&dir, &dir, &[], "my-wf", "reviewer", &params).unwrap();

        assert!(prompt.contains("<<<FLOW_OUTPUT>>>"), "got: {prompt}");
        assert!(prompt.contains("\"approved\""), "got: {prompt}");
    }

    #[test]
    fn load_agent_and_build_prompt_dry_run_prefix_for_actor() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();

        write_file(
            &tmp,
            ".conductor/agents/committer.md",
            "---\nrole: actor\ncan_commit: true\n---\nMake changes.",
        );

        let inputs = HashMap::new();
        let params = BuildPromptParams {
            inputs: &inputs,
            snippet_refs: &[],
            retry_error: None,
            dry_run: true,
            schema: None,
        };

        let (_, prompt) =
            load_agent_and_build_prompt(&dir, &dir, &[], "my-wf", "committer", &params).unwrap();

        assert!(prompt.contains("dry run"), "got: {prompt}");
    }

    #[test]
    fn load_agent_and_build_prompt_missing_agent_returns_error() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();

        let inputs = HashMap::new();
        let params = BuildPromptParams {
            inputs: &inputs,
            snippet_refs: &[],
            retry_error: None,
            dry_run: false,
            schema: None,
        };

        let err =
            load_agent_and_build_prompt(&dir, &dir, &[], "my-wf", "ghost", &params).unwrap_err();
        assert!(err.contains("ghost"), "got: {err}");
    }
}
