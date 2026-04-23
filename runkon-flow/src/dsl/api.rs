use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use super::parser::parse_workflow_file;
use super::types::{collect_workflow_refs, WorkflowDef, WorkflowWarning};

// ---------------------------------------------------------------------------
// Inlined text_util helpers
// ---------------------------------------------------------------------------

fn filename_is_safe(filename: &str) -> bool {
    !filename.contains('/')
        && !filename.contains('\\')
        && !filename.contains("..")
        && !filename.is_empty()
}

fn resolve_conductor_subdir_for_file(
    worktree_path: &str,
    repo_path: &str,
    subdir: &str,
    filename: &str,
) -> Option<PathBuf> {
    if !filename_is_safe(filename) {
        return None;
    }
    if !worktree_path.is_empty() {
        let dir = PathBuf::from(worktree_path).join(".conductor").join(subdir);
        if dir.join(filename).exists() {
            return Some(dir);
        }
    }
    let dir = PathBuf::from(repo_path).join(".conductor").join(subdir);
    if dir.join(filename).exists() {
        return Some(dir);
    }
    None
}

// ---------------------------------------------------------------------------
// Public API / loaders
// ---------------------------------------------------------------------------

fn deduplicate_warnings(warnings: Vec<WorkflowWarning>) -> HashMap<String, WorkflowWarning> {
    let mut map: HashMap<String, WorkflowWarning> = HashMap::new();
    for w in warnings {
        let key = Path::new(&w.file)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or(&w.file)
            .to_string();
        map.insert(key, w);
    }
    map
}

/// Load all workflow definitions from `.conductor/workflows/*.wf`.
pub fn load_workflow_defs(
    worktree_path: &str,
    repo_path: &str,
) -> Result<(Vec<WorkflowDef>, Vec<WorkflowWarning>), String> {
    let mut map: HashMap<String, WorkflowDef> = HashMap::new();
    let mut warnings_map: HashMap<String, WorkflowWarning> = HashMap::new();

    if !repo_path.is_empty() {
        let repo_dir = Path::new(repo_path).join(".conductor").join("workflows");
        if repo_dir.is_dir() {
            let (defs, warnings) = scan_wf_dir(&repo_dir)?;
            for def in defs {
                map.insert(def.name.clone(), def);
            }
            warnings_map.extend(deduplicate_warnings(warnings));
        }
    }

    if !worktree_path.is_empty() && worktree_path != repo_path {
        let wt_dir = Path::new(worktree_path)
            .join(".conductor")
            .join("workflows");
        if wt_dir.is_dir() {
            let (defs, warnings) = scan_wf_dir(&wt_dir)?;
            for def in defs {
                map.insert(def.name.clone(), def);
            }
            warnings_map.extend(deduplicate_warnings(warnings));
        }
    }

    let mut defs: Vec<WorkflowDef> = map.into_values().collect();
    defs.sort_by(|a, b| a.name.cmp(&b.name));

    let mut all_warnings: Vec<WorkflowWarning> = warnings_map.into_values().collect();
    all_warnings.sort_by(|a, b| a.file.cmp(&b.file));

    Ok((defs, all_warnings))
}

fn scan_wf_dir(dir: &Path) -> Result<(Vec<WorkflowDef>, Vec<WorkflowWarning>), String> {
    let mut entries = filter_wf_dir_entries(
        fs::read_dir(dir).map_err(|e| format!("Failed to read {}: {e}", dir.display()))?,
        dir,
    );

    entries.sort_by_key(|e| e.file_name());

    let mut defs = Vec::new();
    let mut warnings = Vec::new();
    for entry in entries {
        let path = entry.path();
        match parse_workflow_file(&path) {
            Ok(def) => defs.push(def),
            Err(e) => {
                let file = path
                    .file_name()
                    .unwrap_or(path.as_os_str())
                    .to_string_lossy()
                    .into_owned();
                tracing::warn!("Failed to parse {file}: {e}");
                warnings.push(WorkflowWarning {
                    file,
                    message: e.to_string(),
                });
            }
        }
    }
    Ok((defs, warnings))
}

pub(crate) fn filter_wf_dir_entries(
    iter: impl Iterator<Item = std::io::Result<fs::DirEntry>>,
    dir_path: &std::path::Path,
) -> Vec<fs::DirEntry> {
    iter.filter_map(|e| match e {
        Ok(entry) => Some(entry),
        Err(err) => {
            tracing::warn!(
                "Failed to read directory entry in {}: {err}",
                dir_path.display()
            );
            None
        }
    })
    .filter(|e| e.path().extension().is_some_and(|ext| ext == "wf"))
    .collect()
}

/// Validate that a workflow name is safe for use in filesystem paths.
pub fn validate_workflow_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("Workflow name must not be empty".to_string());
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(format!(
            "Invalid workflow name '{name}': only alphanumeric characters, hyphens, and underscores are allowed"
        ));
    }
    Ok(())
}

/// Load a single workflow definition by name.
pub fn load_workflow_by_name(
    worktree_path: &str,
    repo_path: &str,
    name: &str,
) -> Result<WorkflowDef, String> {
    validate_workflow_name(name)?;

    let filename = format!("{name}.wf");
    let workflows_dir =
        resolve_conductor_subdir_for_file(worktree_path, repo_path, "workflows", &filename)
            .ok_or_else(|| format!("Workflow '{name}' not found in .conductor/workflows/"))?;

    parse_workflow_file(&workflows_dir.join(&filename))
}

/// Maximum allowed workflow nesting depth.
pub const MAX_WORKFLOW_DEPTH: u32 = 5;

/// Detect circular workflow references via static reachability analysis.
pub fn detect_workflow_cycles<F>(root_name: &str, loader: &F) -> std::result::Result<(), String>
where
    F: Fn(&str) -> std::result::Result<WorkflowDef, String>,
{
    let mut visited = Vec::new();
    detect_cycles_inner(root_name, loader, &mut visited)
}

fn detect_cycles_inner<F>(
    name: &str,
    loader: &F,
    stack: &mut Vec<String>,
) -> std::result::Result<(), String>
where
    F: Fn(&str) -> std::result::Result<WorkflowDef, String>,
{
    if stack.contains(&name.to_string()) {
        stack.push(name.to_string());
        let cycle_path = stack.join(" -> ");
        return Err(format!("Circular workflow reference: {cycle_path}"));
    }

    if stack.len() >= MAX_WORKFLOW_DEPTH as usize {
        return Err(format!(
            "Workflow nesting depth exceeds maximum of {MAX_WORKFLOW_DEPTH}: {}",
            stack.join(" -> ")
        ));
    }

    stack.push(name.to_string());

    // Skip unloadable sub-workflows rather than failing — load errors are
    // reported by the caller's validation pass, which collects all of them.
    // Failing here would only surface the first one.
    let def = match loader(name) {
        Ok(d) => d,
        Err(_) => {
            stack.pop();
            return Ok(());
        }
    };
    let mut child_refs = collect_workflow_refs(&def.body);
    child_refs.extend(collect_workflow_refs(&def.always));
    child_refs.sort();
    child_refs.dedup();

    for child_name in &child_refs {
        detect_cycles_inner(child_name, loader, stack)?;
    }

    stack.pop();
    Ok(())
}
