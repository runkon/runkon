//! Path resolution utilities for workflow script steps.

use std::path::Path;

fn path_is_within_dir(dir: &Path, file: &Path) -> bool {
    match (dir.canonicalize(), file.canonicalize()) {
        (Ok(canon_dir), Ok(canon_file)) => canon_file.starts_with(&canon_dir),
        _ => false,
    }
}

/// Returns the ordered list of `(search_root, candidate_path)` pairs for a
/// script name.
pub(crate) fn script_search_paths(
    run: &str,
    working_dir: &str,
    repo_path: &str,
    skills_dir: Option<&std::path::Path>,
) -> Vec<(std::path::PathBuf, std::path::PathBuf)> {
    let p = std::path::Path::new(run);
    if p.is_absolute() {
        return vec![(p.to_path_buf(), p.to_path_buf())];
    }
    let wd = std::path::Path::new(working_dir);
    let rp = std::path::Path::new(repo_path);
    let mut pairs = vec![
        (wd.to_path_buf(), wd.join(run)),
        (wd.to_path_buf(), wd.join(".conductor/scripts").join(run)),
        (rp.to_path_buf(), rp.join(run)),
        (rp.to_path_buf(), rp.join(".conductor/scripts").join(run)),
    ];
    if let Some(skills) = skills_dir {
        pairs.push((skills.to_path_buf(), skills.join(run)));
    }
    pairs
}

/// Resolve a script name to an existing path using the standard search order.
pub fn resolve_script_path(
    run: &str,
    working_dir: &str,
    repo_path: &str,
    skills_dir: Option<&std::path::Path>,
) -> Option<std::path::PathBuf> {
    let pairs = script_search_paths(run, working_dir, repo_path, skills_dir);
    let is_absolute = std::path::Path::new(run).is_absolute();

    for (root, candidate) in &pairs {
        if candidate.exists() {
            if is_absolute {
                return Some(candidate.clone());
            }
            if run.contains("..") {
                continue;
            }
            let relative = candidate.strip_prefix(root).unwrap_or(candidate.as_path());
            if relative.starts_with(".conductor") {
                return Some(candidate.clone());
            }
            if path_is_within_dir(root, candidate) {
                return Some(candidate.clone());
            }
        }
    }
    None
}

/// Returns the default skills directory (`$HOME/.claude/skills`), or `None`
/// if the `HOME` environment variable is not set.
pub fn default_skills_dir() -> Option<std::path::PathBuf> {
    std::env::var_os("HOME").map(|h| std::path::PathBuf::from(&h).join(".claude/skills"))
}

/// Build a resolver closure suitable for passing to `validate_script_steps`.
pub fn make_script_resolver(
    working_dir: String,
    repo_path: String,
    skills_dir: Option<std::path::PathBuf>,
) -> impl Fn(&str) -> Result<std::path::PathBuf, String> {
    move |run| {
        resolve_script_path(run, &working_dir, &repo_path, skills_dir.as_deref()).ok_or_else(|| {
            let p = std::path::Path::new(run);
            if p.is_absolute() {
                run.to_string()
            } else {
                let pairs =
                    script_search_paths(run, &working_dir, &repo_path, skills_dir.as_deref());
                let mut searched: Vec<String> =
                    pairs.iter().map(|(_, c)| c.display().to_string()).collect();
                if skills_dir.is_none() {
                    searched.push(format!("~/.claude/skills/{run}"));
                }
                searched.join(", ")
            }
        })
    }
}
