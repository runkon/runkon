//! Path resolution utilities for workflow script steps.

use std::path::Path;

fn path_is_within_dir(dir: &Path, file: &Path) -> bool {
    match (dir.canonicalize(), file.canonicalize()) {
        (Ok(canon_dir), Ok(canon_file)) => canon_file.starts_with(&canon_dir),
        _ => false,
    }
}

/// Returns the ordered list of `(search_root, candidate_path)` pairs for a
/// script name. The caller must pass a relative `run`; absolute paths are
/// rejected up-front by [`resolve_script_path`].
pub(crate) fn script_search_paths(
    run: &str,
    working_dir: &str,
    repo_path: &str,
    skills_dir: Option<&std::path::Path>,
) -> Vec<(std::path::PathBuf, std::path::PathBuf)> {
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
///
/// Absolute paths are rejected unconditionally: a workflow `run:` value that
/// resolves outside the standard search roots (working dir, repo, `.conductor/scripts`,
/// skills dir) cannot be executed, even if it exists on disk. This blocks a
/// hostile `.wf` file from invoking arbitrary system binaries via
/// `run: /etc/shadow` or similar.
pub fn resolve_script_path(
    run: &str,
    working_dir: &str,
    repo_path: &str,
    skills_dir: Option<&std::path::Path>,
) -> Option<std::path::PathBuf> {
    if std::path::Path::new(run).is_absolute() {
        return None;
    }
    let pairs = script_search_paths(run, working_dir, repo_path, skills_dir);

    for (root, candidate) in &pairs {
        if candidate.exists() {
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
            if std::path::Path::new(run).is_absolute() {
                format!(
                    "absolute paths are not allowed in `run:` (got '{run}'); use a path relative to the working dir, repo, .conductor/scripts, or skills dir"
                )
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_script_path_rejects_absolute_path_even_if_it_exists() {
        // /bin/sh exists on every Unix system the test runs on; it must still be
        // rejected because it lies outside the standard script search roots.
        let tmp = tempfile::tempdir().expect("tempdir");
        let wd = tmp.path().to_str().unwrap();
        assert_eq!(resolve_script_path("/bin/sh", wd, wd, None), None);
    }

    #[test]
    fn resolve_script_path_rejects_traversal_back_into_search_root() {
        // A relative path that lexically escapes the search root via `..`
        // must be rejected (existing behavior; covered here to lock it in).
        let tmp = tempfile::tempdir().expect("tempdir");
        let wd = tmp.path().to_str().unwrap();
        assert_eq!(resolve_script_path("../foo.sh", wd, wd, None), None);
    }

    #[test]
    fn make_script_resolver_returns_explicit_error_for_absolute_path() {
        let resolver = make_script_resolver("/tmp".into(), "/tmp".into(), None);
        let err = resolver("/etc/shadow").expect_err("absolute path must error");
        assert!(
            err.contains("absolute paths are not allowed"),
            "error should explain why absolute paths fail; got: {err}"
        );
    }
}
