//! Path resolution utilities for workflow script steps.

use std::path::{Path, PathBuf};

fn path_is_within_dir(dir: &Path, file: &Path) -> bool {
    match (dir.canonicalize(), file.canonicalize()) {
        (Ok(canon_dir), Ok(canon_file)) => canon_file.starts_with(&canon_dir),
        _ => false,
    }
}

/// Returns the ordered list of `(search_root, candidate_path)` pairs for a
/// script name. The caller must pass a relative `run`; absolute paths are
/// rejected up-front by [`resolve_script_path`].
pub(crate) fn script_search_paths(run: &str, search_roots: &[&Path]) -> Vec<(PathBuf, PathBuf)> {
    search_roots
        .iter()
        .map(|root| (root.to_path_buf(), root.join(run)))
        .collect()
}

/// Resolve a script name to an existing path using the provided search roots.
///
/// Absolute paths are rejected unconditionally: a workflow `run:` value that
/// resolves outside the supplied search roots cannot be executed, even if it
/// exists on disk. This blocks a hostile `.wf` file from invoking arbitrary
/// system binaries via `run: /etc/shadow` or similar.
pub fn resolve_script_path(run: &str, search_roots: &[&Path]) -> Option<PathBuf> {
    if Path::new(run).is_absolute() {
        return None;
    }
    if run.contains("..") {
        return None;
    }
    let pairs = script_search_paths(run, search_roots);
    for (root, candidate) in &pairs {
        if candidate.exists() && path_is_within_dir(root, candidate) {
            return Some(candidate.clone());
        }
    }
    None
}

/// Returns the default skills directory (`$HOME/.claude/skills`), or `None`
/// if the `HOME` environment variable is not set.
pub fn default_skills_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|h| PathBuf::from(&h).join(".claude/skills"))
}

/// Build a resolver closure suitable for passing to `validate_script_steps`.
pub fn make_script_resolver(
    search_roots: Vec<PathBuf>,
) -> impl Fn(&str) -> Result<PathBuf, String> {
    move |run| {
        let root_refs: Vec<&Path> = search_roots.iter().map(|p| p.as_path()).collect();
        resolve_script_path(run, &root_refs).ok_or_else(|| {
            if Path::new(run).is_absolute() {
                format!(
                    "absolute paths are not allowed in `run:` (got '{run}'); use a path relative to the configured search roots"
                )
            } else {
                let pairs = script_search_paths(run, &root_refs);
                let searched: Vec<String> =
                    pairs.iter().map(|(_, c)| c.display().to_string()).collect();
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
        let wd = tmp.path();
        assert_eq!(resolve_script_path("/bin/sh", &[wd]), None);
    }

    #[test]
    fn resolve_script_path_rejects_traversal_back_into_search_root() {
        // A relative path that lexically escapes the search root via `..`
        // must be rejected (existing behavior; covered here to lock it in).
        let tmp = tempfile::tempdir().expect("tempdir");
        let wd = tmp.path();
        assert_eq!(resolve_script_path("../foo.sh", &[wd]), None);
    }

    #[test]
    fn make_script_resolver_returns_explicit_error_for_absolute_path() {
        let resolver = make_script_resolver(vec![PathBuf::from("/tmp")]);
        let err = resolver("/etc/shadow").expect_err("absolute path must error");
        assert!(
            err.contains("absolute paths are not allowed"),
            "error should explain why absolute paths fail; got: {err}"
        );
    }
}
