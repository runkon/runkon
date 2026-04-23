use std::collections::HashMap;
use std::path::Path;

/// Abstraction over the per-run context consumed by executors and prompt builders.
#[allow(dead_code)]
pub trait RunContext {
    /// Returns the subset of variables that the engine injects from run metadata.
    fn injected_variables(&self) -> HashMap<&'static str, String>;

    /// Absolute path to the working directory for this run.
    fn working_dir(&self) -> &Path;

    /// Working directory as an owned `String` (convenience over `to_string_lossy`).
    fn working_dir_str(&self) -> String {
        self.working_dir().to_string_lossy().into_owned()
    }

    /// Absolute path to the repository root for this run.
    fn repo_path(&self) -> &Path;

    /// Repository path as an owned `String` (convenience over `to_string_lossy`).
    fn repo_path_str(&self) -> String {
        self.repo_path().to_string_lossy().into_owned()
    }

    /// Worktree ID, if this run is tied to a registered worktree.
    fn worktree_id(&self) -> Option<&str> {
        None
    }

    /// Worktree slug (empty string for repo-level runs).
    fn worktree_slug(&self) -> &str {
        ""
    }

    /// Ticket ID linked to this run, if any.
    fn ticket_id(&self) -> Option<&str> {
        None
    }

    /// Repo ID for this run, if any.
    fn repo_id(&self) -> Option<&str> {
        None
    }
}
