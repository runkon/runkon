use std::path::PathBuf;
use std::sync::Arc;

use crate::dsl::parse_workflow_file;
use crate::dsl::WorkflowDef;
use crate::engine_error::EngineError;
use crate::traits::workflow_resolver::WorkflowResolver;

pub struct DirectoryWorkflowResolver {
    root: PathBuf,
}

impl DirectoryWorkflowResolver {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl WorkflowResolver for DirectoryWorkflowResolver {
    fn resolve(&self, name: &str) -> Result<Arc<WorkflowDef>, EngineError> {
        if !name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
        {
            return Err(EngineError::WorkflowNotFound(name.to_string()));
        }
        let path = self.root.join(format!("{name}.wf"));
        if !path.exists() {
            return Err(EngineError::WorkflowNotFound(name.to_string()));
        }
        parse_workflow_file(&path)
            .map(Arc::new)
            .map_err(EngineError::Workflow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_wf(dir: &std::path::Path, name: &str, content: &str) {
        std::fs::write(dir.join(format!("{name}.wf")), content).unwrap();
    }

    #[test]
    fn resolves_valid_workflow() {
        let dir = tempfile::TempDir::new().unwrap();
        write_wf(dir.path(), "deploy", "workflow deploy {\n}");
        let resolver = DirectoryWorkflowResolver::new(dir.path());
        let def = resolver.resolve("deploy").unwrap();
        assert_eq!(def.name, "deploy");
    }

    #[test]
    fn returns_not_found_for_missing_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let resolver = DirectoryWorkflowResolver::new(dir.path());
        let err = resolver.resolve("missing").unwrap_err();
        assert!(matches!(err, EngineError::WorkflowNotFound(n) if n == "missing"));
    }

    #[test]
    fn returns_not_found_for_invalid_chars_slash() {
        let dir = tempfile::TempDir::new().unwrap();
        let resolver = DirectoryWorkflowResolver::new(dir.path());
        let err = resolver.resolve("foo/bar").unwrap_err();
        assert!(matches!(err, EngineError::WorkflowNotFound(n) if n == "foo/bar"));
    }

    #[test]
    fn returns_not_found_for_dot_dot() {
        let dir = tempfile::TempDir::new().unwrap();
        let resolver = DirectoryWorkflowResolver::new(dir.path());
        let err = resolver.resolve("..").unwrap_err();
        assert!(matches!(err, EngineError::WorkflowNotFound(_)));
    }

    #[test]
    fn returns_not_found_for_name_with_spaces() {
        let dir = tempfile::TempDir::new().unwrap();
        let resolver = DirectoryWorkflowResolver::new(dir.path());
        let err = resolver.resolve("foo bar").unwrap_err();
        assert!(matches!(err, EngineError::WorkflowNotFound(_)));
    }

    #[test]
    fn returns_workflow_error_for_invalid_content() {
        let dir = tempfile::TempDir::new().unwrap();
        write_wf(dir.path(), "bad", "this is not valid wf syntax @@@@");
        let resolver = DirectoryWorkflowResolver::new(dir.path());
        let err = resolver.resolve("bad").unwrap_err();
        assert!(matches!(err, EngineError::Workflow(_)));
    }

    #[test]
    fn accepts_hyphens_and_underscores_in_name() {
        let dir = tempfile::TempDir::new().unwrap();
        write_wf(dir.path(), "my-flow_v2", "workflow my-flow_v2 {\n}");
        let resolver = DirectoryWorkflowResolver::new(dir.path());
        let def = resolver.resolve("my-flow_v2").unwrap();
        assert_eq!(def.name, "my-flow_v2");
    }
}
