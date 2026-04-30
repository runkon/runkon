use thiserror::Error;

/// Error type for the runtime layer.
#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("invalid input: {0}")]
    InvalidInput(String),

    #[error("config error: {0}")]
    Config(String),

    #[error("agent error: {0}")]
    Agent(String),

    #[error("workflow error: {0}")]
    Workflow(String),
}

pub type Result<T> = std::result::Result<T, RuntimeError>;
