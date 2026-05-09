use thiserror::Error;

/// Error type for notification dispatch operations.
#[derive(Debug, Error)]
pub enum NotifyError {
    /// Hook script execution or HTTP POST failed.
    #[error("dispatch error: {0}")]
    Dispatch(String),

    /// Malformed URL, bad timeout, or invalid configuration.
    #[error("config error: {0}")]
    Config(String),

    /// Subscription store upsert or delete failure.
    #[error("subscription error: {0}")]
    Subscription(String),
}

pub type Result<T> = std::result::Result<T, NotifyError>;
