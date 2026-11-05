//! Typed errors for agent output that fails to parse or validate.
//!
//! The `Display` text of every variant is the wording hosts have always seen in
//! step errors, so logs and stored messages read the same as before.

/// Why an agent's `<<<FLOW_OUTPUT>>>` block could not be used.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum OutputError {
    /// The agent's text has no complete `<<<FLOW_OUTPUT>>>` block.
    #[error("No <<<FLOW_OUTPUT>>> block found in agent output")]
    Missing,
    /// The block's contents are not valid JSON.
    #[error("Invalid JSON in FLOW_OUTPUT: {message}")]
    InvalidJson {
        /// The JSON parser's message.
        message: String,
        /// 1-based line of the block's opening marker in the result text.
        block_line: usize,
    },
    /// The block is valid JSON but does not conform to the step's schema.
    #[error("{message}")]
    Validation {
        /// What the validator found wrong.
        message: String,
        /// 1-based line of the block's opening marker in the result text.
        block_line: usize,
    },
}

impl OutputError {
    /// The 1-based line of the block's opening marker, or `None` when no block
    /// was found.
    pub fn block_line(&self) -> Option<usize> {
        match self {
            OutputError::Missing => None,
            OutputError::InvalidJson { block_line, .. }
            | OutputError::Validation { block_line, .. } => Some(*block_line),
        }
    }
}

/// Why an agent executor's `execute` failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum AgentError {
    /// The run completed but its output failed to parse or validate.
    #[error("structured output validation: {error}{}", correction_suffix(*correction_attempts))]
    Output {
        error: OutputError,
        /// Correction runs made before giving up (0 when none were possible).
        correction_attempts: u32,
    },
    /// Any other failure: spawn, poll, runtime, API call, a failed correction run.
    #[error("{0}")]
    Other(String),
}

fn correction_suffix(attempts: u32) -> String {
    if attempts > 0 {
        format!(" (after {attempts} output correction attempt(s))")
    } else {
        String::new()
    }
}

impl AgentError {
    /// The output error, when this failure is a FLOW_OUTPUT parse or validation failure.
    pub fn output_error(&self) -> Option<&OutputError> {
        match self {
            AgentError::Output { error, .. } => Some(error),
            AgentError::Other(_) => None,
        }
    }
}

impl From<String> for AgentError {
    fn from(message: String) -> Self {
        AgentError::Other(message)
    }
}

impl From<AgentError> for String {
    fn from(e: AgentError) -> Self {
        e.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_error_display_is_unchanged() {
        assert_eq!(
            OutputError::Missing.to_string(),
            "No <<<FLOW_OUTPUT>>> block found in agent output"
        );
        assert_eq!(
            OutputError::InvalidJson {
                message: "boom".into(),
                block_line: 3
            }
            .to_string(),
            "Invalid JSON in FLOW_OUTPUT: boom"
        );
        assert_eq!(
            OutputError::Validation {
                message: "Missing required field: 'id'".into(),
                block_line: 1
            }
            .to_string(),
            "Missing required field: 'id'"
        );
    }

    #[test]
    fn block_line_accessor() {
        assert_eq!(OutputError::Missing.block_line(), None);
        let e = OutputError::Validation {
            message: "x".into(),
            block_line: 4,
        };
        assert_eq!(e.block_line(), Some(4));
    }

    #[test]
    fn agent_error_display_is_unchanged() {
        let error = OutputError::Validation {
            message: "Missing required field: 'id'".into(),
            block_line: 2,
        };
        let none = AgentError::Output {
            error: error.clone(),
            correction_attempts: 0,
        };
        assert_eq!(
            none.to_string(),
            "structured output validation: Missing required field: 'id'"
        );
        let two = AgentError::Output {
            error,
            correction_attempts: 2,
        };
        assert_eq!(
            two.to_string(),
            "structured output validation: Missing required field: 'id' \
             (after 2 output correction attempt(s))"
        );
        assert_eq!(AgentError::Other("nope".into()).to_string(), "nope");
    }

    #[test]
    fn conversions() {
        let e: AgentError = "spawn failed".to_string().into();
        assert_eq!(e, AgentError::Other("spawn failed".into()));
        assert!(e.output_error().is_none());
        assert_eq!(String::from(e), "spawn failed");
    }
}
