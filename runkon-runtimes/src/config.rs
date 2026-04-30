use serde::{Deserialize, Serialize};

/// Configuration for a named agent runtime (RFC 007).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RuntimeConfig {
    /// "cli", "api", or "script". Defaults to "cli".
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub runtime_type: Option<String>,
    /// For "cli": binary name (e.g. "gemini"). Must be on PATH.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binary: Option<String>,
    /// For "cli": arg template. {{prompt}} and {{model}} are substituted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,
    /// For "cli": how to pass the prompt. "arg" (default) or "stdin".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_via: Option<String>,
    /// Default model ID passed as {{model}}. Overridden by agent frontmatter `model:`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    /// Dot-path into JSON stdout to extract result_text (e.g. "response").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_field: Option<String>,
    /// Dot-path into JSON stdout to extract total token count (optional).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_fields: Option<String>,
    /// For "api": env var name holding the API key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_env: Option<String>,
    /// For "script": shell command to execute.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
}
