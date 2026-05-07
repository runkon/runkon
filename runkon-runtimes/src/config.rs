use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Configuration for a named agent runtime (RFC 007).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RuntimeConfig {
    /// "cli", "api", "script", or "claude". Defaults to "cli".
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
    /// Environment variables injected into the spawned subprocess via `Command::envs()`.
    /// For "claude" runtimes: merged with RuntimeOptions.env; this entry wins on conflict.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub env: HashMap<String, String>,
    /// When non-empty, agent frontmatter `model:` must be one of these values.
    /// Validated before spawn; empty means accept any model.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supported_models: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_config_env_and_supported_models_roundtrip() {
        let toml = r#"
type = "claude"
env.ANTHROPIC_BASE_URL = "http://localhost:8000"
env.ANTHROPIC_AUTH_TOKEN = "local"
supported_models = ["my-model-v1", "my-model-v2"]
"#;
        let rc: RuntimeConfig = toml::from_str(toml).unwrap();
        assert_eq!(rc.runtime_type.as_deref(), Some("claude"));
        assert_eq!(
            rc.env.get("ANTHROPIC_BASE_URL").map(String::as_str),
            Some("http://localhost:8000")
        );
        assert_eq!(
            rc.env.get("ANTHROPIC_AUTH_TOKEN").map(String::as_str),
            Some("local")
        );
        assert_eq!(rc.supported_models, vec!["my-model-v1", "my-model-v2"]);

        let serialized = toml::to_string(&rc).unwrap();
        let roundtripped: RuntimeConfig = toml::from_str(&serialized).unwrap();
        assert_eq!(roundtripped.env, rc.env);
        assert_eq!(roundtripped.supported_models, rc.supported_models);
    }

    #[test]
    fn runtime_config_empty_env_and_supported_models_omitted_on_serialize() {
        let rc: RuntimeConfig = toml::from_str("").unwrap();
        assert!(rc.env.is_empty());
        assert!(rc.supported_models.is_empty());

        let serialized = toml::to_string(&rc).unwrap();
        assert!(
            !serialized.contains("env"),
            "empty env should be omitted, got:\n{serialized}"
        );
        assert!(
            !serialized.contains("supported_models"),
            "empty supported_models should be omitted, got:\n{serialized}"
        );
    }
}
