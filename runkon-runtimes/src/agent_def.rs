use serde::Serialize;

/// Role type for an agent.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentRole {
    #[default]
    Actor,
    Reviewer,
}

impl std::fmt::Display for AgentRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Actor => write!(f, "actor"),
            Self::Reviewer => write!(f, "reviewer"),
        }
    }
}

impl std::str::FromStr for AgentRole {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "actor" => Ok(Self::Actor),
            "reviewer" => Ok(Self::Reviewer),
            _ => Err(format!(
                "unknown AgentRole: {s}. Expected 'actor' or 'reviewer'."
            )),
        }
    }
}

/// A parsed agent definition from a `.md` file.
#[derive(Debug, Clone, Serialize)]
pub struct AgentDef {
    /// Agent identifier (from file stem).
    pub name: String,
    /// Role type: actor or reviewer.
    pub role: AgentRole,
    /// Whether this agent is permitted to commit code.
    pub can_commit: bool,
    /// Optional model override.
    pub model: Option<String>,
    /// The runtime to use for this agent (defaults to "claude").
    pub runtime: String,
    /// The prompt template (full markdown body after frontmatter).
    pub prompt: String,
}

impl Default for AgentDef {
    fn default() -> Self {
        Self {
            name: String::new(),
            role: AgentRole::Actor,
            can_commit: false,
            model: None,
            runtime: "claude".to_string(),
            prompt: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn agent_role_display_actor() {
        assert_eq!(AgentRole::Actor.to_string(), "actor");
    }

    #[test]
    fn agent_role_display_reviewer() {
        assert_eq!(AgentRole::Reviewer.to_string(), "reviewer");
    }

    #[test]
    fn agent_role_from_str_round_trip() {
        assert_eq!(AgentRole::from_str("actor").unwrap(), AgentRole::Actor);
        assert_eq!(
            AgentRole::from_str("reviewer").unwrap(),
            AgentRole::Reviewer
        );
    }

    #[test]
    fn agent_role_from_str_unknown_returns_error() {
        let err = AgentRole::from_str("admin").unwrap_err();
        assert!(err.contains("unknown AgentRole: admin"), "got: {err}");
    }

    #[test]
    fn agent_def_default_runtime_is_claude() {
        assert_eq!(AgentDef::default().runtime, "claude");
    }

    #[test]
    fn agent_def_default_model_is_none() {
        assert!(AgentDef::default().model.is_none());
    }

    #[test]
    fn agent_def_default_can_commit_is_false() {
        assert!(!AgentDef::default().can_commit);
    }

    #[test]
    fn agent_def_default_role_is_actor() {
        assert_eq!(AgentDef::default().role, AgentRole::Actor);
    }

    #[test]
    fn agent_def_serializes_to_json() {
        let def = AgentDef {
            name: "my-agent".to_string(),
            role: AgentRole::Reviewer,
            can_commit: true,
            model: Some("claude-3-opus".to_string()),
            runtime: "claude".to_string(),
            prompt: "Do something".to_string(),
        };
        let json = serde_json::to_string(&def).unwrap();
        assert!(json.contains("my-agent"), "name not found in: {json}");
        assert!(json.contains("reviewer"), "role not found in: {json}");
        assert!(json.contains("claude-3-opus"), "model not found in: {json}");
    }

    #[test]
    fn agent_role_serde_round_trip() {
        let json = serde_json::to_string(&AgentRole::Reviewer).unwrap();
        assert_eq!(json, r#""reviewer""#);
        let back: AgentRole = serde_json::from_str(&json).unwrap();
        assert_eq!(back, AgentRole::Reviewer);
    }
}
