use serde::Serialize;

/// Role type for an agent.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentRole {
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
