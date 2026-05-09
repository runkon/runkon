use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Severity level for a notification event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    Warning,
    Error,
    Critical,
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Info => write!(f, "info"),
            Self::Warning => write!(f, "warning"),
            Self::Error => write!(f, "error"),
            Self::Critical => write!(f, "critical"),
        }
    }
}

/// Generic notification event envelope.
///
/// Callers map their domain-specific event types to this shape before dispatch.
/// Hook scripts receive the envelope as `RUNKON_NOTIFY_*` environment variables;
/// HTTP hooks receive it as the JSON POST body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Dotted event name, e.g. `"stage.completed"` or `"permit.approved"`.
    pub kind: String,
    /// Human-readable title for the event.
    pub title: String,
    /// Longer description or body text.
    pub body: String,
    /// Severity classification.
    pub severity: Severity,
    /// Arbitrary key/value fields for event-specific data.
    pub fields: HashMap<String, String>,
}

impl Event {
    /// Returns environment variables to inject into shell hook processes.
    ///
    /// Keys are `RUNKON_NOTIFY_*`-prefixed. Custom fields from `self.fields`
    /// appear as `RUNKON_NOTIFY_FIELD_<UPPER_KEY>`.
    pub fn to_env_vars(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("RUNKON_NOTIFY_KIND".into(), self.kind.clone());
        map.insert("RUNKON_NOTIFY_TITLE".into(), self.title.clone());
        map.insert("RUNKON_NOTIFY_BODY".into(), self.body.clone());
        map.insert("RUNKON_NOTIFY_SEVERITY".into(), self.severity.to_string());
        for (k, v) in &self.fields {
            map.insert(
                format!("RUNKON_NOTIFY_FIELD_{}", k.to_uppercase()),
                v.clone(),
            );
        }
        map
    }

    /// Returns a JSON representation of the event for HTTP hook payloads.
    pub fn to_json(&self) -> Value {
        serde_json::to_value(self).expect("Event serialization is infallible")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn demo_event() -> Event {
        Event {
            kind: "stage.completed".into(),
            title: "Stage finished".into(),
            body: "All steps passed.".into(),
            severity: Severity::Info,
            fields: [("run_id".into(), "abc123".into())].into(),
        }
    }

    #[test]
    fn to_env_vars_contains_standard_keys() {
        let ev = demo_event();
        let vars = ev.to_env_vars();
        assert_eq!(vars["RUNKON_NOTIFY_KIND"], "stage.completed");
        assert_eq!(vars["RUNKON_NOTIFY_TITLE"], "Stage finished");
        assert_eq!(vars["RUNKON_NOTIFY_BODY"], "All steps passed.");
        assert_eq!(vars["RUNKON_NOTIFY_SEVERITY"], "info");
    }

    #[test]
    fn to_env_vars_uppercases_field_keys() {
        let ev = demo_event();
        let vars = ev.to_env_vars();
        assert_eq!(vars["RUNKON_NOTIFY_FIELD_RUN_ID"], "abc123");
    }

    #[test]
    fn to_json_round_trips() {
        let ev = demo_event();
        let val = ev.to_json();
        assert_eq!(val["kind"], "stage.completed");
        assert_eq!(val["severity"], "info");
    }

    #[test]
    fn severity_display() {
        assert_eq!(Severity::Info.to_string(), "info");
        assert_eq!(Severity::Warning.to_string(), "warning");
        assert_eq!(Severity::Error.to_string(), "error");
        assert_eq!(Severity::Critical.to_string(), "critical");
    }
}
