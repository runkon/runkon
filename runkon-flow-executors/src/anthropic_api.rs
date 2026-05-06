use std::collections::HashMap;

use runkon_flow::constants::metadata_keys;
use runkon_flow::output_schema::OutputSchema;

use crate::output::{derive_output_from_value, schema_to_tool_json};

const ANTHROPIC_API_URL: &str = "https://api.anthropic.com/v1/messages";
const ANTHROPIC_API_VERSION: &str = "2023-06-01";
const MAX_TOKENS: u64 = 8192;

/// Default model used when the step does not specify a model override.
pub const DEFAULT_API_MODEL: &str = "claude-sonnet-4-6";

#[derive(Debug)]
struct ApiCallResult {
    json: serde_json::Value,
    json_string: String,
    input_tokens: i64,
    output_tokens: i64,
}

fn execute_via_api(
    prompt: &str,
    schema: &OutputSchema,
    model: &str,
    timeout: std::time::Duration,
    api_key: &str,
    url: &str,
) -> Result<ApiCallResult, String> {
    let tool_json = schema_to_tool_json(schema);
    let body = serde_json::json!({
        "model": model,
        "max_tokens": MAX_TOKENS,
        "tools": [tool_json],
        "tool_choice": {"type": "tool", "name": schema.name},
        "messages": [{"role": "user", "content": prompt}]
    });
    let agent = ureq::AgentBuilder::new().timeout(timeout).build();
    let response_result = agent
        .post(url)
        .set("x-api-key", api_key)
        .set("anthropic-version", ANTHROPIC_API_VERSION)
        .set("content-type", "application/json")
        .send_json(&body);
    let response_value: serde_json::Value = match response_result {
        Ok(resp) => resp
            .into_json()
            .map_err(|e| format!("Failed to parse API response JSON: {e}"))?,
        Err(ureq::Error::Status(status, resp)) => {
            let body_text = resp
                .into_string()
                .unwrap_or_else(|e| format!("<body read failed: {e}>"));
            let truncated = if body_text.len() > 500 {
                let end = body_text.floor_char_boundary(500);
                format!("{}…", &body_text[..end])
            } else {
                body_text
            };
            tracing::debug!("API error body: {truncated}");
            return Err(format!("API call failed: {status}"));
        }
        Err(e) => return Err(format!("API call failed: {e}")),
    };
    let input = extract_tool_use_input(&response_value)?;
    let json_string = serde_json::to_string(&input)
        .map_err(|e| format!("Failed to serialize tool_use input: {e}"))?;
    let usage = response_value
        .get("usage")
        .unwrap_or(&serde_json::Value::Null);
    let input_tokens = usage
        .get("input_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let output_tokens = usage
        .get("output_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    Ok(ApiCallResult {
        json: input,
        json_string,
        input_tokens,
        output_tokens,
    })
}

fn extract_tool_use_input(response_value: &serde_json::Value) -> Result<serde_json::Value, String> {
    let content = response_value
        .get("content")
        .and_then(|c| c.as_array())
        .ok_or_else(|| "API response missing 'content' array".to_string())?;
    let tool_use_block = content
        .iter()
        .find(|block| block.get("type").and_then(|t| t.as_str()) == Some("tool_use"))
        .ok_or_else(|| "API response contained no tool_use block".to_string())?;
    tool_use_block
        .get("input")
        .ok_or_else(|| "tool_use block missing 'input' field".to_string())
        .cloned()
}

/// Portable output from a successful API call execution.
#[derive(Debug)]
pub struct ApiCallExecutorOutput {
    /// The JSON string of the tool_use input (same value as `structured_output`).
    pub result_text: String,
    /// The JSON string of the structured output.
    pub structured_output: String,
    /// Derived markers from the structured output.
    pub markers: Vec<String>,
    /// Context string from the structured output.
    pub context: String,
    /// Execution metadata (token counts, turn count).
    pub metadata: HashMap<String, String>,
}

/// Stateless executor that calls the Anthropic Messages API with `tool_use` enforcement.
///
/// Takes an API key directly rather than a `Config` struct, keeping this type
/// free of `conductor-core` dependencies.
pub struct ApiCallExecutor {
    api_key: String,
}

impl ApiCallExecutor {
    pub fn new(api_key: String) -> Self {
        Self { api_key }
    }

    /// Execute the API call with the given prompt and schema.
    pub fn execute(
        &self,
        prompt: &str,
        schema: &OutputSchema,
        model: &str,
        timeout: std::time::Duration,
    ) -> Result<ApiCallExecutorOutput, String> {
        self.execute_at(prompt, schema, model, timeout, ANTHROPIC_API_URL)
    }

    fn execute_at(
        &self,
        prompt: &str,
        schema: &OutputSchema,
        model: &str,
        timeout: std::time::Duration,
        url: &str,
    ) -> Result<ApiCallExecutorOutput, String> {
        let result = execute_via_api(prompt, schema, model, timeout, &self.api_key, url)?;

        let structured = derive_output_from_value(result.json, schema);

        let metadata = HashMap::from([
            (metadata_keys::NUM_TURNS.to_string(), "1".to_string()),
            (
                metadata_keys::INPUT_TOKENS.to_string(),
                result.input_tokens.to_string(),
            ),
            (
                metadata_keys::OUTPUT_TOKENS.to_string(),
                result.output_tokens.to_string(),
            ),
        ]);

        Ok(ApiCallExecutorOutput {
            result_text: result.json_string.clone(),
            structured_output: structured.json_string,
            markers: structured.markers,
            context: structured.context,
            metadata,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use runkon_flow::output_schema::{FieldDef, FieldType, OutputSchema};

    fn make_schema() -> OutputSchema {
        OutputSchema {
            name: "test".to_string(),
            fields: vec![FieldDef {
                name: "ok".to_string(),
                required: true,
                field_type: FieldType::Boolean,
                desc: None,
                examples: None,
            }],
            markers: None,
        }
    }

    #[test]
    fn test_extract_tool_use_input_success() {
        let response = serde_json::json!({
            "content": [
                {"type": "tool_use", "input": {"field": "value"}}
            ]
        });
        let result = extract_tool_use_input(&response).unwrap();
        assert_eq!(result["field"], "value");
    }

    #[test]
    fn test_missing_content_array() {
        let response = serde_json::json!({"model": "claude"});
        let err = extract_tool_use_input(&response).unwrap_err();
        assert!(err.contains("'content' array"), "got: {err}");
    }

    #[test]
    fn test_missing_tool_use_block() {
        let response = serde_json::json!({
            "content": [{"type": "text", "text": "hello"}]
        });
        let err = extract_tool_use_input(&response).unwrap_err();
        assert!(err.contains("no tool_use block"), "got: {err}");
    }

    #[test]
    fn test_tool_use_block_missing_input() {
        let response = serde_json::json!({
            "content": [{"type": "tool_use", "name": "my_tool"}]
        });
        let err = extract_tool_use_input(&response).unwrap_err();
        assert!(err.contains("missing 'input' field"), "got: {err}");
    }

    fn serve_json_response(body: serde_json::Value) -> std::net::SocketAddr {
        use std::io::{BufRead, Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let body_str = body.to_string();

        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = std::io::BufReader::new(stream);

            let mut content_length = 0usize;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                let lower = line.to_ascii_lowercase();
                if lower.starts_with("content-length:") {
                    if let Some(v) = lower.split(':').nth(1) {
                        content_length = v.trim().parse().unwrap_or(0);
                    }
                }
            }
            if content_length > 0 {
                let mut buf = vec![0u8; content_length];
                let _ = reader.read_exact(&mut buf);
            }

            let mut stream = reader.into_inner();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
                body_str.len(),
                body_str
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        addr
    }

    #[test]
    fn execute_derives_token_metadata_markers_and_context() {
        let api_response = serde_json::json!({
            "content": [{
                "type": "tool_use",
                "input": {"approved": true, "summary": "all good"}
            }],
            "usage": {"input_tokens": 123, "output_tokens": 45}
        });
        let addr = serve_json_response(api_response);

        let schema = OutputSchema {
            name: "review".to_string(),
            fields: vec![
                FieldDef {
                    name: "approved".to_string(),
                    required: true,
                    field_type: FieldType::Boolean,
                    desc: None,
                    examples: None,
                },
                FieldDef {
                    name: "summary".to_string(),
                    required: true,
                    field_type: FieldType::String,
                    desc: None,
                    examples: None,
                },
            ],
            markers: None,
        };

        let executor = ApiCallExecutor::new("test-key".to_string());
        let output = executor
            .execute_at(
                "review this",
                &schema,
                "claude-sonnet-4-6",
                std::time::Duration::from_secs(5),
                &format!("http://{addr}"),
            )
            .unwrap();

        assert_eq!(output.metadata[metadata_keys::INPUT_TOKENS], "123");
        assert_eq!(output.metadata[metadata_keys::OUTPUT_TOKENS], "45");
        assert_eq!(output.metadata[metadata_keys::NUM_TURNS], "1");
        assert_eq!(output.context, "all good");
        assert!(
            output.structured_output.contains("approved"),
            "got: {}",
            output.structured_output
        );
    }

    #[test]
    fn error_body_not_in_returned_error() {
        use std::io::{BufRead, Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = std::io::BufReader::new(stream);

            let mut content_length = 0usize;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                let lower = line.to_ascii_lowercase();
                if lower.starts_with("content-length:") {
                    if let Some(v) = lower.split(':').nth(1) {
                        content_length = v.trim().parse().unwrap_or(0);
                    }
                }
            }
            if content_length > 0 {
                let mut body = vec![0u8; content_length];
                let _ = reader.read_exact(&mut body);
            }

            let mut stream = reader.into_inner();
            let response = "HTTP/1.1 422 Unprocessable Entity\r\nContent-Length: 13\r\nContent-Type: text/plain\r\n\r\nSENTINEL_BODY";
            stream.write_all(response.as_bytes()).unwrap();
        });

        let schema = make_schema();
        let url = format!("http://{addr}");

        let err_string = execute_via_api(
            "test",
            &schema,
            "claude-sonnet-4-6",
            std::time::Duration::from_secs(5),
            "dummy-key",
            &url,
        )
        .unwrap_err();

        handle.join().unwrap();

        assert!(
            err_string.contains("422"),
            "error should contain status code, got: {err_string}"
        );
        assert!(
            !err_string.contains("SENTINEL_BODY"),
            "error should not contain response body, got: {err_string}"
        );
    }
}
