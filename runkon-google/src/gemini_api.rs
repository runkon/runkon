use std::collections::HashMap;

use runkon_flow::constants::metadata_keys;
use runkon_flow::output_schema::OutputSchema;

use runkon_flow_executors::output::{derive_output_from_value, schema_to_gemini_response_schema};

const GEMINI_API_BASE: &str = "https://generativelanguage.googleapis.com/v1beta/models";

#[derive(Debug)]
struct GeminiApiCallResult {
    json: serde_json::Value,
    json_string: String,
    input_tokens: i64,
    output_tokens: i64,
}

fn execute_via_api(
    prompt: &str,
    schema: &OutputSchema,
    timeout: std::time::Duration,
    api_key: &str,
    url: &str,
) -> Result<GeminiApiCallResult, String> {
    let response_schema = schema_to_gemini_response_schema(schema);
    let body = serde_json::json!({
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": response_schema
        }
    });
    let agent = ureq::AgentBuilder::new().timeout(timeout).build();
    let response_result = agent
        .post(url)
        .set("x-goog-api-key", api_key)
        .set("content-type", "application/json")
        .send_json(&body);
    let response_value: serde_json::Value = match response_result {
        Ok(resp) => resp
            .into_json()
            .map_err(|e| format!("Failed to parse Gemini API response JSON: {e}"))?,
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
            tracing::debug!("Gemini API error body: {truncated}");
            return Err(format!("Gemini API call failed: {status}"));
        }
        Err(e) => return Err(format!("Gemini API call failed: {e}")),
    };

    // Check promptFeedback.blockReason
    if let Some(feedback) = response_value.get("promptFeedback") {
        if let Some(reason) = feedback.get("blockReason").and_then(|v| v.as_str()) {
            if !reason.is_empty() {
                tracing::debug!("Gemini blocked prompt: {reason}");
                return Err(format!("Prompt blocked by safety filter: {reason}"));
            }
        }
    }

    let candidates = response_value
        .get("candidates")
        .and_then(|c| c.as_array())
        .ok_or_else(|| "Gemini response missing 'candidates' array".to_string())?;

    let candidate = candidates
        .first()
        .ok_or_else(|| "Gemini response has empty 'candidates' array".to_string())?;

    // Check finishReason == "SAFETY"
    if let Some(reason) = candidate.get("finishReason").and_then(|v| v.as_str()) {
        if reason == "SAFETY" {
            return Err("Response blocked by safety filter: SAFETY".to_string());
        }
    }

    let text = candidate
        .get("content")
        .and_then(|c| c.get("parts"))
        .and_then(|p| p.as_array())
        .and_then(|arr| arr.first())
        .and_then(|part| part.get("text"))
        .and_then(|t| t.as_str())
        .ok_or_else(|| {
            "Gemini response missing text in candidates[0].content.parts[0].text".to_string()
        })?;

    let json: serde_json::Value = serde_json::from_str(text)
        .map_err(|e| format!("Failed to parse Gemini response text as JSON: {e}"))?;

    let json_string = serde_json::to_string(&json)
        .map_err(|e| format!("Failed to serialize Gemini response: {e}"))?;

    let usage = response_value
        .get("usageMetadata")
        .unwrap_or(&serde_json::Value::Null);
    let input_tokens = usage
        .get("promptTokenCount")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let output_tokens = usage
        .get("candidatesTokenCount")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    Ok(GeminiApiCallResult {
        json,
        json_string,
        input_tokens,
        output_tokens,
    })
}

/// Portable output from a successful Gemini API call execution.
#[derive(Debug)]
pub struct ApiCallExecutorOutput {
    /// The JSON string of the structured output.
    pub result_text: String,
    /// The JSON string of the validated structured output.
    pub structured_output: String,
    /// Derived markers from the structured output.
    pub markers: Vec<String>,
    /// Context string from the structured output.
    pub context: String,
    /// Execution metadata (token counts, turn count).
    pub metadata: HashMap<String, String>,
}

/// Stateless executor that calls the Gemini `generateContent` API with JSON mode enforcement.
///
/// Takes an API key directly rather than a config struct, keeping this type
/// free of `conductor_*` dependencies.
pub struct GeminiApiCallExecutor {
    api_key: String,
}

impl GeminiApiCallExecutor {
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
        let url = format!("{GEMINI_API_BASE}/{model}:generateContent");
        self.execute_at(prompt, schema, timeout, &url)
    }

    fn execute_at(
        &self,
        prompt: &str,
        schema: &OutputSchema,
        timeout: std::time::Duration,
        url: &str,
    ) -> Result<ApiCallExecutorOutput, String> {
        let result = execute_via_api(prompt, schema, timeout, &self.api_key, url)?;

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

    fn serve_raw_response(
        body: String,
        status: u16,
        content_type: &'static str,
    ) -> std::net::SocketAddr {
        use std::io::{BufRead, Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

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
                "HTTP/1.1 {status} \r\nContent-Length: {}\r\nContent-Type: {content_type}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        addr
    }

    fn serve_json_response(body: serde_json::Value) -> std::net::SocketAddr {
        serve_raw_response(body.to_string(), 200, "application/json")
    }

    #[test]
    fn execute_derives_token_metadata_and_output() {
        let api_response = serde_json::json!({
            "candidates": [{
                "content": {
                    "parts": [{"text": "{\"ok\": true}"}]
                },
                "finishReason": "STOP"
            }],
            "usageMetadata": {
                "promptTokenCount": 100,
                "candidatesTokenCount": 42
            }
        });
        let addr = serve_json_response(api_response);

        let schema = OutputSchema {
            name: "test".to_string(),
            fields: vec![FieldDef {
                name: "ok".to_string(),
                required: true,
                field_type: FieldType::Boolean,
                desc: None,
                examples: None,
            }],
            markers: None,
        };

        let executor = GeminiApiCallExecutor::new("test-key".to_string());
        let output = executor
            .execute_at(
                "test prompt",
                &schema,
                std::time::Duration::from_secs(5),
                &format!("http://{addr}/gemini-1.5-pro:generateContent"),
            )
            .unwrap();

        assert_eq!(output.metadata[metadata_keys::INPUT_TOKENS], "100");
        assert_eq!(output.metadata[metadata_keys::OUTPUT_TOKENS], "42");
        assert_eq!(output.metadata[metadata_keys::NUM_TURNS], "1");
        assert!(
            output.structured_output.contains("ok"),
            "got: {}",
            output.structured_output
        );
    }

    #[test]
    fn safety_block_on_prompt_feedback() {
        let api_response = serde_json::json!({
            "promptFeedback": {
                "blockReason": "SAFETY"
            }
        });
        let addr = serve_json_response(api_response);

        let schema = make_schema();
        let err = execute_via_api(
            "test",
            &schema,
            std::time::Duration::from_secs(5),
            "dummy-key",
            &format!("http://{addr}/gemini-1.5-pro:generateContent"),
        )
        .unwrap_err();

        assert!(
            err.to_lowercase().contains("safety"),
            "error should mention safety, got: {err}"
        );
        assert!(
            err.contains("Prompt blocked"),
            "error should mention prompt blocked, got: {err}"
        );
    }

    #[test]
    fn safety_block_on_candidate_finish_reason() {
        let api_response = serde_json::json!({
            "candidates": [{
                "finishReason": "SAFETY"
            }],
            "usageMetadata": {
                "promptTokenCount": 10,
                "candidatesTokenCount": 0
            }
        });
        let addr = serve_json_response(api_response);

        let schema = make_schema();
        let err = execute_via_api(
            "test",
            &schema,
            std::time::Duration::from_secs(5),
            "dummy-key",
            &format!("http://{addr}/gemini-1.5-pro:generateContent"),
        )
        .unwrap_err();

        assert!(
            err.to_lowercase().contains("safety"),
            "error should mention safety, got: {err}"
        );
        assert!(
            err.contains("Response blocked"),
            "error should mention response blocked, got: {err}"
        );
    }

    #[test]
    fn error_status_not_leaked_in_returned_error() {
        let addr = serve_raw_response("SENTINEL_BODY".to_string(), 403, "text/plain");

        let schema = make_schema();
        let err_string = execute_via_api(
            "test",
            &schema,
            std::time::Duration::from_secs(5),
            "dummy-key",
            &format!("http://{addr}/gemini-1.5-pro:generateContent"),
        )
        .unwrap_err();

        assert!(
            err_string.contains("403"),
            "error should contain status code, got: {err_string}"
        );
        assert!(
            !err_string.contains("SENTINEL_BODY"),
            "error should not contain response body, got: {err_string}"
        );
    }
}
