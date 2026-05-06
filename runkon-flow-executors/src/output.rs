use runkon_flow::helpers::{fix_backslash_escapes, parse_flow_output, strip_trailing_commas};
use runkon_flow::output_schema::{ArrayItems, FieldDef, FieldType, OutputSchema};

// ---------------------------------------------------------------------------
// Structured output
// ---------------------------------------------------------------------------

/// Validated structured output from an agent.
#[derive(Debug, Clone)]
pub struct StructuredOutput {
    /// The raw parsed JSON value.
    pub value: serde_json::Value,
    /// Derived markers for `if`/`while` conditions.
    pub markers: Vec<String>,
    /// Context string for `{{prior_context}}` (from `summary` field if present).
    pub context: String,
    /// The full JSON as a string for storage.
    pub json_string: String,
}

/// Find the start position of the real `<<<FLOW_OUTPUT>>>` block.
pub fn find_flow_output_start(text: &str, marker: &str) -> Option<usize> {
    let mut last_valid = None;
    let mut search_pos = 0;
    while let Some(rel) = text[search_pos..].find(marker) {
        let abs = search_pos + rel;
        let after = text[abs + marker.len()..].trim_start();
        if after.starts_with('{') || after.starts_with('[') || after.starts_with('`') {
            last_valid = Some(abs);
        }
        search_pos = abs + 1;
    }
    last_valid
}

/// Extract and clean the raw JSON string from a `<<<FLOW_OUTPUT>>>` block.
pub fn extract_output_block(text: &str) -> Option<String> {
    let start_marker = "<<<FLOW_OUTPUT>>>";
    let end_marker = "<<<END_FLOW_OUTPUT>>>";

    let start = find_flow_output_start(text, start_marker)?;
    let json_start = start + start_marker.len();
    let end = text[json_start..].find(end_marker)?;
    let raw = text[json_start..json_start + end].trim();

    Some(strip_code_fences(raw))
}

/// Strip markdown code fences from the output.
pub fn strip_code_fences(s: &str) -> String {
    let s = s.trim();
    if let Some(rest) = s.strip_prefix("```") {
        let body = if let Some(idx) = rest.find('\n') {
            &rest[idx + 1..]
        } else {
            return s.to_string();
        };
        if let Some(content) = body.strip_suffix("```") {
            return content.trim().to_string();
        }
    }
    s.to_string()
}

/// Parse the `<<<FLOW_OUTPUT>>>` block as structured JSON and validate against the schema.
pub fn parse_structured_output(
    text: &str,
    schema: &OutputSchema,
) -> Result<StructuredOutput, String> {
    let cleaned = extract_output_block(text)
        .ok_or_else(|| "No <<<FLOW_OUTPUT>>> block found in agent output".to_string())?;

    let cleaned = strip_trailing_commas(&cleaned);
    let cleaned = fix_backslash_escapes(&cleaned);

    let value: serde_json::Value =
        serde_json::from_str(&cleaned).map_err(|e| format!("Invalid JSON in FLOW_OUTPUT: {e}"))?;

    validate_value(&value, &schema.fields)?;

    let markers = derive_markers(&value, schema);
    let context = derive_context(&value, schema);
    let json_string = serde_json::to_string(&value)
        .expect("re-serializing a valid serde_json::Value should never fail");

    Ok(StructuredOutput {
        value,
        markers,
        context,
        json_string,
    })
}

/// Derive `StructuredOutput` from a pre-validated `serde_json::Value`.
///
/// Used by the direct API path where the Anthropic API has already enforced schema
/// conformance via `tool_use`.
pub fn derive_output_from_value(
    value: serde_json::Value,
    schema: &OutputSchema,
) -> StructuredOutput {
    let markers = derive_markers(&value, schema);
    let context = derive_context(&value, schema);
    let json_string = serde_json::to_string(&value)
        .expect("re-serializing a valid serde_json::Value should never fail");

    StructuredOutput {
        value,
        markers,
        context,
        json_string,
    }
}

/// Interpret agent output using a schema (if present) or generic `FLOW_OUTPUT` parsing.
///
/// Returns `(markers, context, structured_json)`. The `succeeded` flag controls whether
/// a schema validation failure is treated as an error (`Err`) or silently falls back.
pub fn interpret_agent_output(
    result_text: Option<&str>,
    schema: Option<&OutputSchema>,
    succeeded: bool,
) -> Result<(Vec<String>, String, Option<String>), String> {
    if let Some(s) = schema {
        match result_text.map(|text| parse_structured_output(text, s)) {
            Some(Ok(structured)) => Ok((
                structured.markers,
                structured.context,
                Some(structured.json_string),
            )),
            Some(Err(e)) if succeeded => Err(format!("structured output validation: {e}")),
            _ => {
                let fallback = result_text.and_then(parse_flow_output).unwrap_or_default();
                Ok((fallback.markers, fallback.context, None))
            }
        }
    } else {
        let output = result_text.and_then(parse_flow_output).unwrap_or_default();
        Ok((output.markers, output.context, None))
    }
}

// ---------------------------------------------------------------------------
// Schema → Claude API tool JSON
// ---------------------------------------------------------------------------

/// Convert an `OutputSchema` into a Claude API tool definition JSON value.
pub fn schema_to_tool_json(schema: &OutputSchema) -> serde_json::Value {
    let (properties, required) = fields_to_json_schema(&schema.fields);

    serde_json::json!({
        "name": schema.name,
        "description": "Structured output for this workflow step",
        "input_schema": {
            "type": "object",
            "properties": properties,
            "required": required
        }
    })
}

fn fields_to_json_schema(
    fields: &[FieldDef],
) -> (serde_json::Map<String, serde_json::Value>, Vec<String>) {
    let mut properties = serde_json::Map::new();
    let mut required = Vec::new();

    for field in fields {
        let mut type_schema = field_type_to_schema(&field.field_type);

        if let Some(ref desc) = field.desc {
            if let Some(obj) = type_schema.as_object_mut() {
                obj.insert(
                    "description".to_string(),
                    serde_json::Value::String(desc.clone()),
                );
            }
        }

        properties.insert(field.name.clone(), type_schema);

        if field.required {
            required.push(field.name.clone());
        }
    }

    (properties, required)
}

fn field_type_to_schema(field_type: &FieldType) -> serde_json::Value {
    match field_type {
        FieldType::String => serde_json::json!({"type": "string"}),
        FieldType::Number => serde_json::json!({"type": "number"}),
        FieldType::Boolean => serde_json::json!({"type": "boolean"}),
        FieldType::Enum(variants) => {
            serde_json::json!({"type": "string", "enum": variants})
        }
        FieldType::Array { items } => match items {
            ArrayItems::Scalar(scalar_type) => {
                let items_schema = field_type_to_schema(scalar_type);
                serde_json::json!({"type": "array", "items": items_schema})
            }
            ArrayItems::Object(sub_fields) => {
                let (properties, required) = fields_to_json_schema(sub_fields);
                serde_json::json!({
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": properties,
                        "required": required
                    }
                })
            }
            ArrayItems::Untyped => serde_json::json!({"type": "array"}),
        },
        FieldType::Object { fields } => {
            let (properties, required) = fields_to_json_schema(fields);
            serde_json::json!({
                "type": "object",
                "properties": properties,
                "required": required
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

fn validate_value(value: &serde_json::Value, fields: &[FieldDef]) -> Result<(), String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "FLOW_OUTPUT must be a JSON object".to_string())?;

    for field in fields {
        match obj.get(&field.name) {
            None if field.required => {
                return Err(format!("Missing required field: '{}'", field.name));
            }
            None => continue,
            Some(val) => validate_field_value(val, field)?,
        }
    }

    Ok(())
}

fn validate_field_value(value: &serde_json::Value, field: &FieldDef) -> Result<(), String> {
    match &field.field_type {
        FieldType::String => {
            if !value.is_string() {
                return Err(format!(
                    "Field '{}' expected string, got {}",
                    field.name,
                    json_type_name(value)
                ));
            }
        }
        FieldType::Number => {
            if !value.is_number() {
                return Err(format!(
                    "Field '{}' expected number, got {}",
                    field.name,
                    json_type_name(value)
                ));
            }
        }
        FieldType::Boolean => {
            if !value.is_boolean() {
                return Err(format!(
                    "Field '{}' expected boolean, got {}",
                    field.name,
                    json_type_name(value)
                ));
            }
        }
        FieldType::Enum(variants) => {
            let s = value.as_str().ok_or_else(|| {
                format!(
                    "Field '{}' expected enum string, got {}",
                    field.name,
                    json_type_name(value)
                )
            })?;
            if !variants.contains(&s.to_string()) {
                return Err(format!(
                    "Field '{}' value '{}' is not one of: {}",
                    field.name,
                    s,
                    variants.join(", ")
                ));
            }
        }
        FieldType::Array { items } => {
            let arr = value.as_array().ok_or_else(|| {
                format!(
                    "Field '{}' expected array, got {}",
                    field.name,
                    json_type_name(value)
                )
            })?;
            match items {
                ArrayItems::Scalar(ft) => {
                    let mut synthetic = FieldDef {
                        name: String::new(),
                        required: true,
                        field_type: *ft.clone(),
                        desc: None,
                        examples: None,
                    };
                    for (i, elem) in arr.iter().enumerate() {
                        synthetic.name = format!("{}[{}]", field.name, i);
                        validate_field_value(elem, &synthetic)?;
                    }
                }
                ArrayItems::Object(sub_fields) if !sub_fields.is_empty() => {
                    for (i, elem) in arr.iter().enumerate() {
                        validate_value(elem, sub_fields)
                            .map_err(|e| format!("In '{}[{}]': {e}", field.name, i))?;
                    }
                }
                _ => {}
            }
        }
        FieldType::Object { fields } => {
            if !value.is_object() {
                return Err(format!(
                    "Field '{}' expected object, got {}",
                    field.name,
                    json_type_name(value)
                ));
            }
            if !fields.is_empty() {
                validate_value(value, fields)?;
            }
        }
    }
    Ok(())
}

fn json_type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

// ---------------------------------------------------------------------------
// Marker derivation
// ---------------------------------------------------------------------------

fn derive_markers(value: &serde_json::Value, schema: &OutputSchema) -> Vec<String> {
    if let Some(ref rules) = schema.markers {
        let mut markers = Vec::new();
        for (marker_name, expr) in rules {
            if evaluate_marker_expr(value, expr) {
                markers.push(marker_name.clone());
            }
        }
        markers.sort();
        markers
    } else {
        derive_default_markers(value)
    }
}

pub fn evaluate_marker_expr(value: &serde_json::Value, expr: &str) -> bool {
    let expr = expr.trim();

    if let Some(result) = try_eval_filtered_length(value, expr) {
        return result;
    }

    if let Some(result) = try_eval_length(value, expr) {
        return result;
    }

    if let Some(result) = try_eval_equality(value, expr) {
        return result;
    }

    if let Some(result) = try_eval_numeric_comparison(value, expr) {
        return result;
    }

    false
}

fn try_eval_length(value: &serde_json::Value, expr: &str) -> Option<bool> {
    let (field_part, rest) = expr.split_once(".length")?;
    let rest = rest.trim();

    let field_val = value.get(field_part.trim())?;
    let len = match field_val {
        serde_json::Value::Array(arr) => arr.len(),
        serde_json::Value::String(s) => s.len(),
        _ => return None,
    };

    eval_comparison(len, rest)
}

fn try_eval_filtered_length(value: &serde_json::Value, expr: &str) -> Option<bool> {
    let bracket_start = expr.find('[')?;
    let bracket_end = expr.find(']')?;
    if bracket_start >= bracket_end {
        return None;
    }

    let field_name = expr[..bracket_start].trim();
    let filter_expr = expr[bracket_start + 1..bracket_end].trim();
    let after_bracket = expr[bracket_end + 1..].trim();

    let rest = after_bracket.strip_prefix(".length")?;
    let rest = rest.trim();

    let (sub_field, sub_value) = filter_expr.split_once("==")?;
    let sub_field = sub_field.trim();
    let sub_value = sub_value.trim();

    let arr = value.get(field_name)?.as_array()?;
    let filtered_count = arr
        .iter()
        .filter(|item| {
            item.get(sub_field)
                .and_then(|v| v.as_str())
                .is_some_and(|s| s == sub_value)
        })
        .count();

    eval_comparison(filtered_count, rest)
}

fn try_eval_equality(value: &serde_json::Value, expr: &str) -> Option<bool> {
    let (field, rhs) = expr.split_once("==")?;
    let field = field.trim();
    let rhs = rhs.trim();

    let field_val = value.get(field)?;

    Some(match rhs {
        "true" => field_val.as_bool() == Some(true),
        "false" => field_val.as_bool() == Some(false),
        _ => {
            if let Some(s) = field_val.as_str() {
                s == rhs
            } else if let Some(n) = field_val.as_f64() {
                rhs.parse::<f64>()
                    .ok()
                    .is_some_and(|rn| (n - rn).abs() < f64::EPSILON)
            } else {
                false
            }
        }
    })
}

fn try_eval_numeric_comparison(value: &serde_json::Value, expr: &str) -> Option<bool> {
    for op in ["<", ">"] {
        if let Some((field, rhs)) = expr.split_once(op) {
            let field = field.trim();
            let rhs = rhs.trim();
            if field.ends_with('=') || rhs.starts_with('=') {
                continue;
            }
            let field_val = value.get(field)?.as_f64()?;
            let rhs_val: f64 = rhs.parse().ok()?;
            return Some(match op {
                "<" => field_val < rhs_val,
                ">" => field_val > rhs_val,
                _ => false,
            });
        }
    }
    None
}

fn eval_comparison(len: usize, rest: &str) -> Option<bool> {
    let rest = rest.trim();
    if let Some(rhs) = rest.strip_prefix('>') {
        let n: usize = rhs.trim().parse().ok()?;
        Some(len > n)
    } else if let Some(rhs) = rest.strip_prefix("==") {
        let n: usize = rhs.trim().parse().ok()?;
        Some(len == n)
    } else if let Some(rhs) = rest.strip_prefix('<') {
        let n: usize = rhs.trim().parse().ok()?;
        Some(len < n)
    } else {
        None
    }
}

pub fn derive_default_markers(value: &serde_json::Value) -> Vec<String> {
    let mut markers = Vec::new();
    let obj = match value.as_object() {
        Some(o) => o,
        None => return markers,
    };

    if let Some(approved) = obj.get("approved") {
        if approved.as_bool() == Some(false) {
            markers.push("not_approved".to_string());
        }
    }

    if let Some(findings) = obj.get("findings") {
        if let Some(arr) = findings.as_array() {
            if !arr.is_empty() {
                markers.push("has_findings".to_string());
            }
            for item in arr {
                if let Some(severity) = item.get("severity").and_then(|v| v.as_str()) {
                    match severity {
                        "critical" if !markers.contains(&"has_critical_findings".to_string()) => {
                            markers.push("has_critical_findings".to_string());
                        }
                        "high" if !markers.contains(&"has_high_findings".to_string()) => {
                            markers.push("has_high_findings".to_string());
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    markers.sort();
    markers
}

fn derive_context(value: &serde_json::Value, schema: &OutputSchema) -> String {
    for preferred in &["context", "summary"] {
        if schema
            .fields
            .iter()
            .any(|f| f.name == *preferred && matches!(f.field_type, FieldType::String))
        {
            if let Some(s) = value.get(*preferred).and_then(|v| v.as_str()) {
                return s.to_string();
            }
        }
    }
    for field in &schema.fields {
        if matches!(field.field_type, FieldType::String) {
            if let Some(s) = value.get(&field.name).and_then(|v| v.as_str()) {
                return s.to_string();
            }
        }
    }
    String::new()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use runkon_flow::output_schema::{ArrayItems, FieldDef, FieldType, OutputSchema};

    fn make_field(name: &str, required: bool, field_type: FieldType) -> FieldDef {
        FieldDef {
            name: name.to_string(),
            required,
            field_type,
            desc: None,
            examples: None,
        }
    }

    fn make_schema(name: &str, fields: Vec<FieldDef>) -> OutputSchema {
        OutputSchema {
            name: name.to_string(),
            fields,
            markers: None,
        }
    }

    // ── schema_to_tool_json ──────────────────────────────────────────────────

    #[test]
    fn test_string_field() {
        let schema = make_schema("test", vec![make_field("summary", true, FieldType::String)]);
        let tool = schema_to_tool_json(&schema);
        assert_eq!(tool["name"], "test");
        assert_eq!(
            tool["input_schema"]["properties"]["summary"]["type"],
            "string"
        );
        assert_eq!(tool["input_schema"]["required"][0], "summary");
    }

    #[test]
    fn test_boolean_field() {
        let schema = make_schema(
            "approval",
            vec![make_field("approved", true, FieldType::Boolean)],
        );
        let tool = schema_to_tool_json(&schema);
        assert_eq!(
            tool["input_schema"]["properties"]["approved"]["type"],
            "boolean"
        );
    }

    #[test]
    fn test_enum_field() {
        let schema = make_schema(
            "severity",
            vec![make_field(
                "level",
                true,
                FieldType::Enum(vec!["low".to_string(), "high".to_string()]),
            )],
        );
        let tool = schema_to_tool_json(&schema);
        assert_eq!(
            tool["input_schema"]["properties"]["level"]["type"],
            "string"
        );
        assert_eq!(
            tool["input_schema"]["properties"]["level"]["enum"][0],
            "low"
        );
    }

    #[test]
    fn test_array_scalar() {
        let schema = make_schema(
            "list",
            vec![make_field(
                "tags",
                false,
                FieldType::Array {
                    items: ArrayItems::Scalar(Box::new(FieldType::String)),
                },
            )],
        );
        let tool = schema_to_tool_json(&schema);
        assert_eq!(tool["input_schema"]["properties"]["tags"]["type"], "array");
        assert_eq!(
            tool["input_schema"]["properties"]["tags"]["items"]["type"],
            "string"
        );
        let required = tool["input_schema"]["required"].as_array().unwrap();
        assert!(!required.iter().any(|v| v == "tags"));
    }

    #[test]
    fn test_required_vs_optional() {
        let schema = make_schema(
            "mixed",
            vec![
                make_field("req", true, FieldType::String),
                make_field("opt", false, FieldType::Number),
            ],
        );
        let tool = schema_to_tool_json(&schema);
        let required = tool["input_schema"]["required"].as_array().unwrap();
        assert!(required.iter().any(|v| v == "req"));
        assert!(!required.iter().any(|v| v == "opt"));
    }

    // ── parse_structured_output ──────────────────────────────────────────────

    #[test]
    fn parse_structured_output_happy_path() {
        let schema = make_schema("test", vec![make_field("ok", true, FieldType::Boolean)]);
        let text = "<<<FLOW_OUTPUT>>>\n{\"ok\": true}\n<<<END_FLOW_OUTPUT>>>";
        let result = parse_structured_output(text, &schema).unwrap();
        assert_eq!(result.value["ok"], true);
    }

    #[test]
    fn parse_structured_output_missing_block() {
        let schema = make_schema("test", vec![make_field("ok", true, FieldType::Boolean)]);
        let err = parse_structured_output("no block here", &schema).unwrap_err();
        assert!(err.contains("No <<<FLOW_OUTPUT>>>"), "got: {err}");
    }

    #[test]
    fn parse_structured_output_missing_required_field() {
        let schema = make_schema("test", vec![make_field("ok", true, FieldType::Boolean)]);
        let text = "<<<FLOW_OUTPUT>>>\n{}\n<<<END_FLOW_OUTPUT>>>";
        let err = parse_structured_output(text, &schema).unwrap_err();
        assert!(err.contains("Missing required field"), "got: {err}");
    }

    // ── interpret_agent_output ────────────────────────────────────────────────

    #[test]
    fn interpret_no_schema_uses_flow_output_parser() {
        let text =
            "<<<FLOW_OUTPUT>>>\n{\"markers\":[\"done\"],\"context\":\"ok\"}\n<<<END_FLOW_OUTPUT>>>";
        let (markers, context, structured) =
            interpret_agent_output(Some(text), None, true).unwrap();
        assert_eq!(markers, vec!["done"]);
        assert_eq!(context, "ok");
        assert!(structured.is_none());
    }

    #[test]
    fn interpret_with_schema_returns_structured_json() {
        let schema = make_schema("test", vec![make_field("ok", true, FieldType::Boolean)]);
        let text = "<<<FLOW_OUTPUT>>>\n{\"ok\": true}\n<<<END_FLOW_OUTPUT>>>";
        let (markers, _context, structured) =
            interpret_agent_output(Some(text), Some(&schema), true).unwrap();
        assert!(markers.is_empty());
        assert!(structured.is_some());
    }

    #[test]
    fn interpret_schema_failure_on_success_returns_err() {
        let schema = make_schema("test", vec![make_field("ok", true, FieldType::Boolean)]);
        let text = "no output block at all";
        let err = interpret_agent_output(Some(text), Some(&schema), true).unwrap_err();
        assert!(err.contains("structured output validation"), "got: {err}");
    }

    #[test]
    fn interpret_schema_failure_on_failed_run_falls_back() {
        let schema = make_schema("test", vec![make_field("ok", true, FieldType::Boolean)]);
        let text = "<<<FLOW_OUTPUT>>>\n{\"markers\":[\"fallback\"],\"context\":\"fb\"}\n<<<END_FLOW_OUTPUT>>>";
        let (markers, context, structured) =
            interpret_agent_output(Some(text), Some(&schema), false).unwrap();
        assert_eq!(markers, vec!["fallback"]);
        assert_eq!(context, "fb");
        assert!(structured.is_none());
    }
}
