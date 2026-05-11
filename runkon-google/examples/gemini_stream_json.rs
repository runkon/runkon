//! Parser demo: feed a stubbed Gemini stream-json JSONL stream through
//! `GeminiLineEventParser` without spawning a real `gemini` process.
//!
//! Prints the `DrainOutcome` and any captured `RuntimeEvent`s to stdout.
//!
//! Run:
//!   cargo run -p runkon-google --example gemini_stream_json

use std::io::Cursor;
use std::sync::{Arc, Mutex};

use runkon_google::GeminiLineEventParser;
use runkon_runtimes::headless::drain_stream_json;
use runkon_runtimes::tracker::{EventSink, RuntimeEvent};

struct CaptureSink {
    events: Arc<Mutex<Vec<RuntimeEvent>>>,
}

impl EventSink for CaptureSink {
    fn on_event(&self, _run_id: &str, event: RuntimeEvent) {
        self.events.lock().unwrap().push(event);
    }
}

fn main() {
    let lines: &[&str] = &[
        r#"{"type":"init","session_id":"demo-session","model":"gemini-2.5-flash","timestamp":"2024-01-01T00:00:00Z"}"#,
        r#"{"type":"message","role":"user","content":"Write hello world in Rust.","timestamp":"2024-01-01T00:00:01Z"}"#,
        r#"{"type":"tool_use","tool_name":"read_file","tool_id":"read_file_1700000000_0","parameters":{"path":"src/main.rs"},"timestamp":"2024-01-01T00:00:02Z"}"#,
        r#"{"type":"tool_result","tool_id":"read_file_1700000000_0","status":"success","output":"fn main() {}","timestamp":"2024-01-01T00:00:03Z"}"#,
        r#"{"type":"message","role":"assistant","content":"fn main() {\n    println!(\"Hello, world!\");\n}\n","delta":true,"timestamp":"2024-01-01T00:00:04Z"}"#,
        r#"{"type":"result","status":"success","session_id":"demo-session","stats":{"input_tokens":42,"output_tokens":18,"cached":0,"input":42,"total_tokens":60,"duration_ms":1500,"tool_calls":1},"timestamp":"2024-01-01T00:00:05Z"}"#,
    ];

    let input = lines.join("\n");
    let log_file = std::env::temp_dir().join("gemini_stream_json_demo.log");

    let sink = CaptureSink {
        events: Arc::new(Mutex::new(Vec::new())),
    };

    let outcome = drain_stream_json(
        Cursor::new(input.into_bytes()),
        "demo-run",
        &log_file,
        &sink,
        None,
        None,
        GeminiLineEventParser::default(),
    );

    let _ = std::fs::remove_file(&log_file);

    println!("DrainOutcome: {outcome:?}");
    println!("Events captured:");
    for event in sink.events.lock().unwrap().iter() {
        println!("  {event:?}");
    }
}
