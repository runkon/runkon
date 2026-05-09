//! Demonstrates constructing an Event and firing a shell hook with HookRunner.
//!
//! Run with: `cargo run --example hook_fires -p runkon-notify`

use std::collections::HashMap;
use std::io::Read;

use runkon_notify::{Event, HookConfig, HookRunner, Severity};

fn main() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out_file = dir.path().join("out.txt");
    let out_path = out_file.to_str().unwrap().to_string();

    let script = format!("echo $RUNKON_NOTIFY_KIND >> '{out_path}'");

    let event = Event {
        kind: "demo.fired".into(),
        title: "Demo event".into(),
        body: "This is a demonstration.".into(),
        severity: Severity::Info,
        fields: HashMap::from([("run_id".into(), "demo-001".into())]),
    };

    let hooks = vec![HookConfig {
        on: "demo.*".into(),
        run: Some(script),
        timeout_ms: Some(5_000),
        ..Default::default()
    }];

    let runner = HookRunner::new(&hooks);
    runner.run_test(&event).expect("hook should succeed");

    let mut contents = String::new();
    std::fs::File::open(&out_file)
        .expect("output file must exist after hook ran")
        .read_to_string(&mut contents)
        .unwrap();

    assert_eq!(
        contents.trim(),
        "demo.fired",
        "hook script must receive RUNKON_NOTIFY_KIND"
    );

    println!(
        "hook_fires example passed — hook received kind: {}",
        contents.trim()
    );
}
