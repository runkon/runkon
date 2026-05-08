use runkon_flow::events::{EngineEvent, EngineEventData, EventSink};
use runkon_flow::FlowEngineBuilder;

/// `EventSink` that prints a summary of each engine event to stdout.
struct StdoutEventSink;

impl EventSink for StdoutEventSink {
    fn emit(&self, event: &EngineEventData) {
        let summary = match &event.event {
            EngineEvent::RunStarted { workflow_name } => {
                format!("run started (workflow={})", workflow_name)
            }
            EngineEvent::RunCompleted { succeeded } => {
                format!("run completed (succeeded={})", succeeded)
            }
            EngineEvent::StepStarted { step_name } => {
                format!("step started (step={})", step_name)
            }
            EngineEvent::StepCompleted {
                step_name,
                succeeded,
            } => {
                format!(
                    "step completed (step={}, succeeded={})",
                    step_name, succeeded
                )
            }
            _ => format!("{:?}", event.event),
        };
        println!("[{}] run={} {}", event.timestamp, event.run_id, summary);
    }
}

fn main() {
    let sink = StdoutEventSink;

    sink.emit(&EngineEventData::new(
        "run-001".into(),
        EngineEvent::RunStarted {
            workflow_name: "my-workflow".into(),
        },
    ));
    sink.emit(&EngineEventData::new(
        "run-001".into(),
        EngineEvent::RunCompleted { succeeded: true },
    ));

    // Wire it into a FlowEngine (builder shown; engine not actually run here):
    let _engine = FlowEngineBuilder::new()
        .event_sink(Box::new(StdoutEventSink))
        .build()
        .expect("engine build failed");
    println!("engine built with StdoutEventSink attached");
}
