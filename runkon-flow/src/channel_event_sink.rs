use std::sync::mpsc::Sender;

use crate::events::{EngineEventData, EventSink};

/// An `EventSink` that forwards events over an `mpsc` channel.
///
/// Send errors (i.e. the receiver was dropped) are silently ignored so a
/// disconnected receiver does not affect the run.
pub struct ChannelEventSink(pub Sender<EngineEventData>);

impl EventSink for ChannelEventSink {
    fn emit(&self, event: &EngineEventData) {
        let _ = self.0.send(event.clone());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use super::*;
    use crate::events::EngineEvent;

    #[test]
    fn emit_sends_event_to_receiver() {
        let (tx, rx) = mpsc::channel();
        let sink = ChannelEventSink(tx);
        let data = EngineEventData::new(
            "run-1".to_string(),
            EngineEvent::RunCompleted { succeeded: true },
        );
        sink.emit(&data);
        let received = rx.recv().expect("should receive event");
        assert_eq!(received.run_id, "run-1");
        assert!(matches!(
            received.event,
            EngineEvent::RunCompleted { succeeded: true }
        ));
    }

    #[test]
    fn emit_silently_ignores_disconnected_receiver() {
        let (tx, rx) = mpsc::channel();
        drop(rx);
        let sink = ChannelEventSink(tx);
        let data = EngineEventData::new(
            "run-2".to_string(),
            EngineEvent::RunCancelled {
                reason: crate::cancellation_reason::CancellationReason::UserRequested(None),
            },
        );
        sink.emit(&data);
    }
}
