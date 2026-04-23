#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq)]
pub enum CancellationReason {
    UserRequested(Option<String>),
    Timeout,
    FailFast,
    ParentCancelled,
    EngineShutdown,
}
