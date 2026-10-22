//! Test doubles shared by the runtime crates' tests. Enabled by `cfg(test)`
//! here and by the `test-utils` feature for downstream crates.

use crate::error::Result;
use crate::run::RunHandle;
use crate::tracker::RunTracker;

type RowFn = Box<dyn Fn() -> Result<Option<RunHandle>> + Send + Sync>;

/// A [`RunTracker`] whose `get_run` is supplied by the test and whose other
/// methods are no-ops. Stands in for a host whose write of the terminal event
/// failed, or succeeded, depending on the row it returns.
pub struct FixedRowTracker(RowFn);

impl FixedRowTracker {
    /// `get_run` always returns `Ok(Some(row))`.
    pub fn new(row: RunHandle) -> Self {
        Self(Box::new(move || Ok(Some(row.clone()))))
    }

    /// `get_run` returns whatever `f` produces, including errors or no row.
    pub fn from_fn(f: impl Fn() -> Result<Option<RunHandle>> + Send + Sync + 'static) -> Self {
        Self(Box::new(f))
    }
}

impl RunTracker for FixedRowTracker {
    fn record_pid(&self, _: &str, _: u32) -> Result<()> {
        Ok(())
    }
    fn record_runtime(&self, _: &str, _: &str) -> Result<()> {
        Ok(())
    }
    fn mark_cancelled(&self, _: &str) -> Result<()> {
        Ok(())
    }
    fn mark_failed_if_running(&self, _: &str, _: &str) -> Result<()> {
        Ok(())
    }
    fn get_run(&self, _: &str) -> Result<Option<RunHandle>> {
        (self.0)()
    }
}
