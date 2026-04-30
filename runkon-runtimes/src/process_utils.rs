//! OS-level process utilities used by multiple modules.

/// Check whether a process with the given PID is still alive.
#[cfg(unix)]
pub fn pid_is_alive(pid: u32) -> bool {
    let ret = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if ret == 0 {
        return true;
    }
    let err = std::io::Error::last_os_error();
    err.raw_os_error() == Some(libc::EPERM)
}

/// Send SIGTERM to the process group of `pid`, wait up to 5 seconds for a
/// graceful exit, then escalate to SIGKILL if the process is still alive.
#[cfg(unix)]
pub fn cancel_subprocess(pid: u32) {
    cancel_subprocess_with_grace(pid, std::time::Duration::from_secs(5));
}

/// Inner implementation of [`cancel_subprocess`] with a configurable grace period.
#[cfg(unix)]
pub fn cancel_subprocess_with_grace(pid: u32, grace_period: std::time::Duration) {
    if pid == 0 || pid == 1 {
        tracing::warn!("cancel_subprocess: pid {pid} is invalid, refusing to signal process group");
        return;
    }

    let ret = unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGTERM) };
    if ret != 0 {
        let err = std::io::Error::last_os_error();
        tracing::warn!("cancel_subprocess: SIGTERM to -{pid} failed: {err}");
    }

    let deadline = std::time::Instant::now() + grace_period;
    loop {
        if !pid_is_alive(pid) {
            return;
        }
        if std::time::Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    tracing::warn!(
        "cancel_subprocess: pid {pid} still alive after {}ms, sending SIGKILL",
        grace_period.as_millis()
    );
    let ret = unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGKILL) };
    if ret != 0 {
        let err = std::io::Error::last_os_error();
        tracing::warn!("cancel_subprocess: SIGKILL to -{pid} failed: {err}");
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn cancel_subprocess_pid_zero_is_noop() {
        // pid 0 means "every process in the process group" on Unix; we must not signal it.
        super::cancel_subprocess_with_grace(0, std::time::Duration::from_millis(1));
    }

    #[test]
    fn cancel_subprocess_pid_one_is_noop() {
        // pid 1 is init; we must not signal it.
        super::cancel_subprocess_with_grace(1, std::time::Duration::from_millis(1));
    }
}
