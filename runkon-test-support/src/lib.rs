//! Test helpers (stdin probes, executable stub scripts) shared by the spawn tests in `runkon-runtimes` and `runkon-flow`.
//!
//! A probe test re-runs its own test binary with a pipe holding [`STDIN_PAYLOAD`] as stdin.
//! In that child it spawns the code under test, then checks the payload is still unread.

/// Payload the parent writes to the re-exec'd test process's stdin.
pub const STDIN_PAYLOAD: &str = "host-owned-bytes\n";
/// Env var marking the re-exec'd child half of a stdin probe test.
pub const STDIN_PROBE_ENV: &str = "RUNKON_STDIN_PROBE_CHILD";
/// Printed by the child half so the parent can tell the filter matched a test.
pub const STDIN_PROBE_SENTINEL: &str = "STDIN_PROBE_RAN";

/// Whether this process is the re-exec'd child half of a stdin probe test.
pub fn is_stdin_probe_child() -> bool {
    std::env::var_os(STDIN_PROBE_ENV).is_some()
}

/// Parent half of a stdin probe test: re-run the test binary with only `test_path`,
/// its stdin a pipe holding `STDIN_PAYLOAD`, and assert the child half passed.
pub fn run_in_child_with_piped_stdin(test_path: &str) {
    use std::io::Write;
    use std::process::Stdio;

    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([test_path, "--exact", "--nocapture", "--test-threads=1"])
        .env(STDIN_PROBE_ENV, "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(STDIN_PAYLOAD.as_bytes()).unwrap();
    drop(stdin);
    let out = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "child failed\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains(STDIN_PROBE_SENTINEL),
        "child never ran the probe (wrong test path {test_path:?}?)\nstdout: {stdout}"
    );
}

/// Child half: assert this process's stdin still holds the payload, then print the sentinel.
pub fn assert_stdin_untouched_and_report() {
    use std::io::Read;
    let mut rest = String::new();
    std::io::stdin().read_to_string(&mut rest).unwrap();
    assert_eq!(rest, STDIN_PAYLOAD, "host stdin was consumed");
    println!("{STDIN_PROBE_SENTINEL}");
}

/// Write `contents` to `path` as a 0755 executable without this process ever holding a
/// write fd on it.
///
/// A test that writes a script with `File::create` and then execs it races with other
/// test threads: a concurrent fork copies the open write fd into its child until the
/// child's own exec, and while any process holds it the kernel fails exec with ETXTBSY
/// (#115). Writing through a short-lived `sh` child keeps the write fd out of this process.
#[cfg(unix)]
pub fn write_executable_script(path: &std::path::Path, contents: &str) {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let mut child = Command::new("/bin/sh")
        .args(["-c", r#"cat > "$1" && chmod 755 "$1""#, "sh"])
        .arg(path)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn sh to write {}: {e}", path.display()));
    let mut stdin = child.stdin.take().unwrap();
    let write = stdin.write_all(contents.as_bytes());
    drop(stdin);
    let out = child.wait_with_output().unwrap();
    assert!(
        write.is_ok() && out.status.success(),
        "writing script {} failed: write={write:?} status={} stderr={}",
        path.display(),
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Write a `/bin/sh` script to `path`: prepends `#!/bin/sh\n` to `body`, then delegates to
/// [`write_executable_script`] so callers never spell the shebang themselves.
#[cfg(unix)]
pub fn write_sh_script(path: &std::path::Path, body: &str) {
    write_executable_script(path, &format!("#!/bin/sh\n{body}"));
}
