//! Delivery adapter: for a participant with a registered endpoint (Codex), submit a bounded
//! notification through its queue command. The attempt is persisted before the command starts; the
//! command runs with direct arguments (no shell), a deadline, and is killed and reaped on expiry or
//! shutdown. Submission is not receipt: only the recipient's explicit ack is.

use std::io::Read;
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::journal;
use crate::server::Shared;
use crate::store::{Delivery, now_ms};
use crate::text;

const MAX_STDERR: usize = 4096;
/// Bounds on one stderr pass, and on the final drain after the attempt ends.
const DRAIN_READS: usize = 16;
const DRAIN_BUDGET: Duration = Duration::from_millis(10);
const FINAL_DRAIN: Duration = Duration::from_millis(200);

pub fn run(shared: Arc<Shared>) {
    loop {
        if shared.shutdown.load(Ordering::SeqCst) {
            return;
        }
        let due = match shared.store.lock().map(|s| s.due_deliveries(now_ms())) {
            Ok(Ok(d)) => d,
            Ok(Err(e)) => {
                journal::log(journal::PRIORITY_ERR, &format!("adapter could not read due deliveries: {e}"), &[]);
                Vec::new()
            }
            Err(_) => return,
        };
        for d in due {
            if shared.shutdown.load(Ordering::SeqCst) {
                return;
            }
            deliver(&shared, &d);
        }
        // Wake on the next commit, or after a second to honour retry backoff deadlines.
        let head = shared.hub.head();
        shared.hub.wait_beyond(head, Duration::from_secs(1));
    }
}

fn deliver(shared: &Arc<Shared>, d: &Delivery) {
    let attempt = match shared.store.lock().map(|mut s| s.begin_attempt(d)) {
        Ok(Ok(Some(a))) => a,
        Ok(Ok(None)) => return, // acknowledged, rebound or already attempted since selection
        Ok(Err(e)) => {
            journal::log(journal::PRIORITY_ERR, &format!("could not record an attempt for {}: {e}", d.message.id), &[]);
            return;
        }
        Err(_) => return,
    };
    shared.committed();
    let fields = [("SENDER", d.message.sender.as_str()), ("RECIPIENT", d.endpoint.name.as_str()), ("MESSAGE_ID", d.message.id.as_str())];
    journal::log(
        journal::PRIORITY_INFO,
        &format!("submitting {} to {} (attempt {attempt}, generation {})", d.message.id, d.endpoint.name, d.endpoint.generation),
        &[fields[0], fields[1], fields[2], ("EVENT", "submission_started")],
    );
    let line = text::notification(&d.message, &d.endpoint.name, now_ms());
    let (state, code, error) = run_child(shared, &d.endpoint.exe, &d.endpoint.session, &line);
    let result = shared.store.lock().map(|mut s| s.finish_attempt(attempt, state, code, error.as_deref(), &shared.config.retry_backoff_ms));
    if let Ok(Err(e)) = result {
        journal::log(journal::PRIORITY_ERR, &format!("could not record attempt {attempt}'s result: {e}"), &[]);
    }
    shared.committed();
    let prio = if state == "submitted" { journal::PRIORITY_INFO } else { journal::PRIORITY_WARNING };
    journal::log(
        prio,
        &format!("submission of {} to {}: {state}{}", d.message.id, d.endpoint.name, error.map(|e| format!(" ({e})")).unwrap_or_default()),
        &[fields[0], fields[1], fields[2], ("EVENT", "submission")],
    );
}

/// (state, exit code, error). `uncertain` when the deadline expired or the daemon is stopping: the
/// command may have reached the session.
fn run_child(shared: &Arc<Shared>, exe: &str, session: &str, line: &str) -> (&'static str, Option<i32>, Option<String>) {
    let mut child = match Command::new(exe)
        .args(["queue", "--thread", session, "--message", line])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .process_group(0)
        .spawn()
    {
        Ok(c) => c,
        Err(e) => return ("failed", None, Some(format!("could not start {exe}: {e}"))),
    };
    *shared.adapter_child.lock().unwrap() = Some(child.id());
    // stderr is read without blocking inside the same loop that enforces the deadline, so a
    // descendant holding the pipe open (even one that escaped the process group) can never stall
    // the adapter: once the attempt ends, at most one short final drain happens and the pipe is
    // dropped.
    let mut stderr = child.stderr.take();
    let mut kept = Vec::new();
    if let Some(st) = &stderr {
        // SAFETY: fcntl(2) on an fd we own. Without non-blocking mode we must not read at all.
        let ok = unsafe {
            let fd = st.as_raw_fd();
            let flags = libc::fcntl(fd, libc::F_GETFL);
            flags >= 0 && libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) == 0
        };
        if !ok {
            kept.extend_from_slice(b"(stderr not captured: could not set non-blocking mode)");
            stderr = None;
        }
    }
    // One bounded pass: at most DRAIN_READS reads and DRAIN_BUDGET of wall time, then control returns
    // to the deadline, shutdown and leader-status checks even if more data is readable. Bytes past
    // MAX_STDERR are read and discarded.
    let drain = |stderr: &mut Option<std::process::ChildStderr>, kept: &mut Vec<u8>| {
        let start = Instant::now();
        let mut buf = [0u8; 4096];
        for _ in 0..DRAIN_READS {
            let Some(s) = stderr.as_mut() else { return };
            match s.read(&mut buf) {
                Ok(0) => *stderr = None,
                Ok(n) => {
                    if kept.len() < MAX_STDERR {
                        kept.extend_from_slice(&buf[..n.min(MAX_STDERR - kept.len())]);
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(_) => return, // WouldBlock (or a broken pipe): nothing more right now
            }
            if start.elapsed() >= DRAIN_BUDGET {
                return;
            }
        }
    };
    let deadline = Instant::now() + Duration::from_millis(shared.config.adapter_timeout_ms);
    let outcome = loop {
        drain(&mut stderr, &mut kept);
        match child.try_wait() {
            Ok(Some(status)) => break Ok(status),
            Ok(None) => {}
            Err(e) => break Err(format!("waiting for the adapter failed: {e}")),
        }
        if shared.shutdown.load(Ordering::SeqCst) {
            kill_group(&mut child);
            break Err("stopped by daemon shutdown".into());
        }
        if Instant::now() >= deadline {
            kill_group(&mut child);
            break Err(format!("no result within {} ms; killed", shared.config.adapter_timeout_ms));
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    // Whatever the adapter left running in its group does not outlive the attempt.
    // SAFETY: kill(2) on the adapter's own process group; harmless if the group is gone.
    unsafe { libc::kill(-(child.id() as i32), libc::SIGKILL) };
    *shared.adapter_child.lock().unwrap() = None;
    // The final drain has the same per-pass bound and a total bound.
    let until = Instant::now() + FINAL_DRAIN;
    while stderr.is_some() && Instant::now() < until {
        drain(&mut stderr, &mut kept);
        std::thread::sleep(Duration::from_millis(10));
    }
    drop(stderr);
    let stderr = String::from_utf8_lossy(&kept).into_owned();
    match outcome {
        Ok(status) if status.success() => ("submitted", Some(0), None),
        Ok(status) => ("failed", status.code(), Some(if stderr.is_empty() { format!("exit status {status}") } else { stderr })),
        Err(e) if stderr.is_empty() => ("uncertain", None, Some(e)),
        Err(e) => ("uncertain", None, Some(format!("{e}; stderr: {stderr}"))),
    }
}

/// Kills the adapter's whole process group (it was started as a group leader), so helpers it
/// spawned do not outlive it, then reaps the child.
fn kill_group(child: &mut std::process::Child) {
    // SAFETY: plain kill(2) on the child's own process group.
    unsafe { libc::kill(-(child.id() as i32), libc::SIGKILL) };
    let _ = child.kill();
    let _ = child.wait();
}
