//! Participant health against the real daemon and CLI: a fake Codex sessions directory, a short check
//! interval, and file times set by hand stand in for a clock. Each test has its own state, socket and
//! notify socket; journald is disabled.

use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixDatagram;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use chatd::client::Client;
use chatd::proto::{ErrorCode, HealthState, Kind, ParticipantView, Request, Response, SendReq, StatusLine};

const DAEMON: &str = env!("CARGO_BIN_EXE_chatd");
const CLI: &str = env!("CARGO_BIN_EXE_chatctl");
static NEXT: AtomicU64 = AtomicU64::new(0);
const SESSION: &str = "00000000-0000-4000-8000-0000000c0dex";
const INTERVAL_MS: u64 = 150;

const OK_TURN: &str = r#"{"timestamp":"2026-10-19T14:02:10.500Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"t3","last_agent_message":"done","error":null}}"#;
const FILTERED_TURN: &str = r#"{"timestamp":"2026-10-19T13:16:36.687Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"t2","last_agent_message":null,"error":{"message":"flagged","codex_error_info":"cyber_policy"}}}"#;

struct Env {
    dir: PathBuf,
    daemon: Option<Child>,
}

impl Env {
    fn new(stall_ms: u64) -> Env {
        let dir = std::env::temp_dir().join(format!("chatd-health-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::SeqCst)));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("run")).unwrap();
        std::fs::create_dir_all(dir.join("sessions/2026/10/19")).unwrap();
        std::fs::write(
            dir.join("config.toml"),
            format!(
                "heartbeat_ms = 300\nadapter_timeout_ms = 1500\nhealth_interval_ms = {INTERVAL_MS}\nhealth_stall_ms = {stall_ms}\ncodex_sessions_dir = \"{}\"\n",
                dir.join("sessions").display()
            ),
        )
        .unwrap();
        Env { dir, daemon: None }
    }

    fn rollout(&self) -> PathBuf {
        self.dir.join(format!("sessions/2026/10/19/rollout-2026-10-19T07-57-27-{SESSION}.jsonl"))
    }

    fn append(&self, line: &str) {
        let mut f = std::fs::OpenOptions::new().create(true).append(true).open(self.rollout()).unwrap();
        writeln!(f, "{line}").unwrap();
    }

    fn envs(&self, cmd: &mut Command) {
        cmd.env("CHATD_STATE_DIR", self.dir.join("state"))
            .env("CHATD_SOCKET", self.dir.join("run").join("chatd.sock"))
            .env("CHATD_CONFIG", self.dir.join("config.toml"))
            .env("CHATD_NO_JOURNAL", "1")
            .env("CHATCTL_STATE_DIR", self.dir.join("ctl"))
            .env_remove("CHATD_AS")
            .env_remove("NOTIFY_SOCKET");
    }

    fn start(&mut self) {
        let path = self.dir.join(format!("notify-{}.sock", NEXT.fetch_add(1, Ordering::SeqCst)));
        let sock = UnixDatagram::bind(&path).unwrap();
        sock.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
        let mut cmd = Command::new(DAEMON);
        self.envs(&mut cmd);
        cmd.env("NOTIFY_SOCKET", &path).stdout(Stdio::null()).stderr(Stdio::null());
        self.daemon = Some(cmd.spawn().unwrap());
        let t0 = Instant::now();
        let mut buf = [0u8; 256];
        loop {
            assert!(t0.elapsed() < Duration::from_secs(15), "chatd never became ready");
            if let Ok(n) = sock.recv(&mut buf)
                && String::from_utf8_lossy(&buf[..n]).contains("READY=1")
            {
                return;
            }
            assert!(self.daemon.as_mut().unwrap().try_wait().unwrap().is_none(), "chatd exited during startup");
        }
    }

    fn term(&mut self) {
        let mut d = self.daemon.take().unwrap();
        unsafe { libc::kill(d.id() as i32, libc::SIGTERM) };
        d.wait().unwrap();
    }

    fn cli(&self, args: &[&str], stdin: &str) -> Output {
        let mut cmd = Command::new(CLI);
        self.envs(&mut cmd);
        cmd.args(args).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = cmd.spawn().unwrap();
        child.stdin.take().unwrap().write_all(stdin.as_bytes()).unwrap();
        child.wait_with_output().unwrap()
    }

    fn call(&self, req: Request) -> Response {
        Client::connect(&self.dir.join("run").join("chatd.sock"), Some(Duration::from_secs(10))).unwrap().call(req).unwrap()
    }

    fn send(&self, from: &str, to: &str, key: &str) -> Response {
        self.call(Request::Send(SendReq {
            from: from.into(),
            to: Some(to.into()),
            conversation: Some("review".into()),
            kind: Kind::Request,
            reply_to: None,
            idempotency_key: key.into(),
            summary: None,
            body: "please review".into(),
        }))
    }

    /// Binds codex to SESSION with an adapter that accepts every submission.
    fn register_codex(&self) {
        let exe = self.dir.join("fake-codex");
        std::fs::write(&exe, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
        let r = self.call(Request::EndpointRegister { name: "codex".into(), session: SESSION.into(), exe: exe.to_string_lossy().into() });
        assert!(matches!(r, Response::Registered(_)), "{r:?}");
    }

    fn status(&self, who: Option<&str>) -> chatd::proto::StatusView {
        match self.call(Request::Status {
            as_: who.map(str::to_string),
            conversation: None,
            limit: 200,
            unacknowledged_after: None,
            unanswered_after: None,
        }) {
            Response::Status(s) => s,
            other => panic!("{other:?}"),
        }
    }

    /// The daemon's notices in claude's inbox, oldest first.
    fn notices(&self) -> Vec<StatusLine> {
        self.status(Some("claude")).unacknowledged.into_iter().filter(|l| l.sender == "chatd").collect()
    }

    fn codex(&self) -> ParticipantView {
        self.status(None).participants.unwrap().into_iter().find(|p| p.name == "codex").unwrap()
    }

    fn codex_health(&self) -> Option<HealthState> {
        self.codex().health.map(|h| h.state)
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        if let Some(mut d) = self.daemon.take() {
            let _ = d.kill();
            let _ = d.wait();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn wait_for(what: &str, mut f: impl FnMut() -> bool) {
    let t0 = Instant::now();
    while !f() {
        assert!(t0.elapsed() < Duration::from_secs(10), "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Several check intervals: long enough for a repeat to have appeared if there were one.
fn several_checks() {
    std::thread::sleep(Duration::from_millis(INTERVAL_MS * 6));
}

fn set_mtime(path: &Path, ago: Duration) {
    let f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    f.set_modified(SystemTime::now() - ago).unwrap();
}

#[test]
fn a_filtered_turn_is_reported_once_recovery_once_and_not_again_after_a_restart() {
    let mut e = Env::new(30 * 60_000);
    e.append(OK_TURN);
    e.start();
    e.register_codex();
    wait_for("the first check", || e.codex_health() == Some(HealthState::Ok));
    several_checks();
    assert!(e.notices().is_empty(), "a first OK is not news");

    e.append(FILTERED_TURN);
    wait_for("the FILTERED notice", || e.notices().len() == 1);
    let n = &e.notices()[0];
    assert_eq!(n.summary, "codex FILTERED (cyber_policy) since 2026-10-19T13:16:36Z");
    assert_eq!((n.kind, n.conversation.as_str(), n.recipient.as_str()), (Kind::Info, "health", "claude"));
    let Response::Message(m) = e.call(Request::Receive { as_: "claude".into(), id: n.id.clone() }) else { panic!() };
    assert!(m.body.unwrap().contains("content filter"));
    several_checks();
    assert_eq!(e.notices().len(), 1, "an unchanged state is not repeated");
    let h = e.codex().health.unwrap();
    assert_eq!((h.state, h.detail.as_deref()), (HealthState::Filtered, Some("cyber_policy")));

    // The send warning: delivered anyway, the sender is told.
    let Response::Stored(r) = e.send("claude", "codex", "k1") else { panic!() };
    let w = r.warning.expect("a warning for a filtered recipient");
    assert!(w.starts_with("delivered, but codex's last turn ended with cyber_policy"), "{w}");
    let out = e.cli(&["send", "--as", "claude", "--to", "codex", "--conversation", "review"], "second look\n");
    assert_eq!(out.status.code(), Some(0), "never blocks delivery");
    assert!(String::from_utf8_lossy(&out.stderr).contains("warning: delivered, but codex's last turn ended with cyber_policy"));
    let Response::Stored(r) = e.send("codex", "claude", "k2") else { panic!() };
    assert_eq!(r.warning, None, "a healthy (here: uninspected) recipient gets no warning");

    // peers and status show the column, in text and JSON.
    let peers = String::from_utf8_lossy(&e.cli(&["peers"], "").stdout).into_owned();
    assert!(peers.contains("HEALTH") && peers.contains("FILTERED (cyber_policy)"), "{peers}");
    let json: serde_json::Value = serde_json::from_slice(&e.cli(&["--json", "peers"], "").stdout).unwrap();
    let codex = json.as_array().unwrap().iter().find(|p| p["name"] == "codex").unwrap();
    assert_eq!((codex["health"]["state"].as_str(), codex["health"]["detail"].as_str()), (Some("filtered"), Some("cyber_policy")));
    let status = String::from_utf8_lossy(&e.cli(&["status"], "").stdout).into_owned();
    assert!(status.contains("health:") && status.contains("FILTERED (cyber_policy)"), "{status}");

    e.append(OK_TURN);
    wait_for("the recovery notice", || e.notices().len() == 2);
    assert_eq!(e.notices()[1].summary.split(" since ").next(), Some("codex OK"));
    assert!(e.notices()[1].summary.ends_with("(was FILTERED (cyber_policy))"), "{}", e.notices()[1].summary);
    several_checks();
    assert_eq!(e.notices().len(), 2, "recovery is reported once");

    e.term();
    e.start();
    wait_for("a check after the restart", || e.codex_health() == Some(HealthState::Ok));
    several_checks();
    assert_eq!(e.notices().len(), 2, "the last reported state survives a restart");
}

#[test]
fn a_silent_log_with_unanswered_requests_is_stalled() {
    let mut e = Env::new(60_000);
    e.append(OK_TURN);
    set_mtime(&e.rollout(), Duration::from_secs(3600));
    e.start();
    e.register_codex();
    wait_for("the first check", || e.codex_health() == Some(HealthState::Ok));
    several_checks();
    assert!(e.notices().is_empty(), "quiet with nothing to answer is fine");

    let Response::Stored(r) = e.send("claude", "codex", "k") else { panic!() };
    assert_eq!(r.warning, None, "only FILTERED and ERRORED warn");
    wait_for("the STALLED notice", || e.notices().len() == 1);
    assert!(e.notices()[0].summary.starts_with("codex STALLED since "), "{}", e.notices()[0].summary);
    several_checks();
    assert_eq!(e.notices().len(), 1);

    e.append(OK_TURN);
    wait_for("the recovery notice", || e.notices().len() == 2);
    assert!(e.notices()[1].summary.ends_with("(was STALLED)"));
}

#[test]
fn a_session_without_a_log_is_unknown_and_the_system_sender_is_reserved() {
    let mut e = Env::new(30 * 60_000);
    e.start();
    assert_eq!(e.codex().health, None, "no endpoint, no session to inspect");
    e.register_codex();
    wait_for("the UNKNOWN notice", || e.notices().len() == 1);
    assert!(e.notices()[0].summary.starts_with("codex UNKNOWN since "));
    assert!(!e.status(None).participants.unwrap().iter().any(|p| p.name == "chatd"), "the system sender is not a peer");
    for (from, to) in [("chatd", "codex"), ("claude", "chatd")] {
        match e.send(from, to, "x") {
            Response::Error { code, message } => assert!(code == ErrorCode::BadRequest && message.contains("reserved"), "{message}"),
            other => panic!("{other:?}"),
        }
    }
}

#[test]
fn a_notice_to_a_push_participant_waits_for_its_next_wake() {
    let mut e = Env::new(30 * 60_000);
    let mut config = std::fs::read_to_string(e.dir.join("config.toml")).unwrap();
    config.insert_str(0, "participants = [\"claude\", \"codex\", \"codex2\"]\n");
    std::fs::write(e.dir.join("config.toml"), config).unwrap();
    e.append(OK_TURN);
    e.start();
    let log = e.dir.join("codex2-argv.log");
    let exe = e.dir.join("fake-codex2");
    std::fs::write(&exe, format!("#!/bin/sh\nprintf '%s\\n' \"$@\" >> {}\n", log.display())).unwrap();
    std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
    let r = e.call(Request::EndpointRegister { name: "codex2".into(), session: "s2".into(), exe: exe.to_string_lossy().into() });
    assert!(matches!(r, Response::Registered(_)), "{r:?}");
    e.register_codex();
    wait_for("the first check", || e.codex_health() == Some(HealthState::Ok));

    e.append(FILTERED_TURN);
    wait_for("the FILTERED notice to claude", || e.notices().len() == 1);
    let queued = || e.status(None).participants.unwrap().into_iter().find(|p| p.name == "codex2").unwrap().queued_for_wake;
    wait_for("the notice queued for codex2", || queued() == Some(1));
    several_checks();
    assert!(!log.exists(), "a health notice (kind info) does not wake a push participant by default");

    let Response::Stored(req) = e.send("claude", "codex2", "wake") else { panic!() };
    wait_for("the waking push", || std::fs::read_to_string(&log).is_ok_and(|t| t.contains("codex FILTERED (cyber_policy)")));
    let argv = std::fs::read_to_string(&log).unwrap();
    assert!(argv.contains(&format!("chatctl receive --as codex2 --id {}", req.id)));
    assert!(argv.contains("[chatd digest for codex2: 1 queued"), "{argv}");
    assert!(argv.contains(" chatd health info "), "{argv}");
    wait_for("the digest recorded", || queued() == Some(0));
}

#[test]
fn unregistering_stops_the_inspection_at_once() {
    let mut e = Env::new(30 * 60_000);
    e.append(OK_TURN);
    e.start();
    e.register_codex();
    wait_for("the first check", || e.codex_health() == Some(HealthState::Ok));
    assert!(matches!(e.call(Request::EndpointUnregister { name: "codex".into() }), Response::Unregistered { removed: Some(_), .. }));
    assert_eq!(e.codex().health, None, "health is cleared with the endpoint, not at the next check");
    e.append(FILTERED_TURN);
    several_checks();
    assert_eq!(e.codex().health, None, "the session log is no longer read");
    assert!(e.notices().is_empty(), "nothing is reported about an unregistered session");
    let peers = String::from_utf8_lossy(&e.cli(&["peers"], "").stdout).into_owned();
    let row = peers.lines().find(|l| l.starts_with("codex")).unwrap();
    assert!(row.contains("pull") && !row.contains("OK") && !row.contains("FILTERED"), "{peers}");
}
