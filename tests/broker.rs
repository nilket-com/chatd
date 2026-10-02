//! End-to-end tests against the real daemon and CLI binaries, each in its own temporary state,
//! socket and notify socket. Journald is disabled (CHATD_NO_JOURNAL) so records go to stderr.

use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixDatagram, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use chatd::client::Client;
use chatd::proto::{ErrorCode, Kind, Request, Response, SendReq};

const DAEMON: &str = env!("CARGO_BIN_EXE_chatd");
const CLI: &str = env!("CARGO_BIN_EXE_chatctl");
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Env {
    dir: PathBuf,
    daemon: Option<Child>,
}

impl Env {
    fn new(config: &str) -> Env {
        let dir = std::env::temp_dir().join(format!("chatd-test-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::SeqCst)));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("run")).unwrap();
        std::fs::write(dir.join("config.toml"), config).unwrap();
        Env { dir, daemon: None }
    }

    fn standard() -> Env {
        Env::new("heartbeat_ms = 300\nadapter_timeout_ms = 1500\nretry_backoff_ms = [200, 200, 200]\n")
    }

    fn socket(&self) -> PathBuf {
        self.dir.join("run").join("chatd.sock")
    }

    fn state(&self) -> PathBuf {
        self.dir.join("state")
    }

    fn envs(&self, cmd: &mut Command) {
        cmd.env("CHATD_STATE_DIR", self.state())
            .env("CHATD_SOCKET", self.socket())
            .env("CHATD_CONFIG", self.dir.join("config.toml"))
            .env("CHATD_NO_JOURNAL", "1")
            .env_remove("NOTIFY_SOCKET");
    }

    /// Starts the daemon and returns once it has sent READY=1 on a test notify socket.
    fn start(&mut self) -> Duration {
        self.try_start(false).expect("daemon became ready")
    }

    fn try_start(&mut self, abstract_socket: bool) -> Option<Duration> {
        let path = self.dir.join(format!("notify-{}.sock", NEXT.fetch_add(1, Ordering::SeqCst)));
        let (sock, target) = if abstract_socket {
            use std::os::linux::net::SocketAddrExt;
            let name = format!("chatd-test-{}", NEXT.fetch_add(1, Ordering::SeqCst));
            let addr = std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes()).unwrap();
            (UnixDatagram::bind_addr(&addr).unwrap(), format!("@{name}"))
        } else {
            (UnixDatagram::bind(&path).unwrap(), path.to_string_lossy().into_owned())
        };
        sock.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
        let mut cmd = Command::new(DAEMON);
        self.envs(&mut cmd);
        cmd.env("NOTIFY_SOCKET", &target).stdout(Stdio::null()).stderr(Stdio::piped());
        let t0 = Instant::now();
        self.daemon = Some(cmd.spawn().unwrap());
        let mut buf = [0u8; 256];
        while t0.elapsed() < Duration::from_secs(15) {
            if let Ok(n) = sock.recv(&mut buf)
                && String::from_utf8_lossy(&buf[..n]).contains("READY=1")
            {
                return Some(t0.elapsed());
            }
            if let Ok(Some(_)) = self.daemon.as_mut().unwrap().try_wait() {
                return None;
            }
        }
        None
    }

    fn kill9(&mut self) {
        if let Some(mut d) = self.daemon.take() {
            let _ = d.kill();
            let _ = d.wait();
        }
    }

    fn term(&mut self) -> std::process::ExitStatus {
        let mut d = self.daemon.take().unwrap();
        unsafe { libc::kill(d.id() as i32, libc::SIGTERM) };
        d.wait().unwrap()
    }

    fn cli(&self, args: &[&str], stdin: Option<&str>) -> Output {
        let mut cmd = Command::new(CLI);
        self.envs(&mut cmd);
        cmd.args(args).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = cmd.spawn().unwrap();
        if let Some(s) = stdin {
            child.stdin.take().unwrap().write_all(s.as_bytes()).unwrap();
        } else {
            drop(child.stdin.take());
        }
        child.wait_with_output().unwrap()
    }

    fn client(&self) -> Client {
        Client::connect(&self.socket(), Some(Duration::from_secs(10))).unwrap()
    }

    fn call(&self, req: Request) -> Response {
        self.client().call(req).unwrap()
    }

    fn send(&self, from: &str, to: &str, conv: &str, kind: Kind, key: &str, body: &str) -> String {
        match self.call(Request::Send(SendReq {
            from: from.into(),
            to: Some(to.into()),
            conversation: Some(conv.into()),
            kind,
            reply_to: None,
            idempotency_key: key.into(),
            summary: None,
            body: body.into(),
        })) {
            Response::Stored(r) => r.id,
            other => panic!("send failed: {other:?}"),
        }
    }

    fn reply(&self, from: &str, to: &str, kind: Kind, key: &str) -> Response {
        self.call(Request::Send(SendReq {
            from: from.into(),
            to: None,
            conversation: None,
            kind,
            reply_to: Some(to.into()),
            idempotency_key: key.into(),
            summary: None,
            body: format!("{} reply", kind.as_str()),
        }))
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
}

impl Drop for Env {
    fn drop(&mut self) {
        self.kill9();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn code(o: &Output) -> i32 {
    o.status.code().unwrap_or(-1)
}

// ---------------------------------------------------------------- readiness and startup

#[test]
fn readiness_is_sent_only_with_a_serving_socket_and_health() {
    let mut e = Env::standard();
    e.start();
    match e.call(Request::Health) {
        Response::Health { schema_version, head_seq, .. } => {
            assert_eq!(schema_version, 1);
            assert!(head_seq >= 0);
        }
        other => panic!("{other:?}"),
    }
    let mode = std::fs::metadata(e.socket()).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
    let mode = std::fs::metadata(e.state()).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o700);
}

#[test]
fn readiness_over_an_abstract_notify_socket() {
    let mut e = Env::standard();
    assert!(e.try_start(true).is_some());
    assert!(matches!(e.call(Request::Health), Response::Health { .. }));
}

#[test]
fn an_unwritable_state_never_advertises_readiness() {
    let mut e = Env::standard();
    std::fs::write(e.state(), b"a file where the state directory should be").unwrap();
    assert!(e.try_start(false).is_none(), "readiness must not be sent");
    let status = e.daemon.as_mut().unwrap().wait().unwrap();
    assert!(!status.success());
}

#[test]
fn a_newer_schema_refuses_startup_and_preserves_the_database() {
    let mut e = Env::standard();
    e.start();
    e.send("claude", "codex", "c", Kind::Info, "k1", "kept");
    e.kill9();
    let db = e.state().join("chat.db");
    {
        let c = rusqlite::Connection::open(&db).unwrap();
        c.execute("UPDATE meta SET value = '99' WHERE key = 'schema_version'", []).unwrap();
        c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    let before = std::fs::read(&db).unwrap();
    assert!(e.try_start(false).is_none());
    assert!(!e.daemon.as_mut().unwrap().wait().unwrap().success());
    assert_eq!(std::fs::read(&db).unwrap(), before, "a refused migration leaves the database untouched");
}

#[test]
fn a_second_daemon_refuses_a_served_socket() {
    let mut e = Env::standard();
    e.start();
    let mut cmd = Command::new(DAEMON);
    e.envs(&mut cmd);
    let out = cmd.stderr(Stdio::piped()).output().unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("another daemon"));
}

#[test]
fn the_cli_fails_fast_when_the_daemon_is_down_and_creates_nothing() {
    let e = Env::standard();
    let t0 = Instant::now();
    let out = e.cli(&["status"], None);
    assert_eq!(code(&out), 3);
    assert!(t0.elapsed() < Duration::from_secs(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("systemctl --user status chatd"));
    assert!(!e.state().exists() && !e.socket().exists(), "no second inbox or socket is created");
    let out = e.cli(&["send", "--as", "claude", "--to", "codex", "--conversation", "c", "--idempotency-key", "k"], Some("x"));
    assert_eq!(code(&out), 3);
}

// ---------------------------------------------------------------- messages, keys, replies

#[test]
fn send_receive_ack_and_status() {
    let mut e = Env::standard();
    e.start();
    let key = stdout(&e.cli(&["new-key"], None)).trim().to_string();
    let out = e.cli(
        &["--json", "send", "--as", "claude", "--to", "codex", "--conversation", "adr628", "--idempotency-key", &key],
        Some("Please review\nthe details"),
    );
    assert_eq!(code(&out), 0, "{}", String::from_utf8_lossy(&out.stderr));
    let receipt: serde_json::Value = serde_json::from_str(&stdout(&out)).unwrap();
    let id = receipt["id"].as_str().unwrap().to_string();
    let m = stdout(&e.cli(&["--json", "receive", "--as", "codex", "--id", &id], None));
    let m: serde_json::Value = serde_json::from_str(&m).unwrap();
    assert_eq!(m["body"], "Please review\nthe details");
    assert_eq!(m["summary"], "Please review");
    assert!(m["received_at_ms"].is_null(), "receive does not acknowledge");
    let s = e.status(Some("codex"));
    assert_eq!(s.unacknowledged.len(), 1);
    assert_eq!(s.unanswered.len(), 1);
    assert_eq!(code(&e.cli(&["ack", "--as", "codex", &id], None)), 0);
    let s = e.status(Some("codex"));
    assert!(s.unacknowledged.is_empty());
    assert_eq!(s.unanswered.len(), 1, "acknowledgement is not an answer");
    assert!(s.unanswered[0].received);
    // Only the recipient acknowledges.
    assert_eq!(code(&e.cli(&["ack", "--as", "claude", &id], None)), 5);
}

#[test]
fn identical_retries_return_the_original_and_conflicts_refuse() {
    let mut e = Env::standard();
    e.start();
    let a = e.send("claude", "codex", "c", Kind::Request, "key-1", "same");
    let b = e.send("claude", "codex", "c", Kind::Request, "key-1", "same");
    assert_eq!(a, b);
    let r = e.call(Request::Send(SendReq {
        from: "claude".into(),
        to: Some("codex".into()),
        conversation: Some("c".into()),
        kind: Kind::Request,
        reply_to: None,
        idempotency_key: "key-1".into(),
        summary: None,
        body: "different".into(),
    }));
    assert!(matches!(r, Response::Error { code: ErrorCode::Conflict, .. }), "{r:?}");
    match e.call(Request::Lookup { sender: "claude".into(), idempotency_key: "key-1".into() }) {
        Response::Found(r) => assert_eq!(r.id, a),
        other => panic!("{other:?}"),
    }
    // Distinct keys keep intentionally identical messages distinct.
    let c = e.send("claude", "codex", "c", Kind::Request, "key-2", "same");
    assert_ne!(a, c);
    // The same key under another sender is independent.
    let d = e.send("codex", "claude", "c", Kind::Request, "key-1", "same");
    assert_ne!(a, d);
}

#[test]
fn replies_are_validated_and_only_a_final_reply_answers() {
    let mut e = Env::standard();
    e.start();
    let req = e.send("claude", "codex", "adr628", Kind::Request, "r1", "question");
    let info = e.send("claude", "codex", "adr628", Kind::Info, "i1", "fyi");
    // The requester cannot reply to its own request; info cannot be replied to.
    assert!(matches!(e.reply("claude", &req, Kind::Final, "x1"), Response::Error { code: ErrorCode::InvalidReply, .. }));
    assert!(matches!(e.reply("codex", &info, Kind::Final, "x2"), Response::Error { code: ErrorCode::InvalidReply, .. }));
    assert!(matches!(
        e.reply("codex", "01990000-0000-7000-8000-000000000000", Kind::Final, "x3"),
        Response::Error { code: ErrorCode::InvalidReply, .. }
    ));
    let wrong_conv = e.call(Request::Send(SendReq {
        from: "codex".into(),
        to: None,
        conversation: Some("other".into()),
        kind: Kind::Final,
        reply_to: Some(req.clone()),
        idempotency_key: "x4".into(),
        summary: None,
        body: "b".into(),
    }));
    assert!(matches!(wrong_conv, Response::Error { code: ErrorCode::InvalidReply, .. }));
    // Progress acknowledges receipt but leaves the request unanswered.
    assert!(matches!(e.reply("codex", &req, Kind::Progress, "p1"), Response::Stored(_)));
    let s = e.status(Some("claude"));
    assert!(s.unanswered.iter().any(|l| l.id == req && l.received && l.answered_by.is_none()));
    let Response::Stored(fin) = e.reply("codex", &req, Kind::Final, "f1") else { panic!() };
    let s = e.status(Some("claude"));
    assert!(!s.unanswered.iter().any(|l| l.id == req));
    let Response::Message(m) = e.call(Request::Receive { as_: "claude".into(), id: fin.id.clone() }) else { panic!() };
    assert_eq!((m.sender.as_str(), m.recipient.as_str(), m.conversation.as_str()), ("codex", "claude", "adr628"));
    let Response::Message(orig) = e.call(Request::Receive { as_: "claude".into(), id: req.clone() }) else { panic!() };
    assert_eq!(orig.answered_by.as_deref(), Some(fin.id.as_str()));
    // Information never appears as unanswered.
    assert!(!s.unanswered.iter().any(|l| l.id == info));
}

#[test]
fn unknown_participants_and_bad_input_are_refused() {
    let mut e = Env::standard();
    e.start();
    let r = e.call(Request::Send(SendReq {
        from: "mallory".into(),
        to: Some("codex".into()),
        conversation: Some("c".into()),
        kind: Kind::Request,
        reply_to: None,
        idempotency_key: "k".into(),
        summary: None,
        body: "b".into(),
    }));
    assert!(matches!(r, Response::Error { code: ErrorCode::UnknownParticipant, .. }));
    let r = e.call(Request::Send(SendReq {
        from: "claude".into(),
        to: Some("codex".into()),
        conversation: Some("c".into()),
        kind: Kind::Request,
        reply_to: None,
        idempotency_key: "has space".into(),
        summary: None,
        body: "b".into(),
    }));
    assert!(matches!(r, Response::Error { code: ErrorCode::BadRequest, .. }));
}

#[test]
fn oversized_bodies_and_frames_are_refused_and_the_daemon_survives() {
    let mut e = Env::standard();
    e.start();
    let big = "x".repeat(chatd::proto::MAX_BODY + 1);
    let r = e.call(Request::Send(SendReq {
        from: "claude".into(),
        to: Some("codex".into()),
        conversation: Some("c".into()),
        kind: Kind::Request,
        reply_to: None,
        idempotency_key: "big".into(),
        summary: None,
        body: big,
    }));
    assert!(matches!(r, Response::Error { code: ErrorCode::TooLarge, .. }), "{r:?}");
    // A declared 4 GiB frame is refused from its length prefix alone.
    let mut raw = UnixStream::connect(e.socket()).unwrap();
    raw.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    raw.write_all(&u32::MAX.to_be_bytes()).unwrap();
    let mut reply = Vec::new();
    let _ = raw.read_to_end(&mut reply);
    assert!(String::from_utf8_lossy(&reply).contains("exceeds"), "{:?}", String::from_utf8_lossy(&reply));
    // Garbage JSON is refused too.
    let mut raw = UnixStream::connect(e.socket()).unwrap();
    raw.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    raw.write_all(&5u32.to_be_bytes()).unwrap();
    raw.write_all(b"nope!").unwrap();
    let mut reply = Vec::new();
    let _ = raw.read_to_end(&mut reply);
    assert!(String::from_utf8_lossy(&reply).contains("bad_request"));
    assert!(matches!(e.call(Request::Health), Response::Health { .. }));
}

#[test]
fn a_protocol_version_mismatch_is_refused() {
    let mut e = Env::standard();
    e.start();
    let mut raw = UnixStream::connect(e.socket()).unwrap();
    raw.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let frame = serde_json::to_vec(&serde_json::json!({"v": 999, "req": {"op": "health"}})).unwrap();
    raw.write_all(&(frame.len() as u32).to_be_bytes()).unwrap();
    raw.write_all(&frame).unwrap();
    let mut reply = Vec::new();
    let _ = raw.read_to_end(&mut reply);
    assert!(String::from_utf8_lossy(&reply).contains("version_mismatch"));
}

#[test]
fn concurrent_senders_get_distinct_ordered_receipts() {
    let mut e = Env::standard();
    e.start();
    let socket = e.socket();
    let handles: Vec<_> = (0..8)
        .map(|t| {
            let socket = socket.clone();
            std::thread::spawn(move || {
                let mut ids = Vec::new();
                for i in 0..25 {
                    let mut c = Client::connect(&socket, Some(Duration::from_secs(10))).unwrap();
                    let r = c
                        .call(Request::Send(SendReq {
                            from: "claude".into(),
                            to: Some("codex".into()),
                            conversation: Some("load".into()),
                            kind: Kind::Info,
                            reply_to: None,
                            idempotency_key: format!("t{t}-{i}"),
                            summary: None,
                            body: format!("{t}/{i}"),
                        }))
                        .unwrap();
                    let Response::Stored(r) = r else { panic!("{r:?}") };
                    ids.push((r.seq, r.id));
                }
                ids
            })
        })
        .collect();
    let mut all: Vec<(i64, String)> = handles.into_iter().flat_map(|h| h.join().unwrap()).collect();
    all.sort();
    let seqs: std::collections::BTreeSet<i64> = all.iter().map(|x| x.0).collect();
    let ids: std::collections::BTreeSet<&String> = all.iter().map(|x| &x.1).collect();
    assert_eq!((seqs.len(), ids.len()), (200, 200));
}

// ---------------------------------------------------------------- acknowledgement and replay

#[test]
fn acknowledgement_holes_are_kept() {
    let mut e = Env::standard();
    e.start();
    let a = e.send("codex", "claude", "c", Kind::Info, "a", "first");
    let b = e.send("codex", "claude", "c", Kind::Info, "b", "second");
    let c = e.send("codex", "claude", "c", Kind::Info, "c", "third");
    assert_eq!(code(&e.cli(&["ack", "--as", "claude", &c], None)), 0);
    let s = e.status(Some("claude"));
    let ids: Vec<&str> = s.unacknowledged.iter().map(|l| l.id.as_str()).collect();
    assert_eq!(ids, vec![a.as_str(), b.as_str()]);
    let out = e.cli(&["wait", "--as", "claude", "--timeout", "5"], None);
    assert_eq!(code(&out), 0);
    assert!(stdout(&out).contains(&a), "the oldest hole is replayed first: {}", stdout(&out));
}

#[test]
fn a_message_sent_while_no_waiter_exists_is_delivered_on_the_next_wait() {
    let mut e = Env::standard();
    e.start();
    let id = e.send("codex", "claude", "adr628", Kind::Request, "gap", "arrived during the monitor gap");
    let t0 = Instant::now();
    let out = e.cli(&["wait", "--as", "claude", "--timeout", "30"], None);
    assert_eq!(code(&out), 0);
    assert!(t0.elapsed() < Duration::from_secs(3));
    let line = stdout(&out);
    assert!(line.contains(&id) && line.contains(&format!("chatctl receive --as claude --id {id}")), "{line}");
    assert_eq!(line.lines().count(), 1);
}

#[test]
fn a_printed_but_unacknowledged_message_is_replayed() {
    let mut e = Env::standard();
    e.start();
    let id = e.send("codex", "claude", "c", Kind::Request, "p", "printed once");
    let first = stdout(&e.cli(&["wait", "--as", "claude", "--timeout", "5"], None));
    let second = stdout(&e.cli(&["wait", "--as", "claude", "--timeout", "5"], None));
    assert!(first.contains(&id) && second.contains(&id));
    e.cli(&["ack", "--as", "claude", &id], None);
    let third = e.cli(&["wait", "--as", "claude", "--timeout", "1"], None);
    assert_eq!(code(&third), 6, "nothing left after the ack: {}", stdout(&third));
}

#[test]
fn a_blocked_wait_is_woken_by_a_push() {
    let mut e = Env::standard();
    e.start();
    let mut cmd = Command::new(CLI);
    e.envs(&mut cmd);
    let waiter = cmd.args(["wait", "--as", "claude", "--timeout", "30"]).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    std::thread::sleep(Duration::from_millis(500));
    let t0 = Instant::now();
    let id = e.send("codex", "claude", "c", Kind::Request, "push", "wake up");
    let out = waiter.wait_with_output().unwrap();
    assert_eq!(code(&out), 0);
    assert!(stdout(&out).contains(&id));
    assert!(t0.elapsed() < Duration::from_secs(2), "pushed, not polled: {:?}", t0.elapsed());
}

#[test]
fn no_arrival_is_lost_between_the_inbox_check_and_the_subscription() {
    let mut e = Env::standard();
    e.start();
    for i in 0..25 {
        let mut cmd = Command::new(CLI);
        e.envs(&mut cmd);
        let waiter = cmd.args(["wait", "--as", "claude", "--timeout", "10"]).stdout(Stdio::piped()).stderr(Stdio::null()).spawn().unwrap();
        let id = e.send("codex", "claude", "race", Kind::Info, &format!("race-{i}"), "racing");
        let out = waiter.wait_with_output().unwrap();
        assert_eq!(code(&out), 0, "iteration {i}");
        assert!(stdout(&out).contains(&id), "iteration {i}: {}", stdout(&out));
        e.call(Request::Ack { as_: "claude".into(), ids: vec![id] });
    }
}

#[test]
fn follow_emits_each_id_once_per_connection_and_replays_after_reconnect() {
    let mut e = Env::standard();
    e.start();
    let mut cmd = Command::new(CLI);
    e.envs(&mut cmd);
    let follower =
        cmd.args(["wait", "--as", "claude", "--follow", "--timeout", "3"]).stdout(Stdio::piped()).stderr(Stdio::null()).spawn().unwrap();
    std::thread::sleep(Duration::from_millis(400));
    let ids: Vec<String> =
        (0..5).map(|i| e.send("codex", "claude", "burst", Kind::Info, &format!("b{i}"), &format!("burst {i}"))).collect();
    let out = follower.wait_with_output().unwrap();
    assert_eq!(code(&out), 6, "follow ends at its timeout");
    let lines: Vec<String> = stdout(&out).lines().map(str::to_string).collect();
    assert_eq!(lines.len(), 5, "{lines:?}");
    for id in &ids {
        assert_eq!(lines.iter().filter(|l| l.contains(id.as_str())).count(), 1);
    }
    let again = stdout(&e.cli(&["wait", "--as", "claude", "--follow", "--timeout", "1"], None));
    assert_eq!(again.lines().count(), 5, "a new connection replays all unacknowledged");
    for l in again.lines() {
        assert!(l.len() <= chatd::text::MAX_NOTIFICATION_BYTES);
    }
}

#[test]
fn notifications_neutralise_control_characters_and_stay_bounded() {
    let mut e = Env::standard();
    e.start();
    let nasty = format!("\x1b[2J\x1b]0;title\x07{}\nsecond line", "長".repeat(400));
    let id = e.send("codex", "claude", "c", Kind::Request, "nasty", &nasty);
    let line = stdout(&e.cli(&["wait", "--as", "claude", "--timeout", "5"], None));
    let line = line.trim_end_matches('\n');
    assert!(!line.chars().any(|c| c.is_control()), "{line:?}");
    assert!(line.len() <= chatd::text::MAX_NOTIFICATION_BYTES);
    assert!(line.ends_with(&format!("chatctl receive --as claude --id {id}")));
}

// ---------------------------------------------------------------- watch / resume

struct Snap {
    head_seq: i64,
    resume_token: String,
    resynchronized: Option<String>,
    unacknowledged: Vec<chatd::proto::MessageView>,
    total: i64,
    pages: u64,
}

fn read_snapshot(c: &mut Client) -> Snap {
    let Response::SnapshotBegin { head_seq, resume_token, unacknowledged_total, resynchronized } = c.recv().unwrap() else {
        panic!("no SnapshotBegin")
    };
    let mut unacknowledged = Vec::new();
    let mut seen_pages = 0;
    loop {
        match c.recv().unwrap() {
            Response::SnapshotPage { unacknowledged: page } => {
                assert!(page.len() <= chatd::proto::MAX_PAGE as usize);
                unacknowledged.extend(page);
                seen_pages += 1;
            }
            Response::SnapshotEnd { head_seq: h, pages } => {
                assert_eq!((h, pages), (head_seq, seen_pages));
                return Snap { head_seq, resume_token, resynchronized, unacknowledged, total: unacknowledged_total, pages };
            }
            other => panic!("{other:?}"),
        }
    }
}

fn open_watch(e: &Env, who: &str, token: Option<String>) -> (Client, Snap) {
    let mut c = Client::connect(&e.socket(), Some(Duration::from_secs(5))).unwrap();
    c.send(Request::Watch { as_: who.into(), conversation: None, resume_token: token }).unwrap();
    let snap = read_snapshot(&mut c);
    (c, snap)
}

fn next_event(c: &mut Client) -> (chatd::proto::EventView, String) {
    loop {
        match c.recv().unwrap() {
            Response::Event { event, resume_token } => return (event, resume_token),
            Response::Heartbeat { .. } => continue,
            other => panic!("{other:?}"),
        }
    }
}

#[test]
fn watch_streams_events_and_resumes_by_token() {
    let mut e = Env::standard();
    e.start();
    let (mut c, snap) = open_watch(&e, "claude", None);
    assert!(snap.unacknowledged.is_empty() && snap.resynchronized.is_none());
    let first = e.send("codex", "claude", "w", Kind::Request, "w1", "one");
    let (ev, token) = next_event(&mut c);
    assert_eq!((ev.kind.as_str(), ev.message_id.as_deref()), ("message_stored", Some(first.as_str())));
    drop(c);
    let second = e.send("codex", "claude", "w", Kind::Request, "w2", "two");
    let third = e.send("codex", "claude", "w", Kind::Request, "w3", "three");
    let (mut c, snap) = open_watch(&e, "claude", Some(token));
    assert!(snap.resynchronized.is_none());
    // Event position is not acknowledgement: all three are still in the inbox snapshot.
    assert_eq!(snap.unacknowledged.iter().map(|m| m.id.clone()).collect::<Vec<_>>(), vec![first.clone(), second.clone(), third.clone()]);
    let (a, _) = next_event(&mut c);
    let (b, _) = next_event(&mut c);
    assert_eq!((a.message_id.unwrap(), b.message_id.unwrap()), (second.clone(), third.clone()), "exactly the events after the token");
    // Heartbeats show a live, quiet connection.
    assert!(matches!(c.recv().unwrap(), Response::Heartbeat { .. }));
    // The sender's watch sees receipt and answer events for its message.
    let (mut s, _) = open_watch(&e, "codex", None);
    e.call(Request::Ack { as_: "claude".into(), ids: vec![first.clone()] });
    let (ev, _) = next_event(&mut s);
    assert_eq!((ev.kind.as_str(), ev.message_id.as_deref()), ("received", Some(first.as_str())));
}

#[test]
fn invalid_foreign_and_future_tokens_require_resynchronization() {
    let mut e = Env::standard();
    e.start();
    e.send("codex", "claude", "w", Kind::Info, "k", "x");
    let (_, snap) = open_watch(&e, "claude", None);
    let (resume_token, head_seq) = (snap.resume_token, snap.head_seq);
    let parts: Vec<&str> = resume_token.split('.').collect();
    let future = format!("{}.{}.{}.{}.{}", parts[0], parts[1], parts[2], parts[3], head_seq + 1000);
    let foreign_store = format!("{}.{}.{}.{}.{}", parts[0], "00000000-0000-4000-8000-000000000000", parts[2], parts[3], 0);
    for (token, why) in [("garbage".to_string(), "malformed"), (future, "beyond"), (foreign_store, "another store")] {
        let (_, snap) = open_watch(&e, "claude", Some(token));
        assert!(snap.resynchronized.as_deref().unwrap_or("").contains(why), "{:?}", snap.resynchronized);
        assert_eq!(snap.unacknowledged.len(), 1, "a resync still carries the authoritative inbox");
    }
    // A token from another participant's filter is not accepted either.
    let (_, snap) = open_watch(&e, "codex", Some(resume_token));
    assert!(snap.resynchronized.unwrap().contains("filter"));
}

#[test]
fn send_and_status_are_not_blocked_by_waiting_clients() {
    let mut e = Env::standard();
    e.start();
    let mut waiters = Vec::new();
    for _ in 0..10 {
        let mut cmd = Command::new(CLI);
        e.envs(&mut cmd);
        waiters.push(cmd.args(["wait", "--as", "claude", "--timeout", "5"]).stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap());
    }
    std::thread::sleep(Duration::from_millis(300));
    let t0 = Instant::now();
    e.send("claude", "codex", "c", Kind::Info, "free", "not blocked");
    e.status(None);
    assert!(t0.elapsed() < Duration::from_millis(500), "{:?}", t0.elapsed());
    for mut w in waiters {
        let _ = w.kill();
        let _ = w.wait();
    }
}

// ---------------------------------------------------------------- durability

#[test]
fn a_killed_daemon_restarts_with_every_committed_message_and_ack() {
    let mut e = Env::standard();
    e.start();
    let a = e.send("codex", "claude", "d", Kind::Request, "d1", "one");
    let b = e.send("codex", "claude", "d", Kind::Request, "d2", "two");
    e.call(Request::Ack { as_: "claude".into(), ids: vec![a.clone()] });
    let before = e.status(None);
    e.kill9();
    e.start();
    let after = e.status(None);
    assert_eq!(before.head_seq, after.head_seq);
    assert_eq!(before.store_id, after.store_id);
    assert_eq!(after.unacknowledged.iter().map(|l| l.id.clone()).collect::<Vec<_>>(), vec![b]);
    // An identical retry after the restart (lost receipt) returns the original.
    assert_eq!(e.send("codex", "claude", "d", Kind::Request, "d1", "one"), a);
}

#[test]
fn backup_is_a_consistent_copy() {
    let mut e = Env::standard();
    e.start();
    for i in 0..20 {
        e.send("claude", "codex", "bk", Kind::Info, &format!("bk{i}"), "x");
    }
    let path = e.dir.join("backup.db");
    match e.call(Request::Backup { path: path.to_string_lossy().into_owned() }) {
        Response::BackedUp { head_seq, .. } => assert!(head_seq >= 20),
        other => panic!("{other:?}"),
    }
    let c = rusqlite::Connection::open(&path).unwrap();
    let n: i64 = c.query_row("SELECT count(*) FROM messages", [], |r| r.get(0)).unwrap();
    let ok: String = c.query_row("PRAGMA integrity_check", [], |r| r.get(0)).unwrap();
    assert_eq!((n, ok.as_str()), (20, "ok"));
    assert!(matches!(
        e.call(Request::Backup { path: path.to_string_lossy().into_owned() }),
        Response::Error { code: ErrorCode::Conflict, .. }
    ));
}

#[test]
fn legacy_import_is_inert_and_idempotent() {
    let mut e = Env::standard();
    e.start();
    let f = e.dir.join("to-claude.md");
    std::fs::write(&f, "[codex 10:00]\nhello\n--- end ---\n[codex 10:00]\nrepeated time\n--- end ---\n[codex 11:00]\ncut off\n").unwrap();
    let p = f.to_string_lossy().into_owned();
    let Response::Legacy(dry) = e.call(Request::ImportLegacy { paths: vec![p.clone()], dry_run: true }) else { panic!() };
    assert_eq!((dry.files[0].blocks, dry.files[0].malformed, dry.files[0].newly_imported), (3, 1, 3));
    let Response::Legacy(dry2) = e.call(Request::ImportLegacy { paths: vec![p.clone()], dry_run: true }) else { panic!() };
    assert_eq!(dry2.files[0].newly_imported, 3, "a dry run stores nothing");
    let Response::Legacy(real) = e.call(Request::ImportLegacy { paths: vec![p.clone()], dry_run: false }) else { panic!() };
    assert_eq!(real.files[0].newly_imported, 3);
    let Response::Legacy(again) = e.call(Request::ImportLegacy { paths: vec![p], dry_run: false }) else { panic!() };
    assert_eq!((again.files[0].newly_imported, again.files[0].already_present), (0, 3));
    let s = e.status(None);
    assert!(s.unacknowledged.is_empty() && s.unanswered.is_empty(), "imported history never becomes work");
    assert_eq!(s.head_seq, 0, "and never creates events");
}

// ---------------------------------------------------------------- the Codex adapter

/// A fake `codex` executable: records its argv, then behaves per the mode file.
fn fake_codex(e: &Env, script: &str) -> PathBuf {
    let path = e.dir.join(format!("fake-codex-{}", NEXT.fetch_add(1, Ordering::SeqCst)));
    std::fs::write(
        &path,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" >> {log}\necho $$ >> {pids}\n{script}\n",
            log = e.dir.join("argv.log").display(),
            pids = e.dir.join("pids").display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

fn attempts(e: &Env, id: &str) -> Vec<String> {
    let c = rusqlite::Connection::open(e.state().join("chat.db")).unwrap();
    let mut s = c.prepare("SELECT state FROM attempts WHERE message_id = ?1 ORDER BY id").unwrap();
    s.query_map([id], |r| r.get(0)).unwrap().map(|r| r.unwrap()).collect()
}

fn wait_for(what: &str, timeout: Duration, mut f: impl FnMut() -> bool) {
    let t0 = Instant::now();
    while !f() {
        assert!(t0.elapsed() < timeout, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn register(e: &Env, exe: &Path, session: &str) -> i64 {
    match e.call(Request::EndpointRegister { name: "codex".into(), session: session.into(), exe: exe.to_string_lossy().into_owned() }) {
        Response::Registered(v) => v.generation,
        other => panic!("{other:?}"),
    }
}

#[test]
fn submission_retries_confirmed_failures_and_never_sends_the_body() {
    let mut e = Env::standard();
    e.start();
    let counter = e.dir.join("count");
    let exe = fake_codex(
        &e,
        &format!(
            "n=$(cat {c} 2>/dev/null || echo 0); n=$((n+1)); echo $n > {c}; [ $n -ge 3 ] && exit 0; echo 'transient failure' >&2; exit 1",
            c = counter.display()
        ),
    );
    register(&e, &exe, "00000000-0000-4000-8000-00000000c0de");
    let id = e.send("claude", "codex", "adr628", Kind::Request, "sub", "review please\nSECRET BODY TEXT on a later line");
    wait_for("three attempts", Duration::from_secs(10), || attempts(&e, &id).len() == 3 && attempts(&e, &id)[2] != "started");
    assert_eq!(attempts(&e, &id), vec!["failed", "failed", "submitted"]);
    let argv = std::fs::read_to_string(e.dir.join("argv.log")).unwrap();
    assert!(!argv.contains("SECRET BODY TEXT"), "only the bounded summary, never the body, is passed to the adapter");
    assert!(argv.contains("review please"));
    assert!(argv.contains(&format!("chatctl receive --as codex --id {id}")));
    assert!(argv.lines().take(4).collect::<Vec<_>>() == vec!["queue", "--thread", "00000000-0000-4000-8000-00000000c0de", "--message"]);
    // Submission is not receipt.
    let s = e.status(Some("codex"));
    assert!(s.unacknowledged.iter().any(|l| l.id == id && !l.received));
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(attempts(&e, &id).len(), 3, "a submitted message is not resubmitted");
}

#[test]
fn failures_stop_at_the_bound_and_stay_visible() {
    let mut e = Env::standard();
    e.start();
    let exe = fake_codex(&e, "echo 'session not found' >&2; exit 2");
    register(&e, &exe, "s1");
    let id = e.send("claude", "codex", "c", Kind::Request, "bound", "b");
    wait_for("the retry bound", Duration::from_secs(10), || attempts(&e, &id) == vec!["failed", "failed", "failed"]);
    std::thread::sleep(Duration::from_millis(800));
    assert_eq!(attempts(&e, &id).len(), 3);
    let s = e.status(None);
    let a = s.uncertain_or_failed_submissions.iter().find(|a| a.message_id == id).unwrap();
    assert_eq!((a.state.as_str(), a.exit_code), ("failed", Some(2)));
    assert!(a.error.as_deref().unwrap().contains("session not found"));
    // Explicit redelivery starts a fresh budget.
    assert!(matches!(e.call(Request::Redeliver { id: id.clone() }), Response::Redelivery { .. }));
    wait_for("redelivery attempts", Duration::from_secs(10), || attempts(&e, &id).len() == 6 && attempts(&e, &id)[5] == "failed");
}

#[test]
fn a_hung_adapter_is_killed_marked_uncertain_and_not_retried_and_does_not_block_others() {
    let mut e = Env::standard();
    e.start();
    let exe = fake_codex(&e, "exec sleep 30");
    register(&e, &exe, "s1");
    let id = e.send("claude", "codex", "c", Kind::Request, "hang", "h");
    wait_for("the attempt to start", Duration::from_secs(5), || attempts(&e, &id) == vec!["started"]);
    wait_for("the adapter child to run", Duration::from_secs(5), || e.dir.join("pids").exists());
    let t0 = Instant::now();
    e.send("claude", "codex", "c", Kind::Info, "during", "while the adapter hangs");
    e.status(None);
    assert!(t0.elapsed() < Duration::from_millis(500), "the store is not held during the adapter run");
    wait_for("the deadline", Duration::from_secs(5), || attempts(&e, &id) == vec!["uncertain"]);
    let pid: i32 = std::fs::read_to_string(e.dir.join("pids")).unwrap().lines().next().unwrap().trim().parse().unwrap();
    assert_ne!(unsafe { libc::kill(pid, 0) }, 0, "the timed-out child was killed and reaped");
    std::thread::sleep(Duration::from_millis(800));
    assert_eq!(attempts(&e, &id)[0], "uncertain", "uncertain is never retried automatically");
    assert_eq!(attempts(&e, &id).iter().filter(|s| *s == "uncertain").count(), 1);
}

#[test]
fn a_rebind_fences_failed_attempts_of_the_old_binding() {
    let mut e = Env::new("heartbeat_ms = 300\nadapter_timeout_ms = 1500\nretry_backoff_ms = [1500, 1500, 1500]\n");
    e.start();
    let exe = fake_codex(&e, "exit 1");
    assert_eq!(register(&e, &exe, "old-session"), 1);
    let id = e.send("claude", "codex", "c", Kind::Request, "fence", "f");
    wait_for("one failure", Duration::from_secs(5), || attempts(&e, &id) == vec!["failed"]);
    let ok = fake_codex(&e, "exit 0");
    assert_eq!(register(&e, &ok, "new-session"), 2);
    std::thread::sleep(Duration::from_millis(2500));
    assert_eq!(attempts(&e, &id), vec!["failed"], "no automatic retry against the new session");
    e.call(Request::Redeliver { id: id.clone() });
    wait_for("redelivery under generation 2", Duration::from_secs(5), || attempts(&e, &id) == vec!["failed", "submitted"]);
    let argv = std::fs::read_to_string(e.dir.join("argv.log")).unwrap();
    assert!(argv.contains("new-session"));
}

#[test]
fn a_crash_during_submission_leaves_it_uncertain_after_restart() {
    let mut e = Env::new("heartbeat_ms = 300\nadapter_timeout_ms = 20000\n");
    e.start();
    let exe = fake_codex(&e, "exec sleep 30");
    register(&e, &exe, "s1");
    let id = e.send("claude", "codex", "c", Kind::Request, "crash", "c");
    wait_for("the attempt to start", Duration::from_secs(5), || attempts(&e, &id) == vec!["started"]);
    wait_for("the adapter child to run", Duration::from_secs(5), || e.dir.join("pids").exists());
    e.kill9();
    e.start();
    assert_eq!(attempts(&e, &id), vec!["uncertain"]);
    let s = e.status(None);
    assert!(s.uncertain_or_failed_submissions.iter().any(|a| a.message_id == id && a.state == "uncertain"));
    for pid in std::fs::read_to_string(e.dir.join("pids")).unwrap().lines() {
        unsafe { libc::kill(pid.trim().parse().unwrap(), libc::SIGKILL) };
    }
}

#[test]
fn sigterm_stops_the_adapter_child_and_records_uncertain() {
    let mut e = Env::new("heartbeat_ms = 300\nadapter_timeout_ms = 20000\n");
    e.start();
    let exe = fake_codex(&e, "exec sleep 30");
    register(&e, &exe, "s1");
    let id = e.send("claude", "codex", "c", Kind::Request, "term", "t");
    wait_for("the attempt to start", Duration::from_secs(5), || attempts(&e, &id) == vec!["started"]);
    wait_for("the adapter child to run", Duration::from_secs(5), || e.dir.join("pids").exists());
    let t0 = Instant::now();
    assert!(e.term().success());
    assert!(t0.elapsed() < Duration::from_secs(6));
    let pid: i32 = std::fs::read_to_string(e.dir.join("pids")).unwrap().lines().next().unwrap().trim().parse().unwrap();
    assert_ne!(unsafe { libc::kill(pid, 0) }, 0, "the adapter child does not survive the service");
    assert!(!e.socket().exists(), "the socket is removed on a clean stop");
    e.start();
    assert_eq!(attempts(&e, &id), vec!["uncertain"]);
}

#[test]
fn the_journal_view_colours_by_sender() {
    let e = Env::standard();
    let input = "{\"__REALTIME_TIMESTAMP\":\"0\",\"MESSAGE\":\"stored request\",\"CHATD_SENDER\":\"claude\"}\n{\"__REALTIME_TIMESTAMP\":\"0\",\"MESSAGE\":\"stored final\",\"CHATD_SENDER\":\"codex\"}\n";
    let out = e.cli(&["journal", "--stdin"], Some(input));
    let s = stdout(&out);
    assert_eq!(s.lines().count(), 2);
    assert!(s.contains("claude  stored request") && s.contains("codex  stored final"));
    assert!(!s.contains('\x1b'), "no colour when stdout is not a terminal");
}

#[test]
fn a_closed_watcher_leaves_status_promptly() {
    let mut e = Env::new("heartbeat_ms = 10000\n");
    e.start();
    let (c, _) = open_watch(&e, "claude", None);
    assert_eq!(e.status(None).watchers.len(), 1);
    drop(c);
    wait_for("the watcher to be removed", Duration::from_secs(2), || e.status(None).watchers.is_empty());
}

fn thread_count(pid: u32) -> usize {
    std::fs::read_dir(format!("/proc/{pid}/task")).map(|d| d.count()).unwrap_or(0)
}

#[test]
fn a_refused_second_daemon_changes_nothing_in_the_store() {
    let mut e = Env::new("heartbeat_ms = 300\nadapter_timeout_ms = 20000\n");
    e.start();
    let exe = fake_codex(&e, "exec sleep 30");
    register(&e, &exe, "s1");
    let id = e.send("claude", "codex", "c", Kind::Request, "live", "in flight");
    wait_for("the attempt to start", Duration::from_secs(5), || attempts(&e, &id) == vec!["started"]);
    let before = (e.status(None).head_seq, attempts(&e, &id));
    let other_socket = e.dir.join("run").join("other.sock");
    for round in 0..4 {
        let mut cmd = Command::new(DAEMON);
        e.envs(&mut cmd);
        if round % 2 == 1 {
            cmd.env("CHATD_SOCKET", &other_socket);
        }
        let out = cmd.stderr(Stdio::piped()).output().unwrap();
        assert!(!out.status.success());
        assert!(String::from_utf8_lossy(&out.stderr).contains("another daemon"), "{}", String::from_utf8_lossy(&out.stderr));
    }
    assert_eq!(attempts(&e, &id), vec!["started"], "the live submission is untouched");
    assert_eq!((e.status(None).head_seq, attempts(&e, &id)), before, "no events or attempts were written");
    assert!(!other_socket.exists(), "the refused daemon bound nothing");
    assert!(matches!(e.call(Request::Health), Response::Health { .. }));
    for pid in std::fs::read_to_string(e.dir.join("pids")).unwrap_or_default().lines() {
        unsafe { libc::kill(pid.trim().parse().unwrap(), libc::SIGKILL) };
    }
}

#[test]
fn watches_ended_by_the_server_leave_no_threads_behind() {
    let mut e = Env::new("heartbeat_ms = 1\nwrite_deadline_ms = 200\n");
    e.start();
    let pid = e.daemon.as_ref().unwrap().id();
    std::thread::sleep(Duration::from_millis(300));
    let baseline = thread_count(pid);
    // Watchers that never read: heartbeats fill their sockets until the write deadline ends the
    // watch on the server side, while the clients stay connected.
    let stalled: Vec<UnixStream> = (0..4)
        .map(|_| {
            let mut c = UnixStream::connect(e.socket()).unwrap();
            let frame = serde_json::to_vec(
                &serde_json::json!({"v": 1, "req": {"op": "watch", "as_": "claude", "conversation": null, "resume_token": null}}),
            )
            .unwrap();
            c.write_all(&(frame.len() as u32).to_be_bytes()).unwrap();
            c.write_all(&frame).unwrap();
            c
        })
        .collect();
    wait_for("the stalled watches to end", Duration::from_secs(60), || e.status(None).watchers.is_empty());
    wait_for("their threads to exit", Duration::from_secs(5), || thread_count(pid) <= baseline);
    drop(stalled);
}

#[test]
fn an_adapter_timeout_kills_its_whole_process_group() {
    let mut e = Env::standard();
    e.start();
    let gpid = e.dir.join("grandchild");
    let exe = fake_codex(&e, &format!("sleep 30 & echo $! > {}; wait", gpid.display()));
    register(&e, &exe, "s1");
    let id = e.send("claude", "codex", "c", Kind::Request, "group", "g");
    wait_for("the deadline", Duration::from_secs(6), || attempts(&e, &id) == vec!["uncertain"]);
    let g: i32 = std::fs::read_to_string(&gpid).unwrap().trim().parse().unwrap();
    wait_for("the grandchild to be gone", Duration::from_secs(2), || unsafe { libc::kill(g, 0) } != 0);
}

#[test]
fn a_helper_left_running_after_success_does_not_hang_the_adapter() {
    let mut e = Env::standard();
    e.start();
    let gpid = e.dir.join("helper");
    let exe = fake_codex(&e, &format!("sleep 30 & echo $! > {}; exit 0", gpid.display()));
    register(&e, &exe, "s1");
    let a = e.send("claude", "codex", "c", Kind::Request, "h1", "first");
    wait_for("the first submission", Duration::from_secs(5), || attempts(&e, &a) == vec!["submitted"]);
    let g: i32 = std::fs::read_to_string(&gpid).unwrap().trim().parse().unwrap();
    wait_for("the helper to be gone", Duration::from_secs(2), || unsafe { libc::kill(g, 0) } != 0);
    let b = e.send("claude", "codex", "c", Kind::Request, "h2", "second");
    wait_for("the adapter to continue", Duration::from_secs(5), || attempts(&e, &b) == vec!["submitted"]);
}

fn fd_count(pid: u32) -> usize {
    std::fs::read_dir(format!("/proc/{pid}/fd")).map(|d| d.count()).unwrap_or(0)
}

fn socket_fds(pid: u32) -> usize {
    std::fs::read_dir(format!("/proc/{pid}/fd"))
        .map(|d| {
            d.filter_map(|x| x.ok())
                .filter(|x| std::fs::read_link(x.path()).is_ok_and(|l| l.to_string_lossy().starts_with("socket:")))
                .count()
        })
        .unwrap_or(0)
}

fn start_watch_raw(e: &Env) -> UnixStream {
    let mut c = UnixStream::connect(e.socket()).unwrap();
    let frame = serde_json::to_vec(
        &serde_json::json!({"v": 1, "req": {"op": "watch", "as_": "claude", "conversation": null, "resume_token": null}}),
    )
    .unwrap();
    c.write_all(&(frame.len() as u32).to_be_bytes()).unwrap();
    c.write_all(&frame).unwrap();
    c
}

#[test]
fn half_closed_watchers_release_their_threads_and_sockets_every_round() {
    let mut e = Env::new("heartbeat_ms = 50\nwrite_deadline_ms = 300\n");
    e.start();
    let pid = e.daemon.as_ref().unwrap().id();
    std::thread::sleep(Duration::from_millis(300));
    let (threads0, sockets0, fds0) = (thread_count(pid), socket_fds(pid), fd_count(pid));
    let mut held = Vec::new();
    for round in 0..3 {
        for _ in 0..12 {
            let c = start_watch_raw(&e);
            // The client stops reading but keeps its write half open (Codex's probe).
            c.shutdown(std::net::Shutdown::Read).unwrap();
            held.push(c);
        }
        wait_for(&format!("round {round}: watchers gone"), Duration::from_secs(10), || e.status(None).watchers.is_empty());
        wait_for(&format!("round {round}: threads back to baseline"), Duration::from_secs(5), || thread_count(pid) <= threads0);
        wait_for(&format!("round {round}: sockets back to baseline"), Duration::from_secs(5), || socket_fds(pid) <= sockets0);
        // SQLite keeps the database fds of closed reader connections for reuse while the writer
        // holds its locks. They are bounded by the peak number of concurrent watchers (12 here),
        // never by the cumulative number of watches (36 by the last round).
        let total = fd_count(pid);
        assert!(total <= fds0 + 12, "round {round}: {total} fds against baseline {fds0} with at most 12 concurrent watchers");
    }
    drop(held);
}

/// Fills the store directly (synchronous off, daemon stopped) with `n` unacknowledged requests to
/// claude whose summaries are long multi-byte text, so the inbox is far larger than one frame.
fn backlog(e: &mut Env, n: usize) -> Vec<String> {
    e.kill9();
    let mut s = chatd::store::Store::open(&e.state().join("chat.db")).unwrap();
    s.conn().execute_batch("PRAGMA synchronous = OFF").unwrap();
    let summary = "滞".repeat(150);
    let ids = (0..n)
        .map(|i| {
            s.send(&SendReq {
                from: "codex".into(),
                to: Some("claude".into()),
                conversation: Some("backlog".into()),
                kind: Kind::Request,
                reply_to: None,
                idempotency_key: format!("bl-{i}"),
                summary: Some(format!("{i} {summary}")),
                body: "b".into(),
            })
            .unwrap()
            .id
        })
        .collect();
    drop(s);
    e.start();
    ids
}

#[test]
fn a_backlog_larger_than_one_frame_is_paged_consistently() {
    let mut e = Env::new("heartbeat_ms = 300\n");
    e.start();
    let ids = backlog(&mut e, 6000);
    // Snapshot pages carry the whole inbox in order, with a commit landing mid-snapshot.
    let mut c = Client::connect(&e.socket(), Some(Duration::from_secs(20))).unwrap();
    c.send(Request::Watch { as_: "claude".into(), conversation: None, resume_token: None }).unwrap();
    let Response::SnapshotBegin { head_seq, unacknowledged_total, .. } = c.recv().unwrap() else { panic!() };
    assert_eq!(unacknowledged_total, 6000);
    let late = e.send("codex", "claude", "backlog", Kind::Request, "late", "committed during the snapshot");
    let mut got = Vec::new();
    let mut pages = 0;
    loop {
        match c.recv().unwrap() {
            Response::SnapshotPage { unacknowledged } => {
                assert!(unacknowledged.len() <= chatd::proto::MAX_PAGE as usize);
                got.extend(unacknowledged.into_iter().map(|m| m.id));
                pages += 1;
            }
            Response::SnapshotEnd { head_seq: h, pages: p } => {
                assert_eq!((h, p), (head_seq, pages));
                break;
            }
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(got, ids, "the snapshot at H is exactly the backlog, in order");
    assert!(pages >= 30);
    let (ev, _) = next_event(&mut c);
    assert_eq!(ev.message_id.as_deref(), Some(late.as_str()), "the concurrent commit arrives as the first event after H");
    drop(c);
    // A reconnect replays everything still unacknowledged, including the late one.
    let (_, snap) = open_watch(&e, "claude", None);
    assert_eq!(snap.unacknowledged.len(), 6001);
    assert_eq!(snap.total, 6001);
    assert!(snap.pages >= 31);
    // Status stays usable: bounded pages with totals and a continuation cursor.
    let s = e.status(Some("claude"));
    assert_eq!((s.unacknowledged.len(), s.unacknowledged_total), (200, 6001));
    let next = s.next_unacknowledged_after.expect("a continuation cursor");
    let Response::Status(s2) = e.call(Request::Status {
        as_: Some("claude".into()),
        conversation: None,
        limit: 200,
        unacknowledged_after: Some(next),
        unanswered_after: None,
    }) else {
        panic!()
    };
    assert_eq!(s2.unacknowledged[0].id, ids[200]);
    // The one-line wait still answers at once with the oldest.
    let out = e.cli(&["wait", "--as", "claude", "--timeout", "10"], None);
    assert!(stdout(&out).contains(&ids[0]));
    let out = e.cli(&["status", "--as", "claude"], None);
    assert_eq!(code(&out), 0);
    assert!(stdout(&out).contains("of 6001"));
}

#[test]
fn restoring_a_backup_invalidates_every_earlier_resume_token() {
    let mut e = Env::standard();
    e.start();
    e.send("codex", "claude", "r", Kind::Info, "r1", "before the backup");
    let (_, snap) = open_watch(&e, "claude", None);
    let within = snap.resume_token;
    let backup = e.dir.join("bk.db");
    assert!(matches!(e.call(Request::Backup { path: backup.to_string_lossy().into_owned() }), Response::BackedUp { .. }));
    for i in 0..5 {
        e.send("codex", "claude", "r", Kind::Info, &format!("after-{i}"), "after the backup");
    }
    let (_, snap) = open_watch(&e, "claude", None);
    let beyond = snap.resume_token;
    // Restore refuses while the daemon holds the store.
    let mut cmd = Command::new(DAEMON);
    e.envs(&mut cmd);
    let out = cmd.args(["restore", &backup.to_string_lossy()]).stderr(Stdio::piped()).output().unwrap();
    assert!(!out.status.success());
    e.term();
    let mut cmd = Command::new(DAEMON);
    e.envs(&mut cmd);
    let out = cmd.args(["restore", &backup.to_string_lossy()]).output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    e.start();
    // The restored branch grows past both old positions.
    for i in 0..10 {
        e.send("codex", "claude", "r", Kind::Info, &format!("restored-{i}"), "on the restored branch");
    }
    for token in [within, beyond] {
        let (_, snap) = open_watch(&e, "claude", Some(token));
        assert!(snap.resynchronized.as_deref().unwrap_or("").contains("another store"), "{:?}", snap.resynchronized);
        assert_eq!(snap.unacknowledged.len(), 11, "the authoritative restored inbox: 1 + 10");
    }
    let kept: Vec<_> = std::fs::read_dir(e.state())
        .unwrap()
        .filter_map(|d| d.ok())
        .filter(|d| d.file_name().to_string_lossy().starts_with("replaced-"))
        .collect();
    assert_eq!(kept.len(), 1);
    assert!(kept[0].path().join("chat.db").exists(), "the replaced database is kept");
}

#[test]
fn a_helper_that_escaped_the_group_cannot_stall_the_adapter() {
    let mut e = Env::standard();
    e.start();
    let gpid = e.dir.join("escaped");
    // setsid leaves the process group; the helper keeps stderr open for 30 s.
    let exe = fake_codex(
        &e,
        &format!("setsid sh -c 'echo $$ > {}; exec sleep 30' & sleep 0.2; echo 'leader failed' >&2; exit 3", gpid.display()),
    );
    register(&e, &exe, "s1");
    let a = e.send("claude", "codex", "c", Kind::Request, "esc1", "first");
    wait_for("the leader's failure to be recorded", Duration::from_secs(5), || {
        attempts(&e, &a).first().map(String::as_str) == Some("failed")
    });
    let s = e.status(None);
    let at = s.uncertain_or_failed_submissions.iter().find(|x| x.message_id == a).unwrap();
    assert_eq!(at.exit_code, Some(3));
    assert!(at.error.as_deref().unwrap_or("").contains("leader failed"));
    let ok = fake_codex(&e, "exit 0");
    register(&e, &ok, "s2");
    let b = e.send("claude", "codex", "c", Kind::Request, "esc2", "second");
    wait_for("the next delivery", Duration::from_secs(5), || attempts(&e, &b) == vec!["submitted"]);
    if let Ok(p) = std::fs::read_to_string(&gpid) {
        unsafe { libc::kill(p.trim().parse().unwrap(), libc::SIGKILL) };
    }
}

#[test]
fn sigterm_also_stops_the_adapters_helpers() {
    let mut e = Env::new("heartbeat_ms = 300\nadapter_timeout_ms = 20000\n");
    e.start();
    let gpid = e.dir.join("helper");
    let exe = fake_codex(&e, &format!("sleep 30 & echo $! > {}; wait", gpid.display()));
    register(&e, &exe, "s1");
    let id = e.send("claude", "codex", "c", Kind::Request, "termh", "t");
    wait_for("the helper to start", Duration::from_secs(5), || gpid.exists());
    assert!(e.term().success());
    let g: i32 = std::fs::read_to_string(&gpid).unwrap().trim().parse().unwrap();
    wait_for("the helper to be gone", Duration::from_secs(2), || unsafe { libc::kill(g, 0) } != 0);
    e.start();
    assert_eq!(attempts(&e, &id), vec!["uncertain"]);
}

// ---------------------------------------------------------------- restore failure atomicity

fn facts(e: &Env) -> (String, i64) {
    let c = rusqlite::Connection::open_with_flags(e.state().join("chat.db"), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let id: String = c.query_row("SELECT value FROM meta WHERE key = 'store_id'", [], |r| r.get(0)).unwrap();
    let n: i64 = c.query_row("SELECT count(*) FROM messages", [], |r| r.get(0)).unwrap();
    (id, n)
}

/// A store with 1 message in its backup and 3 live messages; the daemon is SIGKILLed so a WAL is
/// present at the active path. Returns (backup path, backup's store id, live facts).
fn restore_fixture(e: &mut Env) -> (PathBuf, String, (String, i64)) {
    e.start();
    e.send("codex", "claude", "r", Kind::Info, "b1", "in the backup");
    let backup = e.dir.join("bk.db");
    assert!(matches!(e.call(Request::Backup { path: backup.to_string_lossy().into_owned() }), Response::BackedUp { .. }));
    e.send("codex", "claude", "r", Kind::Info, "b2", "after the backup");
    e.send("codex", "claude", "r", Kind::Info, "b3", "after the backup");
    e.kill9();
    assert!(e.state().join("chat.db-wal").exists(), "the live store has a WAL");
    let backup_id: String =
        rusqlite::Connection::open(&backup).unwrap().query_row("SELECT value FROM meta WHERE key = 'store_id'", [], |r| r.get(0)).unwrap();
    let live = facts(e);
    assert_eq!(live.1, 3);
    (backup, backup_id, live)
}

fn run_restore(e: &Env, backup: &Path, fault: Option<&str>) -> Output {
    let mut cmd = Command::new(DAEMON);
    e.envs(&mut cmd);
    if let Some(f) = fault {
        cmd.env("CHATD_TEST_FAULT", f);
    }
    cmd.args(["restore", &backup.to_string_lossy()]).stdout(Stdio::piped()).stderr(Stdio::piped()).output().unwrap()
}

fn no_restore_leftovers(e: &Env) {
    assert!(!e.state().join(chatd::restore::MARKER).exists());
    assert!(!e.state().join(chatd::restore::CANDIDATE_DIR).exists());
}

#[test]
fn a_failed_identity_update_leaves_the_active_store_untouched() {
    let mut e = Env::standard();
    let (backup, _, live) = restore_fixture(&mut e);
    // Codex's probe: the backup refuses the store_id update (integrity and schema stay valid).
    rusqlite::Connection::open(&backup)
        .unwrap()
        .execute_batch("CREATE TRIGGER t BEFORE UPDATE ON meta WHEN old.key = 'store_id' BEGIN SELECT RAISE(ABORT, 'injected identity update failure'); END;")
        .unwrap();
    let out = run_restore(&e, &backup, None);
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains("injected identity update failure"));
    assert_eq!(facts(&e), live, "the active store is unchanged");
    no_restore_leftovers(&e);
    e.start();
    assert_eq!(e.status(None).store_id, live.0);
}

#[test]
fn an_unusable_backup_leaves_the_active_store_untouched() {
    let mut e = Env::standard();
    let (_, _, live) = restore_fixture(&mut e);
    let junk = e.dir.join("junk.db");
    std::fs::write(&junk, b"not a database at all").unwrap();
    assert_eq!(run_restore(&e, &junk, None).status.code(), Some(1));
    let empty = e.dir.join("empty.db");
    rusqlite::Connection::open(&empty).unwrap().execute_batch("CREATE TABLE x (y)").unwrap();
    assert_eq!(run_restore(&e, &empty, None).status.code(), Some(1));
    assert_eq!(facts(&e), live);
    no_restore_leftovers(&e);
}

#[test]
fn a_crash_before_the_commit_point_leaves_the_old_store_and_a_retry_works() {
    for point in ["after_copy", "after_identity"] {
        let mut e = Env::standard();
        let (backup, backup_id, live) = restore_fixture(&mut e);
        assert_eq!(run_restore(&e, &backup, Some(point)).status.code(), Some(86), "{point}");
        assert_eq!(facts(&e), live, "{point}: the active store is untouched");
        e.start(); // recovery discards the unpublished candidate
        assert_eq!(e.status(None).store_id, live.0, "{point}");
        no_restore_leftovers(&e);
        e.term();
        assert!(run_restore(&e, &backup, None).status.success(), "{point}: a later restore works");
        let (id, n) = facts(&e);
        assert_eq!(n, 1);
        assert!(id != live.0 && id != backup_id, "{point}: fresh identity");
    }
}

#[test]
fn a_crash_after_the_commit_point_rolls_forward_to_the_fresh_identity() {
    for point in ["after_marker", "after_move_wal", "before_publish", "before_marker_removal"] {
        let mut e = Env::standard();
        let (backup, backup_id, live) = restore_fixture(&mut e);
        assert_eq!(run_restore(&e, &backup, Some(point)).status.code(), Some(86), "{point}");
        e.start(); // recovery finishes the activation
        let s = e.status(None);
        assert!(s.store_id != live.0 && s.store_id != backup_id, "{point}: never the old identity on restored content");
        assert_eq!(s.unacknowledged.len(), 1, "{point}: the complete restored store");
        no_restore_leftovers(&e);
        e.term();
        let kept: Vec<PathBuf> = std::fs::read_dir(e.state())
            .unwrap()
            .filter_map(|d| d.ok())
            .map(|d| d.path())
            .filter(|p| p.file_name().unwrap().to_string_lossy().starts_with("replaced-"))
            .collect();
        assert_eq!(kept.len(), 1, "{point}");
        assert!(kept[0].join("chat.db").exists() && kept[0].join("chat.db-wal").exists(), "{point}: the old store and its WAL are kept");
        let old = rusqlite::Connection::open(kept[0].join("chat.db")).unwrap();
        let n: i64 = old.query_row("SELECT count(*) FROM messages", [], |r| r.get(0)).unwrap();
        assert_eq!(n, 3, "{point}: the kept store opens with its own WAL intact");
    }
}

#[test]
fn an_unreadable_restore_marker_refuses_startup() {
    let mut e = Env::standard();
    e.start();
    e.kill9();
    std::fs::write(e.state().join(chatd::restore::MARKER), b"{ not json").unwrap();
    assert!(e.try_start(false).is_none());
    assert!(!e.daemon.as_mut().unwrap().wait().unwrap().success());
}

// ---------------------------------------------------------------- bounded stderr drain

#[test]
fn a_continuous_stderr_flood_cannot_defeat_the_deadline() {
    let mut e = Env::standard(); // adapter_timeout_ms = 1500
    e.start();
    let exe = fake_codex(&e, "exec yes flood >&2");
    register(&e, &exe, "s1");
    let id = e.send("claude", "codex", "c", Kind::Request, "flood", "f");
    let t0 = Instant::now();
    wait_for("the deadline under a flood", Duration::from_secs(6), || attempts(&e, &id) == vec!["uncertain"]);
    assert!(t0.elapsed() < Duration::from_secs(4), "{:?}", t0.elapsed());
    let s = e.status(None);
    let a = s.uncertain_or_failed_submissions.iter().find(|a| a.message_id == id).unwrap();
    assert!(a.error.as_deref().unwrap_or("").contains("flood"), "the output is recorded: {:?}", a.error);
    let ok = fake_codex(&e, "exit 0");
    register(&e, &ok, "s2");
    let next = e.send("claude", "codex", "c", Kind::Request, "after-flood", "n");
    wait_for("the next delivery", Duration::from_secs(5), || attempts(&e, &next) == vec!["submitted"]);
}

#[test]
fn a_continuous_stderr_flood_cannot_delay_shutdown() {
    let mut e = Env::new("heartbeat_ms = 300\nadapter_timeout_ms = 20000\n");
    e.start();
    let exe = fake_codex(&e, "exec yes flood >&2");
    register(&e, &exe, "s1");
    let id = e.send("claude", "codex", "c", Kind::Request, "flood-term", "f");
    wait_for("the flood to start", Duration::from_secs(5), || e.dir.join("pids").exists());
    std::thread::sleep(Duration::from_millis(300));
    let t0 = Instant::now();
    assert!(e.term().success());
    assert!(t0.elapsed() < Duration::from_secs(6), "{:?}", t0.elapsed());
    e.start();
    assert_eq!(attempts(&e, &id), vec!["uncertain"]);
}
