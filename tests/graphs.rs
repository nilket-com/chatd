//! Plan 0003: a deterministic harness for agent graphs. chatd does not care what an agent is; only the
//! delivery to it differs. Fake agents run as threads in this test, each a chatd participant:
//!
//! - a Codex-like agent is woken by PUSH: its endpoint is a recording stand-in for `codex queue`, and the
//!   agent reads the notification lines that adapter wrote;
//! - a Claude-like agent PULLS: it holds a watch, as a Monitor running `chatctl wait --follow` does.
//!
//! Every agent acknowledges what it receives and answers requests with a final reply. The scenarios run
//! over all 12 (Claude-count, Codex-count) mixes of 2 to 4 agents. Agent names are stable instance names
//! (`c1`, `x2`), never roles.

use std::collections::HashSet;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixDatagram;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use chatd::client::{Client, ClientError};
use chatd::proto::{Kind, Request, Response, SendReq, StatusView};

const DAEMON: &str = env!("CARGO_BIN_EXE_chatd");
static NEXT: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, PartialEq, Debug)]
enum Product {
    /// Woken by push (a stand-in for `codex queue`).
    Codex,
    /// Pulls over a watch (a stand-in for a Monitor on `chatctl wait --follow`).
    Claude,
}

struct Daemon {
    dir: PathBuf,
    child: Option<Child>,
}

impl Daemon {
    fn start(names: &[String]) -> Daemon {
        let dir = std::env::temp_dir().join(format!("chatd-graph-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::SeqCst)));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("run")).unwrap();
        let list = names.iter().map(|n| format!("\"{n}\"")).collect::<Vec<_>>().join(", ");
        std::fs::write(
            dir.join("config.toml"),
            format!("participants = [{list}]\nheartbeat_ms = 300\nadapter_timeout_ms = 3000\nretry_backoff_ms = [100, 100, 100]\n"),
        )
        .unwrap();
        let notify = dir.join("notify.sock");
        let sock = UnixDatagram::bind(&notify).unwrap();
        sock.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
        let child = Command::new(DAEMON)
            .env("CHATD_STATE_DIR", dir.join("state"))
            .env("CHATD_SOCKET", dir.join("run").join("chatd.sock"))
            .env("CHATD_CONFIG", dir.join("config.toml"))
            .env("CHATD_NO_JOURNAL", "1")
            .env("NOTIFY_SOCKET", &notify)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut d = Daemon { dir, child: Some(child) };
        let t0 = Instant::now();
        let mut buf = [0u8; 256];
        loop {
            assert!(t0.elapsed() < Duration::from_secs(15), "chatd never became ready");
            if let Ok(n) = sock.recv(&mut buf)
                && String::from_utf8_lossy(&buf[..n]).contains("READY=1")
            {
                break;
            }
            if let Some(c) = d.child.as_mut()
                && c.try_wait().unwrap().is_some()
            {
                panic!("chatd exited during startup");
            }
        }
        d
    }

    fn socket(&self) -> PathBuf {
        self.dir.join("run").join("chatd.sock")
    }

    fn client(&self) -> Client {
        Client::connect(&self.socket(), Some(Duration::from_secs(10))).unwrap()
    }

    fn call(&self, req: Request) -> Response {
        self.client().call(req).unwrap()
    }

    fn status(&self) -> StatusView {
        match self.call(Request::Status { as_: None, conversation: None, limit: 200, unacknowledged_after: None, unanswered_after: None }) {
            Response::Status(s) => s,
            other => panic!("status: {other:?}"),
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            unsafe { libc::kill(c.id() as i32, libc::SIGTERM) };
            let _ = c.wait();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A recording stand-in for `codex queue --thread S --message LINE`: appends LINE to `<dir>/<S>.log`.
fn write_fake_queue(dir: &Path) -> PathBuf {
    let exe = dir.join("fake-queue");
    std::fs::write(&exe, format!("#!/bin/sh\nprintf '%s\\n' \"$5\" >> \"{}/$3.log\"\n", dir.display())).unwrap();
    std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
    exe
}

/// What an agent saw, in arrival order.
#[derive(Default)]
struct Seen {
    ids: Vec<String>,
}

struct Agent {
    name: String,
    product: Product,
    seen: Arc<Mutex<Seen>>,
}

struct Graph {
    daemon: Daemon,
    agents: Vec<Agent>,
    stop: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
    dir: PathBuf,
}

impl Drop for Graph {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

/// The agent's reaction to one message id: read it, acknowledge it, answer a request with a final reply.
fn react(daemon_socket: &Path, me: &str, id: &str, seen: &Mutex<Seen>, handled: &mut HashSet<String>) {
    if !handled.insert(id.to_string()) {
        return;
    }
    let call = |req: Request| Client::connect(daemon_socket, Some(Duration::from_secs(10))).and_then(|mut c| c.call(req));
    let Ok(Response::Message(m)) = call(Request::Receive { as_: me.into(), id: id.into() }) else { return };
    if m.recipient != me {
        return; // an event about something this agent sent
    }
    seen.lock().unwrap().ids.push(id.to_string());
    let _ = call(Request::Ack { as_: me.into(), ids: vec![id.into()] });
    if m.kind == Kind::Request {
        let _ = call(Request::Send(SendReq {
            from: me.into(),
            to: None,
            conversation: None,
            kind: Kind::Final,
            reply_to: Some(id.into()),
            idempotency_key: format!("final-{me}-{id}"),
            summary: None,
            body: format!("done by {me}"),
        }));
    }
}

fn id_of_notification(line: &str) -> Option<&str> {
    line.split("--id ").nth(1)?.split_whitespace().next()
}

impl Graph {
    /// `claudes` pull agents named c1.., then `codexes` push agents named x1.. . No agent is started
    /// watching until `run_all` (so a scenario can leave one lapsed).
    fn new(claudes: usize, codexes: usize) -> Graph {
        let mut specs: Vec<(String, Product)> = (1..=claudes).map(|i| (format!("c{i}"), Product::Claude)).collect();
        specs.extend((1..=codexes).map(|i| (format!("x{i}"), Product::Codex)));
        let names: Vec<String> = specs.iter().map(|(n, _)| n.clone()).collect();
        let daemon = Daemon::start(&names);
        let dir = daemon.dir.clone();
        let fake = write_fake_queue(&dir);
        for (n, p) in &specs {
            if *p == Product::Codex {
                let registered = daemon.call(Request::EndpointRegister {
                    name: n.clone(),
                    session: n.clone(),
                    exe: fake.to_string_lossy().into_owned(),
                });
                if let Response::Error { code, message } = registered {
                    panic!("endpoint for {n}: {code:?} {message}");
                }
            }
        }
        let agents =
            specs.into_iter().map(|(name, product)| Agent { name, product, seen: Arc::new(Mutex::new(Seen::default())) }).collect();
        Graph { daemon, agents, stop: Arc::new(AtomicBool::new(false)), threads: vec![], dir }
    }

    fn agent(&self, name: &str) -> &Agent {
        self.agents.iter().find(|a| a.name == name).unwrap()
    }

    /// Start one agent's receiving side.
    fn run(&mut self, name: &str) {
        let (socket, stop, seen, product) = {
            let a = self.agent(name);
            (self.daemon.socket(), self.stop.clone(), a.seen.clone(), a.product)
        };
        let me = name.to_string();
        let dir = self.dir.clone();
        let handle = std::thread::spawn(move || {
            let mut handled = HashSet::new();
            match product {
                Product::Codex => {
                    let log = dir.join(format!("{me}.log"));
                    let mut done = 0usize;
                    while !stop.load(Ordering::SeqCst) {
                        let text = std::fs::read_to_string(&log).unwrap_or_default();
                        let lines: Vec<&str> = text.lines().collect();
                        for line in lines.iter().skip(done) {
                            if let Some(id) = id_of_notification(line) {
                                react(&socket, &me, id, &seen, &mut handled);
                            }
                        }
                        done = lines.len();
                        std::thread::sleep(Duration::from_millis(20));
                    }
                }
                Product::Claude => {
                    let Ok(mut c) = Client::connect(&socket, None) else { return };
                    if c.send(Request::Watch { as_: me.clone(), conversation: None, resume_token: None, exclusive: true }).is_err() {
                        return;
                    }
                    while !stop.load(Ordering::SeqCst) {
                        match c.recv() {
                            Ok(Response::SnapshotPage { unacknowledged }) => {
                                for m in unacknowledged {
                                    react(&socket, &me, &m.id, &seen, &mut handled);
                                }
                            }
                            Ok(Response::Event { event, .. }) if event.kind == "message_stored" => {
                                if let Some(id) = &event.message_id {
                                    react(&socket, &me, id, &seen, &mut handled);
                                }
                            }
                            Ok(_) | Err(ClientError::Io(_)) => {}
                            Err(_) => return,
                        }
                    }
                }
            }
        });
        self.threads.push(handle);
    }

    fn run_all(&mut self) {
        let names: Vec<String> = self.agents.iter().map(|a| a.name.clone()).collect();
        for n in &names {
            self.run(n);
        }
        // A pull agent is reachable once its watch is registered.
        let pulls = self.agents.iter().filter(|a| a.product == Product::Claude).count();
        let t0 = Instant::now();
        while self.daemon.status().watchers.len() != pulls {
            assert!(t0.elapsed() < Duration::from_secs(10), "pull agents never registered their watches");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn request(&self, from: &str, to: &str, conversation: &str, key: &str) -> String {
        match self.daemon.call(Request::Send(SendReq {
            from: from.into(),
            to: Some(to.into()),
            conversation: Some(conversation.into()),
            kind: Kind::Request,
            reply_to: None,
            idempotency_key: key.into(),
            summary: None,
            body: format!("{from} asks {to}"),
        })) {
            Response::Stored(r) => r.id,
            other => panic!("send {from}->{to}: {other:?}"),
        }
    }

    /// Wait until nothing is unacknowledged and no request lacks its final reply.
    fn settle(&self, within: Duration) -> StatusView {
        let t0 = Instant::now();
        loop {
            let s = self.daemon.status();
            // `uncertain_or_failed_total` also counts a submission still in flight: a push agent can
            // acknowledge a message before chatd has recorded the adapter's exit, so wait that out too.
            if s.unacknowledged_total == 0 && s.unanswered_total == 0 && s.uncertain_or_failed_total == 0 {
                return s;
            }
            assert!(
                t0.elapsed() < within,
                "not settled after {within:?}: {} unacknowledged, {} unanswered, {} attention",
                s.unacknowledged_total,
                s.unanswered_total,
                s.uncertain_or_failed_total
            );
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    fn received(&self, name: &str) -> usize {
        self.agent(name).seen.lock().unwrap().ids.len()
    }
}

/// Every ordered pair: each agent sends one request to every other agent. Whatever the product mix,
/// each request must reach its receiver by that receiver's own transport, be acknowledged and answered,
/// and each answer must reach the asker the same way.
fn mesh(claudes: usize, codexes: usize) {
    let mut g = Graph::new(claudes, codexes);
    g.run_all();
    let names: Vec<String> = g.agents.iter().map(|a| a.name.clone()).collect();
    let n = names.len();
    for a in &names {
        for b in names.iter().filter(|b| *b != a) {
            g.request(a, b, "mesh", &format!("mesh-{a}-{b}"));
        }
    }
    let s = g.settle(Duration::from_secs(30));
    if s.uncertain_or_failed_total != 0 {
        let log = match g.daemon.call(Request::Log { conversation: None, since_seq: None, limit: 200 }) {
            Response::Log { events } => events,
            other => panic!("{other:?}"),
        };
        let subs: Vec<String> = log
            .iter()
            .filter(|e| e.kind == "submission")
            .map(|e| format!("{} {}", e.message_id.clone().unwrap_or_default(), e.detail))
            .collect();
        panic!("no delivery needed attention: {:?}\n{}", s.uncertain_or_failed_submissions, subs.join("\n"));
    }
    for a in &names {
        // n-1 requests in and n-1 final replies back.
        assert_eq!(g.received(a), 2 * (n - 1), "{a} ({:?}) in a ({claudes},{codexes}) graph", g.agent(a).product);
    }
}

/// Everyone else sends `per_sender` requests to the first agent at once. A push receiver is woken once
/// per message today (no coalescing), which is the busy-turn pile-up plan 0003 names; this pins the
/// current behaviour so a coalescing change is a visible, deliberate one.
fn fan_in(claudes: usize, codexes: usize, per_sender: usize) {
    let mut g = Graph::new(claudes, codexes);
    g.run_all();
    let names: Vec<String> = g.agents.iter().map(|a| a.name.clone()).collect();
    let target = names[0].clone();
    let senders: Vec<&String> = names.iter().filter(|n| **n != target).collect();
    for s in &senders {
        for k in 0..per_sender {
            g.request(s, &target, "fan-in", &format!("fan-{s}-{k}"));
        }
    }
    g.settle(Duration::from_secs(30));
    let expected = senders.len() * per_sender;
    assert_eq!(g.received(&target), expected, "the target saw every request exactly once");
    if g.agent(&target).product == Product::Codex {
        let pushes = std::fs::read_to_string(g.dir.join(format!("{target}.log"))).unwrap_or_default().lines().count();
        assert_eq!(pushes, expected, "one push per message: nothing coalesces them");
    }
    eprintln!(
        "fan-in ({claudes},{codexes}): {} senders x {per_sender} -> {target} ({:?}): {expected} deliveries",
        senders.len(),
        g.agent(&target).product
    );
}

/// A pull agent whose watcher lapsed: messages for it wait unacknowledged and the directory shows no
/// watcher for it; push agents are unaffected. When it watches again, the backlog is delivered.
fn lapsed_pull_agent(claudes: usize, codexes: usize) {
    let mut g = Graph::new(claudes, codexes);
    let lapsed = "c1".to_string();
    let names: Vec<String> = g.agents.iter().map(|a| a.name.clone()).collect();
    for n in names.iter().filter(|n| **n != lapsed) {
        g.run(n);
    }
    let others_pull = g.agents.iter().filter(|a| a.product == Product::Claude && a.name != lapsed).count();
    let t0 = Instant::now();
    while g.daemon.status().watchers.len() != others_pull {
        assert!(t0.elapsed() < Duration::from_secs(10), "watchers never registered");
        std::thread::sleep(Duration::from_millis(20));
    }
    for n in names.iter().filter(|n| **n != lapsed) {
        g.request(n, &lapsed, "lapse", &format!("lapse-{n}"));
    }
    std::thread::sleep(Duration::from_millis(500));
    let waiting = g.daemon.status();
    assert_eq!(waiting.unacknowledged_total as usize, names.len() - 1, "every request for the lapsed agent waits");
    assert!(waiting.watchers.iter().all(|w| w.participant != lapsed), "the directory shows no watcher for {lapsed}");
    g.run(&lapsed);
    g.settle(Duration::from_secs(30));
    assert_eq!(g.received(&lapsed), names.len() - 1, "the backlog arrives when it returns");
}

macro_rules! mixes {
    ($($name:ident: ($c:expr, $x:expr)),* $(,)?) => {
        mod mesh_all { use super::*; $(#[test] fn $name() { mesh($c, $x); })* }
        mod fan_in_all { use super::*; $(#[test] fn $name() { fan_in($c, $x, 3); })* }
    };
}

mixes! {
    c2_x0: (2, 0), c1_x1: (1, 1), c0_x2: (0, 2),
    c3_x0: (3, 0), c2_x1: (2, 1), c1_x2: (1, 2), c0_x3: (0, 3),
    c4_x0: (4, 0), c3_x1: (3, 1), c2_x2: (2, 2), c1_x3: (1, 3), c0_x4: (0, 4),
}

/// Lapse needs at least one pull agent to lapse; every mix that has one.
mod lapse_all {
    use super::*;
    #[test]
    fn c2_x0() {
        lapsed_pull_agent(2, 0)
    }
    #[test]
    fn c1_x1() {
        lapsed_pull_agent(1, 1)
    }
    #[test]
    fn c3_x0() {
        lapsed_pull_agent(3, 0)
    }
    #[test]
    fn c2_x1() {
        lapsed_pull_agent(2, 1)
    }
    #[test]
    fn c1_x2() {
        lapsed_pull_agent(1, 2)
    }
    #[test]
    fn c4_x0() {
        lapsed_pull_agent(4, 0)
    }
    #[test]
    fn c3_x1() {
        lapsed_pull_agent(3, 1)
    }
    #[test]
    fn c2_x2() {
        lapsed_pull_agent(2, 2)
    }
    #[test]
    fn c1_x3() {
        lapsed_pull_agent(1, 3)
    }
}
