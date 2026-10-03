//! The daemon: one writer connection behind a mutex, a thread per client connection, server-pushed
//! watches, and the delivery adapter. External processes never run while the store lock is held.

use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter};
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::proto::{
    ClientFrame, ErrorCode, EventView, MAX_PAGE, PROTOCOL_VERSION, Request, Response, ServerFrame, WatcherView, read_frame, write_frame,
};
use crate::store::{Store, StoreError, now_ms};
use crate::{adapter, hub::Hub, journal, legacy, notify};

pub const MAX_CONNECTIONS: usize = 64;
pub const WATCH_BATCH: u32 = 256;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_participants")]
    pub participants: Vec<String>,
    /// Watch heartbeat interval.
    #[serde(default = "default_heartbeat_ms")]
    pub heartbeat_ms: u64,
    /// A watcher whose socket accepts no bytes for this long is disconnected (it resumes by token).
    #[serde(default = "default_write_deadline_ms")]
    pub write_deadline_ms: u64,
    /// Bound on one adapter run; on expiry the child is killed and the attempt is `uncertain`.
    #[serde(default = "default_adapter_timeout_ms")]
    pub adapter_timeout_ms: u64,
    /// Backoff after the 1st, 2nd, ... failed submission of one message.
    #[serde(default = "default_retry_backoff_ms")]
    pub retry_backoff_ms: Vec<u64>,
}

fn default_participants() -> Vec<String> {
    vec!["claude".into(), "codex".into()]
}
fn default_heartbeat_ms() -> u64 {
    10_000
}
fn default_write_deadline_ms() -> u64 {
    10_000
}
fn default_adapter_timeout_ms() -> u64 {
    60_000
}
fn default_retry_backoff_ms() -> Vec<u64> {
    crate::store::RETRY_BACKOFF_MS.to_vec()
}

impl Default for Config {
    fn default() -> Self {
        Config {
            participants: default_participants(),
            heartbeat_ms: default_heartbeat_ms(),
            write_deadline_ms: default_write_deadline_ms(),
            adapter_timeout_ms: default_adapter_timeout_ms(),
            retry_backoff_ms: default_retry_backoff_ms(),
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Config, String> {
        match std::fs::read_to_string(path) {
            Ok(s) => toml::from_str(&s).map_err(|e| format!("{}: {e}", path.display())),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Config::default()),
            Err(e) => Err(format!("{}: {e}", path.display())),
        }
    }
}

pub struct Shared {
    pub store: Mutex<Store>,
    pub db_path: PathBuf,
    pub hub: Hub,
    pub config: Config,
    pub shutdown: AtomicBool,
    pub connections: AtomicUsize,
    pub watchers: Mutex<HashMap<u64, WatcherView>>,
    pub next_watcher: AtomicU64,
    /// The running adapter child's pid, so shutdown can stop it.
    pub adapter_child: Mutex<Option<u32>>,
    /// Held (flock) for the daemon's lifetime.
    pub state_lock: std::fs::File,
}

impl Shared {
    pub fn committed(&self) {
        if let Ok(store) = self.store.lock()
            && let Ok(h) = store.head_seq()
        {
            self.hub.committed(h);
        }
    }
}

fn err(e: StoreError) -> Response {
    Response::Error { code: e.code, message: e.message }
}

fn peer_uid(stream: &UnixStream) -> io::Result<u32> {
    let mut cred = libc::ucred { pid: 0, uid: 0, gid: 0 };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: valid fd, correctly sized out-parameter.
    let rc = unsafe {
        libc::getsockopt(stream.as_raw_fd(), libc::SOL_SOCKET, libc::SO_PEERCRED, (&mut cred as *mut libc::ucred).cast(), &mut len)
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(cred.uid)
}

/// Opens, migrates and write-checks the store, binds the socket, and returns the listener. Any
/// failure here happens before readiness is advertised.
/// Takes the state directory's lifetime exclusive lock (flock), or refuses.
fn lock_state(state_dir: &Path) -> Result<std::fs::File, String> {
    std::fs::create_dir_all(state_dir).map_err(|e| format!("{}: {e}", state_dir.display()))?;
    std::fs::set_permissions(state_dir, std::fs::Permissions::from_mode(0o700)).map_err(|e| format!("{}: {e}", state_dir.display()))?;
    let lock_path = state_dir.join("chatd.lock");
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .map_err(|e| format!("{}: {e}", lock_path.display()))?;
    // SAFETY: flock(2) on an fd we own; the lock lives as long as the returned File.
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(format!("{} is held by another daemon ({})", lock_path.display(), io::Error::last_os_error()));
    }
    Ok(lock)
}

/// Offline restore (see `restore.rs` for the failure-atomic protocol). Refuses while a daemon holds
/// the store or answers on the socket. Returns (new store id, kept dir).
pub fn restore(state_dir: &Path, socket: &Path, backup: &Path) -> Result<(String, PathBuf), String> {
    let _lock = lock_state(state_dir)?;
    if socket.exists() && UnixStream::connect(socket).is_ok() {
        return Err(format!("{} is served by a daemon; stop it first", socket.display()));
    }
    crate::restore::restore(state_dir, backup)
}

/// Nothing in the store is touched until this process holds the state directory's exclusive lock
/// and no live daemon answers on the socket: a refused second daemon changes nothing.
pub fn prepare(config: Config, state_dir: &Path, socket: &Path) -> Result<(Arc<Shared>, UnixListener), String> {
    let lock = lock_state(state_dir)?;
    if let Some(dir) = socket.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    if socket.exists() && UnixStream::connect(socket).is_ok() {
        return Err(format!("{} is served by another daemon", socket.display()));
    }
    // An interrupted restore is finished (or an unpublished candidate discarded) before the store
    // is opened.
    if let Some(id) = crate::restore::recover(state_dir)? {
        journal::log(journal::PRIORITY_WARNING, &format!("completed an interrupted restore; store id {id}"), &[]);
    }
    let db_path = state_dir.join("chat.db");
    let mut store = Store::open(&db_path).map_err(|e| e.to_string())?;
    store.migrate(&config.participants).map_err(|e| e.to_string())?;
    store.write_check().map_err(|e| e.to_string())?;
    let interrupted = store.mark_interrupted_attempts().map_err(|e| e.to_string())?;
    if interrupted > 0 {
        journal::log(
            journal::PRIORITY_WARNING,
            &format!("{interrupted} submission(s) were in flight at the last stop; marked uncertain"),
            &[],
        );
    }
    let head = store.head_seq().map_err(|e| e.to_string())?;
    if socket.exists() {
        // Checked above: no live daemon answers, so this is a stale socket file from a crash.
        std::fs::remove_file(socket).map_err(|e| format!("{}: {e}", socket.display()))?;
    }
    let listener = UnixListener::bind(socket).map_err(|e| format!("{}: {e}", socket.display()))?;
    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600)).map_err(|e| format!("{}: {e}", socket.display()))?;
    let shared = Arc::new(Shared {
        store: Mutex::new(store),
        db_path,
        hub: Hub::new(head),
        config,
        shutdown: AtomicBool::new(false),
        connections: AtomicUsize::new(0),
        watchers: Mutex::new(HashMap::new()),
        next_watcher: AtomicU64::new(1),
        adapter_child: Mutex::new(None),
        state_lock: lock,
    });
    Ok((shared, listener))
}

/// Serves until shutdown. Readiness is sent once the accept loop and adapter are running.
pub fn serve(shared: Arc<Shared>, listener: UnixListener) -> io::Result<()> {
    {
        let s = shared.clone();
        std::thread::Builder::new().name("adapter".into()).spawn(move || adapter::run(s))?;
    }
    let my_uid = unsafe { libc::getuid() };
    let accept_shared = shared.clone();
    let acceptor = std::thread::Builder::new().name("accept".into()).spawn(move || {
        for conn in listener.incoming() {
            if accept_shared.shutdown.load(Ordering::SeqCst) {
                break;
            }
            let Ok(stream) = conn else { continue };
            match peer_uid(&stream) {
                Ok(uid) if uid == my_uid => {}
                _ => {
                    journal::log(journal::PRIORITY_WARNING, "refused a connection from another uid", &[]);
                    continue;
                }
            }
            if accept_shared.connections.fetch_add(1, Ordering::SeqCst) >= MAX_CONNECTIONS {
                accept_shared.connections.fetch_sub(1, Ordering::SeqCst);
                let mut w = BufWriter::new(&stream);
                let _ = write_frame(
                    &mut w,
                    &ServerFrame {
                        v: PROTOCOL_VERSION,
                        resp: Response::Error { code: ErrorCode::Busy, message: "too many connections".into() },
                    },
                );
                continue;
            }
            let s = accept_shared.clone();
            let _ = std::thread::Builder::new().name("client".into()).spawn(move || {
                let _ = handle(&s, stream);
                s.connections.fetch_sub(1, Ordering::SeqCst);
            });
        }
    })?;
    let ready = notify::notify("READY=1\nSTATUS=serving");
    if let Err(e) = ready {
        journal::log(journal::PRIORITY_ERR, &format!("readiness notification failed: {e}"), &[]);
        return Err(e);
    }
    journal::log(journal::PRIORITY_NOTICE, "chatd ready", &[]);
    let _ = acceptor.join();
    Ok(())
}

fn send<W: io::Write>(w: &mut W, resp: Response) -> io::Result<()> {
    write_frame(w, &ServerFrame { v: PROTOCOL_VERSION, resp })
}

fn handle(shared: &Arc<Shared>, stream: UnixStream) -> io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(120)))?;
    stream.set_write_timeout(Some(Duration::from_millis(shared.config.write_deadline_ms)))?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = BufWriter::new(stream.try_clone()?);
    loop {
        let frame: ClientFrame = match read_frame(&mut reader) {
            Ok(Some(f)) => f,
            Ok(None) => return Ok(()),
            Err(e) => {
                let _ = send(&mut writer, Response::Error { code: ErrorCode::BadRequest, message: format!("unreadable frame: {e}") });
                return Ok(());
            }
        };
        if frame.v != PROTOCOL_VERSION {
            send(
                &mut writer,
                Response::Error {
                    code: ErrorCode::VersionMismatch,
                    message: format!("daemon speaks protocol {PROTOCOL_VERSION}, client sent {}", frame.v),
                },
            )?;
            return Ok(());
        }
        if let Request::Watch { as_, conversation, resume_token } = frame.req {
            return watch(shared, &stream, &mut writer, &as_, conversation.as_deref(), resume_token.as_deref());
        }
        let resp = dispatch(shared, frame.req);
        send(&mut writer, resp)?;
    }
}

fn dispatch(shared: &Arc<Shared>, req: Request) -> Response {
    let mut store = match shared.store.lock() {
        Ok(s) => s,
        Err(_) => return Response::Error { code: ErrorCode::Storage, message: "store lock poisoned".into() },
    };
    let mut wrote = false;
    let resp = match req {
        Request::Health => match (store.store_id(), store.head_seq()) {
            (Ok(id), Ok(head)) => {
                Response::Health { store_id: id, head_seq: head, schema_version: crate::store::SCHEMA_VERSION, pid: std::process::id() }
            }
            (Err(e), _) | (_, Err(e)) => err(e),
        },
        Request::Send(req) => match store.send(&req) {
            Ok(r) => {
                wrote = !r.duplicate;
                if !r.duplicate {
                    let to = store.receive(&req.from, &r.id).map(|m| m.recipient).unwrap_or_default();
                    journal::log(
                        journal::PRIORITY_INFO,
                        &format!("stored {} {} {}->{}", req.kind.as_str(), r.id, req.from, to),
                        &[("SENDER", &req.from), ("RECIPIENT", &to), ("MESSAGE_ID", &r.id), ("EVENT", "stored")],
                    );
                }
                Response::Stored(r)
            }
            Err(e) => err(e),
        },
        Request::Receive { as_, id } => store.receive(&as_, &id).map(Response::Message).unwrap_or_else(err),
        Request::Ack { as_, ids } => match store.ack(&as_, &ids) {
            Ok((acknowledged, already)) => {
                wrote = !acknowledged.is_empty();
                for id in &acknowledged {
                    let from = store.receive(&as_, id).map(|m| m.sender).unwrap_or_else(|_| "-".into());
                    journal::log(
                        journal::PRIORITY_INFO,
                        &format!("received {id} by {as_}"),
                        &[("SENDER", &as_), ("FROM", &from), ("RECIPIENT", &as_), ("MESSAGE_ID", id), ("EVENT", "received")],
                    );
                }
                Response::Acked { acknowledged, already }
            }
            Err(e) => err(e),
        },
        Request::Lookup { sender, idempotency_key } => store.lookup(&sender, &idempotency_key).map(Response::Found).unwrap_or_else(err),
        Request::Status { as_, conversation, limit, unacknowledged_after, unanswered_after } => {
            match store.status(as_.as_deref(), conversation.as_deref(), limit, unacknowledged_after, unanswered_after) {
                Ok(mut s) => {
                    s.watchers = shared.watchers.lock().map(|w| w.values().cloned().collect()).unwrap_or_default();
                    Response::Status(s)
                }
                Err(e) => err(e),
            }
        }
        Request::Log { conversation, since_seq, limit } => {
            store.log(conversation.as_deref(), since_seq, limit).map(|events| Response::Log { events }).unwrap_or_else(err)
        }
        Request::EndpointRegister { name, session, exe } => match store.endpoint_register(&name, &session, &exe) {
            Ok(v) => {
                wrote = true;
                journal::log(
                    journal::PRIORITY_NOTICE,
                    &format!("endpoint {name} bound, generation {}", v.generation),
                    &[("SENDER", &name), ("EVENT", "endpoint")],
                );
                Response::Registered(v)
            }
            Err(e) => err(e),
        },
        Request::Redeliver { id } => match store.redeliver(&id) {
            Ok(()) => {
                wrote = true;
                journal::log(
                    journal::PRIORITY_NOTICE,
                    &format!("redelivery requested for {id}"),
                    &[("MESSAGE_ID", &id), ("EVENT", "redeliver")],
                );
                Response::Redelivery { message_id: id }
            }
            Err(e) => err(e),
        },
        Request::Backup { path } => {
            let p = PathBuf::from(&path);
            if !p.is_absolute() {
                Response::Error { code: ErrorCode::BadRequest, message: "backup path must be absolute".into() }
            } else {
                store.backup_to(&p).map(|head_seq| Response::BackedUp { path, head_seq }).unwrap_or_else(err)
            }
        }
        Request::ImportLegacy { paths, dry_run } => legacy::import(&mut store, &paths, dry_run).map(Response::Legacy).unwrap_or_else(err),
        Request::Watch { .. } => Response::Error { code: ErrorCode::BadRequest, message: "watch is handled by the connection".into() },
    };
    let head = if wrote { store.head_seq().ok() } else { None };
    drop(store);
    if let Some(h) = head {
        shared.hub.committed(h);
    }
    resp
}

pub fn filter_hash(participant: &str, conversation: Option<&str>) -> String {
    let h = Sha256::digest(format!("{participant}\0{}", conversation.unwrap_or("*")).as_bytes());
    format!("{h:x}")[..16].to_string()
}

pub fn resume_token(store_id: &str, generation: i64, filter: &str, seq: i64) -> String {
    format!("ac1.{store_id}.{generation}.{filter}.{seq}")
}

/// Ok(seq) for a token that names this store, generation and filter, and a position not beyond
/// the head; otherwise the reason a resynchronization is required.
pub fn check_token(token: &str, store_id: &str, generation: i64, filter: &str, head: i64) -> Result<i64, String> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 5 || parts[0] != "ac1" {
        return Err("malformed resume token".into());
    }
    if parts[1] != store_id {
        return Err("the token names another store (restored or replaced)".into());
    }
    if parts[2] != generation.to_string() {
        return Err("the token names another store generation".into());
    }
    if parts[3] != filter {
        return Err("the token was issued for another participant or conversation filter".into());
    }
    let seq: i64 = parts[4].parse().map_err(|_| "malformed resume position".to_string())?;
    if seq < 0 || seq > head {
        return Err(format!("the token's position {seq} is beyond the store head {head}"));
    }
    Ok(seq)
}

fn watch<W: io::Write>(
    shared: &Arc<Shared>,
    stream: &UnixStream,
    w: &mut W,
    who: &str,
    conversation: Option<&str>,
    token: Option<&str>,
) -> io::Result<()> {
    // Each watcher reads through its own connection; the writer is never held while waiting.
    let mut reader = match Store::open(&shared.db_path) {
        Ok(s) => s,
        Err(e) => return send(w, err(e)),
    };
    if !shared.config.participants.iter().any(|p| p == who) {
        return send(w, Response::Error { code: ErrorCode::UnknownParticipant, message: format!("unknown participant {who:?}") });
    }
    let filter = filter_hash(who, conversation);
    // The snapshot is one read transaction, held while its pages are sent: every page is
    // consistent at `head`, and commits made meanwhile arrive as events after it. Memory stays
    // bounded: pages are read and sent one at a time.
    let snapshot = (|| -> Result<_, StoreError> {
        reader.conn().execute_batch("BEGIN")?;
        let store_id = reader.store_id()?;
        let generation = reader.generation()?;
        let head = reader.head_seq()?;
        let total = reader.unacknowledged_count(who, conversation)?;
        Ok((store_id, generation, head, total))
    })();
    let (store_id, generation, head, unacknowledged_total) = match snapshot {
        Ok(s) => s,
        Err(e) => {
            let _ = reader.conn().execute_batch("ROLLBACK");
            return send(w, err(e));
        }
    };
    let (mut last, resynchronized) = match token {
        None => (head, None),
        Some(t) => match check_token(t, &store_id, generation, &filter, head) {
            Ok(seq) => (seq, None),
            Err(reason) => (head, Some(reason)),
        },
    };
    let id = shared.next_watcher.fetch_add(1, Ordering::SeqCst);
    let view = WatcherView {
        participant: who.to_string(),
        conversation: conversation.map(str::to_string),
        connected_at_ms: now_ms(),
        last_event_seq: last,
    };
    shared.watchers.lock().unwrap().insert(id, view);
    journal::log(journal::PRIORITY_INFO, &format!("watch opened by {who}"), &[("SENDER", who), ("EVENT", "watch_open")]);
    // The client sends nothing after Watch: a read returning means it closed (or broke). Noticing
    // it here keeps `status` accurate instead of waiting for the next failed heartbeat write.
    let peer_closed = Arc::new(AtomicBool::new(false));
    let peer_reader = match stream.try_clone() {
        Ok(mut r) => {
            let (flag, s) = (peer_closed.clone(), shared.clone());
            std::thread::Builder::new()
                .name("watch-peer".into())
                .spawn(move || {
                    let _ = r.set_read_timeout(None);
                    let mut b = [0u8; 64];
                    // Returns on EOF, on any byte, or when the watch shuts the socket down.
                    let _ = io::Read::read(&mut r, &mut b);
                    flag.store(true, Ordering::SeqCst);
                    s.hub.wake_all();
                })
                .ok()
        }
        Err(_) => None,
    };
    let result = (|| -> io::Result<()> {
        send(
            w,
            Response::SnapshotBegin {
                head_seq: head,
                resume_token: resume_token(&store_id, generation, &filter, last),
                unacknowledged_total,
                resynchronized,
            },
        )?;
        let (mut after, mut pages) = (0i64, 0u64);
        loop {
            let page = match reader.unacknowledged_page(who, conversation, after, MAX_PAGE) {
                Ok(p) => p,
                Err(e) => return send(w, err(e)),
            };
            let n = page.len();
            if let Some(m) = page.last() {
                after = m.seq;
            }
            if n > 0 || pages == 0 {
                send(w, Response::SnapshotPage { unacknowledged: page })?;
                pages += 1;
            }
            if n < MAX_PAGE as usize {
                break;
            }
        }
        if let Err(e) = reader.conn().execute_batch("COMMIT") {
            return send(w, err(e.into()));
        }
        send(w, Response::SnapshotEnd { head_seq: head, pages })?;
        let heartbeat = Duration::from_millis(shared.config.heartbeat_ms);
        let mut seen_global = head;
        loop {
            if shared.shutdown.load(Ordering::SeqCst) {
                return Ok(());
            }
            let global = shared.hub.head().max(seen_global);
            loop {
                let batch: Vec<EventView> = match reader.events_after(who, conversation, last, WATCH_BATCH) {
                    Ok(b) => b,
                    Err(e) => return send(w, err(e)),
                };
                let n = batch.len();
                for ev in batch {
                    last = ev.seq;
                    let token = resume_token(&store_id, generation, &filter, last);
                    send(w, Response::Event { event: ev, resume_token: token })?;
                }
                if let Some(v) = shared.watchers.lock().unwrap().get_mut(&id) {
                    v.last_event_seq = last;
                }
                if n < WATCH_BATCH as usize {
                    break;
                }
            }
            seen_global = global;
            let now_head = shared.hub.wait_beyond(seen_global, heartbeat);
            if now_head <= seen_global {
                send(w, Response::Heartbeat { head_seq: now_head, at_ms: now_ms() })?;
            }
            if peer_closed.load(Ordering::SeqCst) {
                return Ok(());
            }
        }
    })();
    shared.watchers.lock().unwrap().remove(&id);
    if !reader.conn().is_autocommit() {
        let _ = reader.conn().execute_batch("ROLLBACK");
    }
    // However the watch ended: shut the socket down (unblocking the peer reader and telling the
    // client), then join the reader so no thread or socket clone outlives the watch.
    let _ = stream.shutdown(std::net::Shutdown::Both);
    if let Some(h) = peer_reader {
        let _ = h.join();
    }
    let why = match &result {
        Ok(()) => "closed".to_string(),
        Err(e) if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => {
            "write deadline exceeded (slow watcher); it resumes by token".into()
        }
        Err(e) => format!("ended: {e}"),
    };
    journal::log(journal::PRIORITY_INFO, &format!("watch by {who} {why}"), &[("SENDER", who), ("EVENT", "watch_close")]);
    Ok(())
}
