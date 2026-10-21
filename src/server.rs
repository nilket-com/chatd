//! The daemon: one writer connection behind a mutex, a thread per client connection, server-pushed
//! watches, and the delivery adapter. External processes never run while the store lock is held.

use std::collections::{BTreeMap, HashMap};
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
    ClientFrame, EndpointView, ErrorCode, EventView, HealthView, Kind, MAX_PAGE, PROTOCOL_VERSION, ParticipantView, Request, Response,
    ServerFrame, WatcherView, read_frame, write_frame,
};
use crate::store::{SYSTEM_SENDER, Store, StoreError, now_ms};
use crate::{adapter, health, hub::Hub, journal, legacy, notify};

pub const MAX_CONNECTIONS: usize = 64;
pub const WATCH_BATCH: u32 = 256;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_participants")]
    pub participants: Vec<String>,
    /// Optional settings per participant: `[participant.c1]` with `product`, `tier` and `wake_kinds`. A
    /// name that appears only here is a participant too. Nothing here changes routing; `wake_kinds`
    /// decides which messages wake a push participant.
    #[serde(default)]
    pub participant: BTreeMap<String, ParticipantConf>,
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
    /// How often each inspected participant's session log is checked (`health.rs`); 0 turns the check off.
    #[serde(default = "default_health_interval_ms")]
    pub health_interval_ms: u64,
    /// A participant whose log has not been written for this long while requests addressed to it are
    /// unanswered is STALLED.
    #[serde(default = "default_health_stall_ms")]
    pub health_stall_ms: u64,
    /// Where Codex keeps its session logs; default `$CODEX_HOME/sessions`, else `~/.codex/sessions`.
    #[serde(default)]
    pub codex_sessions_dir: Option<PathBuf>,
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
fn default_health_interval_ms() -> u64 {
    5 * 60_000
}
fn default_health_stall_ms() -> u64 {
    30 * 60_000
}

impl Default for Config {
    fn default() -> Self {
        Config {
            participants: default_participants(),
            participant: BTreeMap::new(),
            heartbeat_ms: default_heartbeat_ms(),
            write_deadline_ms: default_write_deadline_ms(),
            adapter_timeout_ms: default_adapter_timeout_ms(),
            retry_backoff_ms: default_retry_backoff_ms(),
            health_interval_ms: default_health_interval_ms(),
            health_stall_ms: default_health_stall_ms(),
            codex_sessions_dir: None,
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Config, String> {
        match std::fs::read_to_string(path) {
            Ok(s) => {
                let config: Config = toml::from_str(&s).map_err(|e| format!("{}: {e}", path.display()))?;
                config.validate().map_err(|e| format!("{}: {e}", path.display()))?;
                Ok(config)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Config::default()),
            Err(e) => Err(format!("{}: {e}", path.display())),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParticipantConf {
    /// The agent's product, a short lowercase token (`claude`, `codex`, ...). It decides how the agent
    /// is expected to be woken, nothing else.
    pub product: Option<String>,
    /// The model tier, a short lowercase token. Metadata only; never used for routing.
    pub tier: Option<String>,
    /// The message kinds that are pushed to the participant while it has an endpoint (`request`, `info`,
    /// `progress`, `final`, or `all`). Every other message is stored and stays unacknowledged, and the next
    /// waking push carries it in a digest. Default `["request"]`. A pull participant sees every message.
    pub wake_kinds: Option<Vec<String>>,
}

/// The `wake_kinds` token that stands for every kind.
pub const WAKE_ALL: &str = "all";
/// What a push participant is woken for when `wake_kinds` is not set.
pub const DEFAULT_WAKE_KINDS: [Kind; 1] = [Kind::Request];

/// A short lowercase token: letters, digits, `-` and `_`, starting with a letter or digit, at most 32.
fn valid_token(s: &str) -> bool {
    let mut chars = s.chars();
    chars.next().is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && s.len() <= 32
        && s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
}

impl Config {
    /// The participant list plus any name that only appears under `[participant.*]`, in that order.
    pub fn participant_names(&self) -> Vec<String> {
        let mut names: Vec<String> = Vec::new();
        for n in self.participants.iter().chain(self.participant.keys()) {
            if !names.contains(n) {
                names.push(n.clone());
            }
        }
        names
    }

    /// The Codex sessions directory (see `codex_sessions_dir`).
    pub fn codex_sessions(&self) -> PathBuf {
        if let Some(d) = &self.codex_sessions_dir {
            return d.clone();
        }
        match std::env::var_os("CODEX_HOME") {
            Some(h) if !h.is_empty() => PathBuf::from(h).join("sessions"),
            _ => PathBuf::from(std::env::var_os("HOME").unwrap_or_else(|| "/".into())).join(".codex").join("sessions"),
        }
    }

    /// The kinds that wake `name` when it is pushed to (see `ParticipantConf::wake_kinds`).
    pub fn wake_kinds(&self, name: &str) -> Vec<Kind> {
        let Some(tokens) = self.participant.get(name).and_then(|c| c.wake_kinds.as_ref()) else {
            return DEFAULT_WAKE_KINDS.to_vec();
        };
        if tokens.iter().any(|t| t == WAKE_ALL) {
            return Kind::ALL.to_vec();
        }
        Kind::ALL.into_iter().filter(|k| tokens.iter().any(|t| t == k.as_str())).collect()
    }

    pub fn knows(&self, who: &str) -> bool {
        self.participants.iter().any(|p| p == who) || self.participant.contains_key(who)
    }

    fn validate(&self) -> Result<(), String> {
        if self.participant_names().iter().any(|n| n == SYSTEM_SENDER) {
            return Err(format!("{SYSTEM_SENDER:?} is reserved for the daemon's own notices and cannot be a participant"));
        }
        for (name, conf) in &self.participant {
            for (field, value) in [("product", &conf.product), ("tier", &conf.tier)] {
                if let Some(v) = value
                    && !valid_token(v)
                {
                    return Err(format!(
                        "participant.{name}.{field} = {v:?} must be a short lowercase token (a-z, 0-9, '-' and '_', at most 32)"
                    ));
                }
            }
            if let Some(tokens) = &conf.wake_kinds {
                if tokens.is_empty() {
                    return Err(format!(
                        "participant.{name}.wake_kinds is empty: nothing would ever wake it; use [\"request\"] (the default) or [\"all\"]"
                    ));
                }
                if let Some(bad) = tokens.iter().find(|t| *t != WAKE_ALL && Kind::parse(t).is_none()) {
                    return Err(format!(
                        "participant.{name}.wake_kinds: {bad:?} is not a message kind (request, info, progress, final, or all)"
                    ));
                }
            }
        }
        Ok(())
    }
}

/// The directory: every participant, how it is configured, and whether a delivery path is in place now.
pub fn directory(
    config: &Config,
    endpoints: &[EndpointView],
    watchers: &[WatcherView],
    health: &BTreeMap<String, HealthView>,
) -> Vec<ParticipantView> {
    config
        .participant_names()
        .into_iter()
        .map(|name| {
            let conf = config.participant.get(&name).cloned().unwrap_or_default();
            let endpoint = endpoints.iter().find(|e| e.name == name);
            let mine: Vec<&WatcherView> = watchers.iter().filter(|w| w.participant == name).collect();
            let transport = if endpoint.is_some() { "push" } else { "pull" };
            let wake_kinds = endpoint.map(|_| config.wake_kinds(&name));
            let expected = match conf.product.as_deref() {
                Some("codex") => Some("push".to_string()),
                Some("claude") => Some("pull".to_string()),
                _ => None,
            };
            ParticipantView {
                path_ready: endpoint.is_some() || !mine.is_empty(),
                watchers: mine.len() as u32,
                watching_since_ms: mine.iter().map(|w| w.connected_at_ms).min(),
                session: endpoint.map(|e| e.session.clone()),
                health: health.get(&name).cloned(),
                wake_kinds,
                queued_for_wake: None,
                transport: transport.to_string(),
                expected_transport: expected,
                product: conf.product,
                tier: conf.tier,
                name,
            }
        })
        .collect()
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
    /// The last health observation per inspected participant (`health.rs`).
    pub health: Mutex<BTreeMap<String, HealthView>>,
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
    store.migrate(&config.participant_names()).map_err(|e| e.to_string())?;
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
        health: Mutex::new(BTreeMap::new()),
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
    if shared.config.health_interval_ms > 0 {
        let s = shared.clone();
        std::thread::Builder::new().name("health".into()).spawn(move || health::run(s))?;
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
        if let Request::Watch { as_, conversation, resume_token, exclusive } = frame.req {
            return watch(shared, &stream, &mut writer, &as_, conversation.as_deref(), resume_token.as_deref(), exclusive);
        }
        let resp = dispatch(shared, frame.req);
        send(&mut writer, resp)?;
    }
}

/// The name a client used that is reserved for the daemon, if any.
fn reserved_name(req: &Request) -> Option<&str> {
    let names: Vec<&str> = match req {
        Request::Send(r) => [Some(r.from.as_str()), r.to.as_deref()].into_iter().flatten().collect(),
        Request::Receive { as_, .. }
        | Request::Ack { as_, .. }
        | Request::Supersede { as_, .. }
        | Request::ClassifyRehearsal { as_, .. } => vec![as_.as_str()],
        Request::EndpointRegister { name, .. } | Request::EndpointUnregister { name } => vec![name.as_str()],
        _ => Vec::new(),
    };
    names.into_iter().find(|n| *n == SYSTEM_SENDER)
}

fn dispatch(shared: &Arc<Shared>, req: Request) -> Response {
    if let Some(name) = reserved_name(&req) {
        return Response::Error {
            code: ErrorCode::BadRequest,
            message: format!("{name:?} is reserved for the daemon's own notices; it cannot send, receive or be addressed"),
        };
    }
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
            Ok(mut r) => {
                wrote = !r.duplicate;
                let to = store.receive(&req.from, &r.id).map(|m| m.recipient).unwrap_or_default();
                // Never refuses: a recipient whose last turn failed is still sent to, and the sender is told.
                r.warning = shared.health.lock().ok().and_then(|h| h.get(&to).and_then(|v| health::send_warning(&to, v)));
                if !r.duplicate {
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
        Request::Current { as_, conversation, limit, after, history } => {
            store.current(as_.as_deref(), conversation.as_deref(), limit, after, history).map(Response::Current).unwrap_or_else(err)
        }
        Request::Supersede { as_, id, by, reason } => match store.annotate_work(&as_, &id, Some(&by), &reason) {
            Ok(()) => {
                wrote = true;
                Response::Annotated { id }
            }
            Err(e) => err(e),
        },
        Request::ClassifyRehearsal { as_, id, reason } => match store.annotate_work(&as_, &id, None, &reason) {
            Ok(()) => {
                wrote = true;
                Response::Annotated { id }
            }
            Err(e) => err(e),
        },
        Request::Lookup { sender, idempotency_key } => store.lookup(&sender, &idempotency_key).map(Response::Found).unwrap_or_else(err),
        Request::Status { as_, conversation, limit, unacknowledged_after, unanswered_after } => {
            match store.status(as_.as_deref(), conversation.as_deref(), limit, unacknowledged_after, unanswered_after) {
                Ok(mut s) => {
                    s.watchers = shared.watchers.lock().map(|w| w.values().cloned().collect()).unwrap_or_default();
                    let health = shared.health.lock().map(|h| h.clone()).unwrap_or_default();
                    let mut rows = directory(&shared.config, &s.endpoints, &s.watchers, &health);
                    for row in rows.iter_mut() {
                        if let Some(kinds) = &row.wake_kinds {
                            row.queued_for_wake = store.queued_for_wake(&row.name, kinds, None, 0).ok().map(|(_, total)| total);
                        }
                    }
                    s.participants = Some(rows);
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
        Request::EndpointUnregister { name } => match store.endpoint_unregister(&name) {
            Ok(Some((removed, in_flight))) => {
                wrote = true;
                // Under the store lock, as the health check's own update is: a check that read the old
                // binding cannot put its observation back afterwards (see `health::check`).
                if let Ok(mut h) = shared.health.lock() {
                    h.remove(&name);
                }
                journal::log(
                    journal::PRIORITY_NOTICE,
                    &format!(
                        "endpoint {name} unregistered, generation {}{}",
                        removed.generation,
                        if in_flight > 0 { format!(" ({in_flight} submission(s) still in flight)") } else { String::new() }
                    ),
                    &[("SENDER", &name), ("EVENT", "endpoint")],
                );
                Response::Unregistered { name, removed: Some(removed), in_flight }
            }
            Ok(None) => Response::Unregistered { name, removed: None, in_flight: 0 },
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
    exclusive: bool,
) -> io::Result<()> {
    // Each watcher reads through its own connection; the writer is never held while waiting.
    let mut reader = match Store::open(&shared.db_path) {
        Ok(s) => s,
        Err(e) => return send(w, err(e)),
    };
    if !shared.config.knows(who) {
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
    // Count, refuse and register under one lock: exclusivity cannot race.
    let others = {
        let mut registry = shared.watchers.lock().unwrap();
        let n =
            registry.values().filter(|o| o.participant == who && filters_overlap(o.conversation.as_deref(), conversation)).count() as u32;
        if exclusive && n > 0 {
            drop(registry);
            let _ = reader.conn().execute_batch("ROLLBACK");
            journal::log(
                journal::PRIORITY_INFO,
                &format!("exclusive watch by {who} refused: {n} already attached"),
                &[("SENDER", who), ("EVENT", "watch_refused")],
            );
            return send(
                w,
                Response::Error {
                    code: ErrorCode::Conflict,
                    message: format!("{who} already has {n} overlapping watcher(s); an exclusive watch was refused"),
                },
            );
        }
        registry.insert(id, view);
        n
    };
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
                other_watchers: Some(others),
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

/// Two watch filters overlap when either sees every conversation or both name the same one.
fn filters_overlap(a: Option<&str>, b: Option<&str>) -> bool {
    match (a, b) {
        (Some(x), Some(y)) => x == y,
        _ => true,
    }
}

#[cfg(test)]
mod overlap_tests {
    use super::filters_overlap;

    #[test]
    fn a_wildcard_overlaps_everything_and_named_filters_only_themselves() {
        assert!(filters_overlap(None, None));
        assert!(filters_overlap(None, Some("a")));
        assert!(filters_overlap(Some("a"), None));
        assert!(filters_overlap(Some("a"), Some("a")));
        assert!(!filters_overlap(Some("a"), Some("b")));
    }
}

#[cfg(test)]
mod directory_tests {
    use super::*;

    fn config(toml_text: &str) -> Config {
        let c: Config = toml::from_str(toml_text).unwrap();
        c.validate().unwrap();
        c
    }

    fn endpoint(name: &str) -> EndpointView {
        EndpointView { name: name.into(), session: format!("s-{name}"), exe: "/x".into(), generation: 1, registered_at_ms: 0 }
    }

    fn watcher(who: &str, at: i64) -> WatcherView {
        WatcherView { participant: who.into(), conversation: None, connected_at_ms: at, last_event_seq: 0 }
    }

    #[test]
    fn a_name_only_under_participant_is_a_participant_and_the_list_keeps_its_order() {
        let c = config("participants = [\"b\", \"a\"]\n[participant.z]\nproduct = \"codex\"\n[participant.a]\ntier = \"luna\"\n");
        assert_eq!(c.participant_names(), vec!["b", "a", "z"]);
        assert!(c.knows("z") && c.knows("a") && !c.knows("q"));
    }

    #[test]
    fn a_status_without_a_directory_is_an_older_daemon_not_an_empty_one() {
        let old = serde_json::json!({
            "store_id": "s", "head_seq": 0, "unacknowledged": [], "unacknowledged_total": 0, "next_unacknowledged_after": null,
            "unanswered": [], "unanswered_total": 0, "next_unanswered_after": null, "uncertain_or_failed_submissions": [],
            "uncertain_or_failed_total": 0, "endpoints": [], "watchers": []
        });
        let status: crate::proto::StatusView = serde_json::from_value(old).unwrap();
        assert!(status.participants.is_none());
        let mut current = serde_json::to_value(&status).unwrap();
        current["participants"] = serde_json::json!([]);
        let status: crate::proto::StatusView = serde_json::from_value(current).unwrap();
        assert_eq!(status.participants, Some(vec![]), "an empty directory is still a directory");
    }

    #[test]
    fn the_system_sender_cannot_be_configured() {
        for text in ["participants = [\"claude\", \"chatd\"]\n", "[participant.chatd]\nproduct = \"codex\"\n"] {
            let c: Config = toml::from_str(text).unwrap();
            assert!(c.validate().unwrap_err().contains("reserved"), "{text}");
        }
        let c = config("health_interval_ms = 0\nhealth_stall_ms = 5\ncodex_sessions_dir = \"/x/sessions\"\n");
        assert_eq!((c.health_interval_ms, c.health_stall_ms, c.codex_sessions()), (0, 5, PathBuf::from("/x/sessions")));
        let d = Config::default();
        assert_eq!((d.health_interval_ms, d.health_stall_ms), (300_000, 1_800_000));
    }

    #[test]
    fn a_duplicate_in_the_list_does_not_make_two_rows() {
        let c = config("participants = [\"a\", \"b\", \"a\"]\n[participant.b]\nproduct = \"claude\"\n");
        assert_eq!(c.participant_names(), vec!["a", "b"]);
        assert_eq!(directory(&c, &[], &[], &BTreeMap::new()).len(), 2);
    }

    #[test]
    fn tokens_are_short_lowercase_words() {
        for ok in ["claude", "codex", "luna", "gpt-6_luna", "a", "0x"] {
            assert!(valid_token(ok), "{ok}");
        }
        for bad in ["", "Claude", "-lead", "has space", "ü", &"x".repeat(33)] {
            assert!(!valid_token(bad), "{bad:?}");
        }
        let bad: Config = toml::from_str("[participant.c1]\nproduct = \"Claude\"\n").unwrap();
        assert!(bad.validate().unwrap_err().contains("participant.c1.product"));
        assert!(toml::from_str::<Config>("[participant.c1]\nmodel = \"x\"\n").is_err(), "unknown fields are refused");
    }

    #[test]
    fn wake_kinds_default_to_requests_accept_kinds_or_all_and_refuse_anything_else() {
        let c = config(
            "[participant.x1]\nproduct = \"codex\"\n[participant.x2]\nwake_kinds = [\"final\", \"request\"]\n[participant.x3]\nwake_kinds = [\"all\"]\n",
        );
        assert_eq!(c.wake_kinds("x1"), vec![Kind::Request], "the default");
        assert_eq!(c.wake_kinds("claude"), vec![Kind::Request], "a name with no table gets the default too");
        assert_eq!(c.wake_kinds("x2"), vec![Kind::Request, Kind::Final]);
        assert_eq!(c.wake_kinds("x3"), Kind::ALL.to_vec());
        let same = config("[participant.x]\nwake_kinds = [\"request\", \"info\", \"progress\", \"final\"]\n");
        assert_eq!(same.wake_kinds("x"), Kind::ALL.to_vec(), "listing every kind is the same as all");
        for (text, needle) in [
            ("[participant.x]\nwake_kinds = [\"request\", \"reply\"]\n", "participant.x.wake_kinds: \"reply\" is not a message kind"),
            ("[participant.x]\nwake_kinds = [\"Request\"]\n", "\"Request\" is not a message kind"),
            ("[participant.x]\nwake_kinds = []\n", "participant.x.wake_kinds is empty"),
        ] {
            let bad: Config = toml::from_str(text).unwrap();
            let e = bad.validate().unwrap_err();
            assert!(e.contains(needle), "{e}");
        }
        assert!(toml::from_str::<Config>("[participant.x]\nwake_kinds = \"all\"\n").is_err(), "a list, not a string");
        let rows = directory(&c, &[endpoint("x2")], &[], &BTreeMap::new());
        let by = |n: &str| rows.iter().find(|r| r.name == n).unwrap();
        assert_eq!(by("x2").wake_kinds, Some(vec![Kind::Request, Kind::Final]));
        assert_eq!(by("x1").wake_kinds, None, "only a push participant has a wake policy");
    }

    #[test]
    fn push_has_a_path_with_an_endpoint_and_pull_with_a_watcher() {
        let c = config(
            "participants = [\"c1\", \"x1\", \"x2\", \"c2\"]\n[participant.c1]\nproduct = \"claude\"\ntier = \"luna\"\n[participant.x1]\nproduct = \"codex\"\n[participant.x2]\nproduct = \"codex\"\n",
        );
        let rows = directory(&c, &[endpoint("x1")], &[watcher("c1", 50), watcher("c1", 10)], &BTreeMap::new());
        let by = |n: &str| rows.iter().find(|r| r.name == n).unwrap();
        let (c1, x1, x2, c2) = (by("c1"), by("x1"), by("x2"), by("c2"));
        assert!(c1.path_ready && c1.transport == "pull" && c1.watchers == 2 && c1.watching_since_ms == Some(10));
        assert_eq!(
            (c1.product.as_deref(), c1.tier.as_deref(), c1.expected_transport.as_deref()),
            (Some("claude"), Some("luna"), Some("pull"))
        );
        assert!(x1.path_ready && x1.transport == "push" && x1.session.as_deref() == Some("s-x1"));
        assert!(
            !x2.path_ready && x2.transport == "pull" && x2.expected_transport.as_deref() == Some("push"),
            "a codex with no endpoint has no path"
        );
        assert!(!c2.path_ready && c2.product.is_none() && c2.expected_transport.is_none());
    }
}
