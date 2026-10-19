//! Participant health: the daemon periodically reads the tail of each inspected participant's session
//! log and derives a state (OK, FILTERED, ERRORED, STALLED, UNKNOWN). A state change is stored as a
//! durable info message from the reserved sender `chatd` to every other participant; an unchanged
//! state is never repeated, across restarts too (the last reported state is kept in the store).
//!
//! Only local files are read, and only their tails. A product is supported by an `Inspector`, which
//! finds a session's log and reads its last turn; `inspector_for` maps a product to one. Codex is the
//! only one so far. Claude Code keeps its logs at `~/.claude/projects/<project-dir>/<session-id>.jsonl`,
//! but a Claude participant pulls and registers no endpoint, so chatd has no session id to locate it
//! by; adding it needs that id (for example a `session` field under `[participant.*]`) and an
//! `Inspector` that recognizes the end of a Claude turn.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::journal;
use crate::proto::{HealthState, HealthView};
use crate::server::{Config, Shared};
use crate::store::{SYSTEM_SENDER, now_ms};

/// The error name that marks a turn blocked by the provider's content filter.
pub const FILTER_ERROR: &str = "cyber_policy";
/// Bytes read per step while scanning a log backward.
pub const TAIL_CHUNK: usize = 64 * 1024;
/// The most of a log's end that one check reads. A turn end further back is not looked for.
pub const TAIL_MAX: u64 = 8 * 1024 * 1024;
/// A longer line is skipped unread (turn-end lines are small; tool output lines can be huge).
const MAX_LINE: usize = 1024 * 1024;

/// How the newest turn in a log's tail ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LastTurn {
    /// No turn end within the scanned tail.
    NotSeen,
    Completed {
        at_ms: Option<i64>,
    },
    /// The turn ended with an error; `error` is its name (`cyber_policy`, ...).
    Failed {
        error: String,
        at_ms: Option<i64>,
    },
}

/// Reads one product's session logs.
pub trait Inspector: Send + Sync {
    /// The session's log, if it exists.
    fn locate(&self, session: &str) -> Option<PathBuf>;
    /// How the newest turn in the log ended. Reads only the tail.
    fn last_turn(&self, log: &Path) -> io::Result<LastTurn>;
}

/// The inspector for a product, if there is one.
pub fn inspector_for(product: &str, config: &Config) -> Option<Box<dyn Inspector>> {
    match product {
        "codex" => Some(Box::new(CodexInspector { sessions: config.codex_sessions() })),
        _ => None,
    }
}

/// Codex rollout logs: `<sessions>/YYYY/MM/DD/rollout-<timestamp>-<session-id>.jsonl`, JSON lines. A
/// turn ends with an `event_msg` whose payload type is `task_complete`; a failed one carries
/// `payload.error.codex_error_info`.
pub struct CodexInspector {
    pub sessions: PathBuf,
}

fn subdirs(dir: &Path) -> Vec<PathBuf> {
    let Ok(rd) = std::fs::read_dir(dir) else { return Vec::new() };
    rd.filter_map(Result::ok).filter(|e| e.file_type().is_ok_and(|t| t.is_dir())).map(|e| e.path()).collect()
}

impl Inspector for CodexInspector {
    /// The most recently written file named for the session (a resumed session may have several).
    fn locate(&self, session: &str) -> Option<PathBuf> {
        let suffix = format!("-{session}.jsonl");
        let mut best: Option<(std::time::SystemTime, PathBuf)> = None;
        for year in subdirs(&self.sessions) {
            for month in subdirs(&year) {
                for day in subdirs(&month) {
                    let Ok(rd) = std::fs::read_dir(&day) else { continue };
                    for e in rd.filter_map(Result::ok) {
                        let name = e.file_name();
                        let name = name.to_string_lossy();
                        if !(name.starts_with("rollout-") && name.ends_with(&suffix)) {
                            continue;
                        }
                        let Ok(modified) = e.metadata().and_then(|m| m.modified()) else { continue };
                        if best.as_ref().is_none_or(|(t, _)| modified > *t) {
                            best = Some((modified, e.path()));
                        }
                    }
                }
            }
        }
        best.map(|(_, p)| p)
    }

    fn last_turn(&self, log: &Path) -> io::Result<LastTurn> {
        scan_last_turn(log, TAIL_CHUNK, TAIL_MAX)
    }
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

/// The turn end on one Codex log line, if it is one. A line that does not parse (a write in progress)
/// is not one.
pub fn codex_turn_end(line: &[u8]) -> Option<LastTurn> {
    if !contains(line, b"\"task_complete\"") {
        return None;
    }
    let v: Value = serde_json::from_slice(line).ok()?;
    let payload = v.get("payload")?;
    if payload.get("type")?.as_str()? != "task_complete" {
        return None;
    }
    let at_ms = v.get("timestamp").and_then(Value::as_str).and_then(parse_utc);
    match payload.get("error") {
        None | Some(Value::Null) => Some(LastTurn::Completed { at_ms }),
        Some(e) => {
            let error = match e.get("codex_error_info") {
                Some(Value::String(s)) => s.clone(),
                // An error variant with data serializes as {"name": {...}}.
                Some(Value::Object(m)) => m.keys().next().cloned().unwrap_or_else(|| "error".into()),
                _ => "error".into(),
            };
            Some(LastTurn::Failed { error, at_ms })
        }
    }
}

/// Scans `path` backward from its end, `chunk` bytes at a time and at most `max_scan` bytes in all,
/// for the newest Codex turn end. Memory stays bounded by `chunk` plus one line of at most MAX_LINE.
pub fn scan_last_turn(path: &Path, chunk: usize, max_scan: u64) -> io::Result<LastTurn> {
    let mut f = File::open(path)?;
    let len = f.metadata()?.len();
    let floor = len.saturating_sub(max_scan);
    let mut pos = len;
    let mut buf = vec![0u8; chunk.max(1)];
    // The start of the file's region not yet split into lines is the END of a line whose beginning lies
    // further back; `pending` holds those bytes (they follow whatever the next chunk contributes).
    let mut pending: Vec<u8> = Vec::new();
    let mut oversized = false;
    let mut line = Vec::new();
    while pos > floor {
        let n = (buf.len() as u64).min(pos - floor) as usize;
        pos -= n as u64;
        f.seek(SeekFrom::Start(pos))?;
        f.read_exact(&mut buf[..n])?;
        let data = &buf[..n];
        let mut end = n;
        for i in (0..n).rev() {
            if data[i] != b'\n' {
                continue;
            }
            if !oversized {
                line.clear();
                line.extend_from_slice(&data[i + 1..end]);
                line.extend_from_slice(&pending);
                if let Some(t) = codex_turn_end(&line) {
                    return Ok(t);
                }
            }
            pending.clear();
            oversized = false;
            end = i;
        }
        if !oversized {
            if pending.len() + end > MAX_LINE {
                oversized = true;
                pending.clear();
            } else {
                let mut p = data[..end].to_vec();
                p.extend_from_slice(&pending);
                pending = p;
            }
        }
    }
    // The file's first line is whole only if the scan reached the start of the file.
    if pos == 0
        && !oversized
        && let Some(t) = codex_turn_end(&pending)
    {
        return Ok(t);
    }
    Ok(LastTurn::NotSeen)
}

/// What a check found for one session.
#[derive(Debug, Clone, PartialEq)]
pub enum Observation {
    /// No log for the session.
    Missing,
    /// The log exists but could not be read.
    Unreadable {
        log: PathBuf,
        error: String,
    },
    Found {
        log: PathBuf,
        modified_ms: i64,
        last_turn: LastTurn,
    },
}

pub fn observe(inspector: &dyn Inspector, session: &str) -> Observation {
    let Some(log) = inspector.locate(session) else { return Observation::Missing };
    let modified_ms = match std::fs::metadata(&log).and_then(|m| m.modified()) {
        Ok(t) => t.duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0),
        Err(e) => return Observation::Unreadable { log, error: e.to_string() },
    };
    match inspector.last_turn(&log) {
        Ok(last_turn) => Observation::Found { log, modified_ms, last_turn },
        Err(e) => Observation::Unreadable { log, error: e.to_string() },
    }
}

/// The health an observation means. `unanswered` counts requests addressed to the participant without
/// a final reply; `prev` keeps the `since` of an unchanged OK or UNKNOWN state.
pub fn classify(obs: &Observation, unanswered: i64, now: i64, stall_ms: u64, prev: Option<&HealthView>) -> HealthView {
    let (state, detail, since, log, modified) = match obs {
        Observation::Missing => (HealthState::Unknown, None, None, None, None),
        Observation::Unreadable { log, error } => (HealthState::Unknown, Some(format!("unreadable: {error}")), None, Some(log), None),
        Observation::Found { log, modified_ms, last_turn } => {
            let (state, detail, since) = match last_turn {
                LastTurn::Failed { error, at_ms } => {
                    let state = if error == FILTER_ERROR { HealthState::Filtered } else { HealthState::Errored };
                    (state, Some(error.clone()), Some(at_ms.unwrap_or(*modified_ms)))
                }
                _ if unanswered > 0 && now - modified_ms >= stall_ms as i64 => (HealthState::Stalled, None, Some(*modified_ms)),
                _ => (HealthState::Ok, None, None),
            };
            (state, detail, since, Some(log), Some(*modified_ms))
        }
    };
    let since = since.unwrap_or_else(|| match prev {
        Some(p) if p.state == state && p.detail == detail => p.since_ms,
        _ => now,
    });
    HealthView {
        state,
        detail,
        since_ms: since,
        checked_at_ms: now,
        log: log.map(|p| p.to_string_lossy().into_owned()),
        log_modified_ms: modified,
    }
}

/// The state last reported for a participant, as kept in the store.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub state: HealthState,
    pub detail: Option<String>,
}

/// Whether moving from the last reported record to `now` is reported. Only a change is; the first
/// observation of a participant is reported only when it is not OK.
pub fn should_report(prev: Option<&Record>, now: &Record) -> bool {
    match prev {
        None => now.state != HealthState::Ok,
        Some(p) => p != now,
    }
}

/// The one-line summary of a reported change.
pub fn summary(name: &str, view: &HealthView, prev: Option<&Record>) -> String {
    let since = format_utc(view.since_ms);
    match (view.state, prev) {
        (HealthState::Ok, Some(p)) => {
            let was = Record { state: p.state, detail: p.detail.clone() };
            format!("{name} OK since {since} (was {})", label(&was))
        }
        _ => format!("{name} {} since {since}", view.label()),
    }
}

fn label(r: &Record) -> String {
    match &r.detail {
        Some(d) => format!("{} ({d})", r.state.label()),
        None => r.state.label().to_string(),
    }
}

fn body(name: &str, view: &HealthView, prev: Option<&Record>, unanswered: i64, config: &Config) -> String {
    let mut b = summary(name, view, prev);
    b.push_str("\n\n");
    b.push_str(match view.state {
        HealthState::Filtered => {
            "Its last turn was blocked by the provider's content filter. It stays silent until a new turn succeeds; requests sent to it now will likely go unanswered."
        }
        HealthState::Errored => "Its last turn ended with an error. It may stay silent until a new turn starts.",
        HealthState::Stalled => "Its session log has not been written for the stall threshold while requests addressed to it are unanswered.",
        HealthState::Unknown => "Its session log was not found (or could not be read), so its health cannot be checked.",
        HealthState::Ok => "Its session log shows no failed turn.",
    });
    b.push('\n');
    if let Some(log) = &view.log {
        b.push_str(&format!("log: {log}\n"));
    }
    if let Some(m) = view.log_modified_ms {
        b.push_str(&format!("last log write: {}\n", format_utc(m)));
    }
    b.push_str(&format!("unanswered requests addressed to {name}: {unanswered}\n"));
    b.push_str(&format!(
        "checked every {}s; stall threshold {}s. Reported once per change by chatd.\n",
        config.health_interval_ms / 1000,
        config.health_stall_ms / 1000
    ));
    b
}

/// The send warning for a recipient whose last observed health is FILTERED or ERRORED.
pub fn send_warning(name: &str, view: &HealthView) -> Option<String> {
    match view.state {
        HealthState::Filtered | HealthState::Errored => Some(format!(
            "delivered, but {name}'s last turn ended with {} ({} since {})",
            view.detail.as_deref().unwrap_or("an error"),
            view.state.label(),
            format_utc(view.since_ms)
        )),
        _ => None,
    }
}

/// One health check of every participant, at `now`.
pub fn check(shared: &Arc<Shared>, now: i64) {
    let config = &shared.config;
    let names = config.participant_names();
    let read = shared.store.lock().map(|s| -> Result<_, crate::store::StoreError> {
        let endpoints = s.endpoints()?;
        let mut unanswered = BTreeMap::new();
        for n in &names {
            unanswered.insert(n.clone(), s.unanswered_to(n)?);
        }
        Ok((endpoints, unanswered))
    });
    let (endpoints, unanswered) = match read {
        Ok(Ok(x)) => x,
        Ok(Err(e)) => {
            journal::log(journal::PRIORITY_ERR, &format!("health check could not read the store: {e}"), &[]);
            return;
        }
        Err(_) => return,
    };
    for name in &names {
        let product = config.participant.get(name).and_then(|c| c.product.clone());
        // A registered endpoint is a Codex `queue` binding, so an endpoint without a product is Codex.
        let target = endpoints.iter().find(|e| &e.name == name).and_then(|ep| {
            let inspector = inspector_for(product.as_deref().unwrap_or("codex"), config)?;
            Some((ep.session.clone(), inspector))
        });
        let Some((session, inspector)) = target else {
            if let Ok(mut h) = shared.health.lock() {
                h.remove(name);
            }
            continue;
        };
        // File reads happen with no lock held.
        let obs = observe(inspector.as_ref(), &session);
        let prev = shared.health.lock().ok().and_then(|h| h.get(name).cloned());
        let waiting = unanswered.get(name).copied().unwrap_or(0);
        let view = classify(&obs, waiting, now, config.health_stall_ms, prev.as_ref());
        // The endpoint may have been unregistered or rebound while the log was read. Checked and recorded
        // under the store lock, which unregistering holds while it clears the entry, so a stale
        // observation is never put back.
        let current = {
            let Ok(store) = shared.store.lock() else { return };
            let bound = store.endpoints().is_ok_and(|eps| eps.iter().any(|e| &e.name == name && e.session == session));
            if let Ok(mut h) = shared.health.lock() {
                if bound {
                    h.insert(name.clone(), view.clone());
                } else {
                    h.remove(name);
                }
            }
            bound
        };
        if current {
            report(shared, name, &view, waiting);
        }
    }
}

/// Stores the change (and its notices to every other participant) when the state differs from the
/// last reported one.
fn report(shared: &Arc<Shared>, name: &str, view: &HealthView, unanswered: i64) {
    let now = Record { state: view.state, detail: view.detail.clone() };
    let Ok(mut store) = shared.store.lock() else { return };
    let prev = match store.health_record(name) {
        Ok(r) => r.and_then(|s| serde_json::from_str::<Record>(&s).ok()),
        Err(e) => {
            journal::log(journal::PRIORITY_ERR, &format!("health record for {name} unreadable: {e}"), &[]);
            return;
        }
    };
    if prev.as_ref() == Some(&now) {
        return;
    }
    let notices: Vec<(String, String, String)> = if should_report(prev.as_ref(), &now) {
        let summary = summary(name, view, prev.as_ref());
        let body = body(name, view, prev.as_ref(), unanswered, &shared.config);
        shared.config.participant_names().into_iter().filter(|p| p != name).map(|p| (p, summary.clone(), body.clone())).collect()
    } else {
        Vec::new()
    };
    let record = serde_json::to_string(&now).unwrap_or_default();
    match store.health_transition(name, &record, &notices) {
        Ok(receipts) => {
            let head = store.head_seq().ok();
            drop(store);
            if let Some(h) = head {
                shared.hub.committed(h);
            }
            if let Some((_, summary, _)) = notices.first() {
                let prio = if view.state == HealthState::Ok { journal::PRIORITY_NOTICE } else { journal::PRIORITY_WARNING };
                journal::log(prio, &format!("health: {summary}"), &[("SENDER", SYSTEM_SENDER), ("EVENT", "health")]);
                for (r, (to, _, _)) in receipts.iter().zip(&notices) {
                    journal::log(
                        journal::PRIORITY_INFO,
                        &format!("stored info {} {SYSTEM_SENDER}->{to}", r.id),
                        &[("SENDER", SYSTEM_SENDER), ("RECIPIENT", to), ("MESSAGE_ID", &r.id), ("EVENT", "stored")],
                    );
                }
            }
        }
        Err(e) => journal::log(journal::PRIORITY_ERR, &format!("could not record {name}'s health: {e}"), &[]),
    }
}

/// The health thread: one check at start, then one per interval, until shutdown.
pub fn run(shared: Arc<Shared>) {
    let interval = Duration::from_millis(shared.config.health_interval_ms);
    loop {
        if shared.shutdown.load(Ordering::SeqCst) {
            return;
        }
        check(&shared, now_ms());
        let until = Instant::now() + interval;
        while Instant::now() < until {
            if shared.shutdown.load(Ordering::SeqCst) {
                return;
            }
            std::thread::sleep(until.saturating_duration_since(Instant::now()).min(Duration::from_millis(200)));
        }
    }
}

/// Days since 1970-01-01 of a proleptic Gregorian date.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `2026-10-19T13:16:36Z` for a Unix time in milliseconds.
pub fn format_utc(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", rem / 3600, rem % 3600 / 60, rem % 60)
}

/// Milliseconds for an RFC 3339 UTC time such as `2026-10-19T13:16:36.687Z` (`Z` or `+00:00` only).
pub fn parse_utc(s: &str) -> Option<i64> {
    let s = s.strip_suffix('Z').or_else(|| s.strip_suffix("+00:00"))?;
    let (date, time) = s.split_once('T')?;
    let mut d = date.splitn(3, '-').map(|p| p.parse::<i64>().ok());
    let (y, mo, da) = (d.next()??, d.next()??, d.next()??);
    let (hms, frac) = time.split_once('.').unwrap_or((time, ""));
    let mut t = hms.splitn(3, ':').map(|p| p.parse::<i64>().ok());
    let (h, mi, se) = (t.next()??, t.next()??, t.next()??);
    if !(1..=12).contains(&mo) || !(1..=31).contains(&da) || h > 23 || mi > 59 || se > 60 {
        return None;
    }
    let ms = if frac.is_empty() {
        0
    } else {
        let digits: String = frac.chars().take(3).collect();
        if !digits.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        format!("{digits:0<3}").parse::<i64>().ok()?
    };
    Some(((days_from_civil(y, mo, da) * 86_400 + h * 3600 + mi * 60 + se) * 1000) + ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/codex").join(name)
    }

    fn at(s: &str) -> Option<i64> {
        parse_utc(s)
    }

    #[test]
    fn a_turn_blocked_by_the_filter_is_found_in_the_tail() {
        let t = scan_last_turn(&fixture("cyber_policy.jsonl"), TAIL_CHUNK, TAIL_MAX).unwrap();
        assert_eq!(t, LastTurn::Failed { error: "cyber_policy".into(), at_ms: at("2026-10-19T13:16:36.687Z") });
    }

    #[test]
    fn a_normal_completion_after_a_failed_turn_wins() {
        let t = scan_last_turn(&fixture("normal.jsonl"), TAIL_CHUNK, TAIL_MAX).unwrap();
        assert_eq!(t, LastTurn::Completed { at_ms: at("2026-10-19T14:02:10.500Z") });
    }

    #[test]
    fn a_truncated_last_line_is_skipped_for_the_last_whole_turn_end() {
        let t = scan_last_turn(&fixture("truncated.jsonl"), TAIL_CHUNK, TAIL_MAX).unwrap();
        assert_eq!(t, LastTurn::Failed { error: "cyber_policy".into(), at_ms: at("2026-10-19T13:05:38.659Z") });
    }

    #[test]
    fn another_error_is_named_and_an_object_error_by_its_variant() {
        let t = scan_last_turn(&fixture("errored.jsonl"), TAIL_CHUNK, TAIL_MAX).unwrap();
        assert_eq!(t, LastTurn::Failed { error: "context_window_exceeded".into(), at_ms: at("2026-10-19T15:00:00Z") });
        let line = br#"{"timestamp":"2026-10-19T15:00:00Z","type":"event_msg","payload":{"type":"task_complete","error":{"codex_error_info":{"http_connection_failed":{"status":502}}}}}"#;
        assert_eq!(
            codex_turn_end(line),
            Some(LastTurn::Failed { error: "http_connection_failed".into(), at_ms: at("2026-10-19T15:00:00Z") })
        );
    }

    #[test]
    fn a_missing_file_is_an_error_and_a_missing_session_is_not_located() {
        let e = scan_last_turn(&fixture("does-not-exist.jsonl"), TAIL_CHUNK, TAIL_MAX).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::NotFound);
        let codex = CodexInspector { sessions: fixture("no-such-sessions-dir") };
        assert_eq!(codex.locate("01a1543d-04ce-7070-a653-6d2fa85feaa9"), None);
        assert_eq!(observe(&codex, "x"), Observation::Missing);
        let h = classify(&Observation::Missing, 3, 1000, 10, None);
        assert_eq!((h.state, h.since_ms), (HealthState::Unknown, 1000));
    }

    #[test]
    fn every_chunk_size_and_a_short_window_give_the_same_answer() {
        for name in ["cyber_policy.jsonl", "normal.jsonl", "truncated.jsonl", "errored.jsonl"] {
            let whole = scan_last_turn(&fixture(name), TAIL_CHUNK, TAIL_MAX).unwrap();
            for chunk in [1, 2, 7, 64, 100, 333, 4096] {
                assert_eq!(scan_last_turn(&fixture(name), chunk, TAIL_MAX).unwrap(), whole, "{name} chunk {chunk}");
            }
        }
        // A window that ends before any turn end sees none.
        assert_eq!(scan_last_turn(&fixture("cyber_policy.jsonl"), 64, 40).unwrap(), LastTurn::NotSeen);
    }

    #[test]
    fn a_huge_line_is_skipped_without_being_held() {
        let dir = std::env::temp_dir().join(format!("chatd-health-{}-{}", std::process::id(), uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("big.jsonl");
        let mut text = std::fs::read_to_string(fixture("cyber_policy.jsonl")).unwrap();
        text.push_str(&format!("{{\"type\":\"response_item\",\"payload\":{{\"output\":\"{}\"}}}}\n", "x".repeat(MAX_LINE + 10)));
        std::fs::write(&p, text).unwrap();
        let t = scan_last_turn(&p, 4096, TAIL_MAX).unwrap();
        assert!(matches!(t, LastTurn::Failed { ref error, .. } if error == "cyber_policy"), "{t:?}");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn the_session_log_is_located_by_its_id() {
        let dir = std::env::temp_dir().join(format!("chatd-health-{}-{}", std::process::id(), uuid::Uuid::new_v4()));
        let day = dir.join("2026/10/19");
        std::fs::create_dir_all(&day).unwrap();
        let s = "01a1543d-04ce-7070-a653-6d2fa85feaa9";
        std::fs::write(day.join(format!("rollout-2026-10-19T07-57-27-{s}.jsonl")), "").unwrap();
        std::fs::write(day.join("rollout-2026-10-19T07-57-27-other.jsonl"), "").unwrap();
        let codex = CodexInspector { sessions: dir.clone() };
        assert_eq!(codex.locate(s), Some(day.join(format!("rollout-2026-10-19T07-57-27-{s}.jsonl"))));
        assert_eq!(codex.locate("04ce-7070"), None, "only the whole id at the end of the name matches");
        let _ = std::fs::remove_dir_all(dir);
    }

    fn found(modified_ms: i64, last_turn: LastTurn) -> Observation {
        Observation::Found { log: "/l".into(), modified_ms, last_turn }
    }

    #[test]
    fn states_are_derived_in_order_filter_error_stall_ok() {
        let minute = 60_000;
        let now = 100 * minute;
        let filtered = found(now, LastTurn::Failed { error: "cyber_policy".into(), at_ms: Some(5) });
        let h = classify(&filtered, 0, now, 30 * minute as u64, None);
        assert_eq!((h.state, h.detail.as_deref(), h.since_ms), (HealthState::Filtered, Some("cyber_policy"), 5));
        let errored = found(0, LastTurn::Failed { error: "other".into(), at_ms: None });
        assert_eq!(classify(&errored, 9, now, 1, None).state, HealthState::Errored, "an error wins over a stall");
        let quiet = found(now - 31 * minute, LastTurn::Completed { at_ms: None });
        let stalled = classify(&quiet, 1, now, 30 * minute as u64, None);
        assert_eq!((stalled.state, stalled.since_ms), (HealthState::Stalled, now - 31 * minute));
        assert_eq!(classify(&quiet, 0, now, 30 * minute as u64, None).state, HealthState::Ok, "quiet with nothing to answer is fine");
        let recent = found(now - 29 * minute, LastTurn::NotSeen);
        let ok = classify(&recent, 4, now, 30 * minute as u64, None);
        assert_eq!((ok.state, ok.since_ms), (HealthState::Ok, now));
        let later = classify(&recent, 4, now + 5, 30 * minute as u64, Some(&ok));
        assert_eq!(later.since_ms, now, "an unchanged state keeps its start");
    }

    #[test]
    fn only_changes_are_reported_and_a_first_ok_is_silent() {
        let r = |state, detail: Option<&str>| Record { state, detail: detail.map(str::to_string) };
        assert!(!should_report(None, &r(HealthState::Ok, None)));
        assert!(should_report(None, &r(HealthState::Filtered, Some("cyber_policy"))));
        assert!(should_report(None, &r(HealthState::Unknown, None)));
        let f = r(HealthState::Filtered, Some("cyber_policy"));
        assert!(!should_report(Some(&f), &f.clone()));
        assert!(should_report(Some(&f), &r(HealthState::Errored, Some("other"))));
        assert!(should_report(Some(&f), &r(HealthState::Ok, None)), "recovery is reported");
    }

    #[test]
    fn utc_round_trips() {
        assert_eq!(format_utc(0), "1970-01-01T00:00:00Z");
        let ms = parse_utc("2026-10-19T13:16:36.687Z").unwrap();
        assert_eq!(ms, 1_792_415_796_687);
        assert_eq!(format_utc(ms), "2026-10-19T13:16:36Z");
        assert_eq!(parse_utc("2024-02-29T23:59:59+00:00").map(format_utc).as_deref(), Some("2024-02-29T23:59:59Z"));
        assert_eq!(parse_utc("2026-10-19T13:16:36.5Z"), Some(parse_utc("2026-10-19T13:16:36Z").unwrap() + 500));
        for bad in ["", "2026-10-19", "2026-13-01T00:00:00Z", "2026-10-19T13:16:36+02:00", "2026-10-19T13:16:36.x1Z"] {
            assert_eq!(parse_utc(bad), None, "{bad}");
        }
    }
}
