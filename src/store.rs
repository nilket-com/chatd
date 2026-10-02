//! The durable store: SQLite in WAL mode with `synchronous=FULL`. Every mutation and the event that
//! announces it commit in one transaction; a receipt is returned only after that commit.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::json;

use crate::proto::{
    AttemptView, EndpointView, ErrorCode, EventView, Kind, MAX_BODY, MAX_CONVERSATION_LEN, MAX_KEY_LEN, MAX_LOG_PAGE, MAX_PAGE,
    MessageView, Receipt, SendReq, StatusLine, StatusView,
};
use crate::text;

pub const SCHEMA_VERSION: i64 = 1;
/// Failed submissions per message (since its last explicit redelivery) before giving up.
pub const MAX_FAILED_ATTEMPTS: i64 = 3;
/// Default backoff after the n-th failed submission (1-based), in milliseconds.
pub const RETRY_BACKOFF_MS: [u64; 3] = [5_000, 30_000, 120_000];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreError {
    pub code: ErrorCode,
    pub message: String,
}

impl StoreError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        StoreError { code, message: message.into() }
    }
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for StoreError {}

impl From<rusqlite::Error> for StoreError {
    fn from(e: rusqlite::Error) -> Self {
        StoreError::new(ErrorCode::Storage, e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, StoreError>;

pub fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

const SCHEMA: &str = "
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;
CREATE TABLE participants (name TEXT PRIMARY KEY) STRICT;
CREATE TABLE events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    at_ms INTEGER NOT NULL,
    kind TEXT NOT NULL,
    audience TEXT NOT NULL,
    message_id TEXT,
    conversation TEXT,
    detail TEXT NOT NULL
) STRICT;
CREATE INDEX events_audience ON events (audience, seq);
CREATE TABLE messages (
    id TEXT PRIMARY KEY,
    seq INTEGER NOT NULL UNIQUE REFERENCES events (seq),
    conversation TEXT NOT NULL,
    sender TEXT NOT NULL REFERENCES participants (name),
    recipient TEXT NOT NULL REFERENCES participants (name),
    kind TEXT NOT NULL CHECK (kind IN ('request', 'info', 'progress', 'final')),
    reply_to TEXT REFERENCES messages (id),
    summary TEXT NOT NULL,
    body TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    UNIQUE (sender, idempotency_key)
) STRICT;
CREATE INDEX messages_recipient ON messages (recipient, seq);
CREATE INDEX messages_reply_to ON messages (reply_to);
CREATE TABLE acks (
    reader TEXT NOT NULL,
    message_id TEXT NOT NULL REFERENCES messages (id),
    at_ms INTEGER NOT NULL,
    PRIMARY KEY (reader, message_id)
) STRICT;
CREATE TABLE endpoints (
    name TEXT PRIMARY KEY REFERENCES participants (name),
    session TEXT NOT NULL,
    exe TEXT NOT NULL,
    generation INTEGER NOT NULL,
    registered_at_ms INTEGER NOT NULL
) STRICT;
CREATE TABLE attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT NOT NULL REFERENCES messages (id),
    endpoint TEXT NOT NULL,
    generation INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('started', 'submitted', 'failed', 'uncertain')),
    started_at_ms INTEGER NOT NULL,
    finished_at_ms INTEGER,
    exit_code INTEGER,
    error TEXT,
    next_retry_at_ms INTEGER
) STRICT;
CREATE INDEX attempts_message ON attempts (message_id, id);
CREATE TABLE redeliveries (
    message_id TEXT PRIMARY KEY REFERENCES messages (id),
    after_attempt INTEGER NOT NULL,
    at_ms INTEGER NOT NULL
) STRICT;
CREATE TABLE legacy_sources (
    sha256 TEXT PRIMARY KEY,
    path TEXT NOT NULL,
    bytes INTEGER NOT NULL,
    imported_at_ms INTEGER NOT NULL
) STRICT;
CREATE TABLE legacy_blocks (
    source_sha256 TEXT NOT NULL REFERENCES legacy_sources (sha256),
    start_offset INTEGER NOT NULL,
    end_offset INTEGER NOT NULL,
    header TEXT NOT NULL,
    sender TEXT,
    malformed INTEGER NOT NULL,
    raw TEXT NOT NULL,
    PRIMARY KEY (source_sha256, start_offset)
) STRICT;
";

pub struct Store {
    conn: Connection,
}

pub struct Delivery {
    pub message: MessageView,
    pub endpoint: EndpointView,
}

fn kind_of(s: &str) -> Result<Kind> {
    Kind::parse(s).ok_or_else(|| StoreError::new(ErrorCode::Storage, format!("stored kind {s:?} is not valid")))
}

fn valid_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 32
        && s.bytes().enumerate().all(|(i, b)| b.is_ascii_lowercase() || (i > 0 && (b.is_ascii_digit() || b == b'-')))
}

fn valid_token(s: &str, max: usize) -> bool {
    !s.is_empty() && s.len() <= max && s.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b))
}

fn valid_id(s: &str) -> bool {
    uuid::Uuid::parse_str(s).is_ok()
}

impl Store {
    /// Opens (creating if absent) and configures a connection. Does not migrate.
    pub fn open(path: &Path) -> Result<Store> {
        let conn = Connection::open(path)?;
        conn.busy_timeout(std::time::Duration::from_secs(10))?;
        let mode: String = conn.query_row("PRAGMA journal_mode = WAL", [], |r| r.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(StoreError::new(ErrorCode::Storage, format!("journal_mode is {mode}, not wal")));
        }
        conn.execute_batch("PRAGMA synchronous = FULL; PRAGMA foreign_keys = ON;")?;
        Ok(Store { conn })
    }

    /// Creates the schema on a new store, refuses an unknown (newer) schema, and records the
    /// participants from the configuration (participants are only ever added).
    pub fn migrate(&mut self, participants: &[String]) -> Result<()> {
        let tx = self.conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let has_meta: bool =
            tx.query_row("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='meta'", [], |r| r.get::<_, i64>(0))? > 0;
        if !has_meta {
            tx.execute_batch(SCHEMA)?;
            tx.execute("INSERT INTO meta (key, value) VALUES ('schema_version', ?1)", [SCHEMA_VERSION.to_string()])?;
            tx.execute("INSERT INTO meta (key, value) VALUES ('store_id', ?1)", [uuid::Uuid::new_v4().to_string()])?;
            tx.execute("INSERT INTO meta (key, value) VALUES ('generation', '1')", [])?;
        }
        let version: i64 = tx
            .query_row("SELECT value FROM meta WHERE key='schema_version'", [], |r| r.get::<_, String>(0))?
            .parse()
            .map_err(|_| StoreError::new(ErrorCode::Storage, "schema_version is not an integer"))?;
        if version != SCHEMA_VERSION {
            return Err(StoreError::new(
                ErrorCode::Storage,
                format!("schema version {version} is not supported (this build: {SCHEMA_VERSION}); the database is left untouched"),
            ));
        }
        for p in participants {
            if !valid_name(p) {
                return Err(StoreError::new(ErrorCode::BadRequest, format!("participant name {p:?} is not valid")));
            }
            tx.execute("INSERT OR IGNORE INTO participants (name) VALUES (?1)", [p])?;
        }
        tx.commit()?;
        Ok(())
    }

    /// A durable write that proves the store is writable (part of readiness).
    pub fn write_check(&mut self) -> Result<()> {
        let tx = self.conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO meta (key, value) VALUES ('last_start_ms', ?1) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            [now_ms().to_string()],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn store_id(&self) -> Result<String> {
        Ok(self.conn.query_row("SELECT value FROM meta WHERE key='store_id'", [], |r| r.get(0))?)
    }

    pub fn generation(&self) -> Result<i64> {
        let g: String = self.conn.query_row("SELECT value FROM meta WHERE key='generation'", [], |r| r.get(0))?;
        g.parse().map_err(|_| StoreError::new(ErrorCode::Storage, "generation is not an integer"))
    }

    pub fn head_seq(&self) -> Result<i64> {
        Ok(self.conn.query_row("SELECT coalesce(max(seq), 0) FROM events", [], |r| r.get(0))?)
    }

    fn require_participant(&self, name: &str) -> Result<()> {
        let n: i64 = self.conn.query_row("SELECT count(*) FROM participants WHERE name = ?1", [name], |r| r.get(0))?;
        if n == 0 {
            return Err(StoreError::new(ErrorCode::UnknownParticipant, format!("unknown participant {name:?}")));
        }
        Ok(())
    }

    fn message_row(&self, id: &str) -> Result<Option<MessageView>> {
        self.conn
            .query_row(
                "SELECT m.id, m.seq, m.conversation, m.sender, m.recipient, m.kind, m.reply_to, m.summary, m.created_at_ms,
                        m.idempotency_key, m.body,
                        (SELECT at_ms FROM acks a WHERE a.reader = m.recipient AND a.message_id = m.id),
                        (SELECT r.id FROM messages r WHERE r.reply_to = m.id AND r.kind = 'final' ORDER BY r.seq LIMIT 1)
                 FROM messages m WHERE m.id = ?1",
                [id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, String>(4)?,
                        r.get::<_, String>(5)?,
                        r.get::<_, Option<String>>(6)?,
                        r.get::<_, String>(7)?,
                        r.get::<_, i64>(8)?,
                        r.get::<_, String>(9)?,
                        r.get::<_, String>(10)?,
                        r.get::<_, Option<i64>>(11)?,
                        r.get::<_, Option<String>>(12)?,
                    ))
                },
            )
            .optional()?
            .map(|t| -> Result<MessageView> {
                Ok(MessageView {
                    id: t.0,
                    seq: t.1,
                    conversation: t.2,
                    sender: t.3,
                    recipient: t.4,
                    kind: kind_of(&t.5)?,
                    reply_to: t.6,
                    summary: t.7,
                    created_at_ms: t.8,
                    idempotency_key: t.9,
                    body: Some(t.10),
                    received_at_ms: t.11,
                    answered_by: t.12,
                })
            })
            .transpose()
    }

    fn view_without_body(&self, id: &str) -> Result<MessageView> {
        let mut m = self.message_row(id)?.ok_or_else(|| StoreError::new(ErrorCode::NotFound, format!("no message {id}")))?;
        m.body = None;
        Ok(m)
    }

    fn receipt_for(&self, sender: &str, key: &str) -> Result<Option<(Receipt, MessageView)>> {
        let id: Option<String> = self
            .conn
            .query_row("SELECT id FROM messages WHERE sender = ?1 AND idempotency_key = ?2", [sender, key], |r| r.get(0))
            .optional()?;
        match id {
            None => Ok(None),
            Some(id) => {
                let m = self.message_row(&id)?.ok_or_else(|| StoreError::new(ErrorCode::Storage, "message vanished"))?;
                Ok(Some((Receipt { id: m.id.clone(), seq: m.seq, idempotency_key: key.to_string(), duplicate: true }, m)))
            }
        }
    }

    /// Stores a message (or reply). Identical retries with the same sender/key return the original
    /// receipt; the same key with different content refuses.
    pub fn send(&mut self, req: &SendReq) -> Result<Receipt> {
        self.require_participant(&req.from)?;
        if !valid_token(&req.idempotency_key, MAX_KEY_LEN) {
            return Err(StoreError::new(ErrorCode::BadRequest, "idempotency key must be 1-128 of [A-Za-z0-9-_.:]"));
        }
        if req.body.len() > MAX_BODY {
            return Err(StoreError::new(ErrorCode::TooLarge, format!("body is {} bytes; the limit is {MAX_BODY}", req.body.len())));
        }
        let summary = text::summary_of(req.summary.as_deref(), &req.body);

        // Resolve recipient and conversation.
        let (recipient, conversation) = if req.kind.is_reply() {
            let target = req
                .reply_to
                .as_deref()
                .ok_or_else(|| StoreError::new(ErrorCode::InvalidReply, "a progress or final reply needs --reply-to"))?;
            if !valid_id(target) {
                return Err(StoreError::new(ErrorCode::InvalidReply, "reply_to is not a message id"));
            }
            let orig = self
                .message_row(target)?
                .ok_or_else(|| StoreError::new(ErrorCode::InvalidReply, format!("reply_to {target} does not exist")))?;
            if orig.kind != Kind::Request {
                return Err(StoreError::new(
                    ErrorCode::InvalidReply,
                    format!("reply_to {target} is a {}, not a request", orig.kind.as_str()),
                ));
            }
            if orig.recipient != req.from {
                return Err(StoreError::new(
                    ErrorCode::InvalidReply,
                    format!("only {} (the request's recipient) can reply to it", orig.recipient),
                ));
            }
            if let Some(to) = &req.to
                && *to != orig.sender
            {
                return Err(StoreError::new(ErrorCode::InvalidReply, format!("a reply goes to the request's sender {}", orig.sender)));
            }
            if let Some(c) = &req.conversation
                && *c != orig.conversation
            {
                return Err(StoreError::new(ErrorCode::InvalidReply, format!("the request is in conversation {}", orig.conversation)));
            }
            (orig.sender, orig.conversation)
        } else {
            if req.reply_to.is_some() {
                return Err(StoreError::new(ErrorCode::BadRequest, "only progress and final replies take reply_to"));
            }
            let to = req.to.clone().ok_or_else(|| StoreError::new(ErrorCode::BadRequest, "a request or info message needs --to"))?;
            let conv = req
                .conversation
                .clone()
                .ok_or_else(|| StoreError::new(ErrorCode::BadRequest, "a request or info message needs --conversation"))?;
            (to, conv)
        };
        self.require_participant(&recipient)?;
        if recipient == req.from {
            return Err(StoreError::new(ErrorCode::BadRequest, "sender and recipient are the same"));
        }
        if !valid_token(&conversation, MAX_CONVERSATION_LEN) {
            return Err(StoreError::new(ErrorCode::BadRequest, "conversation must be 1-64 of [A-Za-z0-9-_.:]"));
        }

        if let Some((receipt, m)) = self.receipt_for(&req.from, &req.idempotency_key)? {
            let same = m.recipient == recipient
                && m.conversation == conversation
                && m.kind == req.kind
                && m.reply_to == req.reply_to
                && m.summary == summary
                && m.body.as_deref() == Some(req.body.as_str());
            return if same {
                Ok(receipt)
            } else {
                Err(StoreError::new(
                    ErrorCode::Conflict,
                    format!("idempotency key {} already names message {} with different content", req.idempotency_key, m.id),
                ))
            };
        }

        let id = uuid::Uuid::now_v7().to_string();
        let now = now_ms();
        let tx = self.conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO events (at_ms, kind, audience, message_id, conversation, detail) VALUES (?1, 'message_stored', ?2, ?3, ?4, ?5)",
            params![
                now,
                recipient,
                id,
                conversation,
                json!({"sender": req.from, "kind": req.kind.as_str(), "summary": summary, "reply_to": req.reply_to}).to_string()
            ],
        )?;
        let seq = tx.last_insert_rowid();
        tx.execute(
            "INSERT INTO messages (id, seq, conversation, sender, recipient, kind, reply_to, summary, body, idempotency_key, created_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                id,
                seq,
                conversation,
                req.from,
                recipient,
                req.kind.as_str(),
                req.reply_to,
                summary,
                req.body,
                req.idempotency_key,
                now
            ],
        )?;
        if let Some(target) = &req.reply_to {
            // A reply acknowledges receipt of the request it answers (idempotent).
            let fresh =
                tx.execute("INSERT OR IGNORE INTO acks (reader, message_id, at_ms) VALUES (?1, ?2, ?3)", params![req.from, target, now])?;
            if fresh == 1 {
                tx.execute(
                    "INSERT INTO events (at_ms, kind, audience, message_id, conversation, detail) VALUES (?1, 'received', ?2, ?3, ?4, ?5)",
                    params![now, recipient, target, conversation, json!({"by": req.from, "via": "reply"}).to_string()],
                )?;
            }
            if req.kind == Kind::Final {
                tx.execute(
                    "INSERT INTO events (at_ms, kind, audience, message_id, conversation, detail) VALUES (?1, 'answered', ?2, ?3, ?4, ?5)",
                    params![now, recipient, target, conversation, json!({"by": req.from, "reply": id}).to_string()],
                )?;
            }
        }
        tx.commit()?;
        Ok(Receipt { id, seq, idempotency_key: req.idempotency_key.clone(), duplicate: false })
    }

    pub fn lookup(&self, sender: &str, key: &str) -> Result<Receipt> {
        self.receipt_for(sender, key)?
            .map(|(r, _)| r)
            .ok_or_else(|| StoreError::new(ErrorCode::NotFound, format!("no message from {sender} with key {key}")))
    }

    /// The full message. Inspection never acknowledges.
    pub fn receive(&self, reader: &str, id: &str) -> Result<MessageView> {
        self.require_participant(reader)?;
        if !valid_id(id) {
            return Err(StoreError::new(ErrorCode::BadRequest, "not a message id"));
        }
        let m = self.message_row(id)?.ok_or_else(|| StoreError::new(ErrorCode::NotFound, format!("no message {id}")))?;
        if m.recipient != reader && m.sender != reader {
            return Err(StoreError::new(ErrorCode::NotFound, format!("no message {id} for {reader}")));
        }
        Ok(m)
    }

    /// Acknowledges receipt by the recipient. Idempotent; never marks a request answered.
    pub fn ack(&mut self, reader: &str, ids: &[String]) -> Result<(Vec<String>, Vec<String>)> {
        self.require_participant(reader)?;
        let now = now_ms();
        let tx = self.conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (mut acked, mut already) = (Vec::new(), Vec::new());
        for id in ids {
            let row: Option<(String, String, String)> = tx
                .query_row("SELECT recipient, sender, conversation FROM messages WHERE id = ?1", [id], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                })
                .optional()?;
            let Some((recipient, sender, conversation)) = row else {
                return Err(StoreError::new(ErrorCode::NotFound, format!("no message {id}")));
            };
            if recipient != reader {
                return Err(StoreError::new(ErrorCode::BadRequest, format!("message {id} is addressed to {recipient}, not {reader}")));
            }
            let fresh =
                tx.execute("INSERT OR IGNORE INTO acks (reader, message_id, at_ms) VALUES (?1, ?2, ?3)", params![reader, id, now])?;
            if fresh == 1 {
                tx.execute(
                    "INSERT INTO events (at_ms, kind, audience, message_id, conversation, detail) VALUES (?1, 'received', ?2, ?3, ?4, ?5)",
                    params![now, sender, id, conversation, json!({"by": reader, "via": "ack"}).to_string()],
                )?;
                acked.push(id.clone());
            } else {
                already.push(id.clone());
            }
        }
        tx.commit()?;
        Ok((acked, already))
    }

    /// Messages addressed to `reader` without an acknowledgement, oldest first. Holes are kept:
    /// an acknowledged later message never hides an unacknowledged earlier one.
    /// One page of the messages addressed to `reader` without an acknowledgement, oldest first,
    /// after `after_seq`. Holes are kept: an acknowledged later message never hides an
    /// unacknowledged earlier one. Callers page with the last returned `seq`.
    pub fn unacknowledged_page(&self, reader: &str, conversation: Option<&str>, after_seq: i64, limit: u32) -> Result<Vec<MessageView>> {
        let mut stmt = self.conn.prepare(
            "SELECT m.id FROM messages m
             WHERE m.recipient = ?1 AND (?2 IS NULL OR m.conversation = ?2) AND m.seq > ?3
               AND NOT EXISTS (SELECT 1 FROM acks a WHERE a.reader = m.recipient AND a.message_id = m.id)
             ORDER BY m.seq LIMIT ?4",
        )?;
        let ids: Vec<String> = stmt
            .query_map(params![reader, conversation, after_seq, limit.min(MAX_PAGE)], |r| r.get(0))?
            .collect::<std::result::Result<_, _>>()?;
        ids.iter().map(|id| self.view_without_body(id)).collect()
    }

    pub fn unacknowledged_count(&self, reader: &str, conversation: Option<&str>) -> Result<i64> {
        Ok(self.conn.query_row(
            "SELECT count(*) FROM messages m
             WHERE m.recipient = ?1 AND (?2 IS NULL OR m.conversation = ?2)
               AND NOT EXISTS (SELECT 1 FROM acks a WHERE a.reader = m.recipient AND a.message_id = m.id)",
            params![reader, conversation],
            |r| r.get(0),
        )?)
    }

    /// Events for `audience` after `after_seq` (optionally one conversation), ascending, bounded.
    pub fn events_after(&self, audience: &str, conversation: Option<&str>, after_seq: i64, limit: u32) -> Result<Vec<EventView>> {
        let mut stmt = self.conn.prepare(
            "SELECT seq, at_ms, kind, audience, message_id, conversation, detail FROM events
             WHERE audience = ?1 AND seq > ?2 AND (?3 IS NULL OR conversation IS NULL OR conversation = ?3)
             ORDER BY seq LIMIT ?4",
        )?;
        let rows = stmt.query_map(params![audience, after_seq, conversation, limit], event_row)?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    pub fn log(&self, conversation: Option<&str>, since_seq: Option<i64>, limit: u32) -> Result<Vec<EventView>> {
        let mut stmt = self.conn.prepare(
            "SELECT seq, at_ms, kind, audience, message_id, conversation, detail FROM events
             WHERE seq > ?1 AND (?2 IS NULL OR conversation = ?2) ORDER BY seq LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![since_seq.unwrap_or(0), conversation, limit.min(MAX_LOG_PAGE)], event_row)?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    fn status_line(&self, m: &MessageView, now: i64) -> Result<StatusLine> {
        let submission: Option<String> = self
            .conn
            .query_row(
                "SELECT state || ' (attempt ' || id || ', generation ' || generation || ')' FROM attempts WHERE message_id = ?1 ORDER BY id DESC LIMIT 1",
                [&m.id],
                |r| r.get(0),
            )
            .optional()?;
        Ok(StatusLine {
            id: m.id.clone(),
            conversation: m.conversation.clone(),
            sender: m.sender.clone(),
            recipient: m.recipient.clone(),
            kind: m.kind,
            summary: m.summary.clone(),
            age_ms: now - m.created_at_ms,
            received: m.received_at_ms.is_some(),
            answered_by: m.answered_by.clone(),
            submission,
        })
    }

    /// Bounded status: each list is one page of at most `limit` (<= MAX_PAGE) items, with totals and
    /// continuation cursors, so history size never makes status unusable.
    pub fn status(
        &self,
        who: Option<&str>,
        conversation: Option<&str>,
        limit: u32,
        unacknowledged_after: Option<i64>,
        unanswered_after: Option<i64>,
    ) -> Result<StatusView> {
        if let Some(w) = who {
            self.require_participant(w)?;
        }
        let limit = limit.clamp(1, MAX_PAGE) as i64;
        let now = now_ms();
        const UNACK: &str = "FROM messages m
             WHERE (?1 IS NULL OR m.recipient = ?1) AND (?2 IS NULL OR m.conversation = ?2)
               AND NOT EXISTS (SELECT 1 FROM acks a WHERE a.reader = m.recipient AND a.message_id = m.id)";
        const UNANS: &str = "FROM messages m
             WHERE m.kind = 'request' AND (?1 IS NULL OR m.sender = ?1 OR m.recipient = ?1) AND (?2 IS NULL OR m.conversation = ?2)
               AND NOT EXISTS (SELECT 1 FROM messages r WHERE r.reply_to = m.id AND r.kind = 'final')";
        let page = |filter: &str, after: Option<i64>| -> Result<(Vec<StatusLine>, i64, Option<i64>)> {
            let total: i64 = self.conn.query_row(&format!("SELECT count(*) {filter}"), params![who, conversation], |r| r.get(0))?;
            let mut stmt = self.conn.prepare(&format!("SELECT m.id, m.seq {filter} AND m.seq > ?3 ORDER BY m.seq LIMIT ?4"))?;
            let rows: Vec<(String, i64)> = stmt
                .query_map(params![who, conversation, after.unwrap_or(0), limit + 1], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect::<std::result::Result<_, _>>()?;
            let more = rows.len() as i64 > limit;
            let mut lines = Vec::new();
            let mut last = None;
            for (id, seq) in rows.into_iter().take(limit as usize) {
                lines.push(self.status_line(&self.view_without_body(&id)?, now)?);
                last = Some(seq);
            }
            Ok((lines, total, if more { last } else { None }))
        };
        let (unack, unack_total, next_unack) = page(UNACK, unacknowledged_after)?;
        let (unanswered, unans_total, next_unans) = page(UNANS, unanswered_after)?;
        const ATTEMPTS: &str = "FROM attempts a JOIN messages m ON m.id = a.message_id
             WHERE a.id = (SELECT max(id) FROM attempts b WHERE b.message_id = a.message_id)
               AND a.state IN ('failed', 'uncertain', 'started')
               AND (?1 IS NULL OR m.sender = ?1 OR m.recipient = ?1) AND (?2 IS NULL OR m.conversation = ?2)";
        let attempts_total: i64 = self.conn.query_row(&format!("SELECT count(*) {ATTEMPTS}"), params![who, conversation], |r| r.get(0))?;
        let mut stmt = self.conn.prepare(&format!(
            "SELECT a.id, a.message_id, a.endpoint, a.generation, a.state, a.started_at_ms, a.finished_at_ms, a.exit_code, a.error {ATTEMPTS} ORDER BY a.id LIMIT ?3"
        ))?;
        let attempts = stmt.query_map(params![who, conversation, limit], attempt_row)?.collect::<std::result::Result<_, _>>()?;
        Ok(StatusView {
            store_id: self.store_id()?,
            head_seq: self.head_seq()?,
            unacknowledged: unack,
            unacknowledged_total: unack_total,
            next_unacknowledged_after: next_unack,
            unanswered,
            unanswered_total: unans_total,
            next_unanswered_after: next_unans,
            uncertain_or_failed_submissions: attempts,
            uncertain_or_failed_total: attempts_total,
            endpoints: self.endpoints()?,
            watchers: Vec::new(),
        })
    }

    pub fn endpoints(&self) -> Result<Vec<EndpointView>> {
        let mut stmt = self.conn.prepare("SELECT name, session, exe, generation, registered_at_ms FROM endpoints ORDER BY name")?;
        let rows = stmt.query_map([], |r| {
            Ok(EndpointView { name: r.get(0)?, session: r.get(1)?, exe: r.get(2)?, generation: r.get(3)?, registered_at_ms: r.get(4)? })
        })?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// Binds (or explicitly rebinds) a participant's delivery adapter. Each binding has a new
    /// generation; failed attempts made under an older generation are fenced (never retried
    /// automatically against the new session).
    pub fn endpoint_register(&mut self, name: &str, session: &str, exe: &str) -> Result<EndpointView> {
        self.require_participant(name)?;
        if !valid_token(session, 128) {
            return Err(StoreError::new(ErrorCode::BadRequest, "session must be 1-128 of [A-Za-z0-9-_.:]"));
        }
        if !exe.starts_with('/') {
            return Err(StoreError::new(ErrorCode::BadRequest, "the adapter executable must be an absolute path"));
        }
        let now = now_ms();
        let tx = self.conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let generation: i64 =
            tx.query_row("SELECT coalesce(max(generation), 0) + 1 FROM endpoints WHERE name = ?1", [name], |r| r.get(0))?;
        // Generations only grow, even across a deleted row.
        let floor: i64 = tx.query_row("SELECT coalesce(max(generation), 0) + 1 FROM attempts WHERE endpoint = ?1", [name], |r| r.get(0))?;
        let generation = generation.max(floor);
        tx.execute(
            "INSERT INTO endpoints (name, session, exe, generation, registered_at_ms) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (name) DO UPDATE SET session = excluded.session, exe = excluded.exe, generation = excluded.generation, registered_at_ms = excluded.registered_at_ms",
            params![name, session, exe, generation, now],
        )?;
        tx.execute(
            "INSERT INTO events (at_ms, kind, audience, message_id, conversation, detail) VALUES (?1, 'endpoint_registered', ?2, NULL, NULL, ?3)",
            params![now, name, json!({"session": session, "exe": exe, "generation": generation}).to_string()],
        )?;
        tx.commit()?;
        Ok(EndpointView { name: name.into(), session: session.into(), exe: exe.into(), generation, registered_at_ms: now })
    }

    /// Explicit operator redelivery: later attempts start a fresh retry budget under the current
    /// binding. This is the only way past an uncertain or fenced attempt.
    pub fn redeliver(&mut self, id: &str) -> Result<()> {
        let now = now_ms();
        let tx = self.conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let exists: i64 = tx.query_row("SELECT count(*) FROM messages WHERE id = ?1", [id], |r| r.get(0))?;
        if exists == 0 {
            return Err(StoreError::new(ErrorCode::NotFound, format!("no message {id}")));
        }
        let running: i64 = tx.query_row("SELECT count(*) FROM attempts WHERE message_id = ?1 AND state = 'started'", [id], |r| r.get(0))?;
        if running > 0 {
            return Err(StoreError::new(ErrorCode::Busy, "an attempt for this message is in flight"));
        }
        let last: i64 = tx.query_row("SELECT coalesce(max(id), 0) FROM attempts WHERE message_id = ?1", [id], |r| r.get(0))?;
        tx.execute(
            "INSERT INTO redeliveries (message_id, after_attempt, at_ms) VALUES (?1, ?2, ?3)
             ON CONFLICT (message_id) DO UPDATE SET after_attempt = excluded.after_attempt, at_ms = excluded.at_ms",
            params![id, last, now],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Messages due a submission attempt now, per the declared policy:
    /// - addressed to a participant with a registered endpoint, and not yet acknowledged;
    /// - considering only attempts after the last explicit redelivery: none yet; or the latest
    ///   failed under the CURRENT generation, fewer than MAX_FAILED_ATTEMPTS failures, and its
    ///   backoff elapsed. A submitted, started or uncertain latest attempt, or a failure under an
    ///   older generation, is never retried automatically.
    pub fn due_deliveries(&self, now: i64) -> Result<Vec<Delivery>> {
        let mut out = Vec::new();
        for ep in self.endpoints()? {
            let mut stmt = self.conn.prepare(
                "SELECT m.id FROM messages m
                 WHERE m.recipient = ?1
                   AND NOT EXISTS (SELECT 1 FROM acks a WHERE a.reader = m.recipient AND a.message_id = m.id)
                 ORDER BY m.seq",
            )?;
            let ids: Vec<String> = stmt.query_map([&ep.name], |r| r.get(0))?.collect::<std::result::Result<_, _>>()?;
            for id in ids {
                if is_due(&self.conn, &id, &ep, now)? {
                    out.push(Delivery { message: self.view_without_body(&id)?, endpoint: ep.clone() });
                }
            }
        }
        Ok(out)
    }

    /// Persists an attempt BEFORE the adapter runs. The selection is re-validated inside the same
    /// write transaction: if the message was acknowledged, redelivered or already attempted, or the
    /// endpoint was rebound (generation, session or executable changed) since `due_deliveries`,
    /// nothing is recorded and `None` is returned.
    pub fn begin_attempt(&mut self, d: &Delivery) -> Result<Option<i64>> {
        let now = now_ms();
        let tx = self.conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current: Option<EndpointView> = tx
            .query_row("SELECT name, session, exe, generation, registered_at_ms FROM endpoints WHERE name = ?1", [&d.endpoint.name], |r| {
                Ok(EndpointView { name: r.get(0)?, session: r.get(1)?, exe: r.get(2)?, generation: r.get(3)?, registered_at_ms: r.get(4)? })
            })
            .optional()?;
        let unchanged = current
            .as_ref()
            .is_some_and(|c| c.generation == d.endpoint.generation && c.session == d.endpoint.session && c.exe == d.endpoint.exe);
        let acked: i64 = tx.query_row(
            "SELECT count(*) FROM acks WHERE reader = ?1 AND message_id = ?2",
            params![d.message.recipient, d.message.id],
            |r| r.get(0),
        )?;
        if !unchanged || acked > 0 || d.message.recipient != d.endpoint.name || !is_due(&tx, &d.message.id, &d.endpoint, now)? {
            return Ok(None);
        }
        tx.execute(
            "INSERT INTO attempts (message_id, endpoint, generation, state, started_at_ms) VALUES (?1, ?2, ?3, 'started', ?4)",
            params![d.message.id, d.endpoint.name, d.endpoint.generation, now],
        )?;
        let attempt = tx.last_insert_rowid();
        tx.execute(
            "INSERT INTO events (at_ms, kind, audience, message_id, conversation, detail) VALUES (?1, 'submission', ?2, ?3, ?4, ?5)",
            params![
                now,
                d.message.sender,
                d.message.id,
                d.message.conversation,
                json!({"attempt": attempt, "endpoint": d.endpoint.name, "generation": d.endpoint.generation, "state": "started"})
                    .to_string()
            ],
        )?;
        tx.commit()?;
        Ok(Some(attempt))
    }

    /// Records an attempt's result: `submitted` (the adapter returned success; not receipt),
    /// `failed` (a confirmed failure; retried per policy) or `uncertain` (timeout or crash).
    pub fn finish_attempt(
        &mut self,
        attempt: i64,
        state: &str,
        exit_code: Option<i32>,
        error: Option<&str>,
        backoff_ms: &[u64],
    ) -> Result<()> {
        if !["submitted", "failed", "uncertain"].contains(&state) {
            return Err(StoreError::new(ErrorCode::BadRequest, format!("attempt state {state:?}")));
        }
        let now = now_ms();
        let tx = self.conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (message_id, endpoint, generation): (String, String, i64) =
            tx.query_row("SELECT message_id, endpoint, generation FROM attempts WHERE id = ?1 AND state = 'started'", [attempt], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })?;
        let next = if state == "failed" {
            let after: i64 = tx
                .query_row("SELECT after_attempt FROM redeliveries WHERE message_id = ?1", [&message_id], |r| r.get(0))
                .optional()?
                .unwrap_or(0);
            let failures: i64 = tx.query_row(
                "SELECT count(*) FROM attempts WHERE message_id = ?1 AND id > ?2 AND state = 'failed'",
                params![message_id, after],
                |r| r.get(0),
            )?;
            let step = backoff_ms.get((failures as usize).min(backoff_ms.len().saturating_sub(1))).copied().unwrap_or(0);
            Some(now + step as i64)
        } else {
            None
        };
        let error = error.map(|e| text::sanitize_line(e, 400));
        tx.execute(
            "UPDATE attempts SET state = ?1, finished_at_ms = ?2, exit_code = ?3, error = ?4, next_retry_at_ms = ?5 WHERE id = ?6",
            params![state, now, exit_code, error, next, attempt],
        )?;
        let (sender, conversation): (String, String) =
            tx.query_row("SELECT sender, conversation FROM messages WHERE id = ?1", [&message_id], |r| Ok((r.get(0)?, r.get(1)?)))?;
        let current: Option<i64> =
            tx.query_row("SELECT generation FROM endpoints WHERE name = ?1", [&endpoint], |r| r.get(0)).optional()?;
        tx.execute(
            "INSERT INTO events (at_ms, kind, audience, message_id, conversation, detail) VALUES (?1, 'submission', ?2, ?3, ?4, ?5)",
            params![
                now,
                sender,
                message_id,
                conversation,
                json!({"attempt": attempt, "endpoint": endpoint, "generation": generation, "state": state, "exit_code": exit_code,
                           "stale_generation": current != Some(generation)})
                .to_string()
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// At startup: attempts left `started` by a crash have an unknown external result.
    pub fn mark_interrupted_attempts(&mut self) -> Result<u64> {
        let ids: Vec<i64> = {
            let mut stmt = self.conn.prepare("SELECT id FROM attempts WHERE state = 'started' ORDER BY id")?;
            stmt.query_map([], |r| r.get(0))?.collect::<std::result::Result<_, _>>()?
        };
        for id in &ids {
            self.finish_attempt(*id, "uncertain", None, Some("the daemon stopped while this submission was in flight"), &[])?;
        }
        Ok(ids.len() as u64)
    }

    pub fn attempts_for(&self, message_id: &str) -> Result<Vec<AttemptView>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, message_id, endpoint, generation, state, started_at_ms, finished_at_ms, exit_code, error FROM attempts WHERE message_id = ?1 ORDER BY id",
        )?;
        Ok(stmt.query_map([message_id], attempt_row)?.collect::<std::result::Result<_, _>>()?)
    }

    /// An SQLite-consistent online copy (the backup API), never a raw file copy of a live WAL store.
    pub fn backup_to(&self, path: &Path) -> Result<i64> {
        if path.exists() {
            return Err(StoreError::new(ErrorCode::Conflict, format!("{} already exists", path.display())));
        }
        let head = self.head_seq()?;
        let mut dst = Connection::open(path)?;
        {
            let backup = rusqlite::backup::Backup::new(&self.conn, &mut dst)?;
            backup.run_to_completion(256, std::time::Duration::from_millis(5), None)?;
        }
        Ok(head)
    }

    pub fn conn(&mut self) -> &mut Connection {
        &mut self.conn
    }

    pub fn participants(&self) -> Result<BTreeSet<String>> {
        let mut stmt = self.conn.prepare("SELECT name FROM participants")?;
        Ok(stmt.query_map([], |r| r.get(0))?.collect::<std::result::Result<_, _>>()?)
    }
}

/// The retry policy for one message and endpoint (see `Store::due_deliveries`).
fn is_due(conn: &Connection, id: &str, ep: &EndpointView, now: i64) -> Result<bool> {
    let after: i64 =
        conn.query_row("SELECT after_attempt FROM redeliveries WHERE message_id = ?1", [id], |r| r.get(0)).optional()?.unwrap_or(0);
    let latest: Option<(String, i64, Option<i64>)> = conn
        .query_row(
            "SELECT state, generation, next_retry_at_ms FROM attempts WHERE message_id = ?1 AND id > ?2 ORDER BY id DESC LIMIT 1",
            params![id, after],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    Ok(match latest {
        None => true,
        Some((state, generation, next)) if state == "failed" && generation == ep.generation => {
            let failures: i64 = conn.query_row(
                "SELECT count(*) FROM attempts WHERE message_id = ?1 AND id > ?2 AND state = 'failed'",
                params![id, after],
                |r| r.get(0),
            )?;
            failures < MAX_FAILED_ATTEMPTS && next.is_none_or(|n| now >= n)
        }
        Some(_) => false,
    })
}

fn event_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<EventView> {
    let detail: String = r.get(6)?;
    Ok(EventView {
        seq: r.get(0)?,
        at_ms: r.get(1)?,
        kind: r.get(2)?,
        audience: r.get(3)?,
        message_id: r.get(4)?,
        conversation: r.get(5)?,
        detail: serde_json::from_str(&detail).unwrap_or(serde_json::Value::String(detail)),
    })
}

fn attempt_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<AttemptView> {
    Ok(AttemptView {
        attempt: r.get(0)?,
        message_id: r.get(1)?,
        endpoint: r.get(2)?,
        generation: r.get(3)?,
        state: r.get(4)?,
        started_at_ms: r.get(5)?,
        finished_at_ms: r.get(6)?,
        exit_code: r.get(7)?,
        error: r.get(8)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (Store, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("chatd-store-{}-{}", std::process::id(), uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut s = Store::open(&dir.join("chat.db")).unwrap();
        s.migrate(&["claude".into(), "codex".into()]).unwrap();
        (s, dir)
    }

    fn send(s: &mut Store, key: &str) -> String {
        s.send(&SendReq {
            from: "claude".into(),
            to: Some("codex".into()),
            conversation: Some("c".into()),
            kind: Kind::Request,
            reply_to: None,
            idempotency_key: key.into(),
            summary: None,
            body: "b".into(),
        })
        .unwrap()
        .id
    }

    #[test]
    fn a_selection_acknowledged_before_its_attempt_is_not_attempted() {
        let (mut s, dir) = store();
        s.endpoint_register("codex", "old", "/bin/true").unwrap();
        let id = send(&mut s, "k");
        let due = s.due_deliveries(now_ms()).unwrap();
        assert_eq!(due.len(), 1);
        s.ack("codex", std::slice::from_ref(&id)).unwrap();
        assert_eq!(s.begin_attempt(&due[0]).unwrap(), None);
        assert!(s.attempts_for(&id).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_selection_rebound_before_its_attempt_is_not_sent_to_the_old_session() {
        let (mut s, dir) = store();
        s.endpoint_register("codex", "old", "/bin/true").unwrap();
        let id = send(&mut s, "k");
        let due = s.due_deliveries(now_ms()).unwrap();
        s.endpoint_register("codex", "new", "/bin/true").unwrap();
        assert_eq!(s.begin_attempt(&due[0]).unwrap(), None, "the old binding's selection is fenced");
        let fresh = s.due_deliveries(now_ms()).unwrap();
        assert_eq!(fresh[0].endpoint.session, "new");
        assert!(s.begin_attempt(&fresh[0]).unwrap().is_some());
        assert_eq!(s.attempts_for(&id).unwrap()[0].generation, 2);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Codex's reproduction: two queued requests, the first attempt running, then a rebind and/or
    /// an acknowledgement before the second selection is claimed.
    fn queued_pair(rebind: bool, ack: bool) {
        let (mut s, dir) = store();
        let first = send(&mut s, "k1");
        let second = send(&mut s, "k2");
        s.endpoint_register("codex", "old-session", "/bin/true").unwrap();
        let due = s.due_deliveries(now_ms()).unwrap();
        assert_eq!(due.len(), 2);
        assert!(s.begin_attempt(&due[0]).unwrap().is_some(), "the first old-session attempt is genuinely in flight");
        if rebind {
            s.endpoint_register("codex", "new-session", "/bin/true").unwrap();
        }
        if ack {
            s.ack("codex", std::slice::from_ref(&second)).unwrap();
        }
        assert_eq!(s.begin_attempt(&due[1]).unwrap(), None, "the stale selection must not spawn (rebind {rebind}, ack {ack})");
        assert!(s.attempts_for(&second).unwrap().is_empty());
        let first_attempts = s.attempts_for(&first).unwrap();
        assert_eq!((first_attempts.len(), first_attempts[0].generation, first_attempts[0].state.as_str()), (1, 1, "started"));
        let fresh = s.due_deliveries(now_ms()).unwrap();
        if ack {
            assert!(fresh.iter().all(|d| d.message.id != second));
        } else {
            let d = fresh.iter().find(|d| d.message.id == second).unwrap();
            assert_eq!(d.endpoint.session, "new-session");
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_queued_selection_is_fenced_by_a_rebind() {
        queued_pair(true, false);
    }

    #[test]
    fn a_queued_selection_is_dropped_by_an_acknowledgement() {
        queued_pair(false, true);
    }

    #[test]
    fn a_queued_selection_is_dropped_by_both() {
        queued_pair(true, true);
    }

    #[test]
    fn a_selection_is_attempted_at_most_once() {
        let (mut s, dir) = store();
        s.endpoint_register("codex", "old", "/bin/true").unwrap();
        let id = send(&mut s, "k");
        let due = s.due_deliveries(now_ms()).unwrap();
        assert!(s.begin_attempt(&due[0]).unwrap().is_some());
        assert_eq!(s.begin_attempt(&due[0]).unwrap(), None);
        assert_eq!(s.attempts_for(&id).unwrap().len(), 1);
        let _ = std::fs::remove_dir_all(dir);
    }
}
