//! The CLI <-> daemon protocol: length-prefixed JSON frames over a Unix socket, decoded into typed
//! request and response enums. Every frame carries the protocol version.

use std::io::{self, Read, Write};

use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u32 = 1;
/// Largest frame either side accepts. Checked against the declared length before allocation.
pub const MAX_FRAME: usize = 2 * 1024 * 1024;
/// Largest message body. Larger material goes in a file whose path (and hash) is in the body.
pub const MAX_BODY: usize = 1024 * 1024;
pub const MAX_SUMMARY_CHARS: usize = 160;
pub const MAX_KEY_LEN: usize = 128;
pub const MAX_CONVERSATION_LEN: usize = 64;
/// Largest number of items in one snapshot page, status list or log page. A message view without
/// its body is bounded (~1.5 KiB), so a page stays far below MAX_FRAME.
pub const MAX_PAGE: u32 = 200;
pub const MAX_LOG_PAGE: u32 = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// Expects a final reply; shows as unanswered until one is stored.
    Request,
    /// Information only; never shows as unanswered.
    Info,
    /// A non-final reply to a request; does not answer it.
    Progress,
    /// The final reply to a request; answers it.
    Final,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Request => "request",
            Kind::Info => "info",
            Kind::Progress => "progress",
            Kind::Final => "final",
        }
    }
    pub fn parse(s: &str) -> Option<Kind> {
        Some(match s {
            "request" => Kind::Request,
            "info" => Kind::Info,
            "progress" => Kind::Progress,
            "final" => Kind::Final,
            _ => return None,
        })
    }
    pub fn is_reply(self) -> bool {
        matches!(self, Kind::Progress | Kind::Final)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendReq {
    pub from: String,
    /// Required for request/info; derived from the replied-to message for progress/final.
    pub to: Option<String>,
    /// Required for request/info; must match the replied-to message for progress/final.
    pub conversation: Option<String>,
    pub kind: Kind,
    pub reply_to: Option<String>,
    pub idempotency_key: String,
    pub summary: Option<String>,
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    Health,
    Send(SendReq),
    Receive {
        as_: String,
        id: String,
    },
    Ack {
        as_: String,
        ids: Vec<String>,
    },
    Lookup {
        sender: String,
        idempotency_key: String,
    },
    /// Lists are bounded by `limit` (at most MAX_PAGE) and continue after the given sequences.
    Status {
        as_: Option<String>,
        conversation: Option<String>,
        limit: u32,
        unacknowledged_after: Option<i64>,
        unanswered_after: Option<i64>,
    },
    Log {
        conversation: Option<String>,
        since_seq: Option<i64>,
        limit: u32,
    },
    EndpointRegister {
        name: String,
        session: String,
        exe: String,
    },
    Redeliver {
        id: String,
    },
    Watch {
        as_: String,
        conversation: Option<String>,
        resume_token: Option<String>,
    },
    Backup {
        path: String,
    },
    ImportLegacy {
        paths: Vec<String>,
        dry_run: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientFrame {
    pub v: u32,
    pub req: Request,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageView {
    pub id: String,
    pub seq: i64,
    pub conversation: String,
    pub sender: String,
    pub recipient: String,
    pub kind: Kind,
    pub reply_to: Option<String>,
    pub summary: String,
    pub created_at_ms: i64,
    pub idempotency_key: String,
    /// Present on `receive`; absent in notifications and listings.
    pub body: Option<String>,
    pub received_at_ms: Option<i64>,
    pub answered_by: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Receipt {
    pub id: String,
    pub seq: i64,
    pub idempotency_key: String,
    /// True when an identical earlier send was found: the original id is returned.
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventView {
    pub seq: i64,
    pub at_ms: i64,
    pub kind: String,
    pub audience: String,
    pub message_id: Option<String>,
    pub conversation: Option<String>,
    pub detail: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AttemptView {
    pub attempt: i64,
    pub message_id: String,
    pub endpoint: String,
    pub generation: i64,
    pub state: String,
    pub started_at_ms: i64,
    pub finished_at_ms: Option<i64>,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatusLine {
    pub id: String,
    pub conversation: String,
    pub sender: String,
    pub recipient: String,
    pub kind: Kind,
    pub summary: String,
    pub age_ms: i64,
    pub received: bool,
    pub answered_by: Option<String>,
    pub submission: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatusView {
    pub store_id: String,
    pub head_seq: i64,
    /// Messages for the filtered participant(s) not yet acknowledged by their recipient (one page).
    pub unacknowledged: Vec<StatusLine>,
    pub unacknowledged_total: i64,
    /// Present when more follow: pass it as `unacknowledged_after`.
    pub next_unacknowledged_after: Option<i64>,
    /// Requests without a stored final reply ("no final reply recorded in chatd"; one page).
    pub unanswered: Vec<StatusLine>,
    pub unanswered_total: i64,
    pub next_unanswered_after: Option<i64>,
    /// Latest attempt per message where it is failed, uncertain or in flight (oldest first, one page).
    pub uncertain_or_failed_submissions: Vec<AttemptView>,
    pub uncertain_or_failed_total: i64,
    pub endpoints: Vec<EndpointView>,
    pub watchers: Vec<WatcherView>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EndpointView {
    pub name: String,
    pub session: String,
    pub exe: String,
    pub generation: i64,
    pub registered_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WatcherView {
    pub participant: String,
    pub conversation: Option<String>,
    pub connected_at_ms: i64,
    pub last_event_seq: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LegacyReport {
    pub dry_run: bool,
    pub files: Vec<LegacyFileReport>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LegacyFileReport {
    pub path: String,
    pub sha256: String,
    pub bytes: u64,
    pub blocks: u64,
    pub malformed: u64,
    pub newly_imported: u64,
    pub already_present: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "r", rename_all = "snake_case")]
pub enum Response {
    Health {
        store_id: String,
        head_seq: i64,
        schema_version: i64,
        pid: u32,
    },
    Stored(Receipt),
    Message(MessageView),
    Acked {
        acknowledged: Vec<String>,
        already: Vec<String>,
    },
    Found(Receipt),
    Status(StatusView),
    Log {
        events: Vec<EventView>,
    },
    Registered(EndpointView),
    Redelivery {
        message_id: String,
    },
    BackedUp {
        path: String,
        head_seq: i64,
    },
    Legacy(LegacyReport),
    /// A watch starts with SnapshotBegin, then SnapshotPage frames (oldest first, each at most
    /// MAX_PAGE messages) holding the authoritative unacknowledged inbox at `head_seq`, then
    /// SnapshotEnd; events after `head_seq` follow.
    SnapshotBegin {
        head_seq: i64,
        resume_token: String,
        unacknowledged_total: i64,
        resynchronized: Option<String>,
    },
    SnapshotPage {
        unacknowledged: Vec<MessageView>,
    },
    SnapshotEnd {
        head_seq: i64,
        pages: u64,
    },
    Event {
        event: EventView,
        resume_token: String,
    },
    Heartbeat {
        head_seq: i64,
        at_ms: i64,
    },
    Error {
        code: ErrorCode,
        message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    BadRequest,
    UnknownParticipant,
    NotFound,
    Conflict,
    InvalidReply,
    TooLarge,
    VersionMismatch,
    Busy,
    Storage,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerFrame {
    pub v: u32,
    pub resp: Response,
}

pub fn write_frame<W: Write, T: Serialize>(w: &mut W, value: &T) -> io::Result<()> {
    let bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
    if bytes.len() > MAX_FRAME {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "frame exceeds MAX_FRAME"));
    }
    let len = u32::try_from(bytes.len()).map_err(io::Error::other)?;
    w.write_all(&len.to_be_bytes())?;
    w.write_all(&bytes)?;
    w.flush()
}

/// Reads one frame. `Ok(None)` is a clean end of stream before a length prefix.
pub fn read_frame<R: Read, T: for<'de> Deserialize<'de>>(r: &mut R) -> io::Result<Option<T>> {
    let mut len = [0u8; 4];
    match r.read_exact(&mut len) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME {
        return Err(io::Error::new(io::ErrorKind::InvalidData, format!("declared frame length {len} exceeds {MAX_FRAME}")));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    serde_json::from_slice(&buf).map(Some).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}
