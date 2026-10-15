//! chatctl: the chatd command-line client.

use std::collections::HashSet;
use std::io::{self, BufRead, IsTerminal, Read, Write};
use std::path::PathBuf;
use std::process::{Command, ExitCode, Stdio};
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};

use chatd::client::{Client, ClientError};
use chatd::proto::{ErrorCode, EventView, Kind, MessageView, Request, Response, SendReq};
use chatd::store::now_ms;
use chatd::{journal, paths, pending, text};

const EXIT_UNAVAILABLE: u8 = 3;
const EXIT_NOT_FOUND: u8 = 4;
const EXIT_REFUSED: u8 = 5;
const EXIT_TIMEOUT: u8 = 6;

#[derive(Parser)]
#[command(name = "chatctl", version, about = "Durable local messages between coding agents")]
struct Cli {
    /// Machine-readable JSON output.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Store a request or information message.
    Send {
        #[arg(long = "as", alias = "from", env = "CHATD_AS")]
        who: String,
        #[arg(long)]
        to: String,
        #[arg(long)]
        conversation: String,
        #[arg(long, default_value = "request", value_parser = ["request", "info"])]
        kind: String,
        /// Optional. Omit it and chatctl stores a generated key before sending and reuses it when the
        /// same command is re-run, until a receipt is seen. Give one to manage retries yourself.
        #[arg(long)]
        idempotency_key: Option<String>,
        #[arg(long)]
        summary: Option<String>,
        /// Read the body from this file (default: stdin).
        #[arg(long)]
        body_file: Option<PathBuf>,
    },
    /// Store a progress or final reply to a request (goes to the request's sender).
    Reply {
        #[arg(long = "as", env = "CHATD_AS")]
        who: String,
        #[arg(long)]
        to_message: String,
        /// A final reply answers the request; without it the reply is progress.
        #[arg(long)]
        r#final: bool,
        /// Optional; see `send`.
        #[arg(long)]
        idempotency_key: Option<String>,
        #[arg(long)]
        summary: Option<String>,
        #[arg(long)]
        body_file: Option<PathBuf>,
    },
    /// Print a message in full. Does not acknowledge it.
    Receive {
        #[arg(long = "as", env = "CHATD_AS")]
        who: String,
        #[arg(long)]
        id: String,
    },
    /// Print a message in full, then acknowledge it (receive + ack). The body is written and
    /// flushed first, so a failed write never marks an unseen message received.
    Take {
        #[arg(long = "as", env = "CHATD_AS")]
        who: String,
        #[arg(long)]
        id: String,
    },
    /// Acknowledge receipt (not an answer).
    Ack {
        #[arg(long = "as", env = "CHATD_AS")]
        who: String,
        #[arg(required = true)]
        ids: Vec<String>,
    },
    /// Block until an unacknowledged message is available; print one notification line and exit.
    Wait {
        #[arg(long = "as", env = "CHATD_AS")]
        who: String,
        #[arg(long)]
        conversation: Option<String>,
        /// Seconds; exit status 6 on expiry. Default 1800, or unbounded with --follow (0 also means
        /// unbounded with --follow).
        #[arg(long)]
        timeout: Option<u64>,
        /// Print every unacknowledged and new message (once per id per connection) until the timeout.
        #[arg(long)]
        follow: bool,
    },
    /// Stream this participant's events as JSON lines (server-pushed; resumable).
    Watch {
        #[arg(long = "as", env = "CHATD_AS")]
        who: String,
        #[arg(long)]
        conversation: Option<String>,
        #[arg(long)]
        resume_token: Option<String>,
        #[arg(long, default_value_t = 1800)]
        timeout: u64,
    },
    /// Unacknowledged messages, unanswered requests, submissions, endpoints and watchers.
    Status {
        #[arg(long = "as")]
        who: Option<String>,
        #[arg(long)]
        conversation: Option<String>,
        /// Items per list (at most 200); totals and continuation cursors are always shown.
        #[arg(long, default_value_t = 50)]
        limit: u32,
        #[arg(long)]
        unacknowledged_after: Option<i64>,
        #[arg(long)]
        unanswered_after: Option<i64>,
    },
    /// The durable event log.
    Log {
        #[arg(long)]
        conversation: Option<String>,
        #[arg(long)]
        since: Option<i64>,
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
    /// Find a receipt by sender and idempotency key (after a lost receipt).
    Lookup {
        #[arg(long)]
        sender: String,
        #[arg(long)]
        idempotency_key: String,
    },
    /// Print a fresh random idempotency key (local; contacts nothing).
    NewKey,
    /// Delivery endpoints.
    Endpoint {
        #[command(subcommand)]
        cmd: EndpointCmd,
    },
    /// Explicitly allow new submission attempts for a message (past uncertain or fenced attempts).
    Redeliver { id: String },
    /// Consistent online backup of the store to a new file.
    Backup { path: PathBuf },
    /// Import the old Markdown bridge files as inert history.
    ImportLegacy {
        #[arg(long)]
        dry_run: bool,
        #[arg(required = true)]
        paths: Vec<PathBuf>,
    },
    /// Daemon health.
    Health,
    /// The daemon's journal, one colour per sender (claude orange, codex electric blue).
    Journal {
        #[arg(short, long)]
        follow: bool,
        #[arg(short = 'n', long, default_value_t = 50)]
        lines: u32,
        #[arg(long)]
        no_color: bool,
        /// Read `journalctl -o json` lines from stdin instead of running journalctl.
        #[arg(long)]
        stdin: bool,
        /// The systemd user unit to read.
        #[arg(long, default_value = "chatd.service")]
        unit: String,
    },
}

#[derive(Subcommand)]
enum EndpointCmd {
    /// Bind a participant to its queue adapter: `<exe> queue --thread <session> --message <line>`.
    Register {
        #[arg(long = "as")]
        who: String,
        #[arg(long)]
        session: String,
        #[arg(long)]
        exe: PathBuf,
    },
}

fn body(file: &Option<PathBuf>) -> Result<String, String> {
    match file {
        Some(p) => std::fs::read_to_string(p).map_err(|e| format!("{}: {e}", p.display())),
        None => {
            let mut s = String::new();
            io::stdin().read_to_string(&mut s).map_err(|e| format!("stdin: {e}"))?;
            Ok(s)
        }
    }
}

fn connect(timeout: Option<Duration>) -> Result<Client, ExitCode> {
    Client::connect(&paths::socket_path(), timeout).map_err(|e| {
        eprintln!("chatctl: {e}");
        ExitCode::from(EXIT_UNAVAILABLE)
    })
}

fn call(req: Request) -> Result<Response, ExitCode> {
    let mut c = connect(Some(Duration::from_secs(60)))?;
    match c.call(req) {
        Ok(Response::Error { code, message }) => {
            eprintln!("chatctl: {code:?}: {message}");
            Err(ExitCode::from(match code {
                ErrorCode::NotFound => EXIT_NOT_FOUND,
                ErrorCode::Unavailable => EXIT_UNAVAILABLE,
                _ => EXIT_REFUSED,
            }))
        }
        Ok(r) => Ok(r),
        Err(e @ ClientError::Unavailable(_)) => {
            eprintln!("chatctl: {e}");
            Err(ExitCode::from(EXIT_UNAVAILABLE))
        }
        Err(e) => {
            eprintln!("chatctl: {e} (the result is unknown; retry with the same idempotency key)");
            Err(ExitCode::from(EXIT_UNAVAILABLE))
        }
    }
}

/// An explicit key is used as given. Otherwise a key is stored (or reused) under the request's
/// identity before anything is sent; the returned record is cleared once a receipt is seen.
fn resolve_key(explicit: Option<String>, f: &pending::Fields) -> Result<(String, Option<(PathBuf, String)>), ExitCode> {
    if let Some(k) = explicit {
        return Ok((k, None));
    }
    let dir = paths::pending_dir();
    let id = pending::identity(f);
    match pending::key_for(&dir, &id) {
        Ok((key, reused)) => {
            if reused {
                eprintln!("chatctl: reusing the pending key for this identical request (a previous attempt may have been sent)");
            }
            Ok((key, Some((dir, id))))
        }
        Err(e) => {
            eprintln!("chatctl: cannot store a pending key ({e}); nothing was sent. Pass --idempotency-key to manage it yourself");
            Err(ExitCode::from(EXIT_REFUSED))
        }
    }
}

/// The receipt line (or JSON), written and flushed with failures reported, so a pending key is only
/// cleared after the caller really has the receipt.
fn print_receipt(r: &chatd::proto::Receipt, json: bool) -> io::Result<()> {
    let mut out = if json {
        serde_json::to_string_pretty(r).unwrap_or_default()
    } else {
        format!("{} (seq {}, key {}{})", r.id, r.seq, r.idempotency_key, if r.duplicate { ", already stored" } else { "" })
    };
    out.push('\n');
    let mut stdout = io::stdout().lock();
    stdout.write_all(out.as_bytes())?;
    stdout.flush()
}

/// Print the receipt, and only then forget the pending key. If the receipt cannot be written the
/// message may be stored: keep the key, so the identical command retried stores it once.
fn deliver_receipt(r: &chatd::proto::Receipt, json: bool, pending: Option<(PathBuf, String)>) -> Result<(), ExitCode> {
    match print_receipt(r, json) {
        Ok(()) => {
            settle(pending);
            Ok(())
        }
        Err(e) => {
            eprintln!(
                "chatctl: could not write the receipt ({e}); the message may be stored as {} (key {}); re-run the same command to confirm",
                r.id, r.idempotency_key
            );
            Err(ExitCode::FAILURE)
        }
    }
}

fn settle(pending: Option<(PathBuf, String)>) {
    if let Some((dir, id)) = pending {
        pending::clear(&dir, &id);
    }
}

fn print_json<T: serde::Serialize>(v: &T) {
    println!("{}", serde_json::to_string_pretty(v).unwrap_or_default());
}

fn print_message(m: &MessageView, json: bool) -> io::Result<()> {
    let mut out = String::new();
    if json {
        out.push_str(&serde_json::to_string_pretty(m).unwrap_or_default());
        out.push('\n');
    } else {
        out.push_str(&format!("id:           {}\n", m.id));
        out.push_str(&format!("from -> to:   {} -> {}\n", m.sender, m.recipient));
        out.push_str(&format!("conversation: {}  kind: {}  seq: {}\n", m.conversation, m.kind.as_str(), m.seq));
        if let Some(r) = &m.reply_to {
            out.push_str(&format!("reply to:     {r}\n"));
        }
        out.push_str(&format!("received:     {}\n", if m.received_at_ms.is_some() { "acknowledged" } else { "not acknowledged" }));
        if let Some(a) = &m.answered_by {
            out.push_str(&format!("answered by:  {a}\n"));
        }
        out.push_str(&format!("summary:      {}\n---\n", m.summary));
        out.push_str(m.body.as_deref().unwrap_or_default());
        out.push('\n');
    }
    let mut stdout = io::stdout().lock();
    stdout.write_all(out.as_bytes())?;
    stdout.flush()
}

fn age(ms: i64) -> String {
    let s = ms / 1000;
    if s < 120 {
        format!("{s}s")
    } else if s < 7200 {
        format!("{}m", s / 60)
    } else {
        format!("{}h", s / 3600)
    }
}

fn view_from_event(ev: &EventView, me: &str) -> Option<MessageView> {
    if ev.kind != "message_stored" {
        return None;
    }
    let d = &ev.detail;
    Some(MessageView {
        id: ev.message_id.clone()?,
        seq: ev.seq,
        conversation: ev.conversation.clone().unwrap_or_default(),
        sender: d.get("sender")?.as_str()?.to_string(),
        recipient: me.to_string(),
        kind: Kind::parse(d.get("kind")?.as_str()?)?,
        reply_to: d.get("reply_to").and_then(|v| v.as_str()).map(str::to_string),
        summary: d.get("summary").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        created_at_ms: ev.at_ms,
        idempotency_key: String::new(),
        body: None,
        received_at_ms: None,
        answered_by: None,
    })
}

fn emit(line: &str) {
    let mut out = io::stdout().lock();
    let _ = writeln!(out, "{line}");
    let _ = out.flush();
}

/// Seconds a `wait` lasts; `None` is no deadline. Follow mode with no timeout (or 0) is unbounded.
fn wait_seconds(timeout: Option<u64>, follow: bool) -> Option<u64> {
    const DEFAULT: u64 = 1800;
    match timeout {
        None if follow => None,
        Some(0) if follow => None,
        None => Some(DEFAULT),
        Some(n) => Some(n),
    }
}

/// Time left before `deadline`; an absent deadline never runs out (a slice longer than any wait step).
fn left(deadline: Option<Instant>) -> Duration {
    deadline.map_or(Duration::from_secs(u32::MAX as u64), |d| d.saturating_duration_since(Instant::now()))
}

/// wait / wait --follow over a server-pushed watch, reconnecting within the deadline. Printing
/// never acknowledges; a reconnect replays everything still unacknowledged.
fn wait(who: &str, conversation: Option<String>, timeout: Option<u64>, follow: bool) -> ExitCode {
    let deadline = wait_seconds(timeout, follow).map(|n| Instant::now() + Duration::from_secs(n));
    let mut printed_any = false;
    loop {
        let remaining = left(deadline);
        if remaining.is_zero() {
            break;
        }
        let mut c = match Client::connect(&paths::socket_path(), Some(remaining.min(Duration::from_secs(35)))) {
            Ok(c) => c,
            Err(ClientError::Unavailable(e)) if printed_any || follow => {
                eprintln!("chatctl: chatd unreachable ({e}); retrying");
                std::thread::sleep(Duration::from_secs(1).min(remaining));
                continue;
            }
            Err(e) => {
                eprintln!("chatctl: {e}");
                return ExitCode::from(EXIT_UNAVAILABLE);
            }
        };
        if let Err(e) = c.send(Request::Watch { as_: who.to_string(), conversation: conversation.clone(), resume_token: None }) {
            eprintln!("chatctl: {e}");
            std::thread::sleep(Duration::from_secs(1).min(remaining));
            continue;
        }
        let mut emitted: HashSet<String> = HashSet::new();
        loop {
            let remaining = left(deadline);
            if remaining.is_zero() {
                break;
            }
            let _ = c.set_read_timeout(Some(remaining.min(Duration::from_secs(35))));
            match c.recv() {
                Ok(Response::SnapshotBegin { resynchronized, .. }) => {
                    if let Some(r) = resynchronized {
                        eprintln!(
                            "chatctl: resynchronized: {r}; the inbox below is authoritative; refetch `chatctl status` for submissions and unanswered requests"
                        );
                    }
                }
                Ok(Response::SnapshotPage { unacknowledged }) => {
                    for m in unacknowledged {
                        if emitted.insert(m.id.clone()) {
                            emit(&text::notification(&m, who, now_ms()));
                            printed_any = true;
                            if !follow {
                                return ExitCode::SUCCESS;
                            }
                        }
                    }
                }
                Ok(Response::Event { event, .. }) => {
                    if let Some(m) = view_from_event(&event, who)
                        && emitted.insert(m.id.clone())
                    {
                        emit(&text::notification(&m, who, now_ms()));
                        printed_any = true;
                        if !follow {
                            return ExitCode::SUCCESS;
                        }
                    }
                }
                Ok(Response::Heartbeat { .. }) => {}
                Ok(Response::Error { code, message }) => {
                    eprintln!("chatctl: {code:?}: {message}");
                    return ExitCode::from(if code == ErrorCode::UnknownParticipant { EXIT_REFUSED } else { EXIT_UNAVAILABLE });
                }
                Ok(_) => {}
                Err(ClientError::Io(e)) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {
                    if left(deadline).is_zero() {
                        break;
                    }
                    eprintln!("chatctl: no heartbeat from chatd; reconnecting");
                    break;
                }
                Err(e) => {
                    eprintln!("chatctl: watch interrupted ({e}); reconnecting");
                    std::thread::sleep(Duration::from_millis(500));
                    break;
                }
            }
        }
    }
    eprintln!("chatctl: timeout");
    ExitCode::from(EXIT_TIMEOUT)
}

fn watch(who: &str, conversation: Option<String>, token: Option<String>, timeout: u64) -> ExitCode {
    let deadline = Instant::now() + Duration::from_secs(timeout);
    let mut c = match connect(Some(Duration::from_secs(35))) {
        Ok(c) => c,
        Err(code) => return code,
    };
    if let Err(e) = c.send(Request::Watch { as_: who.to_string(), conversation, resume_token: token }) {
        eprintln!("chatctl: {e}");
        return ExitCode::from(EXIT_UNAVAILABLE);
    }
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return ExitCode::from(EXIT_TIMEOUT);
        }
        let _ = c.set_read_timeout(Some(remaining.min(Duration::from_secs(35))));
        match c.recv() {
            Ok(Response::Error { code, message }) => {
                eprintln!("chatctl: {code:?}: {message}");
                return ExitCode::from(EXIT_REFUSED);
            }
            Ok(r) => emit(&serde_json::to_string(&r).unwrap_or_default()),
            Err(ClientError::Io(e)) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {
                if deadline.saturating_duration_since(Instant::now()).is_zero() {
                    return ExitCode::from(EXIT_TIMEOUT);
                }
                eprintln!("chatctl: no heartbeat; resume with the last resume_token");
                return ExitCode::from(EXIT_UNAVAILABLE);
            }
            Err(e) => {
                eprintln!("chatctl: {e}; resume with the last resume_token");
                return ExitCode::from(EXIT_UNAVAILABLE);
            }
        }
    }
}

fn journal_view(follow: bool, lines: u32, no_color: bool, from_stdin: bool, unit: &str) -> ExitCode {
    let color = !no_color && io::stdout().is_terminal();
    emit(journal::HEADER);
    let render = |line: &str| {
        if let Some(s) = journal::colorize(line, color) {
            emit(&s);
        }
    };
    if from_stdin {
        for line in io::stdin().lock().lines().map_while(Result::ok) {
            render(&line);
        }
        return ExitCode::SUCCESS;
    }
    let mut cmd = Command::new("journalctl");
    cmd.args(["--user", "--unit", unit, "--output", "json", "--lines", &lines.to_string()]);
    if follow {
        cmd.arg("--follow");
    } else {
        cmd.arg("--no-pager");
    }
    let mut child = match cmd.stdout(Stdio::piped()).spawn() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("chatctl: journalctl: {e}");
            return ExitCode::from(EXIT_UNAVAILABLE);
        }
    };
    if let Some(out) = child.stdout.take() {
        for line in io::BufReader::new(out).lines().map_while(Result::ok) {
            render(&line);
        }
    }
    let _ = child.wait();
    ExitCode::SUCCESS
}

fn run(cli: Cli) -> Result<(), ExitCode> {
    let json = cli.json;
    match cli.cmd {
        Cmd::Send { who, to, conversation, kind, idempotency_key, summary, body_file } => {
            let body = body(&body_file).map_err(|e| {
                eprintln!("chatctl: {e}");
                ExitCode::from(EXIT_REFUSED)
            })?;
            let (idempotency_key, pending) = resolve_key(
                idempotency_key,
                &pending::Fields {
                    from: &who,
                    to: Some(&to),
                    conversation: Some(&conversation),
                    kind: &kind,
                    reply_to: None,
                    summary: summary.as_deref(),
                    body: &body,
                },
            )?;
            let req = SendReq {
                from: who,
                to: Some(to),
                conversation: Some(conversation),
                kind: Kind::parse(&kind).unwrap(),
                reply_to: None,
                idempotency_key,
                summary,
                body,
            };
            let Response::Stored(r) = call(Request::Send(req))? else { return Err(ExitCode::FAILURE) };
            deliver_receipt(&r, json, pending)?;
        }
        Cmd::Reply { who, to_message, r#final, idempotency_key, summary, body_file } => {
            let body = body(&body_file).map_err(|e| {
                eprintln!("chatctl: {e}");
                ExitCode::from(EXIT_REFUSED)
            })?;
            let kind = if r#final { Kind::Final } else { Kind::Progress };
            let (idempotency_key, pending) = resolve_key(
                idempotency_key,
                &pending::Fields {
                    from: &who,
                    to: None,
                    conversation: None,
                    kind: kind.as_str(),
                    reply_to: Some(&to_message),
                    summary: summary.as_deref(),
                    body: &body,
                },
            )?;
            let req = SendReq { from: who, to: None, conversation: None, kind, reply_to: Some(to_message), idempotency_key, summary, body };
            let Response::Stored(r) = call(Request::Send(req))? else { return Err(ExitCode::FAILURE) };
            deliver_receipt(&r, json, pending)?;
        }
        Cmd::Receive { who, id } => {
            let Response::Message(m) = call(Request::Receive { as_: who, id })? else { return Err(ExitCode::FAILURE) };
            print_message(&m, json).map_err(|e| {
                eprintln!("chatctl: {e}");
                ExitCode::FAILURE
            })?;
        }
        Cmd::Take { who, id } => {
            let Response::Message(m) = call(Request::Receive { as_: who.clone(), id: id.clone() })? else { return Err(ExitCode::FAILURE) };
            // Never acknowledge what was not written out: a stdout failure leaves the message unacknowledged.
            print_message(&m, json).map_err(|e| {
                eprintln!("chatctl: could not write the message ({e}); not acknowledged");
                ExitCode::FAILURE
            })?;
            let Response::Acked { acknowledged, already } = call(Request::Ack { as_: who, ids: vec![id] })? else {
                return Err(ExitCode::FAILURE);
            };
            eprintln!("chatctl: acknowledged {} (already {})", acknowledged.len(), already.len());
        }
        Cmd::Ack { who, ids } => {
            let r = call(Request::Ack { as_: who, ids })?;
            if json {
                print_json(&r)
            } else if let Response::Acked { acknowledged, already } = r {
                println!("acknowledged {} (already {})", acknowledged.len(), already.len());
            }
        }
        Cmd::Wait { who, conversation, timeout, follow } => {
            return match wait(&who, conversation, timeout, follow) {
                c if c == ExitCode::SUCCESS => Ok(()),
                c => Err(c),
            };
        }
        Cmd::Watch { who, conversation, resume_token, timeout } => return Err(watch(&who, conversation, resume_token, timeout)),
        Cmd::Status { who, conversation, limit, unacknowledged_after, unanswered_after } => {
            let Response::Status(s) = call(Request::Status { as_: who, conversation, limit, unacknowledged_after, unanswered_after })?
            else {
                return Err(ExitCode::FAILURE);
            };
            if json {
                print_json(&s);
                return Ok(());
            }
            println!("store {}  head {}", s.store_id, s.head_seq);
            println!("unacknowledged ({} shown of {}):", s.unacknowledged.len(), s.unacknowledged_total);
            for l in &s.unacknowledged {
                println!(
                    "  {} {}->{} {} {} {}  {}{}",
                    l.id,
                    l.sender,
                    l.recipient,
                    l.conversation,
                    l.kind.as_str(),
                    age(l.age_ms),
                    l.summary,
                    l.submission.as_ref().map(|x| format!("  [submission: {x}]")).unwrap_or_default()
                );
            }
            if let Some(n) = s.next_unacknowledged_after {
                println!("  … more: chatctl status --unacknowledged-after {n}");
            }
            println!("no final reply recorded in chatd ({} shown of {}):", s.unanswered.len(), s.unanswered_total);
            for l in &s.unanswered {
                println!(
                    "  {} {}->{} {} {}  received: {}  {}",
                    l.id,
                    l.sender,
                    l.recipient,
                    l.conversation,
                    age(l.age_ms),
                    if l.received { "yes" } else { "no" },
                    l.summary
                );
            }
            if let Some(n) = s.next_unanswered_after {
                println!("  … more: chatctl status --unanswered-after {n}");
            }
            println!(
                "submissions needing attention ({} shown of {}):",
                s.uncertain_or_failed_submissions.len(),
                s.uncertain_or_failed_total
            );
            for a in &s.uncertain_or_failed_submissions {
                println!(
                    "  attempt {} {} -> {} gen {} {}{}",
                    a.attempt,
                    a.message_id,
                    a.endpoint,
                    a.generation,
                    a.state,
                    a.error.as_ref().map(|e| format!(": {e}")).unwrap_or_default()
                );
            }
            println!("endpoints:");
            for e in &s.endpoints {
                println!("  {} session {} gen {} exe {}", e.name, e.session, e.generation, e.exe);
            }
            println!("watchers ({}):", s.watchers.len());
            for w in &s.watchers {
                println!(
                    "  {} conversation {} since {} last event {}",
                    w.participant,
                    w.conversation.as_deref().unwrap_or("*"),
                    age(now_ms() - w.connected_at_ms),
                    w.last_event_seq
                );
            }
        }
        Cmd::Log { conversation, since, limit } => {
            let Response::Log { events } = call(Request::Log { conversation, since_seq: since, limit })? else {
                return Err(ExitCode::FAILURE);
            };
            if json {
                print_json(&events)
            } else {
                for e in events {
                    println!("{:>6} {} {:<20} {:<7} {} {}", e.seq, e.at_ms, e.kind, e.audience, e.message_id.unwrap_or_default(), e.detail);
                }
            }
        }
        Cmd::Lookup { sender, idempotency_key } => {
            let r = call(Request::Lookup { sender, idempotency_key })?;
            print_json(&r);
        }
        Cmd::NewKey => println!("{}", uuid::Uuid::new_v4()),
        Cmd::Endpoint { cmd: EndpointCmd::Register { who, session, exe } } => {
            let r = call(Request::EndpointRegister { name: who, session, exe: exe.to_string_lossy().into_owned() })?;
            print_json(&r);
        }
        Cmd::Redeliver { id } => {
            let r = call(Request::Redeliver { id })?;
            print_json(&r);
        }
        Cmd::Backup { path } => {
            let r = call(Request::Backup { path: path.to_string_lossy().into_owned() })?;
            print_json(&r);
        }
        Cmd::ImportLegacy { dry_run, paths } => {
            let paths = paths
                .iter()
                .map(|p| std::path::absolute(p).map(|a| a.to_string_lossy().into_owned()))
                .collect::<io::Result<Vec<_>>>()
                .map_err(|e| {
                    eprintln!("chatctl: {e}");
                    ExitCode::from(EXIT_REFUSED)
                })?;
            let r = call(Request::ImportLegacy { paths, dry_run })?;
            print_json(&r);
        }
        Cmd::Health => {
            let r = call(Request::Health)?;
            print_json(&r);
        }
        Cmd::Journal { follow, lines, no_color, stdin, unit } => {
            return match journal_view(follow, lines, no_color, stdin, &unit) {
                c if c == ExitCode::SUCCESS => Ok(()),
                c => Err(c),
            };
        }
    }
    Ok(())
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(c) => c,
    }
}

#[cfg(test)]
mod tests {
    use super::wait_seconds;

    #[test]
    fn follow_without_a_timeout_is_unbounded_and_everything_else_is_as_before() {
        assert_eq!(wait_seconds(None, false), Some(1800));
        assert_eq!(wait_seconds(Some(5), false), Some(5));
        assert_eq!(wait_seconds(Some(0), false), Some(0), "a plain wait with 0 still expires at once");
        assert_eq!(wait_seconds(Some(5), true), Some(5));
        assert_eq!(wait_seconds(None, true), None);
        assert_eq!(wait_seconds(Some(0), true), wait_seconds(None, true));
    }
}
