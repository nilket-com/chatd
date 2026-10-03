//! Logging to journald with structured fields (native protocol), falling back to stderr with a
//! syslog priority prefix. Records carry ids, participants and state transitions — never message
//! bodies or keys. `colorize` renders `journalctl -o json` lines with one colour per sender.

use std::os::unix::net::UnixDatagram;
use std::sync::OnceLock;

pub const PRIORITY_ERR: u8 = 3;
pub const PRIORITY_WARNING: u8 = 4;
pub const PRIORITY_NOTICE: u8 = 5;
pub const PRIORITY_INFO: u8 = 6;
pub const HEADER: &str = "TIME        FROM      TO        MESSAGE ID                            EVENT";

static SOCKET: OnceLock<Option<UnixDatagram>> = OnceLock::new();

fn socket() -> Option<&'static UnixDatagram> {
    SOCKET
        .get_or_init(|| {
            if std::env::var_os("CHATD_NO_JOURNAL").is_some() {
                return None;
            }
            let s = UnixDatagram::unbound().ok()?;
            s.connect("/run/systemd/journal/socket").ok()?;
            Some(s)
        })
        .as_ref()
}

fn clean(v: &str) -> String {
    v.chars().map(|c| if c == '\n' || c.is_control() { ' ' } else { c }).take(512).collect()
}

fn participant_column(name: &str) -> String {
    let name = clean(name);
    if name.chars().count() > 8 { format!("{}...", name.chars().take(5).collect::<String>()) } else { name }
}

/// Emits one record. `fields` are extra `CHATD_*` journal fields (upper-case names).
pub fn log(priority: u8, message: &str, fields: &[(&str, &str)]) {
    let mut rec = format!("MESSAGE={}\nPRIORITY={priority}\nSYSLOG_IDENTIFIER=chatd\n", clean(message));
    for (k, v) in fields {
        rec.push_str(&format!("CHATD_{}={}\n", k, clean(v)));
    }
    if let Some(s) = socket()
        && s.send(rec.as_bytes()).is_ok()
    {
        return;
    }
    let extra: Vec<String> = fields.iter().map(|(k, v)| format!("{}={}", k.to_lowercase(), clean(v))).collect();
    eprintln!("<{priority}>{} {}", clean(message), extra.join(" "));
}

/// ANSI colour per sender: claude orange, codex electric blue (#0087ff), anything else (the daemon itself) default.
pub fn color_for(sender: Option<&str>) -> &'static str {
    match sender {
        Some("claude") => "\x1b[38;5;208m",
        Some("codex") => "\x1b[38;5;33m",
        Some(_) => "\x1b[35m",
        None => "",
    }
}

/// Renders one `journalctl -o json` line, coloured by the message's author (FROM); watch and
/// legacy records without an author use the event actor. Errors and warnings keep
/// a bold marker. Returns None for lines that are not JSON objects.
pub fn colorize(json_line: &str, color: bool) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(json_line).ok()?;
    let field = |k: &str| v.get(k).and_then(|x| x.as_str()).map(str::to_string);
    let usec: i64 = field("__REALTIME_TIMESTAMP").and_then(|s| s.parse().ok()).unwrap_or(0);
    let secs = usec / 1_000_000;
    let (h, m, s) = ((secs / 3600) % 24, (secs / 60) % 60, secs % 60);
    let sender = field("CHATD_SENDER");
    let msg = clean(&field("MESSAGE").unwrap_or_default());
    let prio: u8 = field("PRIORITY").and_then(|p| p.parse().ok()).unwrap_or(PRIORITY_INFO);
    let mark = if prio <= PRIORITY_WARNING { "!" } else { " " };
    let event = field("CHATD_EVENT").unwrap_or_default();
    let id = clean(&field("CHATD_MESSAGE_ID").unwrap_or_else(|| "-".into()));
    let recipient = field("CHATD_RECIPIENT");
    // Older acknowledgement records name the reader as SENDER and omit the author.
    // Show that author as unknown rather than reversing the message's direction.
    let from = field("CHATD_FROM").or_else(|| if event == "received" { None } else { sender.clone() }).unwrap_or_else(|| "-".into());
    let to = recipient.or_else(|| if event == "received" { sender.clone() } else { None }).unwrap_or_else(|| "-".into());
    let detail = match event.as_str() {
        "stored" => msg.strip_prefix("stored ").and_then(|s| s.split_whitespace().next()).map(|kind| format!("stored {kind}")),
        "received" => Some("received".into()),
        "submission_started" => msg.strip_prefix(&format!("submitting {id} to {to}")).map(|tail| format!("submitting{tail}")),
        "submission" => msg.strip_prefix(&format!("submission of {id} to {to}: ")).map(str::to_string),
        "watch_open" => Some("watch opened".into()),
        "watch_close" => {
            msg.strip_prefix(&format!("watch by {} ", sender.as_deref().unwrap_or_default())).map(|tail| format!("watch {tail}"))
        }
        _ => None,
    }
    .unwrap_or(msg);
    let line = format!("{h:02}:{m:02}:{s:02}Z {mark} {:<8}  {:<8}  {id:<36}  {detail}", participant_column(&from), participant_column(&to));
    let colour_sender = if from == "-" { sender.as_deref() } else { Some(from.as_str()) };
    Some(if color && !color_for(colour_sender).is_empty() { format!("{}{line}\x1b[0m", color_for(colour_sender)) } else { line })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn colours_follow_the_sender_field() {
        let l = r#"{"__REALTIME_TIMESTAMP":"3723000000","MESSAGE":"stored","PRIORITY":"6","CHATD_SENDER":"codex"}"#;
        let out = colorize(l, true).unwrap();
        assert!(out.starts_with("\x1b[38;5;33m01:02:03Z"), "{out:?}");
        assert!(out.ends_with("\x1b[0m"));
        let c = r#"{"__REALTIME_TIMESTAMP":"0","MESSAGE":"x","CHATD_SENDER":"claude"}"#;
        assert!(colorize(c, true).unwrap().starts_with("\x1b[38;5;208m"));
        assert!(!colorize(c, false).unwrap().contains('\x1b'));
    }

    #[test]
    fn message_control_characters_are_neutralised() {
        let l = "{\"MESSAGE\":\"a\\u001b[2Jb\\nc\"}";
        let out = colorize(l, false).unwrap();
        assert!(!out.chars().any(|c| c.is_control()), "{out:?}");
    }

    #[test]
    fn message_ids_and_details_align_across_events() {
        let id = "00000000-0000-7000-8000-000000000001";
        let render = |event: &str, msg: &str, from: &str, to: &str| {
            colorize(
                &serde_json::json!({"MESSAGE": msg, "CHATD_EVENT": event,
                "CHATD_MESSAGE_ID": id, "CHATD_SENDER": from, "CHATD_RECIPIENT": to})
                .to_string(),
                false,
            )
            .unwrap()
        };
        let stored = render("stored", &format!("stored request {id} claude->codex"), "claude", "codex");
        let submit = render("submission_started", &format!("submitting {id} to codex (attempt 25, generation 5)"), "claude", "codex");
        let result = render("submission", &format!("submission of {id} to claude: failed (timeout)"), "codex", "claude");
        let long_name = render("stored", &format!("stored info {id} long-participant->claude"), "long-participant", "claude");
        for line in [&stored, &submit, &result, &long_name] {
            assert_eq!(line.find(id), Some(32), "{line}");
            assert_eq!(line.matches(id).count(), 1);
        }
        assert!(stored.ends_with("stored request"));
        assert!(submit.ends_with("submitting (attempt 25, generation 5)"));
        assert!(result.ends_with("failed (timeout)"));
    }

    #[test]
    fn receipt_direction_is_not_the_reader_as_author() {
        let legacy = serde_json::json!({"MESSAGE": "received id by claude", "CHATD_EVENT": "received", "CHATD_SENDER": "claude", "CHATD_MESSAGE_ID": "id"});
        let old = colorize(&legacy.to_string(), false).unwrap();
        assert!(old.contains("-         claude    id"), "{old}");
        let mut current = legacy;
        current["CHATD_FROM"] = "codex".into();
        current["CHATD_RECIPIENT"] = "claude".into();
        let new = colorize(&current.to_string(), false).unwrap();
        assert!(new.contains("codex     claude    id"), "{new}");
        assert!(new.ends_with("received"));
        assert!(colorize(&current.to_string(), true).unwrap().starts_with("\x1b[38;5;33m"));
        current["CHATD_FROM"] = "claude".into();
        current["CHATD_SENDER"] = "codex".into();
        current["CHATD_RECIPIENT"] = "codex".into();
        assert!(colorize(&current.to_string(), true).unwrap().starts_with("\x1b[38;5;208m"));
        assert!(!colorize(&current.to_string(), false).unwrap().contains('\x1b'));
    }

    #[test]
    fn watch_records_have_no_fake_message_id_or_repeated_actor() {
        let v = serde_json::json!({"MESSAGE": "watch by claude ended: Broken pipe (os error 32)", "CHATD_EVENT": "watch_close", "CHATD_SENDER": "claude"});
        let line = colorize(&v.to_string(), false).unwrap();
        assert_eq!(line.matches("claude").count(), 1);
        assert!(line.ends_with("watch ended: Broken pipe (os error 32)"));
    }
}
