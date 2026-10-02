//! Logging to journald with structured fields (native protocol), falling back to stderr with a
//! syslog priority prefix. Records carry ids, participants and state transitions — never message
//! bodies or keys. `colorize` renders `journalctl -o json` lines with one colour per sender.

use std::os::unix::net::UnixDatagram;
use std::sync::OnceLock;

pub const PRIORITY_ERR: u8 = 3;
pub const PRIORITY_WARNING: u8 = 4;
pub const PRIORITY_NOTICE: u8 = 5;
pub const PRIORITY_INFO: u8 = 6;

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

/// ANSI colour per sender: claude orange, codex cyan, anything else (the daemon itself) default.
pub fn color_for(sender: Option<&str>) -> &'static str {
    match sender {
        Some("claude") => "\x1b[38;5;208m",
        Some("codex") => "\x1b[36m",
        Some(_) => "\x1b[35m",
        None => "",
    }
}

/// Renders one `journalctl -o json` line, coloured by `CHATD_SENDER`; errors and warnings keep
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
    let who = sender.clone().unwrap_or_else(|| "daemon".into());
    let line = format!("{h:02}:{m:02}:{s:02}Z {mark} {who:>6}  {msg}");
    Some(if color && !color_for(sender.as_deref()).is_empty() { format!("{}{line}\x1b[0m", color_for(sender.as_deref())) } else { line })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn colours_follow_the_sender_field() {
        let l = r#"{"__REALTIME_TIMESTAMP":"3723000000","MESSAGE":"stored","PRIORITY":"6","CHATD_SENDER":"codex"}"#;
        let out = colorize(l, true).unwrap();
        assert!(out.starts_with("\x1b[36m01:02:03Z"), "{out:?}");
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
}
