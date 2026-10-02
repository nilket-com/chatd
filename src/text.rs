//! Rendering of untrusted text: summaries and one-line notifications. Bodies are never rendered here.

use crate::proto::{MAX_SUMMARY_CHARS, MessageView};

/// Replaces every control character (including newlines, tabs and ESC) with a space, collapses runs
/// of whitespace, trims, and bounds the result to `max_chars` characters (never splitting a char).
pub fn sanitize_line(s: &str, max_chars: usize) -> String {
    let mut out = String::new();
    let mut last_space = true;
    for c in s.chars() {
        let c = if c.is_control() || c.is_whitespace() { ' ' } else { c };
        if c == ' ' {
            if last_space {
                continue;
            }
            last_space = true;
        } else {
            last_space = false;
        }
        out.push(c);
    }
    let trimmed = out.trim_end();
    let mut bounded: String = trimmed.chars().take(max_chars).collect();
    if trimmed.chars().count() > max_chars && max_chars > 0 {
        bounded.pop();
        bounded.push('…');
    }
    bounded
}

/// The stored summary: the caller's, or the body's first non-empty line, sanitized and bounded.
pub fn summary_of(summary: Option<&str>, body: &str) -> String {
    let raw = match summary {
        Some(s) => s,
        None => body.lines().find(|l| !l.trim().is_empty()).unwrap_or(""),
    };
    sanitize_line(raw, MAX_SUMMARY_CHARS)
}

/// Largest notification line, in bytes.
pub const MAX_NOTIFICATION_BYTES: usize = 480;

/// One bounded line naming the message, ending with the exact command that fetches its body. The
/// summary is truncated first, so the id and the command always survive.
pub fn notification(m: &MessageView, reader: &str, now_ms: i64) -> String {
    let age_s = ((now_ms - m.created_at_ms).max(0)) / 1000;
    let head =
        format!("[chatd {} {}->{} {} {} {}s]", m.id, m.sender, m.recipient, sanitize_line(&m.conversation, 64), m.kind.as_str(), age_s);
    let tail = format!(" | chatctl receive --as {reader} --id {}", m.id);
    let room = MAX_NOTIFICATION_BYTES.saturating_sub(head.len() + tail.len() + 1);
    let mut summary = String::new();
    for c in m.summary.chars() {
        if summary.len() + c.len_utf8() + '…'.len_utf8() > room {
            summary.push('…');
            break;
        }
        summary.push(c);
    }
    format!("{head} {summary}{tail}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::Kind;

    fn view(summary: &str) -> MessageView {
        MessageView {
            id: "018f0000-0000-7000-8000-000000000000".into(),
            seq: 1,
            conversation: "adr628".into(),
            sender: "codex".into(),
            recipient: "claude".into(),
            kind: Kind::Request,
            reply_to: None,
            summary: summary.into(),
            created_at_ms: 0,
            idempotency_key: "k".into(),
            body: None,
            received_at_ms: None,
            answered_by: None,
        }
    }

    #[test]
    fn control_characters_never_survive() {
        let s = sanitize_line("a\x1b[31mred\x1b[0m\r\nnext\tline\u{7}", 100);
        assert!(!s.chars().any(|c| c.is_control()), "{s:?}");
        assert_eq!(s, "a [31mred [0m next line");
    }

    #[test]
    fn summaries_are_bounded_on_char_boundaries() {
        let long = "é".repeat(1000);
        let s = summary_of(None, &long);
        assert_eq!(s.chars().count(), MAX_SUMMARY_CHARS);
        assert!(s.ends_with('…'));
    }

    #[test]
    fn default_summary_is_the_first_non_empty_line() {
        assert_eq!(summary_of(None, "\n\n  Review this\nmore"), "Review this");
        assert_eq!(summary_of(Some("given"), "body"), "given");
    }

    #[test]
    fn notification_keeps_id_and_command_within_bound() {
        let m = view(&"漢".repeat(MAX_SUMMARY_CHARS));
        let line = notification(&m, "claude", 5_000);
        assert!(line.len() <= MAX_NOTIFICATION_BYTES, "{}", line.len());
        assert!(line.contains(&m.id));
        assert!(line.ends_with(&format!("chatctl receive --as claude --id {}", m.id)));
        assert!(!line.contains('\n'));
    }
}
