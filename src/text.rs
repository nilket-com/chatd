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

/// Most messages listed in one digest, and its bound in bytes (header and `+N more` included).
pub const DIGEST_MAX_ITEMS: usize = 20;
pub const DIGEST_MAX_BYTES: usize = 4096;
/// Characters of each summary shown in a digest.
const DIGEST_SUMMARY_CHARS: usize = 100;

/// The digest a waking push carries: a header, one line per queued message (oldest first, newest last),
/// and `+N more` for the rest, within DIGEST_MAX_ITEMS lines and DIGEST_MAX_BYTES. `total` counts every
/// queued message, listed or not. Returns the text and how many of `items` it lists (a prefix); an empty
/// text when `total` is 0.
pub fn digest(items: &[MessageView], total: i64, reader: &str, now_ms: i64) -> (String, usize) {
    if total <= 0 {
        return (String::new(), 0);
    }
    let mut out = format!(
        "[chatd digest for {reader}: {total} queued, oldest first; not acknowledged: chatctl take --as {reader} --id ID, or chatctl ack --as {reader} ID...]"
    );
    let more = |n: i64| format!("\n  +{n} more (chatctl status --as {reader})");
    let mut listed = 0usize;
    for m in items.iter().take(DIGEST_MAX_ITEMS) {
        let age_s = ((now_ms - m.created_at_ms).max(0)) / 1000;
        let line = format!(
            "\n  {} {} {} {} {}s: {}",
            m.id,
            sanitize_line(&m.sender, 32),
            sanitize_line(&m.conversation, 64),
            m.kind.as_str(),
            age_s,
            sanitize_line(&m.summary, DIGEST_SUMMARY_CHARS)
        );
        let rest = total - listed as i64 - 1;
        let reserve = if rest > 0 { more(rest).len() } else { 0 };
        if out.len() + line.len() + reserve > DIGEST_MAX_BYTES {
            break;
        }
        out.push_str(&line);
        listed += 1;
    }
    let rest = total - listed as i64;
    if rest > 0 {
        out.push_str(&more(rest));
    }
    (out, listed)
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
    fn a_digest_lists_oldest_first_within_its_caps_and_counts_the_rest() {
        let items: Vec<MessageView> = (0..30)
            .map(|i| {
                let mut m = view(&format!("note {i}"));
                m.id = format!("018f0000-0000-7000-8000-{i:012}");
                m.kind = Kind::Info;
                m.created_at_ms = i * 1000;
                m
            })
            .collect();
        let (text, listed) = digest(&items, 35, "codex", 40_000);
        assert_eq!(listed, DIGEST_MAX_ITEMS);
        let lines: Vec<&str> = text.lines().collect();
        assert!(lines[0].starts_with("[chatd digest for codex: 35 queued, oldest first;"), "{}", lines[0]);
        assert_eq!(lines.len(), 1 + DIGEST_MAX_ITEMS + 1);
        assert_eq!(lines[1], "  018f0000-0000-7000-8000-000000000000 codex adr628 info 40s: note 0");
        assert!(lines[DIGEST_MAX_ITEMS].contains("note 19"), "newest last");
        assert_eq!(lines[DIGEST_MAX_ITEMS + 1], "  +15 more (chatctl status --as codex)");
        assert!(text.len() <= DIGEST_MAX_BYTES);
        // Long summaries hit the byte cap before the line cap.
        let long: Vec<MessageView> = items
            .iter()
            .cloned()
            .map(|mut m| {
                m.summary = "漢".repeat(MAX_SUMMARY_CHARS);
                m
            })
            .collect();
        let (text, listed) = digest(&long, 30, "codex", 40_000);
        assert!(text.len() <= DIGEST_MAX_BYTES, "{}", text.len());
        assert!(listed < DIGEST_MAX_ITEMS && listed > 0, "{listed}");
        assert!(text.ends_with(&format!("+{} more (chatctl status --as codex)", 30 - listed)));
        assert!(!text.chars().any(|c| c.is_control() && c != '\n'));
        assert_eq!(digest(&[], 0, "codex", 0), (String::new(), 0));
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
