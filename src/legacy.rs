//! Inert import of the old Markdown bridge files. Blocks keep their source hash, byte offsets and raw
//! text; malformed spans are kept and labelled; nothing is inferred (no reply links, no acks) and
//! nothing imported ever becomes new work or wakes anyone. Repeat imports are idempotent.

use std::path::Path;

use rusqlite::{OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};

use crate::proto::{ErrorCode, LegacyFileReport, LegacyReport};
use crate::store::{Result, Store, StoreError, now_ms};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    pub start: usize,
    pub end: usize,
    pub header: String,
    pub sender: Option<String>,
    pub malformed: bool,
}

fn header_sender(line: &str) -> Option<Option<String>> {
    let rest = line.strip_prefix('[')?;
    let close = rest.find(']')?;
    let tag = &rest[..close];
    let name = tag.split_whitespace().next()?;
    match name {
        "codex" | "claude" => Some(Some(name.to_string())),
        "from" => Some(None),
        _ => None,
    }
}

/// Splits a bridge file into blocks. A block starts at a `[codex …]`/`[claude …]` header line and
/// ends after its `--- end ---` line; a block without its end (cut by the next header or EOF) is
/// malformed. Text outside any block becomes a malformed block of its own, so every byte is covered.
pub fn parse(bytes: &[u8]) -> Vec<Block> {
    let text = String::from_utf8_lossy(bytes);
    let mut blocks = Vec::new();
    let mut cur: Option<Block> = None;
    let mut stray_start: Option<usize> = None;
    let mut offset = 0usize;
    for line in text.split_inclusive('\n') {
        let start = offset;
        offset += line.len();
        let trimmed = line.trim_end_matches(['\n', '\r']);
        if let Some(sender) = header_sender(trimmed) {
            if let Some(mut b) = cur.take() {
                b.end = start;
                b.malformed = true;
                blocks.push(b);
            }
            if let Some(s) = stray_start.take() {
                blocks.push(Block { start: s, end: start, header: String::new(), sender: None, malformed: true });
            }
            cur = Some(Block { start, end: offset, header: trimmed.chars().take(80).collect(), sender, malformed: false });
            continue;
        }
        match cur.as_mut() {
            Some(b) => {
                b.end = offset;
                if trimmed.trim() == "--- end ---" {
                    blocks.push(cur.take().unwrap());
                }
            }
            None => {
                if !trimmed.trim().is_empty() && stray_start.is_none() {
                    stray_start = Some(start);
                }
            }
        }
    }
    if let Some(mut b) = cur.take() {
        b.end = offset;
        b.malformed = true;
        blocks.push(b);
    }
    if let Some(s) = stray_start.take() {
        blocks.push(Block { start: s, end: offset, header: String::new(), sender: None, malformed: true });
    }
    blocks.sort_by_key(|b| b.start);
    blocks
}

pub fn import(store: &mut Store, paths: &[String], dry_run: bool) -> Result<LegacyReport> {
    let mut files = Vec::new();
    for p in paths {
        let path = Path::new(p);
        if !path.is_absolute() {
            return Err(StoreError::new(ErrorCode::BadRequest, format!("{p}: legacy paths must be absolute")));
        }
        let bytes = std::fs::read(path).map_err(|e| StoreError::new(ErrorCode::NotFound, format!("{p}: {e}")))?;
        let sha = format!("{:x}", Sha256::digest(&bytes));
        let blocks = parse(&bytes);
        let conn = store.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let known: Option<String> = tx.query_row("SELECT path FROM legacy_sources WHERE sha256 = ?1", [&sha], |r| r.get(0)).optional()?;
        if known.is_none() {
            tx.execute(
                "INSERT INTO legacy_sources (sha256, path, bytes, imported_at_ms) VALUES (?1, ?2, ?3, ?4)",
                params![sha, p, bytes.len() as i64, now_ms()],
            )?;
        }
        let (mut fresh, mut present) = (0u64, 0u64);
        for b in &blocks {
            let raw = String::from_utf8_lossy(&bytes[b.start..b.end]).into_owned();
            let n = tx.execute(
                "INSERT OR IGNORE INTO legacy_blocks (source_sha256, start_offset, end_offset, header, sender, malformed, raw) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![sha, b.start as i64, b.end as i64, b.header, b.sender, b.malformed as i64, raw],
            )?;
            if n == 1 { fresh += 1 } else { present += 1 }
        }
        if dry_run {
            tx.rollback()?;
        } else {
            tx.commit()?;
        }
        files.push(LegacyFileReport {
            path: p.clone(),
            sha256: sha,
            bytes: bytes.len() as u64,
            blocks: blocks.len() as u64,
            malformed: blocks.iter().filter(|b| b.malformed).count() as u64,
            newly_imported: fresh,
            already_present: present,
        });
    }
    Ok(LegacyReport { dry_run, files })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_cover_well_formed_cut_and_stray_text() {
        let src = b"intro\n[codex 12:00]\nhello\n--- end ---\n\n[claude 12:01] inline\nno end\n[codex 12:02]\nok\n--- end ---\n";
        let blocks = parse(src);
        let kinds: Vec<(Option<&str>, bool)> = blocks.iter().map(|b| (b.sender.as_deref(), b.malformed)).collect();
        assert_eq!(kinds, vec![(None, true), (Some("codex"), false), (Some("claude"), true), (Some("codex"), false)]);
        assert_eq!(&src[blocks[1].start..blocks[1].end], b"[codex 12:00]\nhello\n--- end ---\n");
        assert_eq!(blocks.last().unwrap().end, src.len());
    }
}
