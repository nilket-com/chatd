//! Pending idempotency keys (plan 0002, item 3). When a caller omits `--idempotency-key`, chatctl
//! derives an identity from every semantic field of the request and persists a generated key under
//! it *before* dispatch. Re-running the same command after a lost receipt reuses the key; the record
//! is removed once a receipt is seen.

use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// The semantic fields of a send or reply. A change to any of them is a different request.
pub struct Fields<'a> {
    pub from: &'a str,
    pub to: Option<&'a str>,
    pub conversation: Option<&'a str>,
    pub kind: &'a str,
    pub reply_to: Option<&'a str>,
    pub summary: Option<&'a str>,
    pub body: &'a str,
}

/// Hex SHA-256 over length-prefixed fields, so no two field splits can collide.
pub fn identity(f: &Fields) -> String {
    let mut h = Sha256::new();
    h.update(b"chatctl-pending-v1");
    for part in [Some(f.from), f.to, f.conversation, Some(f.kind), f.reply_to, f.summary, Some(f.body)] {
        match part {
            Some(s) => {
                h.update([1u8]);
                h.update((s.len() as u64).to_le_bytes());
                h.update(s.as_bytes());
            }
            None => h.update([0u8]),
        }
    }
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

fn valid(key: &str) -> bool {
    !key.is_empty() && key.len() <= 128 && key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

fn path(dir: &Path, id: &str) -> PathBuf {
    dir.join(id)
}

fn read_key(p: &Path) -> io::Result<String> {
    let mut s = String::new();
    File::open(p)?.read_to_string(&mut s)?;
    let key = s.trim().to_string();
    if valid(&key) {
        Ok(key)
    } else {
        Err(io::Error::new(io::ErrorKind::InvalidData, format!("{}: not a valid pending key; remove it to start over", p.display())))
    }
}

/// The stored key for `id`, or a newly generated one written atomically (0600, fsynced, never
/// overwriting) before this returns. The bool is true when the key already existed.
pub fn key_for(dir: &Path, id: &str) -> io::Result<(String, bool)> {
    DirBuilder::new().recursive(true).mode(0o700).create(dir)?;
    let target = path(dir, id);
    match read_key(&target) {
        Ok(k) => return Ok((k, true)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let key = uuid::Uuid::new_v4().to_string();
    let tmp = dir.join(format!(".{id}.{}.tmp", std::process::id()));
    {
        let mut f = OpenOptions::new().write(true).create_new(true).mode(0o600).open(&tmp)?;
        f.write_all(key.as_bytes())?;
        f.write_all(b"\n")?;
        f.sync_all()?;
    }
    // A hard link refuses to replace an existing record, so two racing chatctls agree on one key.
    let linked = fs::hard_link(&tmp, &target);
    let _ = fs::remove_file(&tmp);
    match linked {
        Ok(()) => {
            File::open(dir)?.sync_all()?;
            Ok((key, false))
        }
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => read_key(&target).map(|k| (k, true)),
        Err(e) => Err(e),
    }
}

/// Forget the record once a receipt has been seen.
pub fn clear(dir: &Path, id: &str) {
    let _ = fs::remove_file(path(dir, id));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn fields<'a>() -> Fields<'a> {
        Fields { from: "claude", to: Some("codex"), conversation: Some("c"), kind: "request", reply_to: None, summary: None, body: "hi" }
    }

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("chatd-pending-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn every_semantic_field_changes_the_identity() {
        let base = identity(&fields());
        let variants = [
            Fields { from: "codex", ..fields() },
            Fields { to: Some("claude"), ..fields() },
            Fields { to: None, ..fields() },
            Fields { conversation: Some("d"), ..fields() },
            Fields { kind: "info", ..fields() },
            Fields { reply_to: Some("m"), ..fields() },
            Fields { summary: Some("s"), ..fields() },
            Fields { body: "hi!", ..fields() },
        ];
        for v in &variants {
            assert_ne!(identity(v), base);
        }
        assert_eq!(identity(&fields()), base);
    }

    #[test]
    fn field_boundaries_cannot_collide() {
        let a = Fields { to: Some("ab"), conversation: Some("c"), ..fields() };
        let b = Fields { to: Some("a"), conversation: Some("bc"), ..fields() };
        assert_ne!(identity(&a), identity(&b));
        let none = Fields { summary: None, ..fields() };
        let empty = Fields { summary: Some(""), ..fields() };
        assert_ne!(identity(&none), identity(&empty));
    }

    #[test]
    fn key_is_stable_private_and_cleared() {
        let dir = tmp("stable").join("pending");
        let (k1, existed) = key_for(&dir, "abc").unwrap();
        assert!(!existed);
        let (k2, existed) = key_for(&dir, "abc").unwrap();
        assert!(existed);
        assert_eq!(k1, k2);
        assert_eq!(fs::metadata(dir.join("abc")).unwrap().permissions().mode() & 0o777, 0o600);
        assert_eq!(fs::metadata(&dir).unwrap().permissions().mode() & 0o777, 0o700);
        clear(&dir, "abc");
        let (k3, existed) = key_for(&dir, "abc").unwrap();
        assert!(!existed);
        assert_ne!(k1, k3);
        let leftovers: Vec<_> = fs::read_dir(&dir).unwrap().map(|e| e.unwrap().file_name()).collect();
        assert_eq!(leftovers.len(), 1, "no temp files left: {leftovers:?}");
    }

    #[test]
    fn corrupt_record_is_refused_not_replaced() {
        let dir = tmp("corrupt").join("pending");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("abc"), "not a key!\n").unwrap();
        assert!(key_for(&dir, "abc").is_err());
        assert_eq!(fs::read_to_string(dir.join("abc")).unwrap(), "not a key!\n");
    }
}
