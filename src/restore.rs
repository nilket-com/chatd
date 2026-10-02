//! Offline restore, failure-atomic and recoverable.
//!
//! Protocol (all under the state directory's exclusive lock):
//! 1. Prepare: copy the backup into `restore-candidate/chat.db` with the SQLite backup API, check
//!    integrity and schema, give it a fresh random store id (durably committed), checkpoint it to a
//!    single file (no WAL), fsync it and re-verify the id. Failure or a crash here leaves the
//!    active store untouched; a leftover candidate is never active and is discarded.
//! 2. Commit point: write `RESTORE-IN-PROGRESS` (new id, kept directory), fsync it and the
//!    directory. From here the restore always rolls forward.
//! 3. Activate: move the active `chat.db-wal`, `chat.db-shm` and `chat.db` (in that order) into
//!    `replaced-<ms>/`, rename the candidate to `chat.db`, fsync, remove the marker. The old WAL is
//!    always moved out BEFORE the candidate arrives, so it can never be applied to it.
//! 4. Recovery: every daemon start and every restore first runs `recover`. With a marker present
//!    it finishes step 3 idempotently and verifies the active id. Without a marker it discards any
//!    candidate.
//!
//! So after any failure or interruption, the active path holds either the untouched old store or
//! the complete restored store under its fresh identity, never an old-identity restored branch.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};

use crate::store::{SCHEMA_VERSION, now_ms};

pub const MARKER: &str = "RESTORE-IN-PROGRESS";
pub const CANDIDATE_DIR: &str = "restore-candidate";
const ACTIVE: [&str; 3] = ["chat.db-wal", "chat.db-shm", "chat.db"];

#[derive(Debug, Serialize, Deserialize)]
struct Marker {
    new_store_id: String,
    kept: PathBuf,
    backup: String,
}

/// Test hook: `CHATD_TEST_FAULT=<point>` exits the process at that point (a simulated
/// crash), so the fault-injection tests can interrupt each step of the protocol.
fn fault(point: &str) {
    if std::env::var("CHATD_TEST_FAULT").is_ok_and(|p| p == point) {
        std::process::exit(86);
    }
}

fn sync_dir(dir: &Path) -> Result<(), String> {
    fs::File::open(dir).and_then(|d| d.sync_all()).map_err(|e| format!("{}: fsync: {e}", dir.display()))
}

fn sync_file(path: &Path) -> Result<(), String> {
    fs::File::open(path).and_then(|f| f.sync_all()).map_err(|e| format!("{}: fsync: {e}", path.display()))
}

fn store_id_of(path: &Path) -> Result<String, String> {
    let c = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(|e| format!("{}: {e}", path.display()))?;
    c.query_row("SELECT value FROM meta WHERE key = 'store_id'", [], |r| r.get(0)).map_err(|e| format!("{}: {e}", path.display()))
}

/// Builds and verifies the candidate. Returns the new store id. Touches nothing active.
fn prepare_candidate(state_dir: &Path, backup: &Path) -> Result<(PathBuf, String), String> {
    let dir = state_dir.join(CANDIDATE_DIR);
    if dir.exists() {
        fs::remove_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    fs::create_dir(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let cand = dir.join("chat.db");
    let src = Connection::open_with_flags(backup, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(|e| format!("{}: {e}", backup.display()))?;
    let check: String = src.query_row("PRAGMA integrity_check", [], |r| r.get(0)).map_err(|e| format!("{}: {e}", backup.display()))?;
    if check != "ok" {
        return Err(format!("{}: integrity_check: {check}", backup.display()));
    }
    let version: String = src
        .query_row("SELECT value FROM meta WHERE key = 'schema_version'", [], |r| r.get(0))
        .map_err(|e| format!("{}: not a chatd store ({e})", backup.display()))?;
    if version != SCHEMA_VERSION.to_string() {
        return Err(format!("{}: schema version {version} is not supported by this build", backup.display()));
    }
    let old_id: String = src
        .query_row("SELECT value FROM meta WHERE key = 'store_id'", [], |r| r.get(0))
        .map_err(|e| format!("{}: {e}", backup.display()))?;
    let mut dst = Connection::open(&cand).map_err(|e| format!("{}: {e}", cand.display()))?;
    rusqlite::backup::Backup::new(&src, &mut dst)
        .and_then(|b| b.run_to_completion(256, std::time::Duration::from_millis(5), None))
        .map_err(|e| format!("copying the backup: {e}"))?;
    fault("after_copy");
    let new_id = uuid::Uuid::new_v4().to_string();
    {
        dst.execute_batch("PRAGMA foreign_keys = ON; PRAGMA synchronous = FULL;").map_err(|e| e.to_string())?;
        let tx = dst.transaction().map_err(|e| e.to_string())?;
        let n = tx
            .execute("UPDATE meta SET value = ?1 WHERE key = 'store_id'", [&new_id])
            .map_err(|e| format!("assigning the new identity: {e}"))?;
        if n != 1 {
            return Err("assigning the new identity: no store_id row".into());
        }
        tx.execute(
            "INSERT INTO meta (key, value) VALUES ('restored_from', ?1) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            [serde_json::json!({"store_id": old_id, "path": backup.display().to_string(), "at_ms": now_ms()}).to_string()],
        )
        .map_err(|e| format!("recording the restore: {e}"))?;
        tx.commit().map_err(|e| format!("committing the new identity: {e}"))?;
    }
    fault("after_identity");
    // A single self-contained file: no WAL may travel with the candidate.
    let mode: String = dst.query_row("PRAGMA journal_mode = DELETE", [], |r| r.get(0)).map_err(|e| e.to_string())?;
    if !mode.eq_ignore_ascii_case("delete") {
        return Err(format!("candidate journal_mode is {mode}"));
    }
    drop(dst);
    if dir.join("chat.db-wal").exists() {
        return Err("the candidate still has a WAL".into());
    }
    sync_file(&cand)?;
    sync_dir(&dir)?;
    let check = store_id_of(&cand)?;
    if check != new_id {
        return Err(format!("candidate identity {check} is not the assigned {new_id}"));
    }
    Ok((cand, new_id))
}

/// Finishes (or discards) an interrupted restore. Idempotent; run before anything opens the store.
pub fn recover(state_dir: &Path) -> Result<Option<String>, String> {
    let marker_path = state_dir.join(MARKER);
    let cand_dir = state_dir.join(CANDIDATE_DIR);
    if !marker_path.exists() {
        if cand_dir.exists() {
            // Never published: discard.
            fs::remove_dir_all(&cand_dir).map_err(|e| format!("{}: {e}", cand_dir.display()))?;
        }
        return Ok(None);
    }
    let marker: Marker = serde_json::from_slice(&fs::read(&marker_path).map_err(|e| format!("{}: {e}", marker_path.display()))?)
        .map_err(|e| format!("{}: unreadable restore marker ({e}); refusing to start", marker_path.display()))?;
    let cand = cand_dir.join("chat.db");
    fs::create_dir_all(&marker.kept).map_err(|e| format!("{}: {e}", marker.kept.display()))?;
    if cand.exists() {
        for name in ACTIVE {
            let p = state_dir.join(name);
            if p.exists() {
                let to = marker.kept.join(name);
                if to.exists() {
                    return Err(format!("{} and {} both exist; refusing to overwrite", p.display(), to.display()));
                }
                fs::rename(&p, &to).map_err(|e| format!("{}: {e}", p.display()))?;
                if name == "chat.db-wal" {
                    fault("after_move_wal");
                }
            }
        }
        sync_dir(&marker.kept)?;
        sync_dir(state_dir)?;
        fault("before_publish");
        fs::rename(&cand, state_dir.join("chat.db")).map_err(|e| format!("{}: {e}", cand.display()))?;
        sync_dir(state_dir)?;
    }
    for name in ["chat.db-wal", "chat.db-shm"] {
        if state_dir.join(name).exists() {
            return Err(format!("{} is present beside the restored store; refusing to start", state_dir.join(name).display()));
        }
    }
    let active = store_id_of(&state_dir.join("chat.db"))?;
    if active != marker.new_store_id {
        return Err(format!("the active store id {active} is not the restore's {}; refusing to start", marker.new_store_id));
    }
    fault("before_marker_removal");
    if cand_dir.exists() {
        fs::remove_dir_all(&cand_dir).map_err(|e| format!("{}: {e}", cand_dir.display()))?;
    }
    fs::remove_file(&marker_path).map_err(|e| format!("{}: {e}", marker_path.display()))?;
    sync_dir(state_dir)?;
    Ok(Some(marker.new_store_id))
}

/// Runs the whole protocol. The caller holds the state lock and has checked that no daemon runs.
pub fn restore(state_dir: &Path, backup: &Path) -> Result<(String, PathBuf), String> {
    recover(state_dir)?;
    let (_cand, new_id) = match prepare_candidate(state_dir, backup) {
        Ok(x) => x,
        Err(e) => {
            let _ = fs::remove_dir_all(state_dir.join(CANDIDATE_DIR));
            return Err(e);
        }
    };
    let kept = state_dir.join(format!("replaced-{}", now_ms()));
    fs::create_dir(&kept).map_err(|e| format!("{}: {e}", kept.display()))?;
    let marker = Marker { new_store_id: new_id.clone(), kept: kept.clone(), backup: backup.display().to_string() };
    let tmp = state_dir.join(format!("{MARKER}.tmp"));
    {
        let mut f = fs::File::create(&tmp).map_err(|e| format!("{}: {e}", tmp.display()))?;
        f.write_all(&serde_json::to_vec(&marker).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
        f.sync_all().map_err(|e| e.to_string())?;
    }
    fs::rename(&tmp, state_dir.join(MARKER)).map_err(|e| format!("{}: {e}", tmp.display()))?;
    sync_dir(state_dir)?;
    fault("after_marker");
    recover(state_dir)?;
    Ok((new_id, kept))
}
