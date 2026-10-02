#!/usr/bin/env python3
"""chatd rollback handoff export: every outstanding message with its full body, replies,
acknowledgements, submission attempts and identifiers, plus the context needed to read it. The
export comes from a SNAPSHOT of the store, never from the live files.

usage:
  handoff_export.py online  <out-dir>    # chatd running: `chatctl backup` makes a consistent snapshot
  handoff_export.py offline <out-dir>    # chatd stopped or dead: copies chat.db (+ -wal, -shm)

offline holds the daemon's own exclusive lock (`<state>/chatd.lock`, flock LOCK_EX|LOCK_NB) from
before the copy until the copy is complete. A held lock means a writer (or a starting daemon) owns
the store, and the export refuses. It also refuses a pending restore (`RESTORE-IN-PROGRESS` or
`restore-candidate/`): resolve that first (starting chatd completes it), because the active file
may not be the store that is about to be active. It never runs recovery on the live store.

Every snapshot must pass `integrity_check`, carry schema version 1, and have a store id. The export
is built in `<out-dir>.partial`, reserved exclusively (an existing one is refused and left
untouched), fsynced, and renamed to `<out-dir>` only when complete (then the parent is fsynced).

Exit status:
- 0: success (published and durable);
- 1: refused or failed BEFORE publication. Nothing was published, and only a partial directory
  this run created is removed;
- 3: failed AFTER publication. The output is retained but its durability is not confirmed. This
  is NOT a successful handoff, and the existence of the directory never justifies proceeding.

**Outstanding** means: a request with no final reply, or any message its recipient has not
acknowledged. Each outstanding item carries, as separate `context`, its parent request (if it is a
reply) with that request's replies, acknowledgements and attempts.

**Delivery** is judged from the WHOLE attempt history, never from the last attempt alone:
- recipient acknowledgement: receipt is established;
- any `started` or `uncertain` attempt: UNCERTAIN, even if later attempts failed;
- any `submitted` attempt: submitted at least once, which is not proof of receipt;
- failed only: every RECORDED attempt failed (delivery outside the record is not ruled out);
- none: no recorded submission attempt.

The watermark (store id, head sequence, snapshot time and mode) bounds what is known. Activity after
it is unknown. Never repeat uncertain work automatically, and never treat it as cleared.
"""

import fcntl
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time

SCHEMA_VERSION = "1"
TIMEOUT_S = 60


def state_dir():
    if os.environ.get("CHATD_STATE_DIR"):
        return os.environ["CHATD_STATE_DIR"]
    base = os.environ.get("XDG_STATE_HOME") or os.path.expanduser("~/.local/state")
    return os.path.join(base, "chatd")


def chatctl():
    return os.environ.get("CHATD_CTL", "chatctl")


class Refused(Exception):
    """Before publication: nothing was published."""


class PublishedNotDurable(Exception):
    """After the rename: the output exists, but its durability was not confirmed."""


def snapshot_online(snap):
    db = os.path.join(snap, "chat.db")
    try:
        r = subprocess.run([chatctl(), "--json", "backup", db], capture_output=True, text=True, timeout=TIMEOUT_S)
    except subprocess.TimeoutExpired:
        raise Refused(f"`chatctl backup` gave no result within {TIMEOUT_S} s")
    if r.returncode != 0:
        raise Refused(f"online snapshot failed (exit {r.returncode}): {r.stderr.strip()} (if chatd is down, use offline)")
    return db


def test_hook(name):
    """Test-only interleaving points (CHATD_EXPORT_TEST_HOOK=<name>:<action>), never set in use."""
    spec = os.environ.get("CHATD_EXPORT_TEST_HOOK", "")
    if not spec.startswith(name + ":"):
        return
    action = spec.split(":", 1)[1]
    src = state_dir()
    if action == "restore-marker":
        with open(os.path.join(src, "RESTORE-IN-PROGRESS"), "w") as f:
            f.write("{}")
    elif action == "restore-candidate":
        os.makedirs(os.path.join(src, "restore-candidate"), exist_ok=True)
    elif action == "remove-db":
        os.remove(os.path.join(src, "chat.db"))
    elif action == "fail-fsync":
        raise OSError(5, "injected fsync failure (test hook)")
    elif action.startswith("sleep="):
        time.sleep(float(action.split("=", 1)[1]))


def snapshot_offline(snap):
    src = state_dir()
    lock_path = os.path.join(src, "chatd.lock")
    try:
        lock = open(lock_path, "a")
    except OSError as e:
        raise Refused(f"cannot open {lock_path}: {e}")
    try:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise Refused(f"{lock_path} is held: a daemon (or another tool) owns the store; use online, or stop it")
        test_hook("after-lock")
        # Every decision about which store is active, and the copy itself, happens under the lock.
        if os.path.exists(os.path.join(src, "RESTORE-IN-PROGRESS")) or os.path.exists(os.path.join(src, "restore-candidate")):
            raise Refused(f"{src} has a pending restore (RESTORE-IN-PROGRESS or restore-candidate/); start chatd to resolve it first")
        if not os.path.exists(os.path.join(src, "chat.db")):
            raise Refused(f"{src}/chat.db does not exist")
        for name in ("chat.db", "chat.db-wal", "chat.db-shm"):
            p = os.path.join(src, name)
            if os.path.exists(p):
                shutil.copy2(p, os.path.join(snap, name))
    finally:
        lock.close()  # releases the flock after the copy is complete
    return os.path.join(snap, "chat.db")


def delivery(attempts, acknowledged):
    states = [a["state"] for a in attempts]
    if acknowledged:
        return "receipt established: the recipient acknowledged it"
    if any(s in ("started", "uncertain") for s in states):
        return "UNCERTAIN: an attempt may have reached the session; do not repeat automatically or treat as cleared"
    if "submitted" in states:
        return "submitted at least once (not proof of receipt)"
    if states:
        return f"all {len(states)} recorded attempt(s) failed; delivery outside the recorded attempts is not ruled out"
    return "no recorded submission attempt"


def fsync_path(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def export(mode, out, owned):
    """`owned` records the partial directory only once THIS invocation has created it, so failure
    cleanup never touches another export's (or an interrupted export's) directory."""
    if os.path.exists(out):
        raise Refused(f"{out} already exists")
    part = out + ".partial"
    try:
        os.mkdir(part, 0o700)  # exclusive reservation
    except FileExistsError:
        raise Refused(f"{part} exists (another export in progress, or an interrupted one); it is left untouched")
    owned.append(part)
    test_hook("after-reserve")
    snap = os.path.join(part, "snapshot")
    os.makedirs(snap, mode=0o700)
    db = snapshot_online(snap) if mode == "online" else snapshot_offline(snap)

    c = sqlite3.connect(db)  # the snapshot copy only
    c.row_factory = sqlite3.Row
    q = lambda sql, *a: c.execute(sql, a).fetchall()
    try:
        check = q("PRAGMA integrity_check")[0][0]
    except sqlite3.DatabaseError as e:
        raise Refused(f"snapshot is not a readable database: {e}")
    if check != "ok":
        raise Refused(f"snapshot integrity_check: {check}")
    try:
        meta = {r["key"]: r["value"] for r in q("SELECT key, value FROM meta")}
    except sqlite3.DatabaseError as e:
        raise Refused(f"snapshot is not a chatd store: {e}")
    if meta.get("schema_version") != SCHEMA_VERSION or not meta.get("store_id"):
        raise Refused(f"snapshot schema {meta.get('schema_version')!r} / store id {meta.get('store_id')!r} is not supported")
    head = q("SELECT coalesce(max(seq), 0) AS h FROM events")[0]["h"]

    def acks(mid):
        return [dict(r) for r in q("SELECT reader, at_ms FROM acks WHERE message_id = ? ORDER BY at_ms", mid)]

    def attempts(mid):
        return [dict(r) for r in q("SELECT * FROM attempts WHERE message_id = ? ORDER BY id", mid)]

    def full(mid):
        m = dict(q("SELECT * FROM messages WHERE id = ?", mid)[0])
        m["acknowledgements"] = acks(mid)
        m["attempts"] = attempts(mid)
        recipient_acked = any(a["reader"] == m["recipient"] for a in m["acknowledgements"])
        m["delivery"] = delivery(m["attempts"], recipient_acked)
        return m

    def with_replies(mid):
        m = full(mid)
        m["replies"] = [full(r["id"]) for r in q("SELECT id FROM messages WHERE reply_to = ? ORDER BY seq", mid)]
        return m

    ids = [r["id"] for r in q(
        """SELECT m.id FROM messages m
           WHERE (m.kind = 'request' AND NOT EXISTS (SELECT 1 FROM messages r WHERE r.reply_to = m.id AND r.kind = 'final'))
              OR NOT EXISTS (SELECT 1 FROM acks a WHERE a.reader = m.recipient AND a.message_id = m.id)
           ORDER BY m.seq""")]
    items = []
    for mid in ids:
        m = with_replies(mid)
        acked = any(a["reader"] == m["recipient"] for a in m["acknowledgements"])
        answered = any(r["kind"] == "final" for r in m["replies"])
        m["status"] = ("answered" if answered else "received, no final reply" if acked else "not acknowledged by its recipient")
        m["context"] = {"parent_request": with_replies(m["reply_to"])} if m["reply_to"] else {}
        items.append(m)

    watermark = {"store_id": meta["store_id"], "head_seq": head, "snapshot_mode": mode,
                 "snapshot_at_unix_ms": int(time.time() * 1000),
                 "note": "activity after head_seq is unknown to this handoff"}
    with open(os.path.join(part, "handoff.json"), "w") as f:
        json.dump({"watermark": watermark, "outstanding": items}, f, indent=1)
    with open(os.path.join(part, "handoff.md"), "w") as f:
        f.write(f"# chatd handoff ({mode})\n\nWatermark: store {meta['store_id']}, head seq {head}, "
                f"snapshot at {watermark['snapshot_at_unix_ms']} ms. Activity after it is unknown.\n\n"
                f"{len(items)} outstanding message(s). Context sections are NOT outstanding work.\n")

        def section(m, level, label):
            f.write(f"\n{'#' * level} {label} {m['id']} ({m['kind']}, seq {m['seq']})\n\n"
                    f"- {m['sender']} -> {m['recipient']}, conversation `{m['conversation']}`, key `{m['idempotency_key']}`, created {m['created_at_ms']}\n"
                    f"- delivery: {m['delivery']}\n- summary: {m['summary']}\n")
            if m.get("status"):
                f.write(f"- status: {m['status']}\n")
            for a in m["acknowledgements"]:
                f.write(f"- acknowledged by {a['reader']} at {a['at_ms']}\n")
            for at in m["attempts"]:
                f.write(f"- attempt {at['id']} gen {at['generation']} {at['state']} exit {at['exit_code']} {at['error'] or ''}\n")
            f.write("\n```text\n" + m["body"] + "\n```\n")
            for r in m.get("replies", []):
                section(r, level + 1, "reply")

        for m in items:
            section(m, 2, "OUTSTANDING")
            if m["context"]:
                section(m["context"]["parent_request"], 3, "context (parent request, not outstanding)")
    c.close()
    # Persistence boundary: every file and directory is durable before publication, and the
    # rename is durable before success is reported.
    for root, dirs, files in os.walk(part, topdown=False):
        for name in files:
            os.chmod(os.path.join(root, name), 0o600)
            fsync_path(os.path.join(root, name))
        for name in dirs:
            os.chmod(os.path.join(root, name), 0o700)
            fsync_path(os.path.join(root, name))
    fsync_path(part)
    if os.path.exists(out):
        raise Refused(f"{out} appeared during the export; not overwritten")
    os.rename(part, out)
    owned.clear()  # published: never remove it now
    try:
        test_hook("after-rename")
        fsync_path(os.path.dirname(out))
    except OSError as e:
        raise PublishedNotDurable(f"{out}: {e}")
    return {"watermark": watermark, "outstanding": [(m["id"], m["kind"], m["status"], m["delivery"]) for m in items]}


def main(argv):
    if len(argv) != 3 or argv[1] not in ("online", "offline"):
        print(__doc__, file=sys.stderr)
        return 2
    out = os.path.abspath(argv[2])
    owned = []
    try:
        summary = export(argv[1], out, owned)
    except PublishedNotDurable as e:
        print(f"handoff_export: FAILED after publication: published output retained at {out}; durability not "
              f"confirmed ({e}); this is NOT a successful handoff. Do not proceed with rollback or cutover on it.",
              file=sys.stderr)
        return 3
    except (Refused, OSError, sqlite3.Error) as e:
        for part in owned:  # only a partial directory this invocation created
            shutil.rmtree(part, ignore_errors=True)
        print(f"handoff_export: REFUSED, no handoff written: {e}", file=sys.stderr)
        return 1
    print(json.dumps(summary, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
