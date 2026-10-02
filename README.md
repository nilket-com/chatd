# chatd

A durable local message broker between coding agents (Claude, Codex). It replaces the Markdown
bridge files. Design and decisions: `plans/0001_durable_local_agent_chat.md` (written when the
project was called agent-chat).

- `chatd`: the daemon. It runs as a systemd user service (`Type=notify`), keeps all state in
  SQLite (WAL, `synchronous=FULL`) under `$XDG_STATE_HOME/chatd`, and serves a 0600 Unix
  socket at `$XDG_RUNTIME_DIR/chatd/chatd.sock`.
- `chatctl`: the CLI. (Not `chat`: `/usr/sbin/chat` is ppp's chat program.)

## Use

```sh
KEY=$(chatctl new-key)        # make and keep the key BEFORE sending: after a lost receipt,
                                 # retry with the same key (or `chatctl lookup`); never a new one
chatctl send --as claude --to codex --conversation adr628 --idempotency-key "$KEY" < note.md
chatctl wait --as codex                         # blocks; prints one notification line
chatctl receive --as codex --id <id>            # full body; does not acknowledge
chatctl ack --as codex <id>                     # receipt (not an answer)
RKEY=$(chatctl new-key)                         # likewise kept before the reply is sent
chatctl reply --as codex --to-message <id> --final --idempotency-key "$RKEY" < answer.md
chatctl status [--limit N]                      # bounded pages with totals: unacknowledged / no final reply recorded / submissions
chatctl watch --as claude [--resume-token T]    # server-pushed event stream (JSON lines)
chatctl journal -f                              # daemon journal, claude orange, codex cyan
```

## The states of a message

| State | Meaning |
| --- | --- |
| stored | the receipt's id was committed |
| submitted | a delivery adapter (`codex queue`) returned success; this is NOT receipt |
| uncertain | the adapter timed out or the daemon stopped mid-submission; never retried automatically (`chatctl redeliver`) |
| received | the recipient acknowledged it |
| answered | a `final` reply references it (progress replies do not answer) |

A watch starts with a paged snapshot of the authoritative unacknowledged inbox (bounded pages;
`SnapshotBegin`, `SnapshotPage`…, `SnapshotEnd`), then streams events after it. If its resume
token is refused (malformed, from another store or filter, beyond the head, or from before a
restore), the watch says `resynchronized` and the snapshot is the authoritative inbox. Submission,
receipt and answer state is NOT in that snapshot: refetch it with `chatctl status`.

Printing a notification never acknowledges a message. `wait`, `wait --follow` and `watch` replay
everything that is still unacknowledged whenever they reconnect.

## Install (after review)

Build on a separate build host (`cargo build --release`), then copy `chatctl` and `chatd` to
`~/.local/bin` and `units/chatd.service` to `~/.config/systemd/user/`. Run
`systemctl --user daemon-reload && systemctl --user enable --now chatd`.

The service follows the user manager's lifetime. Lingering is not enabled.

## Backup and restore

`chatctl backup /abs/path.db` makes a consistent online copy. To restore it, stop the
service and run `chatd restore /abs/path.db`. The restore is failure-atomic:
- it first prepares a validated candidate with a fresh random identity;
- the active store is replaced only after a durable `RESTORE-IN-PROGRESS` marker;
- every start finishes an interrupted activation.

The old database and its WAL are moved to `replaced-<ms>/` and never deleted. Every watcher must
resynchronize, and refetch `chatctl status`.

## Colours in the journal

`journalctl` colours lines by priority only. The daemon writes structured fields instead
(`CHATD_SENDER`, `CHATD_RECIPIENT`, `CHATD_MESSAGE_ID`, `CHATD_EVENT`; never
bodies or keys), so you can filter with `journalctl --user -u chatd CHATD_SENDER=codex`.
`chatctl journal` renders the same records with one colour per sender.
