# chatd

A durable local message broker between coding agents (Claude, Codex): request/reply messages
with stable ids, explicit acknowledgements and final replies, retry-safe keys, server-pushed
watches, and push delivery into a running Codex session.

The original design record is `plans/0001_durable_local_agent_chat.md`, written when the project
was called agent-chat. It is historical; this README is the current setup.

- `chatd`: the daemon. It runs as a systemd user service (`Type=notify`), keeps all state in
  SQLite (WAL, `synchronous=FULL`) under `$XDG_STATE_HOME/chatd`, and serves a 0600 Unix
  socket at `$XDG_RUNTIME_DIR/chatd/chatd.sock`.
- `chatctl`: the CLI. (Not `chat`: `/usr/sbin/chat` is ppp's chat program.)

## Use

```sh
KEY=$(chatctl new-key)        # make and keep the key BEFORE sending: after a lost receipt,
                                 # retry with the same key (or `chatctl lookup`); never a new one
chatctl send --as claude --to codex --conversation review --idempotency-key "$KEY" < note.md
chatctl wait --as codex                         # blocks; prints one notification line
chatctl receive --as codex --id <id>            # full body; does not acknowledge
chatctl ack --as codex <id>                     # receipt (not an answer)
RKEY=$(chatctl new-key)                         # likewise kept before the reply is sent
chatctl reply --as codex --to-message <id> --final --idempotency-key "$RKEY" < answer.md
chatctl status [--limit N]                      # bounded pages with totals: unacknowledged / no final reply recorded / submissions
chatctl watch --as claude [--resume-token T]    # server-pushed event stream (JSON lines)
chatctl journal -f                              # daemon journal, claude orange, codex electric blue
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

## Requirements

- Linux, with a systemd **user** manager (`systemctl --user`). The daemon is `Type=notify`.
- Rust 1.95 or newer (`rust-version` in `Cargo.toml`) to build.
- For push delivery to Codex: a compatible Codex CLI on the same host that exposes
  `codex queue --thread <session> --message <line>`. Check with `codex queue --help`. Without
  it, the broker and inboxes still work, but native push to Codex is unavailable.
- Python 3, only for the optional `ops/` helper.

## Install

```sh
cargo build --locked --release
install -d ~/.local/bin ~/.config/systemd/user
install -m 755 target/release/chatd target/release/chatctl ~/.local/bin/
install -m 644 units/chatd.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now chatd        # returns once chatd reports READY
export PATH="$HOME/.local/bin:$PATH"       # if ~/.local/bin is not already on your PATH
chatctl health
```

The unit runs `%h/.local/bin/chatd`. State lives in `~/.local/state/chatd/` and the socket in
`$XDG_RUNTIME_DIR/chatd/`. The participants default to `claude` and `codex`; set them in
`~/.config/chatd/config.toml` with `participants = ["claude", "codex"]`.

The service follows the user manager's lifetime. chatd does not enable lingering; without
lingering, the service may stop after your last login session ends.

## Connecting Codex (push)

chatd's adapter is for the Codex CLI `queue` interface. It delivers a short notification line
(never the message body) into a running Codex session with
`<exe> queue --thread <session> --message <line>`. You register the session explicitly; chatd
never discovers it:

```sh
chatctl endpoint register --as codex --session <codex-session-id> --exe /absolute/path/to/codex-adapter
```

`<codex-session-id>` is the id of an existing Codex session you choose (for example, from the
session's own `CODEX_THREAD_ID`). Re-registering creates a new generation, and failed attempts
under the old binding are fenced: they are never retried against the new session automatically.

The executable must be an absolute path that works under the systemd user manager's environment,
which usually lacks shell additions such as nvm. If `codex` is an `env node` script installed by
nvm, use a small wrapper with absolute paths:

```sh
#!/bin/sh
exec /path/to/node /path/to/lib/node_modules/@openai/codex/bin/codex.js "$@"
```

A successful `codex queue` means **submitted**, not received. Codex's acknowledgement is the
receipt. A timeout or a stop mid-submission leaves an **uncertain** attempt, which is never
repeated automatically (`chatctl redeliver <id>` is explicit).

## Connecting Claude (wake-up)

The tested Claude setup uses a Monitor for wake-up. A native injection path into an existing
Claude session was not established. A Claude Code Monitor runs

```sh
chatctl wait --as claude --follow --timeout 1800
```

and re-arms it on expiry. Each notification line wakes Claude, and chatd owns the backlog: a
`wait` started after any gap replays everything unacknowledged.

## Scope and limits

- **One local Unix user.** The socket is 0600 and the daemon checks the peer's uid.
  Participant names (`--as claude`) are routing labels, not identities isolated from each other.
- **No network listener or federation.** Remote use would go through SSH to the one host; it is
  not built.
- **What has been demonstrated** with real sessions: delivery to an idle and to a busy Codex
  session, invalid-session failure with fencing and explicit redelivery, and Claude's gap replay.
  Waking a fully closed Codex CLI was **not** tested.
- **ack means received, not done.** After any restart, check `chatctl status` for
  acknowledged-but-unanswered requests as well as the unacknowledged inbox. chatd's durability does
  not resume interrupted agent work, and nothing is executed exactly-once on an agent's behalf.

## Rollback handoff

`ops/handoff_export.py online|offline <dir>` writes every outstanding message with its full
body, replies, acknowledgements, attempts and a watermark, from a consistent snapshot. Offline mode
holds chatd's lock and works when the daemon is down. Exit status 0 means a durable, successful
handoff; anything else is not one. Its tests are in `ops/test_handoff_export.py`.

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

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in
the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without
any additional terms or conditions.
