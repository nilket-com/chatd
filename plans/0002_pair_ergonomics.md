# 0002: Codex-Claude pair ergonomics

Status: implemented on main (items 1-4) after Codex review in the `chatd-ergonomics-pairs` conversation. Item 1 removes chatctl's deadline; the Claude Code Monitor is capped at 30 minutes.
Scope: a two-agent pair (Claude + Codex) on one host. Three-agent graphs are deferred until this is smooth.

## Observed friction

1. **Claude wake-up is pull, and expires.** The documented Monitor runs
   `chatctl wait --as claude --follow --timeout 1800`; it ends every 30 minutes and Claude must re-arm
   it. A missed expiry is silent: the peer sees an unacknowledged message and no watcher.
   Codex, by contrast, gets push through `codex queue`.
2. **`--as` repeats on every call** (`who` is a required argument of send, reply, receive, ack, wait,
   watch; `src/bin/chatctl.rs`).
3. **Idempotency keys are hand-managed.** `--idempotency-key` is required on send and reply. The
   caller must `new-key`, store it, and reuse it after a lost receipt.
4. **Taking a message is three calls**: `receive` (body, no ack), `ack`, then `reply`.

## Proposals

### 1. Untimed follow (chatctl) and the Monitor cap (Claude Code)
chatctl: `wait --follow` with no `--timeout` (or `0`) has no deadline and reconnects across a chatd
restart (Codex's review of src/bin/chatctl.rs: the old default was a fixed 1800 s deadline).
Claude Code's Monitor is a separate limit: `timeout_ms` defaults to 5 minutes and is capped at 30;
the tool has no `persistent` option. An earlier draft of this note claimed the flag was tried and
did not help; that was wrong (the flag does not exist and the 5 minutes was the default). Use
`timeout_ms: 1800000` and re-arm every 30 minutes. Verified 2026-10-15: that setting reports
"expires in 30m".

### 2. Default participant
Give `who` clap's `env = "CHATD_AS"` so `export CHATD_AS=claude` removes the flag. An explicit `--as`
still wins. No wire change. Participant names remain routing labels (README, Scope and limits).

### 3. Keys that cannot be lost
Make `--idempotency-key` optional on send and reply. When omitted, chatctl generates a key and
writes it to `$XDG_STATE_HOME/chatctl/pending/<hash>` *before* the request is sent. The hash covers
every semantic field of the request: sender, recipient, conversation, kind, summary, reply target,
final flag, and body (a changed request must never collide with a stored key). The record is written
atomically with private (0600) permissions before dispatch. The same command with the same body reuses the stored key, and the file is
removed once a receipt is printed. A lost receipt then retries safely by re-running the same
command. Explicit keys keep today's behavior.

### 4. `chatctl take`
`chatctl take --as X --id ID` prints the message (as `receive` does) and acknowledges it in one
call. `receive` and `ack` stay separate for callers that need to read before committing. Ack still
means received, not done. Ordering: `take` writes the body to stdout successfully and flushes it
before acking, so a stdout failure never marks an unseen message received.

## Not changing
- Delivery states (stored / submitted / uncertain / received / answered) and the rule that an
  adapter success is not receipt.
- Automatic retry of uncertain attempts: still explicit via `redeliver`.

## Verification
- Item 1: a persistent Monitor receives a message 45+ minutes after arming, and after
  `systemctl --user restart chatd`.
- Item 2: unit test for flag-over-env precedence.
- Item 3: kill chatctl between key write and receipt; the rerun yields `already stored`, not a
  duplicate.
- Item 4: `take` on an already-acknowledged id reports `already 1` and still prints the body.

## Cut 2: duplicate watchers (Codex's first pick)

Observed live: overlapping Claude Monitors each printed every notification. `SnapshotBegin` now
carries `other_watchers` (overlapping watchers of the same participant at registration); chatctl
warns once. `Watch.exclusive` makes the daemon refuse (Conflict, chatctl exit 7) when any overlap
exists, counted and registered under one lock. Both fields are optional on the wire (protocol
version unchanged); a client that asked for `--exclusive` and got no count refuses, because the
daemon predates the option.

Not in this cut (Codex-side ranking, to revisit): deferring or prioritising push into a busy turn;
reading before acknowledging as the default (`take` is for the immediate case); a direct reply path
for info messages (today a new `send`).

Decision (Codex review of 01e6ea9, P2): `exclusive` binds only the watch that requests it, at
registration. A later plain watch is still admitted and warns, because refusing it would block a
one-shot manual `wait` next to a standing exclusive Monitor. If a stronger guarantee is wanted, track
the bit in WatcherView and refuse overlapping registrations while it is set.

## Spike: is a `codex queue` equivalent for Claude patchable from outside? (2026-10-15)

Throwaway background session (`claude --bg`, Haiku, text only), driven three ways:

- `claude --bg --resume <session-id> "msg"` does NOT append to a live session. With the short id or
  the full id, on an idle background session, it starts a COPY ("session ... is already running in
  the background, so this started a copy"). Not a queue equivalent. (Stray copies were stopped and
  removed.)
- `SendMessage` from another Claude session DOES wake an idle session: the idle background session
  took the message, answered, and its reply arrived in the sender's conversation as a cross-session
  message while the sender was mid-turn. `notify_when_idle` is available. Sender must be a Claude
  session; chatd is not one.
- Transport: each session listens on a Unix socket under `/run/user/<uid>/cc-socks/<pid>.sock`
  (the reply's `from` was `uds:/run/user/1000/cc-socks/<pid>.sock`). Its protocol is undocumented
  and was NOT probed. A chatd adapter that wrote to it would depend on a private interface and would
  inject text into a session as a trusted "teammate" message, so it needs an explicit decision, not an
  incidental one.

Consequence for graphs: two Claude sessions can already wake each other natively. chatd still adds
durability, acks, retry-safe keys and Codex in the same conversation, which SendMessage does not.

## Cut 3: whole messages in the stream

`wait --follow --full [--take]`: one JSON line per message, body included (cut at 16 KiB), instead of
a notification line followed by `receive`/`take`. Bodies are fetched with a short `Receive` per
message (the watch stream carries none). Acknowledgement stays opt-in and happens only after the
line is flushed, the same ordering as `take`, so a crash between seeing and handling a message
leaves it unacknowledged and replayed. Motivation: the Monitor is the only way events reach Claude
between tool calls, and one line per event means a body must be a single line.

Correction to cut 3 (found live, 2026-10-15): the Claude Code Monitor shows only 500 characters of an
event, not 16 KiB. The first `--full` put `truncated`, `body_bytes` and the `take` hint AFTER the body,
so a cut line hid them, and `--take` would have acknowledged a message whose tail was never seen. Fixed:
the line is built to fit `--max-line` (default 480 bytes), flags precede the body, and `--take`
acknowledges only messages shown whole.
