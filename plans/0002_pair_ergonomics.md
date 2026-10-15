# 0002: Codex-Claude pair ergonomics

Status: implemented on main (items 1-4) after Codex review in the `chatd-ergonomics-pairs` conversation. Item 1 fixes chatctl's deadline only; the Claude Code Monitor still expires.
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

### 1. Persistent wake-up (not solved by the Monitor flag)
Tested 2026-10-15: starting the Claude Code Monitor with `persistent: true` and
`chatctl wait --as claude --follow` (no `--timeout`) still reported "expires in 5m". The flag did not
remove the expiry, so this is NOT a no-code fix; the earlier draft of this item was wrong.
Codex's source review (src/bin/chatctl.rs) found the cause: `--timeout` is a `u64` defaulting to
1800 and `wait()` always computes a fixed deadline, so `--follow` is never unbounded. Follow mode
does reconnect after a chatd restart while that deadline remains (it retries failed connects and
interrupted watches, then registers a fresh watch with no resume token; the initial snapshot
replays the unacknowledged inbox).
Proposed change: in follow mode, an omitted `--timeout` (or `0`) means unbounded. Then a Monitor
running `chatctl wait --as claude --follow` still ends only when the Monitor itself expires, so the
Claude Code side of the re-arm remains separate from chatd's. Verify after the change: a watcher
survives 45+ minutes and a `systemctl --user restart chatd`.

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
