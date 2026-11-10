# ADR 0008: Wake filter for pull watchers

Status: Accepted

Implementation plan: ../impl/0008_pull_wake_filter.md. Reviewed by Codex before implementation.
Scope: `chatctl wait`, CLI only. No protocol, server, store or configuration change.

## Problem

A push participant is woken only for its `wake_kinds`; everything else rides in the next push's
digest. A pull participant has no equivalent: `wait` returns on every message, and each return is a
model turn. On 2026-11-10 a Claude watcher on a busy conversation was woken by each routine
development-build notice and worked around it with a wrapper that matched summary text and
acknowledged matches. Summary matching guesses the sender's intent, and acknowledging by filter is
the failure mode seen on 2026-10-17, when a filter that acknowledged `progress` swallowed two
messages that asked for a stop.

## Contract

`wait --wake-on KINDS` (comma-separated `request`, `info`, `progress`, `final`, or `all`; the existing
kind parser). Omitted means every kind, so current behavior is unchanged. An empty or unknown set is
refused at start.

1. **Selection.** A message whose kind is in KINDS is *selected*. Only a selected message ends a
   one-shot `wait`, and in `--follow` mode only a selected message triggers a batch (held messages
   appear only as context in a batch). The filter applies to the backlog snapshot and to live events
   alike. With `--wake-on` omitted or `all`, nothing can be held and the existing streaming path is
   used unchanged, including immediate one-shot output without waiting for `SnapshotEnd`.
2. **Held messages are shown, never consumed silently.** A message not selected is *held*: it stays
   unacknowledged and visible to `status`, `receive` and other readers. When a selected message is
   emitted, held messages older than it follow it in the same output as context, oldest first, up to
   a cap (the push digest's 20 items). Nothing is acknowledged by the filter itself.
3. **Selected first.** The selected message is printed and flushed before its context, matching the
   push layout (notification, then digest), so the reason for waking is never behind a backlog.
4. **Acknowledgement is unchanged per message.** Without `--take`, nothing is acknowledged. With
   `--full --take`, each line, selected or context, is acknowledged only after it was written and
   flushed and only if its body was shown whole. A truncated or summary-only item stays
   unacknowledged. A write failure partway through a batch leaves the rest unacknowledged; there is
   no batch atomicity, and the docs say so.
5. **Output shape is unchanged.** With `--full`, every stdout line is an existing message object.
   Overflow is reported on stderr (`more held messages may remain: chatctl status --as X`); an exact
   count is optional and must not be wrong. Plain mode may print the overflow line on stdout.
6. **Snapshot boundary.** Output for the backlog snapshot starts at `SnapshotEnd`, so a later page
   cannot reveal older held messages after a one-shot `wait` has returned. One-shot keeps only the
   first selected message in the existing oldest-first order as its trigger. `--follow` keeps the
   metadata of every selected snapshot message and, after `SnapshotEnd`, emits each in sequence
   order, each followed by its eligible held context (held messages older than it, not yet shown on
   this connection). Context never precedes its own trigger. For a live event the boundary is that
   event's sequence. Gathering never waits for the inbox to go quiet.
7. **State and overflow.** Held-candidate metadata is capped at the oldest 20 pending candidates;
   bodies are fetched at emission. When the buffer is full, further held messages are not retained:
   they stay unacknowledged and are found with `status`, `receive` or a fresh `wait`. Draining the
   buffer does not recover them on the same connection; new held messages may fill the freed slots.
   Once overflow happened, later batches on that connection warn on stderr; a reconnect clears the
   flag and rebuilds held state from the new snapshot (so without `--take` a replay can repeat a
   message, as today). A held message is shown as context at most once per connection. Selected
   snapshot metadata in `--follow` (ids, sequences, kinds) and the existing deduplication state grow
   with the backlog; only held candidates and each context batch are capped.
8. **Timeout.** The absolute `--timeout` deadline is not extended by held traffic or reconnects. It is
   checked while reading the snapshot and events and before each message announcement starts: no
   announcement starts after expiry, and messages not started stay unacknowledged. An announcement
   already in progress (fetch, write, acknowledgement) completes under today's per-message rules and
   may overrun the deadline. Exit statuses are unchanged (6 timeout, 7 overlap), and diagnostics stay
   on stderr.
9. **Watcher overlap.** Different `--wake-on` sets do not make watchers disjoint; overlap and
   `--exclusive` checks are unchanged.

## Limits (to state in the docs)

- A misclassified urgent message is not shown promptly. Up to the context cap, held messages are
  shown when a later selected message arrives; overflow may need `status` or a fresh `wait`. If no
  selected message ever arrives, it waits in the inbox. There is no forced wake after a quiet period
  in this record. Senders choose the kind: actionable work is a `request`, routine steps are
  `progress` replies linked to the originating request, and `final` completes it.
- A request/final-only watcher also holds chatd's own health notices (they are `info`).
- One-shot `wait` returns the whole bounded batch in one invocation; that is the recommended recipe
  for one wake per batch. In `--follow` mode a consumer may see one event per line.
- Acknowledged means the CLI wrote the line, not that the agent read it.

## Not in this record

Receipt delivery policy (0009). Endpoint takeover protection and adding participants without a
restart (later records). A server-side or advisory per-watcher filter.
