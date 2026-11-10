# ADR 0009: Delivery policy on send and reply receipts

Status: Accepted

Implementation plan: ../impl/0009_receipt_delivery_policy.md. Reviewed by Codex before implementation.
Scope: the `Send` receipt (send and reply) and its chatctl rendering. Additive protocol field; no
change to routing, wake policy, throttling or acknowledgement.

## Problem

A receipt says only that a message was stored (`id (seq, key)`, plus a health warning). On
2026-11-10 two different failures looked identical from the sender's side:

- A go-ahead sent as `info` to a push participant that wakes on `request` (the default) was stored
  and never pushed; the recipient sat waiting until the user noticed.
- A `request` was stored but held back by the push throttle (an earlier push still unacknowledged).

When it stored each message, chatd had the policy and attempt evidence to say so, and said nothing.

## Contract

The receipt gains an optional `delivery` object with what chatd **observed** right after storing the
message: current policy and recorded evidence, never a predicted outcome, and never a submission or
receipt that has not happened. It carries `observed_at_ms`. An absent field (an older daemon) means
unknown, and chatctl prints nothing extra.

1. **Push recipient, waking kind.** "wake kind for X (generation G); submission not confirmed", plus
   throttle candidates from stored attempt evidence, in the adapter's precedence (minimum interval
   first): the minimum-interval window (latest relevant attempt start + interval) and a possible
   in-flight hold (the qualifying unacknowledged attempt with the latest start, ties by attempt id;
   its message id, attempt id, state and start time, and start + window). Times are observations,
   not promised release times: a later attempt can extend a hold, and an acknowledgement or a
   completed turn can end it. The receipt does not read the session log, so it says the
   completed-turn release was not checked (`completion_checked: false`) and the adapter may already
   consider the hold released. The candidates come from a function shared with the adapter
   (extracted from `Store::push_allowed`) that returns the same result for the same inputs.
2. **Push recipient, non-waking kind.** "not a wake kind for X (wakes on request); normally carried
   by X's next waking push", and the explicit way out: `chatctl redeliver ID` (which still passes the
   throttle, so it is not described as immediate).
3. **Pull recipient.** "pull inbox", and whether at least one attached watcher of the recipient
   covers this message's conversation (a watcher with no conversation filter covers all). A watcher
   for another conversation does not count. Presence is an instantaneous observation, not acceptance
   and not evidence that the agent wakes for this kind (0008's `--wake-on` is client-side and
   invisible to the daemon). A known participant without an endpoint is a pull participant.
4. **Duplicate send.** Observed message evidence is kept separate from current policy. Evidence, any
   of which may coexist: attempts (id, generation, state including `started`, start and finish
   times), acknowledgement, a carrying digest, a final reply id. Current policy is reported as kind
   policy (rules 1 to 3), not as an eligibility verdict: explicit redelivery, earlier attempts,
   fencing and retry limits decide actual eligibility, and this record does not build a scheduler
   inspector. With no evidence: "already stored; no submission or receipt evidence recorded", plus
   the policy. Enrichment never triggers delivery or changes idempotency behavior.
5. **Retirement.** A request that is itself superseded or classified as a rehearsal is reported as
   retired (no automatic push or digest, redelivery refused). A reply to a retired request is not
   retired by its parent, and this record does not change delivery for it.
6. **Enrichment cannot fail a send.** It runs after the durable store. If a query or lock fails, the
   Stored receipt and its key are returned as today with `delivery` marked unavailable (or omitted
   with a diagnostic), and the normal hub wake still happens. The send path keeps its locking and
   never waits for the adapter.

## Rendering

- Plain mode: stdout keeps today's single receipt line unchanged (scripts parse it). The delivery
  statement goes to stderr as one line, `chatctl: delivery: ...`, next to the existing health warning.
- `--json`: the `delivery` object is included as structured fields (state, reasons, times as
  milliseconds, ids, generation, `completion_checked`) with a human `text` field; consumers must not
  need to parse `text`. No delivery line on stderr in JSON mode; the health warning is unchanged.
- The pending key is still cleared only after the receipt is flushed. `lookup` receipts are not
  enriched in this record.

## Not in this record

Endpoint takeover protection and adding participants without a restart (0010 and later). A
per-message delivery inspection command. Advisory pull filters registered with the daemon.
