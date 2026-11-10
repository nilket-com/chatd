# ADR 0014: Bulk backlog retirement

Status: Accepted

Implementation plan: ../impl/0014_bulk_backlog_retirement.md.
Scope: two explicit, owner-scoped bulk operations and the `status` attention rule. No deletion, no
age-based hiding, no change to delivery of anything not explicitly retired or acknowledged.

## Problem

On 2026-11-10 the live store reported 1,030 unacknowledged messages, 3,349 requests with no final
reply and 1,714 submissions needing attention, nearly all from October (finished work, old health
notices to a retired participant, `queue cannot contain more than 100 submissions` failures). The
current state is buried, and `status --current` is buried too because those requests are
genuinely unanswered. Supersession and rehearsal annotations (ADR 0007) work one id at a time.

## Decision

1. **Sender-owned bulk retirement of requests.** `chatctl retire --as X --before-seq N` with
   optional `--to Y` and `--conversation C`, and a required `--reason`. It selects requests sent by
   X with sequence below N that have no final reply and no annotation. Retired is a new explicit
   annotation state, distinct from superseded and rehearsal: immutable, audited, atomic, never a
   fabricated final reply and never an acknowledgement. Its effects are those retirement already
   has, for future selection only: no later push or digest selection or attempt claim, no replay in
   later watch snapshots, redelivery refused, absent from `status --current` (shown under
   `--history` as retired with its reason). It does not cancel a push already claimed, a digest
   already captured or a watch snapshot already being written, and it is not proof that the
   recipient never saw the request. Bulk acknowledgement has the same limit.
2. **Recipient-owned bulk acknowledgement.** `chatctl ack --as X --before-seq N` with optional
   `--from Y`, `--conversation C` and `--kind K`, acknowledging X's own unacknowledged messages below
   N, annotated requests included. An acknowledgement still means received, not done; requests
   acknowledged this way stay unanswered unless also retired. Positional ids cannot be mixed with
   the bulk flags, and the existing id-based `ack` is unchanged.
3. **Preview, then apply.** Both operations show a preview by default, from one consistent read:
   the filters and exclusive bound, the count, the oldest and newest by sequence, a fixed-size sample
   (summaries only, sanitized) and a revision. The revision is a SHA-256 over a canonical encoding of
   the operation, actor, normalized filters and bound, the retirement reason, the store identity and
   the full ordered list of matched ids; it does not include the head, so unrelated traffic does not
   invalidate it (with N at or below the head, a new message cannot join the set). Apply recomputes
   the set and compares the revision inside the same write transaction that writes every annotation
   or acknowledgement and its audit events; a mismatch is `Conflict` and a failure rolls everything
   back. The response is bounded (counts, revision, bounds), never the full id list. The bound is
   mandatory and exclusive, 0 <= N <= head; an empty set previews and applies as an explicit no-change
   result.
3a. **Retries.** After an apply that changed a nonempty set, the same revision conflicts, because
   its set no longer matches (an empty-set apply stays a repeatable no-change). A lost reply is resolved by a fresh preview (which shows what remains), not by assuming
   the first apply did not commit.
4. **Attention means actionable.** A submission (the latest attempt of a message, failed,
   uncertain or started) is listed under "needing attention" only while its message is
   unacknowledged and carries no retirement annotation (retired, superseded or rehearsal). `status`
   reports how many candidates were omitted, counted once each, after the same filters and before
   pagination, so nothing disappears silently. The other `status` totals are unchanged.
5. **Ownership.** As with existing annotations, `--as` is a local assertion, not authentication; the
   operations are scoped so a participant can only retire requests it sent and acknowledge messages
   addressed to it.

## Not in this record

Deleting or compacting history. Automatic retirement by age. Retiring non-request messages
(they are acknowledged instead).
