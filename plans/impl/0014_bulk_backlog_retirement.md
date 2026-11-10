# Implementation plan 0014: Bulk backlog retirement

Status: Accepted

Paired ADR: ../adr/0014_bulk_backlog_retirement.md.

1. Store: the retired annotation state with a forward-only schema change that preserves every
   existing annotation and keeps upgrades from schema 1 and 2 working in both `Store::migrate` and
   restore candidate preparation; older binaries refuse the new schema before mutating. Annotation
   readers and the exact-repeat comparison distinguish retired, superseded and rehearsal (a retired
   row is never taken as an idempotent rehearsal); delivery evidence and receipt text name retired
   correctly. Matched-set queries and the revision; apply as one transaction with the comparison
   inside it; hub notification after commit; bounded memory and response for large sets.
2. Protocol and CLI: preview and apply operations for retire and bulk ack; human and JSON output
   showing filters, bound, counts, sample and revision; reasons validated with the existing limits;
   positional ids with bulk flags refused; an older daemon refuses the new operations.
3. Status: the attention filter and the omitted count; `status --current --history` lists retired
   in its groups and terminal-state precedence, with the reason in human and JSON output.
4. Docs: README section on cleaning up a backlog: preview, read the sample, apply with the revision,
   and what each operation does and does not mean (including in-flight work it cannot recall).
5. Tests: preview and apply for each filter; a new message after preview neither joins the set nor
   invalidates the revision; `Conflict` from a matching final reply, annotation or acknowledgement
   (including a progress reply's automatic parent acknowledgement) between preview and apply;
   competing overlapping applies; same revision after success conflicts and a fresh preview is an
   empty no-change; bound above head refused, empty set no-change; only the sender's requests and
   only the reader's inbox are touched; a set larger than a protocol page with a bounded response;
   injected failure and stale revision leave no partial acknowledgements, annotations or audit
   events; retirement between selection and attempt claim refuses the claim, and an already claimed
   push may finish and is not retried; migration of a populated schema-2 copy (superseded and
   rehearsal rows, acknowledgements, attempts, pending requests) and of schema 1, plus restore of a
   schema-2 backup; attention list and omitted count with both exclusions counted once.

## Implemented details

Schema 3 adds a default-false `retired` flag to the existing annotation table, preserving
schema-1/2 upgrades and existing fields. All annotation writers use named columns; exact
rehearsal retries also check the flag. Current history and delivery evidence distinguish the
three annotation states. Restore accepts schema-2 backups and changes store identity as before.

`BulkPreview` and `BulkApply` carry a typed selection; CLI apply uses `--revision` alongside
the same filters. Unknown selection/action fields are refused. Ack kind is one existing Kind
or omitted for all. Preview/result samples contain at most ten sanitized summaries. Selection
is read in sequence-ordered pages of at most MAX_PAGE, keeping application memory bounded.
SHA-256 hashes a length-framed, versioned JSON header (store identity, restore generation and
selection, including exact reason), then length-framed IDs from every page. No global head is
hashed. Zero matches is a repeatable no-change; successful nonempty applies require a fresh
preview before retrying.

Apply takes an immediate SQLite transaction, scans/hashes and compares before any mutation,
then rereads pages and writes all per-message audit events before one commit. Preview uses one
read transaction. Notifications follow commit. Ack includes annotated messages, while retire
selects only unanswered, unannotated requests owned by the actor. Existing in-flight delivery
is not cancelled. Attention counts the union of acknowledged/annotated latest-attempt candidates
once, separately from its actionable total, without changing historical message totals.

Twelve isolated store tests and two real-daemon CLI tests cover filters/ownership, per-operation
invalidation, bounded 450-message batches, rollback, overlapping concurrent applies, empty
retries, new arrivals, stale delivery selections and in-flight outcomes, schema-1/2 migration,
schema-2 restore with old-revision rejection, attention counts, CLI mode conflicts, history and
receipt rendering, and post-commit watcher notification. Existing schema/attention assertions
are updated for the intended policy. An additional isolated compatibility smoke builds the
pre-change daemon and verifies that it refuses schema 3 and both new operations without changes.
