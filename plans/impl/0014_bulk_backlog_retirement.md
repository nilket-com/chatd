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
