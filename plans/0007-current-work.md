# Current-work triage and explicit retirement

Motivation: long-lived chatd stores accumulate genuine unresolved requests and
synthetic probes. Age-based filtering would silently hide unfinished work, and
receipt is not completion. Preserve the existing status wire response and offer
an explicit concise view instead.

Implemented scope:
- `status --current`, request-only, bounded and newest-activity-paginated, with optional
  history, endpoint session IDs first, and linked progress/submission details.
  Opaque snapshot/activity cursors keep pagination stable when new progress moves
  older requests; every genuinely pending request remains reachable.
- Sender-owned immutable supersession and rehearsal annotations in an additive
  schema-2 table with atomic migration from schema 1. Durable audience audit events are atomic with the annotation.
- Forward-only same-conversation replacements prevent cycles without graph walks.
- Retired requests cannot start new delivery attempts, join wake digests, or be
  explicitly redelivered. In-flight attempts finish normally with durable results.
  Acknowledgement and reply semantics remain unchanged.

Validation uses temporary databases only. Tests cover receipt vs completion,
linked progress, pagination, old status JSON, explicit rehearsal, ownership,
replacement causality, idempotency, audit and reopen/migration persistence.
Future work may add richer delivery-specific triage; ordinary status retains all
non-request messages and failed/uncertain submission history now.
