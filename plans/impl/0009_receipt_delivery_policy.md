# Implementation plan 0009: Delivery policy on send and reply receipts

Status: Accepted

Paired ADR: ../adr/0009_receipt_delivery_policy.md.

## Docs

README "The states of a message" and "Which messages wake a push participant": what the delivery
line means and does not mean, with one example per case, and the rule that anything a push
recipient must act on is a `request` (or a `final` when its policy wakes on finals).

## Tests

Assert behavior through the CLI and protocol: pull recipient with no watcher, with a matching
watcher, with a watcher for another conversation only, and with an all-conversations watcher; push
recipient with waking and non-waking kinds under default and configured `wake_kinds`; minimum-interval
hold and in-flight candidate come from the shared function the adapter uses (one test drives both
with identical inputs); completed-turn evidence that already released the adapter's hold while the
receipt still reports its candidate with `completion_checked: false`; reply receipts; duplicate send
with no evidence, after a started, submitted, failed and uncertain attempt, after acknowledgement,
after digest, after a final reply, and after explicit redelivery; a superseded request's duplicate
receipt and a new reply to that request; generation rebind does not report a candidate from the old
binding; enrichment failure still returns the Stored receipt; an old receipt without the field is
accepted by the new CLI and the new field is ignored by the old receipt shape; plain stdout
unchanged; JSON field present and parseable.
