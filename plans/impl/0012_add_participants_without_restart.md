# Implementation plan 0012: Add participants from validated config without a restart

Status: Accepted

Paired ADR: ../adr/0012_add_participants_without_restart.md.

## Docs

README: how to add a pair (edit `config.toml`, `chatctl config preview`, `chatctl config apply
--revision R`, `chatctl peers`), what is refused and why, and that removal or changes still need a
restart. Link from the "Several agent pairs on one host" section added by ADR 0010.

## Tests

On isolated daemons: adding names works without interrupting an attached watcher or an in-flight
send of the existing pair; the new names can send, receive, watch and register endpoints right
after apply; invalid candidate refused with nothing changed; removal, metadata change of an existing
name, and a non-reloadable setting change each refused by name; apply with a stale revision
conflicts; repeat apply is a no-op; concurrent sends and registrations during apply see a consistent
view (no request admitted by one source and refused by the other); a deterministic test at the
publication boundary that no reader sees committed additions before the new configuration, and that
an injected database failure leaves both sources unchanged; file replaced between preview and apply;
missing or unreadable file; two candidates racing; identical reapply; formatting-only edit as a
no-op; a name declared only as a table, a name in both spellings, omitted wake policy, `all` versus
the explicit full set; a retained store name with an endpoint and outstanding messages refused with
no delivery, health or endpoint change; restart parity.

## Implemented publication and validation

`Shared` retains the absolute startup-selected config path and a mutex-protected `Arc<Config>`.
Refresh reads the file once before locks, validates those bytes (including the store's startup
participant-name rule), and compares the SHA-256 candidate revision. Effective revisions hash a
normalized semantic representation: participant union, defaults, canonical wake-kind sets and
resolved default session directory. Formatting-only applies retain the existing snapshot/revision.

Apply acquires store then config, revalidates against that current view and retained DB names, and
prepares its response before mutation. A single insertion transaction commits before an infallible
Arc assignment while both guards remain held. The publication helper has an empty production
callback and a deterministic test callback at the commit/swap boundary. Database fault tests inject
a SQLite abort trigger and verify rollback of earlier inserts in the same transaction.

Store-dependent policy readers snapshot inside the store barrier; detached snapshots carry no guard
into sockets, subprocesses or session-log reads. Watch admission uses the barrier; its independent
read transaction remains safe afterward because active participant definitions cannot be removed or
changed. Retained-name admission remains unchanged. No apply event, endpoint update, replay or
health reset is introduced. Restart parity assumes the applied file has not subsequently changed.

Nine added tests cover semantic equivalence and aggregate named refusals, revisions/file errors,
rollback, retained names with an endpoint and outstanding request, competing captured candidates,
the publication boundary, isolated live watcher/send continuity and restart parity, CLI source-path
selection/no-ops/refusals, and concurrent send/register admission. An unreadable-file test uses a
directory at the config path so it remains effective even under privileged test execution.
