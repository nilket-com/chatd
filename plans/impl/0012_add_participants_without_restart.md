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
