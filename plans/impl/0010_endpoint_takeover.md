# Implementation plan 0010: Refuse silent endpoint takeover

Status: Accepted

Paired ADR: ../adr/0010_endpoint_takeover.md.

## Docs

A README section "Several agent pairs on one host": give every agent its own participant name
(`c1`/`x1`, `c2`/`x2`) in `config.toml`; separate conversations; `CHATD_AS` per pane; an exclusive
watcher per pull participant; register each push endpoint once (`--as` is explicit for endpoint
commands) and replace only your own. Conversations separate topics, not routing: the participant
name decides who receives a message. Exclusive watches are start-time checks, not ownership. Say plainly
that names are labels on one Unix user, so this prevents accidents, not a hostile local process.
Note that adding participants currently needs a daemon restart (see "Not in this record").

## Tests

Same binding twice: one generation, one event, unchanged registration time and health. Same binding
in each mode, including `--replace` and a matching `--replace-generation`. Same binding with a stale
generation conflicts. Different session, and different executable only, refused with the current
binding in the error and nothing changed (generation, throttle windows, fenced attempts, health
cache). `--replace` rebinds and fences as before. `--replace-generation` with the current generation
succeeds; with a stale one, or with no binding, conflicts; two racing replacements to a different
binding at the same generation give exactly one change. Conflicting flags and a non-positive
generation are refused. No binding: plain binds. After `unregister`, plain binds above the floor. A
server that does not know the new operation (old-shaped fixture) receives no registration and keeps
its binding; chatctl exits 5. Legacy `EndpointRegister` on the new daemon refuses a different
binding. Existing rebind, fencing and
throttle-reset tests keep passing with `--replace` where they rebind deliberately.
