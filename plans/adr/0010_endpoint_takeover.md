# ADR 0010: Refuse silent endpoint takeover

Status: Accepted

Implementation plan: ../impl/0010_endpoint_takeover.md. Reviewed by Codex before implementation.
Scope: `endpoint register` (protocol, store, chatctl) and docs for several agent pairs on one host.
No change to delivery, wake policy, throttling or acknowledgement.

## Problem

Endpoints are keyed by participant name, and `endpoint register` unconditionally upserts: any caller
that names `codex` rebinds it to a new session and executable, fences the old binding's failed
attempts and resets throttles (`Store::endpoint_register`). Participant names are routing labels,
not identities (README, Scope and limits). With one pair per host that was tolerable; on 2026-11-10 a
second Claude+Codex pair started on the same host and chatd, and a new Codex session following the
README ("register --as codex") would have silently redirected the first pair's pushes into itself.
The first pair would see `submitted` and get nothing.

## Contract

1. **A new guarded operation.** chatctl registers through a new protocol operation (for example
   `EndpointBind { name, session, exe, mode }`). An older daemon does not recognize it and refuses it
   before any mutation, so a new chatctl can never fall back to the old unconditional upsert, and no
   capability handshake is needed. chatctl reports the refusal (exit 5, "chatd predates guarded
   registration; restart chatd on the new binary"). The legacy `EndpointRegister` operation stays
   decodable, and on a new daemon it behaves like plain mode below (refuses a different binding);
   its error text names the upgrade and `--replace`.
2. **Modes.** Plain (default), `--replace`, and `--replace-generation G`. The two flags are mutually
   exclusive; G must be a positive integer, refused at argument and protocol validation otherwise.
3. **Order of checks, all inside one immediate write transaction:**
   1. With `--replace-generation G`, the current binding must exist and have generation G; otherwise
      `Conflict` (a missing binding is described as missing). This check comes first.
   2. Then an exact same binding (same session string and same executable path string; no
      canonicalization or probing) is a no-op success in every mode: no new generation, no event, no
      health clearing, no throttle reset, no registration-time change, no hub wake. The response says
      it was already registered.
   3. A different existing binding: plain mode is `Conflict`, naming the current session, executable,
      generation and registration time, and suggesting `chatctl peers` and a concrete
      `--replace-generation <current>` command (never unregister-then-register, which bypasses the
      guard). `--replace` rebinds unconditionally; `--replace-generation G` rebinds after check 1.
   4. No existing binding: plain and `--replace` bind; `--replace-generation` already conflicted in
      check 1. Binding after `unregister` keeps the monotonic generation floor.
4. **Races.** At most one replacement mutation succeeds per expected generation: two callers asking
   for a different binding at G get one change and one `Conflict`, even if both name the same new
   session. Callers asking for the unchanged binding both get no-op success.
5. **Replacement semantics are unchanged.** A deliberate rebind fences as today (queued selections,
   failed attempts of the old generation), resets throttles and clears cached health. A submission
   already running under the old generation is not cancelled.
6. **Recovery.** Replacement is explicit; redelivery stays a separate step, used only when needed.
   Registration never replays old submitted messages.

## Not in this record

Adding participants without a restart is a later record (add-only, validated, explicit apply).
Authentication between participants is out of scope (one Unix user).
