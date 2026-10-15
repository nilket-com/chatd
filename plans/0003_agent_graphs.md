# 0003: Agent graphs over chatd

Status: draft for review (Claude wrote it; Codex asked for its shape in `chatd-ergonomics-pairs`).
Scope: designing the space of 2 to 4 agents. Nothing here is implemented. The standing cap on live
graphs is 3 agents; 4 is included to see the whole space.

## Three things that must stay separate

| Axis | Examples | What it decides |
| --- | --- | --- |
| Product | Claude, Codex | HOW an agent is woken (the transport) |
| Model tier | Astra, Sol, Luna (a tier exists on both vendors' sides; "Luna" alone is ambiguous) | what the agent is good at; not routing |
| Instance | `a1`, `a2`, `scout-host` | WHO a message is for: a STABLE participant name, the routing identity |
| Role | `reviewer`, `impl`, `owner of request R` | what an instance does NOW; assigned per conversation or request, may change per task |

A chatd participant name is a stable routing label for one instance. Do not use it for a role: roles move
between instances, and a name that is a role breaks when it does (review feedback, Codex). Name instances,
not products or models, so a model can be swapped without rewriting the graph, and keep roles as data on
the conversation or request. Record the product (and tier, as metadata) per participant so the daemon and
humans can tell how it is reached. Today the names are `claude` and `codex`, which only works
for one of each.

## Transport is decided by the receiver's product and the sender's product

| Receiver | Sender | Transport | Wake-up |
| --- | --- | --- | --- |
| Codex | anyone | chatd + `codex queue` push | true push, interrupts a busy turn |
| Claude | Claude | native `SendMessage`, or chatd + Monitor | native wakes an idle session; chatd path is pull |
| Claude | Codex | chatd + Monitor | pull, re-armed every 30 min (Monitor cap) |

Native Claude-to-Claude messaging has no durability, acks or retry-safe keys, and a peer in a different
permission mode may hold messages for approval. Whether it wakes a fully idle session reliably was seen
once, in the spike (plan 0002), and should be re-checked. chatd adds the durable parts.

## The compositions

`c` Claudes and `x` Codexes give C(c,2) Claude-Claude, C(x,2) Codex-Codex and c*x Claude-Codex POTENTIAL
pairwise links. The realized graph can be sparse or hub-shaped, and each realized edge chooses a
transport from the table above.

| Agents | Mix (c,x) | CC | XX | CX | What dominates |
| --- | --- | --- | --- | --- | --- |
| 2 | (2,0) | 1 | 0 | 0 | native is enough; chatd optional |
| 2 | (1,1) | 0 | 0 | 1 | the asymmetric pair we run today |
| 2 | (0,2) | 0 | 1 | 0 | symmetric push |
| 3 | (3,0) | 3 | 0 | 0 | native mesh possible |
| 3 | (2,1) | 1 | 0 | 2 | Codex is a hub; two pull-side Claudes |
| 3 | (1,2) | 0 | 1 | 2 | one Claude is the pull bottleneck |
| 3 | (0,3) | 0 | 3 | 0 | all push; worst busy-turn pile-up |
| 4 | (4,0) | 6 | 0 | 0 | native mesh gets large |
| 4 | (3,1) | 3 | 0 | 3 | |
| 4 | (2,2) | 1 | 1 | 4 | CX-heavy |
| 4 | (1,3) | 0 | 3 | 3 | |
| 4 | (0,4) | 0 | 6 | 0 | |

## Problems the pair work did not need to solve

1. **Busy-turn fan-in.** Push into a busy Codex turn adds a task with no defer or priority control
   (Codex's top friction). Several senders into one Codex multiplies it. Options: coalesce
   notifications per conversation, a per-participant "do not interrupt" window, or a priority field.
2. **Per-request ownership.** With three or more agents, "who owes the answer" must be explicit: one
   named owner per request, others informed. Today `status` lists every unanswered request, with no
   owner.
3. **Broadcast.** A send has exactly one recipient. Telling N agents costs N sends and N keys. A group
   send is the first addition. Decided shape: ONE logical idempotency key per broadcast; the daemon
   stores one message per recipient, derived deterministically from (key, recipient), each with its own
   delivery, ack and answer state. Retrying with the same key and the same recipient set returns the
   original messages; the same key with a different recipient set or body is a Conflict, as for a single
   send today. Per-recipient keys chosen by the caller are not offered, so a partial retry cannot
   produce a different set.
4. **Peer reachability.** Nothing tells a sender whether a peer is watching, how it is woken, or whether
   its watcher lapsed. A directory (participant, product, tier, watching since, transport) is the
   natural place; `status` already lists watchers.
5. **Identity.** Names are routing labels; any local process can send `--as` any name. Fine for two
   trusted agents, riskier for four.
6. **The Claude pull limit.** Every Claude receiving over chatd needs its own Monitor (30 min cap).
   `--exclusive` stops duplicates per participant.

## Proposed order

1. This note, reviewed.
2. A deterministic harness: fake participants with three delivery adapters (push, pull, native stand-in)
   to exercise every composition without real model sessions, since chatd does not care about product.
3. Participant records carrying product and tier; group send; owner per request; reachability in
   `status`.
4. Live graphs with real sessions: (1,1) today, then (2,1) with a second Claude (a background session,
   as in the spike), then (1,2) only if a second Codex is available.

## Not decided here

- Whether native Claude-to-Claude should be an adapter (wrapping `SendMessage`) or stay outside chatd.
- Whether to let chatd write to a Claude session's private socket. It is undocumented and would inject
  text as a trusted teammate message; it needs an explicit decision (see plan 0002, spike).
- A 4-agent live run: the cap is 3 until the user lifts it.

## Harness findings (tests/graphs.rs)

Built first, as proposed: fake agents over all 12 mixes, scenarios mesh / fan-in / lapsed pull agent.
All 12 mixes pass the mesh and fan-in scenarios, and every mix with a pull agent passes the lapse one.
What it found:

- **Lost wakeup in the push adapter (fixed).** `adapter::run` sampled the commit counter after
  delivering, so a message committed while an earlier push was in flight was missed and waited out the
  one-second poll: the second of two back-to-back pushes arrived about 1.0 s late (1.66 s measured against
  a 0.65 s floor, 5 of 5). The counter is now sampled before the due-deliveries read. Regression test:
  `a_push_is_not_delayed_by_a_delivery_in_progress`. This is the busy-turn friction's cousin on the
  daemon side: a push target under load was being delayed by the daemon, not just interrupted.
- **`status` counts an in-flight submission as "needing attention".** For a few milliseconds after a push
  agent acknowledges, chatd has not yet recorded the adapter's exit, and `uncertain_or_failed_total`
  is 1. Harmless, but a human or an agent reading `status` can mistake it for a failure. Candidate fix:
  report in-flight attempts separately from uncertain or failed ones.
- **Fan-in is one push per message.** Three senders into one push agent produce three pushes; nothing
  coalesces. Pinned by the fan-in test so a coalescing change is deliberate.

## Cut 4: participant records and the directory (done)

`[participant.<name>]` in the config carries `product` and `tier` (short lowercase tokens, validated at
start; metadata, never routing). `status` returns a `participants` directory and `chatctl peers` prints it:
product, tier, transport (push with an endpoint, else pull), expected transport for the product,
watchers, oldest watcher, and `reachable`. The harness asserts the directory for every mix, including a
lapsed pull agent shown unreachable and reachable again on return. The attributes live in the config, so
there is no store schema change; a participant added only under `[participant.*]` is migrated in like any
other. Not done here: group send, per-request owner, and any change to routing based on product.
