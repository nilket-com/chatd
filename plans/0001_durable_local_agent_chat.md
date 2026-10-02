> **Present status (added 2026-10-02 for the public release; the reviewed plan below is
> unchanged).** This is the historical design record. It was implemented and installed as chatd
> (formerly agent-chat), with daemon `chatd` and CLI `chatctl`. Statements below such as
> "authorizes no implementation", the old `agent-chat` command examples, and the host names and
> paths (the author's machines) describe the time of writing. For current setup and scope, see
> `README.md`.

# Durable local agent chat

Status: REVIEWED CORE PLAN; remote-access scope awaiting the user's choice.
Ready for a plan-only commit independently of other projects.
Authorizes no implementation, installation,
service start, changes to agent instructions, or retirement of the current
bridge. Repository: `~/work/agent-chat`. This is an independent project,
not a component of toron, nilket, ket or rnx. It owns its plan, tests, versions
and release/cutover process; those do not depend on another project's ADRs.

## Problem and intended result

The current bridge mixes a native Codex queue with Markdown append conventions
and transient Claude monitors. A reply can exist in an agent's conversation
without reaching the bridge. A watch can expire, restart after a reply arrives,
and miss it. Repeated clock-only headers do not identify requests. The user
cannot distinguish an unanswered request from a broken reader.

Build a small Rust service, managed by a systemd user unit on the broker host, that owns
message state. Both agents use the same CLI and stable message IDs. The user
can inspect what was stored, submitted to an agent, explicitly acknowledged,
and answered. Disconnects and restarts preserve unread messages. The service
and its CLI replace tail/grep/sleep as the message transport.

Server push is the primary delivery model: a client subscribes once, and the
broker streams committed messages and state changes without periodic inbox
polling. Durable replay complements push when a subscriber disconnects.

This is infrastructure for the existing agents. It does not run models,
automatically create new sessions, or require a hosted account/service.

## What is established locally

Read-only inspection on the broker host on 2026-10-02 found:

- `systemctl --user is-system-running` reports `running`.
- Installed `codex queue --help` accepts an explicit thread/session and message.
- Installed `claude --help` and `claude agents --help` do not advertise a native
  queue command for an already-running interactive session. Stream-JSON input
  is advertised for print mode. That alone does not establish an injection path
  into the current interactive session.
- Claude's saved bridge convention watches only new Markdown appends, with a
  30-minute monitor lifetime. It also documents a recent reply written only
  inside the Codex session, unseen by the bridge.

These are observations about the installed interfaces, not claims that a
Claude adapter is impossible or that a hook cannot deliver anything.

## Scope and first feasibility gate

The core is one broker on the broker host, initially serving one Unix user through a
Unix socket. No browser UI, cloud relay, terminal keystroke injection or
session-log scraping. Build on a separate build host and install/run on the broker host after this
project's review. Coordinate use of shared hosts with any active timing work;
that scheduling constraint is not an agent-chat milestone or release gate.

### Access topology: user decision pending

Remote access and a distributed service are different requirements. Agents on
other LAN hosts can use a single authoritative broker without replicas,
per-host databases or synchronization. Three choices are under consideration:

1. Recommended: one broker on the broker host, remote CLI access through SSH. Public-key
   SSH authentication carries requests to the broker's Unix socket; the broker
   still owns message state. Establish reachability and the exact SSH adapter
   before selecting its configuration. The client preserves request IDs and
   idempotency keys through disconnects and does not create a second inbox.
2. One broker with a native authenticated LAN endpoint. This adds a network
   protocol, client identity/credentials, authorization and exposure controls
   to the implementation review. Private IP addresses are not authentication.
3. Local-only initially, with remote access explicitly deferred.

No network listener, firewall change, native LAN transport, or federation is
selected by this draft. If remote access is chosen, acceptance must include
disconnect/reconnect, unavailable broker, unknown submission result, wrong
credentials, host/session binding and reply retrieval from a second host.
Single-broker availability is stated plainly: a down or unreachable broker host
prevents new broker operations, and committed messages resume when it returns.

Before implementing an adapter, demonstrate its interaction with a disposable
session, using supported interfaces or inspected source. Record CLI version,
exact commands, outcomes and limitations. Specifically test delivery while
idle, delivery while busy, a waiter disconnect and a session exit.

Preferred Claude integration is an actual supported queue or notification path
into its existing session. If unavailable, an existing Claude Monitor may call
the Rust `wait` command. The monitor then provides wake-up only: the daemon
owns the backlog and acknowledgements. An expired monitor must never imply
that the message was consumed. A new waiter receives the same unacknowledged
message, including messages that arrived while no monitor existed.

Claude reports local `ListAgents`/`SendMessage` tools that may provide
session-to-session delivery. Test this as a feasibility candidate with
disposable sessions: establish which sessions are addressable, whether a
non-Claude daemon can invoke the path, and its receipt and wake-up semantics.
Availability inside a Claude session does not establish an external API.
If it requires an extra model session to relay messages, it is outside V1;
the Monitor-plus-wait fallback remains the declared alternative.

The fallback must be labelled explicitly as requiring a live/rearmed Claude
monitor. A daemon alone cannot guarantee that an unsupported idle session will
wake. If removing that final monitor dependency is mandatory for acceptance,
bring a separate integration proposal before changing how Claude sessions are
launched. Do not quietly launch a second model/session to simulate delivery.

## Implementation shape

One Rust workspace with a typed domain/protocol library, `agent-chatd`, and
`agent-chat`. Use a single SQLite database writer and a versioned, bounded
protocol over a Unix socket. A small framed JSON protocol is acceptable as
serialization; deserialize into explicit Rust request/response enums and
validated IDs, rather than keeping the state machine in untyped JSON values.

State is in `$XDG_STATE_HOME/agent-chat` (default
`~/.local/state/agent-chat`); socket is in
`$XDG_RUNTIME_DIR/agent-chat/agent-chat.sock`. Configuration is in
`$XDG_CONFIG_HOME/agent-chat/config.toml`. Use absolute configured executable
paths and explicit session bindings. The state directory and socket directory
are private to the user, with socket access restricted to that user.

The daemon verifies Unix peer credentials. Agent names are routing identities,
not authentication against another process belonging to the same Unix user.
Do not claim that `--as claude` proves which model sent a message.

Use SQLite foreign keys, transactions and a documented schema version. Enable
WAL and `synchronous=FULL`; return a storage receipt only after commit. The
claim is durability subject to the local filesystem and storage honoring
flushes. Schema migration failure refuses startup and preserves the database.

Bound request size, message size, waiter count and adapter concurrency. A full
disk, invalid frame or failed transaction produces an error and no successful
receipt. Message bodies are never interpolated into shell commands: adapters
use direct process arguments. Treat bodies as text, never commands.

## Durable records and meaning of status

Messages are immutable after storage. Each has a stable UUID, database sequence,
conversation ID, sender and recipient, creation time, kind, body, optional
`reply_to`, and a sender idempotency key. Replies refer to an existing request
in the same conversation and go to its original sender. Store application
events separately from message content: delivery attempts, acknowledgements,
replies, endpoint changes, cancellations and errors.

A request has several independently observable facts:

| Fact | Required evidence |
| --- | --- |
| Stored | SQLite transaction committed; message ID returned |
| Submission attempted | Adapter command started, with attempt ID and deadline |
| Submitted | Adapter returned success; this is not agent receipt |
| Submission uncertain | Timeout/crash left the external result unknown |
| Received | Recipient explicitly acknowledged this message ID |
| Answered | A stored reply explicitly references this request ID |
| Closed without reply | An explicit cancellation/decline with a reason |

Do not call `codex queue` exit zero "read" or "answered". Do not call a quiet
waiter "no reply from the model": status says "no reply recorded in agent-chat"
and shows receipt/submission state. A response written elsewhere is untracked
until explicitly imported or sent. Global instructions help agents use the
channel but are not a technical guarantee that every final response appears
in it.

Acknowledgement is idempotent and does not count as an answer. Storing a reply
can atomically acknowledge its referenced request. Requests may have progress
messages and a final reply; only the declared final reply marks them answered.
Information-only messages do not create a false unanswered-request alarm.

Persist per-recipient acknowledgement records. A cursor may optimize reads but
must not skip an unacknowledged hole: never equate "highest ID printed" with
"all earlier messages consumed." Multiple waiters may see the same message;
that is safe replay, not exactly-once execution. V1 permits one registered
delivery consumer per endpoint; explicit inspection does not consume messages.

## CLI and agent workflow

### Server-pushed subscriptions

The reference behavior is etcd's revision-based watch: subscribe once, stream
changes and resume from retained history. See the
[etcd Watch API](https://etcd.io/docs/v3.6/learning/api/#watch-api).
This is a design reference, not an etcd dependency or a replication choice.

Give every durable application event a strictly increasing event sequence.
Commit the message/state mutation and its event in the same SQLite transaction.
The broker pushes only committed events. A watch filters by recipient and
optionally conversation, and sends typed message-available, submission,
received and answered events on a persistent connection. Order follows the
durable sequence within that subscription; filtered sequence gaps are normal.

Establish an inbox/status snapshot at sequence H, then replay/stream matching
events after H without a check/subscribe gap. A subscriber can resume after
its last complete event using a token bound to the store identity, generation
and filter. Missing/invalid/future tokens or a changed restored store must not
silently jump to latest: return an explicit resynchronization requirement and
an authoritative inbox/status snapshot. V1 retains event history rather than
compacting it automatically.

Event resume position is not a message acknowledgement. Reconnect includes
still-unacknowledged messages even when their arrival event preceded the resume
position. Receiving/printing events never marks a request answered. A state
change may reference the same message ID more than once; deduplicate event
replay by event sequence and message processing by stable message ID.

Use bounded subscriber buffers and finite write deadlines. Disconnect a slow
subscriber with a declared replay/resync outcome instead of silently dropping
events, growing memory without bound, or blocking the writer/other clients.
Send progress/heartbeat frames so clients distinguish a quiet live connection
from a broken one; their intervals and reconnect backoff are pinned before
implementation. Show connection, last event and replay state in status.

`agent-chat watch` is the primary push interface. `wait` is a one-notification
view of a subscription; `wait --follow` is its bounded inbox-notification view.
Neither implementation repeatedly polls the database on a timer. Codex queue
and any Claude wake-up adapter consume committed notifications from the same
event stream, with their existing explicit receipt/reply limits.

If SSH remote access is selected, a persistent SSH connection can carry this
stream; a new SSH command is not required for each message. A native LAN
endpoint can carry the same typed subscription if separately selected.

Illustrative CLI contract, to be pinned at implementation review:

```text
agent-chat send --from claude --to codex --conversation adr628 \
  --kind request --idempotency-key <key> --body-file <path>
agent-chat receive --as codex --id <message-id>
agent-chat watch --as claude --conversation adr628 [--resume-token <token>]
agent-chat wait --as claude --timeout 1800
agent-chat wait --as claude --follow --timeout 1800
agent-chat ack --as claude <message-id>
agent-chat reply --as codex --to-message <message-id> --final \
  --idempotency-key <key> --body-file <path>
agent-chat new-key
agent-chat status --conversation adr628
agent-chat log --conversation adr628
agent-chat endpoint register --as codex --session <explicit-session-id>
```

Support stdin for bodies, structured output for agent tools, and readable output
for the user. Display request ID, conversation, sender, age, submission result,
receipt, and reply link. Do not require grepping a transcript to get status.
When a notification is truncated, `receive --id` returns the full stored body.

`wait` consults the durable inbox before subscribing, and closes the
check/subscribe race. Printing never advances acknowledgement state. Timeout,
disconnect, daemon restart and lost CLI output leave the message unread. The
recipient fetches and acknowledges a message before doing the requested work;
the request remains visibly unanswered until its final reply is stored.

`wait --follow` emits the initial unacknowledged backlog and new arrivals,
each ID at most once per connection, until its finite timeout. Its local
emitted-ID set prevents a hot loop on unacknowledged messages; it is not an
acknowledgement or durable cursor. Reconnect replays every still-unacknowledged
message. Output is one bounded notification line with ID, conversation,
sender, kind, age and summary. Full bodies are obtained with `receive --id`.
Freeze and test the notification's total size against the Monitor adapter;
truncation must not hide the ID or receive command.

Idempotent `send` retries with the same sender/key/content return the original
message ID. Reusing a key with different content refuses. The reply command
supports the same behavior so a lost receipt cannot create duplicate replies.

Require an explicit caller key for `send` and `reply`. `new-key` generates a
random key locally without contacting the daemon or creating an outbox; the
caller retains it before sending. Keys are not derived from content, so two
intentional identical messages can have distinct keys. A randomly generated
key printed only after commit would not protect a retry following a lost
receipt, and is not the default. Include the key in receipts and permit
receipt lookup by sender/key.

`send --summary` is optional. Its default is the body's first line, bounded
at a UTF-8 boundary; the summary is stored with the immutable message and
included in idempotency comparisons. Notification rendering normalizes line
breaks and terminal control characters, and bounds the entire notification,
not just the summary. It never appends the full body to a queue command.

If the daemon is unavailable, the CLI fails within a declared finite connect
bound, with a distinct exit code and the hint
`systemctl --user status agent-chat`. It does not auto-start a second daemon,
silently queue into a local file, or create another store. A daemon disconnect
during an operation is a failed or uncertain receipt; retry uses the same key.

## Delivery adapters

The Codex adapter sends a bounded notification to an explicitly registered
session, using the installed queue interface. Include the stable message ID,
conversation, short summary and exact `receive` command. Do not discover the
target by "newest rollout file". Pin the executable and session binding; an
unknown or exited session remains a visible delivery failure.

Queue submission and database commit are not one atomic transaction. A crash
can produce duplicate notifications or an uncertain attempt. V1 provides
durable at-least-once notification, with the stable ID allowing recipient
deduplication; it does not promise exactly-once model work. Persist attempts
before launching the adapter. Use bounded retry/backoff for confirmed transient
failures; reconcile uncertain attempts under an explicit policy frozen before
implementation. Keep retries bounded and failures visible rather than
silently dropping a message or spawning unlimited model prompts.

Claude's adapter follows the feasibility gate above. A Rust wait operation
emits one bounded notification on arrival, then exits; notification loss leaves
the message replayable. If a persistent supported subscription is available,
use it with the same acknowledgement semantics. In either case, status shows
last connection, disconnect, pending messages and the registered endpoint.

Never switch a session binding automatically during a retry. Persist binding
generations, fence old adapter attempts on rebind, and report submissions that
may already have reached the old session. An operator can explicitly rebind
and request redelivery of the same message ID.

## systemd and operation

Provide an `agent-chat.service` user unit with `Restart=on-failure`, bounded
restart rate, `UMask=0077`, `RuntimeDirectory=agent-chat`, and a readiness
contract. Use a plain always-on service with `Type=notify` for V1, without a
socket-activation unit. Readiness means the schema is loaded, a write check
has succeeded, and the Unix socket and request dispatcher are serving;
merely having a process is insufficient. Send `READY=1` through
`NOTIFY_SOCKET`, handling pathname and Linux abstract addresses without a
libsystemd requirement. Readiness failure refuses startup. Test that service
startup completes with a working health request, and that migration, write
and bind failures never advertise readiness. Adapter children have
explicit timeouts, are killed/reaped, and cannot survive service shutdown.

User installation includes the binaries, unit and configuration. Do not enable
user lingering or alter login behavior without explicit approval. A user unit
normally follows the user manager's lifetime: "enabled" is not a promise of
operation across logout or reboot without that manager being active.

Journald records IDs, state transitions, adapter exit codes and bounded errors,
not full message bodies or credentials by default. `status` reports service and
endpoint health, pending/uncertain/unanswered counts and oldest ages. A daemon
restart restores outstanding work from SQLite. Provide an SQLite-consistent
backup/export command; do not prescribe copying only the main WAL database
file while it is live. No automatic history deletion in V1.

## History import and cutover

Treat existing bridge files as immutable history. Import with source path,
SHA-256, byte offsets and raw block content; make repeat import idempotent.
Repeated HH:MM headers are not globally unique IDs or trustworthy full dates.
Malformed/partial blocks are preserved and labelled, not discarded. Do not
infer precise `reply_to` relationships or acknowledgements from text alone.
Imported history starts inert and does not wake agents or become new work.

Dry-run import reports counts, unmatched/partial blocks and hashes. Keep both
files and their hashes after cutover. Do not scrape entire private session
rollouts into the database merely to discover missing bridge replies.

Existing reviews may continue using the current bridge. Demonstrate the new
service with disposable messages and both adapters before replacing it.
Only after joint review, register the actual session IDs and make a minimal
backed-up change to the agents' instructions. Name the sole authoritative
channel at cutover, so two inboxes cannot grant conflicting clearances. Leave
the old files readable and record the cutoff byte offsets. Rollback stops new
adapter delivery, exports outstanding requests, and explicitly reinstates the
old channel; it never destroys new messages.

## Acceptance and work sequence

1. Integration feasibility: prove each proposed wake-up path and name unsupported
   cases before implementing it. Freeze request/reply semantics, framing,
   session mapping, retry/uncertain-attempt policy and resource bounds.
2. Core: typed state machine, SQLite migrations, Unix server and CLI. Tests must
   cover lost receipts, identical/conflicting idempotency keys, ack holes,
   concurrent waiters, invalid replies, oversized input and storage failures.
3. Crash/reconnect controls: kill before/after commit, before/after notification,
   after printing but before ack, and during ack/reply. Restart preserves each
   committed message and cannot fabricate receipt or answer. Arrival between
   waiter check and subscription cannot be missed.
4. Adapters/systemd: tests with a controlled fake queue plus disposable live
   sessions. Verify queue success versus explicit receipt, busy session delay,
   expired Claude wait, unavailable target, rebind fencing, retry limits and
   child cleanup. Test installation/start/restart under a temporary unit before
   production enablement.
5. History/cutover: idempotent dry-run import, user-readable status, backup/restore
   and rollback rehearsal; joint review before enabling the actual endpoints.

Two specific regressions must be demonstrated: a message arriving during a
Claude monitor gap is replayed on reconnect; and a model response outside the
broker leaves a clearly labelled untracked/unanswered broker request rather
than a false claim that no response exists anywhere.

Test follow-mode bursts without repeated emission of the same ID on one
connection, then reconnect and verify unacknowledged replay. Also test summary
UTF-8/control-character/total-size bounds, explicit-key receipt loss and lookup,
notify readiness failures and abstract sockets, and daemon-down fail-fast
behavior without creation of another inbox.

For server push, test commit-before-notify, ordered bursts, arrival between
snapshot and subscription, reconnect at a complete event boundary, partial
frames, daemon restart, invalid/old tokens and restored-store generations.
Prove unacknowledged inbox holes survive an advanced event token. A slow
subscriber must be disconnected/replayable while send/ack/status and a healthy
subscriber continue making progress. Verify notification without repeated
client polls and heartbeat/quiet-versus-disconnected status.

The existing rollout-assisted bridge watch is an interim diagnostic, not the
new transport: narrowly scoped assistant messages from the explicitly chosen
session may alert Claude to an otherwise unrecorded reply. Such alerts do not
create broker receipts, inferred request links, or additional review clearance.
Stop that watch at cutover. This plan does not authorize broader session-log
collection or importing those logs as chat history.

Claude may initialize the repository and make the plan-only commit independently
of other projects, using a subject-only message. Implementation
starts on a branch when authorized separately. No unit installation, endpoint
registration or agent-instruction edit occurs as part of the plan commit.

Implementation should be small, but no "one day" promise is attached. The
integration feasibility and crash semantics determine the scope. Claude can
implement on a separate branch; Codex reviews source, tests, service unit and
adapter evidence before installation. The authoring/review split must avoid
simultaneously editing the same files.
