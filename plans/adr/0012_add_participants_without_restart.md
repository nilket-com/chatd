# ADR 0012: Add participants from validated config without a restart

Status: Accepted

Implementation plan: ../impl/0012_add_participants_without_restart.md.
Scope: daemon configuration publication, a preview/apply operation and its CLI, docs. Add-only.

## Problem

Participants come from `config.toml` at start: `Store::migrate` inserts them into the `participants`
table (add-only), and the in-memory `Config` in `Shared` is immutable and consulted separately
(`Config::knows` for watch admission, `wake_kinds`, product/tier for the directory and health). A
second agent pair therefore needs a daemon restart to get its own names. Restarting interrupts every
watcher and adapter, and on 2026-10-16 a restart was followed by Codex replaying many old requests.
With two pairs live on one daemon, "restart to add a team" is the wrong cost.

## Contract

1. **Add-only.** A refresh may add new participant names with their initial metadata (`product`,
   `tier`, `wake_kinds`). It refuses, explicitly and naming each item, any removal or change of an
   existing participant's definition and any change to a setting that is not reloadable (everything
   else in the file). Nothing is silently ignored. Changing an existing participant's wake policy is
   a possible later extension (it can release a backlog of pushes) and is not in this record.
2. **Validated whole, from the daemon's own file.** The candidate is the configuration path chatd
   selected at startup (kept in shared state; the client cannot name another file). chatd reads its
   bytes without holding the store or shared-state locks and validates exactly those bytes with the
   startup rules. A missing or unreadable file is an error here (not startup's defaults). An invalid
   candidate changes nothing.
2a. **What counts as a change.** Comparison is by normalized effective definition: the union of
   names from `participants` and `[participant.*]`; `product` and `tier`; the parsed wake-kind set
   (omitted equals the default `request`, `all` equals the full explicit set); every other setting
   after startup defaults. Moving a name between the two spellings, reordering, and formatting are
   not changes. Every removed or changed existing definition and every changed non-reloadable
   setting is reported by name in one refusal. Directory order is not part of the contract.
2b. **Retained names are not new.** A name absent from the active configuration but already present
   in the store (from an earlier start, possibly with an endpoint or messages) is refused as a
   retained name that needs a restart or a separately reviewed reactivation; it is never given
   fresh initial metadata at runtime. Existing admission of such names is unchanged.
3. **Preview, then apply.** `chatctl config preview` shows the effective base revision, the
   candidate revision (SHA-256 of the exact bytes read) and what would be added, or why the
   candidate is refused. `chatctl config apply --revision R` reads the file once; if those bytes do
   not hash to R it is `Conflict` and nothing is applied. It then revalidates the add-only delta
   against the current effective configuration under the publication lock, so a preview is not a
   reservation and two concurrent candidates cannot remove or overwrite each other's additions. A
   candidate with no effective additions (including a formatting-only edit) is a no-op that says
   so. The result names the added participants, the applied candidate revision and the new
   effective revision. A later edit to the file is a new candidate.
4. **One consistent view for the additions.** Store admission (the `participants` table) and the
   in-memory effective configuration (watch admission, wake kinds, directory, health inspection)
   switch together: apply prepares the new configuration first, then, holding the store mutex,
   commits the insert and publishes the new configuration by an infallible swap before releasing it.
   Nothing fallible (lock acquisition, file access, validation) happens between commit and swap. A
   database failure rolls back and publishes nothing. Readers take a configuration snapshot and never
   hold the configuration across acquiring the store mutex, so the lock order is store, then
   configuration, everywhere (send and receipt enrichment, endpoint registration, status and
   directory, watch admission and snapshot, adapter selection and digests, health). No lock is held
   while waiting on sockets, adapter subprocesses or session logs. A crash between commit and swap
   is a restart case: the next start reads the same file.
5. **No collateral effects.** No endpoint rebind, throttle reset, health clearing, message replay,
   watcher interruption or process restart. The file on disk is the source, so the next normal start
   sees the same participants.
6. **Restart parity.** A daemon started on the applied file and a daemon that applied it at runtime
   have the same participants, metadata and wake policy (directory order aside).

## Not in this record

Changing existing participants or other settings at runtime. Removing participants. SIGHUP-driven
reload (the explicit CLI result is easier to review than a signal).
