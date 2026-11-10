# Implementation plan 0008: Wake filter for pull watchers

Status: Accepted

Paired ADR: ../adr/0008_pull_wake_filter.md.

## Docs

- "Connecting Claude": the recommended watcher, `chatctl wait --as X --full --take --wake-on
  request,final` (one-shot, re-armed after each return), and the `--follow` variant with its caveat;
  how to tell a timeout (exit 6) from a message; not to pipe through `tail` or `head`. Also the
  default 480-byte `--max-line`, `truncated: true`, and recovering a cut message with `chatctl take
  --as X --id ID` (otherwise a re-armed one-shot `wait` returns the same unacknowledged truncated
  trigger again).
- Move "Current work without losing history" (`status --current`) above License, next to `status`.
- `wait --help`: one line naming the `--full --take --wake-on` recipe.

## Tests

Assert behavior, not helpers: default unchanged; invalid or empty sets refused; backlog and live
filtering; held messages not shown (suppressed or overflow) stay unacknowledged with and without
`--take`, while held context shown whole is acknowledged under `--full --take`; selected-first
order; multi-page snapshot whose trigger is on a later page than older held context; several selected
snapshot messages across pages with held messages between them under `--follow` (each selected once,
context never before its trigger); context cap and stderr overflow with valid JSON on stdout; overflow
followed by another selected trigger on the same connection; one-shot prints the whole bounded batch;
follow shows a held message as context once per connection; reconnect rebuilds and replays; a timeout
with only held traffic exits 6; expiry between batch entries starts no further announcement; a failed
write mid-batch leaves the rest unacknowledged; a truncated context body stays unacknowledged; the
default and `all` keep immediate one-shot output. Existing `--full`/`--take`, overlap and exclusivity tests keep passing.
