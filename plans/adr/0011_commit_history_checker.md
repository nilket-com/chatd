# ADR 0011: Commit history checker

Status: Accepted

Implementation plan: ../impl/0011_commit_history_checker.md. Reviewed by Codex before implementation.

## Problem and scope

chatd's commit subjects and plan files have followed no fixed form (`feat:`, `fix:`, `docs:`,
plain sentences; `plans/NNNN_*.md` and `plans/0007-*.md`). The sibling repositories (ket, rnx)
use one discipline, enforced by a committed read-only checker, so a convention is a rule in code
rather than something agents remember. From ADR 0008 on, chatd uses the same form; this record
makes it checked.

## Decision

1. **Subjects.** `plan: adr NNNN <terse>` or `impl: adr NNNN <terse>`: four-digit number, a
   summary whose first character is a lowercase letter or digit and the rest printable ASCII, no
   parentheses, no body, no trailers. rnx's ban list of its own delivery labels is not carried over. Author and committer are the repository's established identity, and every checked
   commit has a valid signature.
2. **Documents.** A plan commit changes exactly its paired `plans/adr/NNNN_<snake>.md` and
   `plans/impl/NNNN_<snake>.md` (underscore after the number, lowercase snake name), and both exist
   from the plan commit on. The ADR has one opening `Status:` line within its first ten lines and at
   most 400 lines. An implementation commit
   requires an earlier plan with the same number on first-parent history and may change no other
   record's documents (it may change its own pair). One implementation per ADR (the rnx cap); a
   correction amends the implementation before it is published, or gets a new ADR after. A
   withdrawal is the record's single implementation, changes only its own pair and sets the ADR to
   exactly `Status: Withdrawn`.
3. **Cadence.** From the adoption cutoff, records alternate plan, impl, plan, impl, and each new ADR
   number is one more than the highest adopted ADR in the parent tree (floor 7). A plan may be
   pending (committed, not yet implemented) only at the head.
4. **Numbering.** chatd keeps its own sequence, continuing from its earlier plans (0001 to 0007);
   the first checked record is 0008. It does not share the Rune/rnx sequence.
5. **Adoption cutoff.** History up to and including
   `1c0ac6827ad44a9e663f5d6c4b3f6353b75af77e` (the last commit before `plan: adr 0008`) is
   grandfathered by exact hash; the checker validates cutoff..HEAD, and the cutoff must be on the
   complete first-parent chain. The flat `plans/0001`..`0007` files are left as they are. The
   production command has no cutoff override; tests inject one through the Python API.
6. **Read-only and fail closed.** The checker reads committed objects only (a dirty tree is not
   evidence), rejects shallow clones, replace refs, grafts and merges (every adopted commit has
   exactly one parent), and prints a JSON receipt, exiting non-zero on any violation with the commit
   and rule named where known.
7. **Run by the repository's own tests.** The check runs as part of `cargo test`, so the usual
   verification step enforces it; no hosted CI is involved.

## Consequences

Agents and humans get a mechanical answer to "is this history acceptable" before pushing. The rnx
checker is the source: chatd copies it and changes only what this repository needs, so fixes can be
compared line by line.
