# Implementation plan 0011: Commit history checker

Status: Accepted

Paired ADR: ../adr/0011_commit_history_checker.md.

1. Copy `ci/commit_subjects.py`, `ci/test_commit_subjects.py` and the commit section of
   `ci/README.md` from rnx (cite the rnx source commit in the README), then adapt: chatd identity
   and full cutoff hash, record numbers from 0008 with sequence floor 7, one first-parent segment
   (remove the engine, import, follower, `rune/` and second sequence-base machinery and the rnx
   label list), underscore-only document paths, one implementation per ADR. Keep the Python
   tab-indented as copied (the owner's preference; Rust formatting is untouched).
2. Keep the unit tests for every retained rule, on small signed fixture repositories in temporary
   directories with isolated git configuration and a throwaway signing key (never the user's key or
   live refs): valid pair, pending plan, consecutive numbers, the cap; unsigned commit, wrong author,
   wrong committer, body or trailers, bad subjects; missing or duplicate documents, Status missing,
   ambiguous or late, ADR over 400 lines; plan changing extra files, implementation changing
   another record; skipped or repeated numbers, impl before plan, wrong impl number, interleaved
   plans; valid withdrawal, withdrawal with product edits, withdrawn opening plan; wrong, unknown or
   non-first-parent cutoff, merge, shallow, replace refs, grafts; uncommitted files cannot satisfy a
   missing document; a cutoff tree with only flat 0001-0007 files admits 0008 then 0009; JSON
   failure receipts name commit and rule. Drop the engine, import, follower and rnx tool tests.
3. Add a Rust integration test that runs the checker against this repository and fails with its
   JSON output when it does not pass, plus one that runs the Python unit tests; both skip only with
   an explicit message when `python3` is missing.
4. README: a short "Commit conventions" section pointing at `ci/README.md`.
5. Verify: the checker passes on the current history (0008 to 0011), the unit tests pass, and
   `cargo fmt`, Clippy with warnings denied and the full suite pass.
