# Commit history checks

`cargo test` runs the committed-history checker and its signed temporary-history
controls through `tests/commit_history.rs`. There is no separate preflight or
hosted CI requirement. To run either check directly:

```sh
python3 -B ci/commit_subjects.py
python3 -B -m unittest discover -s ci -p test_commit_subjects.py
```

The checker reads signed committed objects only. A dirty working tree is not
evidence for a missing document or a valid signature. Subjects are
`plan: adr NNNN summary` or `impl: adr NNNN summary`: four-digit numbers, a
lowercase letter or digit first in the summary, then printable ASCII, without
parentheses, body or trailers. There is no rnx delivery-label ban list.
Both author and committer must be `Joel Bondurant <em@joelbondurant.com>`.
Every adopted commit must pass `git verify-commit` using the verifier's configured
signature trust.

Each plan changes exactly its paired `plans/adr/NNNN_name.md` and
`plans/impl/NNNN_name.md`. Both files must exist in the plan commit, with matching
names, an underscore after the number, and a lowercase alphanumeric snake name.
The ADR has exactly one opening `Status:` line in its first ten lines and no more
than 400 lines. The implementation plan is not subject to that size check.
An implementation may edit its own pair, but cannot edit another record's
files, including grandfathered flat plans. It need not change its own documents
if the implementation follows them without correction.

The exclusive adoption cutoff is
`1c0ac6827ad44a9e663f5d6c4b3f6353b75af77e`. Earlier history is grandfathered;
flat 0001–0007 plans stay in place. From 0008 onward, first-parent history
alternates a plan and its immediately following implementation. Each new plan
uses one more than the highest adopted ADR in its parent's committed tree, with
floor 7. `MAX_IMPLEMENTATIONS = 1` is the single cap. A pending plan is allowed
only at HEAD. An unpublished implementation can be amended; a published
correction requires a new record rather than rewriting published history.

To abandon a record, close it with its single implementation, changing only its
paired documents and setting the ADR to exactly `Status: Withdrawn`. Finish or
withdraw the open record before starting another. Merges, shallow histories,
replace refs and grafts fail closed. The cutoff must exist on the complete
first-parent chain and has no production command-line override. Tests inject
a temporary cutoff through the Python API.

The checker prints a terminal JSON PASS/FAIL receipt and returns nonzero on
failure, including the offending commit and rule when known. Both it and its
controls are read-only with respect to this repository. The controls create
signed temporary repositories, isolate Git configuration and use disposable SSH
keys; they never use the owner's signing key or live refs.

The Rust tests use `CARGO_MANIFEST_DIR`, run `python3 -B`, and inspect each child's
result after exit. If and only if spawning Python returns NotFound they print an
explicit SKIP notice. A suite with that notice has **not verified history**.
Other spawn failures, Python errors, missing Git/ssh-keygen, missing scripts,
failed controls, or missing/invalid signature trust fail the tests. Install
Python 3 (3.9 or newer), Git with signature support, and OpenSSH tools to exercise
both checks. The real-history check uses the repository's existing verifier
configuration; the fixtures' isolated signing configuration does not replace it.

While implementing a record, the tests validate its pending committed plan.
After creating the signed implementation commit, run the checker once more and
retain that JSON receipt: pre-commit tests cannot validate a commit not yet
created. No recursive cargo invocation is needed.

## Provenance and local adaptations

Adapted from rnx's `ci/commit_subjects.py`, `ci/test_commit_subjects.py`, and commit
sections of `ci/README.md` at
`1a3196d4ea88dcb397f544644d44bd4d784a81e2`. rnx in turn attributes its original
checker to ket at `9e7e3ad122f9ca6a3042bd12845f89ab9c1426ce`.

chatd keeps tab-indented Python from that source. Rust retains its existing
space-based rustfmt configuration. The port removes rnx's engine/import/follower
exceptions, subtree paths, second sequencing boundary and domain-specific label
list. It uses chatd's own cutoff and numbering, underscore-only paired paths,
and the one-implementation cap. rnx's measurement launchers, execute guard,
hygiene checker, and disk-admission tooling are not imported by this record.
