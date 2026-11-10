# Implementation plan 0013: Tab indentation

Status: Accepted

Paired ADR: ../adr/0013_tab_indentation.md.

1. Set `hard_tabs = true` in `rustfmt.toml` and run `cargo fmt`.
2. Add a Rust integration test that runs `cargo fmt --check` (via `$CARGO`) from
   `CARGO_MANIFEST_DIR` and fails with its output. It skips, printing directly to stderr, only on
   positive evidence that the formatter component is unavailable; any other failure fails. Control
   the enforcement path on an isolated temporary Cargo fixture with misformatted code, never by
   dirtying this repository.
3. Replace the `ci/README.md` paragraph that says Rust keeps spaces: Rust uses tabs via
   `rustfmt.toml`, and `git blame --ignore-rev <reformat commit>` skips the reformat (the hash is
   found after committing).
4. Verify: `git diff -w` over pre-existing tracked Rust files is empty (supporting evidence of a
   mechanical change); the new test, `rustfmt.toml` and docs are inspected separately; Clippy with
   warnings denied and the full suite including the history check pass.

## Implemented verification detail

`tests/formatting.rs` invokes Cargo's supplied executable from `CARGO_MANIFEST_DIR`.
It permits a directly printed skip only for Rustup's explicit missing `rustfmt` or
`cargo-fmt` component diagnostic for the selected toolchain; other command failures
retain stdout/stderr and fail. Five tests cover the repository, a misformatted
isolated fixture (also proving check does not rewrite it), malformed formatter
configuration, missing Cargo, and strict missing-component diagnostic recognition.

Verification adjustment: `git diff -w` is not empty because rustfmt reflows two
`params!` calls in `src/store.rs` and one `execute_batch` call in `tests/broker.rs`
(the latter gains a trailing argument comma). No pre-existing Rust file was hand
edited. Formatting an isolated clean archive of the plan commit after adding only
`hard_tabs = true` reproduces all 22 pre-existing Rust files byte-for-byte. Retain
that reproduction receipt alongside the reviewed three-hunk `git diff -w` output;
this establishes the mechanical change despite Git's line-oriented comparison.
