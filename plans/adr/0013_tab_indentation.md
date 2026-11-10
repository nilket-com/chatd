# ADR 0013: Tab indentation

Status: Accepted

Implementation plan: ../impl/0013_tab_indentation.md. Reviewed by Codex before implementation.

## Problem and scope

The owner's standing rule is tab indentation everywhere (the Rune fork and rnx use `hard_tabs`).
chatd's Rust sources use rustfmt's default four spaces; its Python (ADR 0011) already uses tabs.
Scope: Rust formatting configuration and a whole-repository reformat; no behavior change.

## Decision

1. `rustfmt.toml` sets `hard_tabs = true`, keeping `max_width = 140` and
   `use_small_heuristics = "Max"`.
2. Pre-existing Rust files change only through `cargo fmt`, with no hand edits. The single
   implementation commit also adds the formatting test (decision 3), changes `rustfmt.toml` and
   updates `ci/README.md`. No application behavior changes.
3. Formatting is a rule in code: `cargo test` fails when `cargo fmt --check` would fail, with the
   formatter's output in the failure; skipped only, and visibly, when rustfmt is not installed.
4. Python stays tab-indented. Other files (Markdown, TOML, shell, unit files) are not reformatted.

## Consequences

Every indented Rust line changes once; `git blame --ignore-rev` skips that commit. Later diffs are free of indentation noise.
