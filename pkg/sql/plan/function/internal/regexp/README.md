# SQL regular matcher

This private package is copied from Go 1.27.1's `src/regexp`: `regexp.go`,
`exec.go`, `backtrack.go`, and `onepass.go`. Those files retain the Go Authors'
BSD headers and the accompanying `LICENSE`. MatrixOne extensions and tests use
Apache-2.0. Parsing and compilation use the pinned standard `regexp/syntax`.

The local changes are an immutable `Regexp.lineMode`, a shared empty-width
assertion policy passed by all three executors, the original-context offset
search methods in `lines.go`, and compile-time classification of capture-free
greedy any-rune loops whose only exit is EndText followed by Match. These loops
use the existing AltMatch instruction; ordinary Alt execution stays unchanged,
the NFA preserves the same graph, and one-pass programs are excluded. The
bitstate shortcut uses one input-local LF witness to preserve linear search
work; its exact eligibility and proof are in
[the terminal-repeat design](../../../../../../docs/design/20261025-regexp-terminal-repeat.md).
Unused experimental `_All*` wrappers and three ineffective upstream assignments
are removed to satisfy repository lint; matching behavior is unchanged. Regular
SQL matching has no subject-dependent fallback to the ICU engine.

Ownership, exact upstream hashes, compatibility decisions, validation requirements,
and the update procedure are recorded in
[the design](../../../../../../docs/design/20261024-regexp-wide-line-assertions.md).
MatrixOne SQL function maintainers must reconcile upstream changes when updating
Go, reconcile the classification producer with every instruction consumer, then
run default and terminal-repeat differential tests, wide assertions, SQL
normal/race tests, and the vector predicate benchmarks. Do not silently replace
these files with a new upstream release or extend this private package into
another public API.
