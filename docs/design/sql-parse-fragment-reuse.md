# Reuse parsed SQL statement fragments

Issues: #32207, #32210. Implementation: #32348.

## Problem and invariant

For an ordinary text request, the frontend parses the SQL to construct ASTs and
then parses the same text again to obtain the statement spans used for routing
and diagnostic records. Both parses use the same MySQL grammar, including
compound statements and executable comments. On the three-CN benchmark for
#32210, reusing the routing split for records removed one extra parse and raised
100-column throughput from about 4,684 to 6,470 rows/s. #32207 still shows about
1,500 rows/s for a remote-lock CN: this parser change alone is not expected to
remove its network-lock bottleneck.

A single successful parse may supply both ASTs and source fragments only when
it is equivalent to the existing two parses. Their order, exact executable
comment balancing, whitespace, source classification, statement recording,
credential redaction, SQL mode, errors, and AST ownership must stay unchanged.
SQL rewriting, prepared execution, AST-only input, and cached plans keep their
existing authorization and execution semantics. Nothing may retain a parser's
mutable lexer or a pooled AST beyond its existing owner.

## Design

The MySQL parser already records top-level delimiter offsets while constructing
ASTs. Factor the existing splitter's fragment construction into one helper
operating on a completed lexer and its exact input/mode. Add a parser method
which returns the ASTs and fragments from that same successful parse. It calls
the existing parsing method, transfers AST ownership to its caller unchanged,
and creates standalone fragment structs before the parser can be reused. The
existing splitter calls the same helper and continues to free its temporary
ASTs. Parse errors retain their existing error and cleanup path.

Frontend text parsing supplies these fragments to the existing scheduling and
recording code only if its actual `lower_case_table_names` is `1` and the
parser's actual SQL mode matches the mode used by scheduling. The existing
splitter hard-codes `1`; other case modes, a changed mode, cached plans without
a fresh AST parse, synthetic commands, and AST/binary prepared inputs use the
existing splitter or their existing special path. Even reused fragments pass
the existing source-span validation and statement-count checks. The request
owns them only until scheduling/recording finishes; record text is sanitized by
the existing sanitizer. Staged multi-statement execution still parses each
statement under its then-current SQL mode.

The parser API is additive. It changes neither MySQL wire format nor catalog,
transaction, lock, storage, cluster, or session configuration. The method does
not introduce a cache, global state, goroutine, or protocol gate. Its fragment
slice is bounded by the current SQL input and has the same lifetime and
asymptotic memory cost as the existing split. Context cancellation and parsing
limits remain those of the existing parser entry point; no new blocking work is
introduced. Shutdown upgrades require no migration. Removing the optimization
restores the existing independent split.

## Alternatives

* Keep the independent AST and fragment parses. This is simplest but repeats
  grammar work on every eligible request and scales with wide VALUES input.
* Cache ASTs or results across statements. This requires generation,
  invalidation, session-mode, diagnostics, and ownership state and is unnecessary
  to eliminate this within-request duplication.
* Reuse delimiter offsets for every parser case mode. This may be safe, but the
  current splitter's independent parse uses case mode 1. Until equivalence is
  proved, preserving the existing path for modes 0 and 2 is safer.

This design is a hot-path, cross-package API/ownership change and is reviewed
before its implementation. It does not assume that skipping parser work fixes
the single-row remote-lock bottleneck or that the opt-in shared lock proxy is
safe to enable.

## Validation and acceptance

Compare fragments and AST results against the existing independent parser and
splitter for empty and comment-only input, blank separators, ordinary and
compound multi-statements, executable comments, quoted semicolons, parser SQL
modes, invalid SQL, and parser reuse. Check modes 0/2 and changed modes take the
existing path. Test actual frontend recording/redaction, scheduling/source
alignment, rewritten SQL, cached and prepared inputs, stale metadata, and error
cleanup. Execute representative text and binary-prepared SQL against a real
three-CN cluster and compare stored values, errors, rollback, and affected rows.

Measure release builds against the same base with matching lock-owner and
remote-lock CN roles: wide batches and single rows, with and without a primary
key, text and prepared. For this additional parser API, require at least a 5% median improvement in
either wide batch relative to the immediately preceding implementation, using
at least three alternating pairs on matched roles; otherwise discard the API
change. Reject a material regression above 10% outside measurement noise.
Report smaller or unresolved differences rather than attributing a lock-bound
result to parsing. Run owning-package normal and race tests and the
repository's incremental formatting, vet, and lint checks. Final review covers
both the complete PR and this ownership boundary.
