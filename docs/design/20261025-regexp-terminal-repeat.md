# Classify terminal-any loops once at compilation

Tracking: [#27665](https://github.com/matrixorigin/matrixone/issues/27665); implementation [#31079](https://github.com/matrixorigin/matrixone/pull/31079).

## Problem and ownership

The bounded bitstate matcher visits every rune and loop instruction for a greedy
any-rune repetition followed only by the end-of-text assertion and match. Its
only accepting exit is EOF, so those visits cannot change the result. This work
is significant in repeated SQL matching and replacement, including ClickBench
Q29. The optimization recognizes compiled instruction topology, without SQL,
schema, pattern-literal or input-content special cases.

The compiler owns immutable program structure before publication. It classifies
eligible loops once; execution owns the input-dependent newline check. Ordinary
Alt execution must retain its original path without extra classification or
metadata checks. Earlier runtime classification violated this boundary and
measurably slowed unrelated patterns.

## Eligibility and executor integration

After ordinary metadata and one-pass compilation, annotate a program only when
`onepass == nil` and its existing `maxBitStateLen` is positive. For an original
InstAlt, all of the following must hold:

1. Its greedy Out arm is InstRuneAny or InstRuneAnyNotNL, returning directly to
   the same Alt instruction.
2. Its Arg arm is exactly EmptyWidth(EmptyEndText).
3. That assertion leads directly to Match.

Change only the Alt opcode to the existing InstAltMatch tag. Preserve every
instruction's position, edges, rune data and capture metadata. Lazy repetitions,
captured tails, other rune classes, intermediate assertions/captures, multiline
EndLine exits, one-pass programs and oversized programs are excluded.

The bitstate InstAltMatch handler recognizes the annotated EndText variant. For
AnyNotNL it first checks that the suffix has no LF; for Any it can go directly to
EOF. Both then execute the existing EndText/Match path. Ordinary InstAlt and the
existing other InstAltMatch handling remain unchanged.

The NFA already expands Alt and AltMatch identically, in the same Out/Arg order.
Long inputs and RuneReader therefore retain the original graph and behavior.
One-pass compilation creates its own program before annotation, and a successful
one-pass result excludes annotation. Prefix, start-condition, capture and pool
metadata are derived before the change; counts and edges stay unchanged. Copy
continues to share an immutable program. No matching call modifies the program.

## Correctness and work bound

EOF is the loop's unique accepting exit. No captures occur inside the skipped
loop or between its exit and Match, so taking that exit at EOF preserves prefix
capture choices and alternative priority. The existing Match handling at EOF
ends both first-match and Longest execution. A visited EndText-at-EOF state
cannot represent an earlier failure: it would already have accepted and ended
this search.

AnyNotNL excludes exactly LF. Searching its input bytes for LF is equivalent to
decoding runes for this predicate, including invalid UTF-8: an ASCII LF cannot
be hidden inside another decoded rune. CR, Unicode separators and the existing
line-assertion policies retain their previous behavior.

The pooled bitState owns one `failedNL` integer, reset to -1 once per complete
search. It records the furthest LF found by an eligible suffix scan. A later
position at or before that witness must also fail; a later failed scan starts
strictly after it and records a later LF. Failed scan intervals are disjoint,
including across alternatives and unanchored start positions. A successful scan
ends the search at EOF. Total suffix scanning is therefore O(input bytes), with
no repeated whole-suffix scans and no new per-row allocation. Resetting the
witness at search entry prevents state from leaking across pooled reuse.

Search admission precedes VM reset: an unanchored search first locates its
required literal prefix. An absent prefix returns the pooled state without
initializing unread captures, visited bits, jobs or the LF witness. An admitted
search resets exactly once. Later prefix lookups start after the current rune
at the loop tail, retaining the original next-candidate order without scanning
the first prefix twice. A matched nonempty prefix proves the current position
is before EOF, so this step has positive width. Anchored searches reset before
their existing first try. The witness follows the cached inputs in the state
layout, preserving their original offsets. Prefix rejection never reads stale
VM state, and every executed generation starts with a fresh witness.

## Cost and alternatives

Classification walks the existing program once per successful compile/cache
miss, bounded by the current bitstate limit of 500 instructions. Annotation adds
no retained program storage or allocation. The existing combined 100-entry
regexp cache and its eviction stay unchanged. Per-search state grows by one
integer. SQL error, NULL, offset, replacement and output admission contracts,
wire formats, persistence, configuration and spill execution are unaffected.

A Regexp flag or per-PC side table would add ordinary-loop checks or retained
state for information already expressible by the opcode. A new opcode would
require additional consumer cases. Duplicating the VM would duplicate matching
semantics and maintenance. The existing tag keeps classification and execution
with their current owners. Future upstream updates must reconcile this producer
with every opcode consumer; the package README records that obligation.

## Validation and acceptance

| Risk | Required evidence |
|---|---|
| Wrong annotation | Typed eligibility controls; all graph fields unchanged apart from the tag; mixed eligible/ordinary branches; one-pass and size exclusions. |
| Changed results | Standard-library differential tests for captures, replacement, invalid bytes, LF, default/Longest/POSIX; NFA and RuneReader controls around the bitstate size boundary. |
| Reuse or policy errors | Offset and all line-mode comparisons against the unchanged NFA, shared-program concurrent reuse, and race validation. Public SQL checks cover binary/text domains, NULL, errors, positions and occurrences. |
| Generic regression | Interleaved controls for nonterminal, captured, lazy and multiline repetitions, including an ordinary loop in a program with an eligible branch. Record host noise explicitly. |
| Performance benefit | Eligible, short, early-failure and failed-start scaling benchmarks, then 10M-row non-spill public Q29 with neighboring queries, result parity and a source-built DuckDB reference. A microbenchmark alone is insufficient. |

Final checks include the private matcher and directly affected SQL function
normal/race tests, incremental static analysis and an independent overall review.
A repeatable generic regression remains a blocker even with a large Q29 gain.

Design approved before implementation by gpt-6.1-sol/xhigh: original proof in
commit `b59e94775997`, compile-owner amendment in `8c682a2f87e2`. This document
consolidates that approved contract. The rejection-path admission amendment was
approved before repair by gpt-6.1-sol/xhigh, which also performs the final review
for this repair. The earlier published revision used gpt-6-astra/xhigh review.
