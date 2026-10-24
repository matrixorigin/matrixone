# SQL regular-expression wide line assertions

Owner: MatrixOne SQL function maintainers. Issues: #30210, #30417, #30550. Delivery: PR #30646.
Design v2; source Go 1.27.1, matching this repository's pinned Go toolchain.

## Problem and invariant

The original per-line implementation reparses patterns for every row and scans a 4 KiB prefix before a predicate can return. It sends ordinary cross-line patterns to regexp2, introducing an 8 MiB evaluation budget and a 1 MiB subject limit. An unrelated trailing CR changes a successful `^a\s` match into an error. Identical 256-row vector benchmarks on the same machine measure a 36.6x long-text slowdown and roughly 209 KiB of additional allocation per batch.

Every pattern accepted by the regular engine must remain in that engine at every supported subject size. Matching must retain original input positions, capture priorities, scoped flags, NULL validation and replacement limits. A successful prefix predicate must not inspect an unrelated tail. Genuine ICU-only syntax continues to use regexp2 with its existing bounds.

## Accepted ownership and compatibility decisions

The user explicitly accepted maintaining a controlled Go engine copy in this PR on 2026-10-24. The required gpt-6.1-sol/xhigh design is recorded in session 01a16c08-26c8-7460-8928-e94dd076b6a2. This document records that design before implementation.

Copy the four Go regexp implementation files into `pkg/sql/plan/function/internal/regexp`, preserving their Go copyright headers and BSD license. This is the SQL function's regular matcher only; unrelated repository regexp users keep the standard library. Reuse upstream parsing/compiler, matching algorithms, capture handling, prefix optimization and machine pools. Do not introduce another interpreter or a second SQL iterator.

Extend the shared empty-width assertion boundary with an immutable per-compiled-pattern line mode. NFA, bounded bit-state and one-pass execution use the same policy. Default mode must remain equivalent to Go 1.27.1. Wide multiline (`m` without `u`) recognizes CR, LF, CRLF, NEL, LS and PS in text and CR/LF/byte 0x85 in binary. Between CR and LF neither line assertion holds. Dollar holds before a terminator and at EOF. Caret holds at the beginning and after terminators, except EOF after a lone CR/NEL/LS/PS. Preserve MatrixOne's existing EOF-caret exception after LF, including CRLF. Whole-subject anchors and anchors scoped under `-m` remain unchanged.

Preserve main's consuming-atom semantics, including dot and scoped dotall; this change implements line assertions. The splitter's incidental rejection of wide terminators by ordinary dot is removed, and explicit tests must record the coherent contract across regular and ICU paths. This is not a claim of full ICU equivalence.

## Cache and execution integration

Use existing `regexpSet` ownership and combined 100-entry eviction. Look up the raw pattern, normalized match type and binary/folding domain before constructing wrappers, scanning syntax or compiling. Clone retained key strings. Classification and nullable metadata are computed on a miss. Do not add a routing cache or retain an AST.

Expose internal index/submatch search methods taking a byte offset, using the existing executor's search position. Reuse the SQL occurrence iterator with original-subject context, rather than generated context wrappers or Go FindAll's different adjacent-empty-match suppression. INSTR retains its documented suffix-relative starting-position contract; SUBSTR and REPLACE retain whole-subject context. Binary encoding, byte-offset conversion and replacement expansion remain with their existing owners.

Remove the line splitter, subject-based routing, prefix scan, wide-only size error and synthetic terminal-empty-segment evaluator. Keep wide-anchor lookarounds only for genuine regexp2 syntax. No persistence, wire, catalog or configuration behavior changes.

## Failure and resource closure

Compile a present pattern before the applicable NULL/range shortcuts; masked rows remain unevaluated. Reuse existing invalid-pattern/error mappings. Regular execution inherits upstream linear-time algorithms and pools, without a subject-size gate or full-input text copy. The existing SQL iterator advances nonempty matches to their end and empty matches by one input unit, including EOF termination. Replacement appends keep the shared output budget. ICU reservations, deadlines, release and cache-byte budgets remain unchanged.

## Verification and maintenance

Before broad checks, reproduce the four text/binary admission failures against the old PR and passing main overlay; run identical warmed vector benchmark fixtures on main, old PR and final implementation (three samples). Short/long text and binary prefix medians target <=1.25x base, no additional per-row allocations and <=4 KiB/batch extra bytes. Warm execution performs no parse, rewrite or prefix scan.

Differential-test default compilation/results/captures/replacement against the standard library; exercise all three executor algorithms, concurrent reuse and original-context offset searches. Compare wide assertions to independent boundary oracles, including CRLF, Unicode/binary separators, EOF, scoped flags, captures, zero-width matches and NULL/error precedence. Cover regular cross-line families and every public REGEXP entry point, including >1 MiB inputs. Refresh normal/race owning packages, directly affected SQL LIKE and positional/replacement consumers, incremental static checks and actual SQL/BVT.

The pinned engine source hashes below bind provenance. A future Go toolchain update must compare upstream changes, preserve the local assertion/offset extension, refresh the hashes and rerun default differential, wide-mode, normal/race and performance tests. The private package is not an independent general-purpose API. Fork source files retain upstream BSD headers; license checks exclude only those exact copied files and verify the complete policy separately. New MatrixOne tests/extensions use normal repository licensing.

```json
{
  "regexp.go": "a3e6d904aba2f2e5e4fb8fbbdbe817b317f9ce4bd1e1f1edfe4bcb0b0c552fcc",
  "exec.go": "5b6b8516900648fdd657d7fc464b7c51bd358012fa632aa5e73a4daa9831df74",
  "backtrack.go": "ac5a1e4cf49b1c7f4372c0a7c2e8d156100c17d3a114fcf9539f55d223d06092",
  "onepass.go": "fd0e29803438c6ea3021c78227d4f69de163f693e82c155dc62d721eaacbd24b"
}
```
