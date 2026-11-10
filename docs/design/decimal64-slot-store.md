# Decimal64 grouped-combiner slot stores

Owner: #30847. Implementation: branch `perf/decimal64-slot-store`, runtime
commit `5c816098da`. This proposal changes only
the initialization of the existing local slot table in Decimal64 SUM/AVG.

## Problem and invariant

The current combiner writes 256 sentinel bytes in a scalar loop on every
non-constant-NULL call. Previous copy-based revisions reduced that work but
introduced independently measured control or SF10 Q18 regressions. They remain
rejected; their gains are not acceptance evidence for this revision.

Every byte in the 256-entry table must be `0xff` before probing. The existing
hash, 255-entry capacity, wraparound, fallback, accumulation and scatter must
keep the same operations and indexing. The executor owns the stack table for
one call; no state survives the call or crosses an ownership boundary.

## Proposed change and alternatives

Use `encoding/binary.LittleEndian.PutUint64` to write eight sentinel bytes per
iteration. All bits are one, so byte order and alignment do not change the
sentinel. The standard API provides bounds safety on supported architectures.
There is no source copy, new allocation, mutable global, unsafe conversion,
state-shape branch or architecture-specific intrinsic. Generated bounds checks
and stores must be inspected rather than assumed away.

Keeping the byte loop is the baseline and remains preferable if this revision
loses on caller controls or queries. The eight-byte copy loop can emit overlap
checks; the whole-table constant copy adds a read source and has measured
control regressions. A zero sentinel changes per-row indexing and has already
failed controls. None of these rejected revisions is mixed into this patch.

The compiler can change register allocation and code placement despite this
small edit. Fewer initialization instructions alone do not establish a query
win. Do not add alignment directives or select benchmark-specific branches to
force one favorable code layout.

## Scope and terminal paths

Only `sumDecimal64FastExec.batchFill` changes. DISTINCT dispatch, constant-NULL
return, NULL/constant/offset/unmatched processing, Decimal128 unchecked addition,
overflow finalization, state allocation, merge, restore and Free retain their
existing owners and paths. No API, serialized format, configuration, planner,
background task, cancellation or resource-admission contract changes.

## Validation and acceptance

Build clean current-main and candidate sources separately with identical native
inputs, Go settings and test fixtures. Preserve the focused SUM/AVG collision,
255/0 wrap, full-table fallback and multi-chunk regressions. Run the owning
aggregate and group tests, scoped static analysis and the mandatory pre-push
gate. New race or topology tests are unnecessary for call-local initialization;
reuse existing executor lifecycle coverage.

Compare complete caller-shaped controls, including real 256-row calls and the
8192-row API controls, NULL/constants, empty/tiny, sparse/permuted and many-chunk
inputs, growth and DISTINCT. Exclude the first result and independently confirm
every slower signal. Inspect fixed-work instructions/cycles separately from
latency. Do not offset one control's regression with another control's win.

If controls permit, measure unchanged local SF10 Q1/Q18 and then Q1-Q22 with
balanced process order, separate profiles, a 16 GiB CN cache and 32 GiB total
application budget. Require recorded zero measured physical I/O, spill, swap
and OOM, and bounded external CPU activity. Never inspect benchmark inputs,
tables, schemas, row counts, references or query results or compute checksums.
The requester accepts lower observed target medians with uncertainty stated;
this does not waive confirmed regressions. No server129 measurement is allowed.

Publish a templated PR only after the required tests, performance checks and
self-review pass. Local SF10 evidence does not close the umbrella's SF100 or
mixed-I/O acceptance requirements.

## Design and implementation review record

The design at `f1cc97380f` passed the separate design-first review for an isolated
prototype. The implementation follows that revision without a state or lifecycle
change. R3 applies to hot-path performance; concurrency, persistence, protocol,
security and new-resource dimensions have no changed transition to audit.

The focused SUM/AVG tests pass on both builds. All 615 aggregate test roots and
the group consumer suite pass, and the SIMD/v3 scoped static check has zero
issues. The complete 30-case component screen and the independent longer
large-batch/NULL/DISTINCT check have no slower interval, with unchanged allocation
counts. Fixed-work counters show about 13% fewer user cycles for four-group and
10% fewer for sequential caller-shaped updates.

Local SF10 full-suite paired medians are Q1 -1.24% and Q18 -1.72%; both intervals
include no change. The separate Q1/Q18 context measured +0.76% and +0.84%, also
inconclusive. These observations remain separate. The requester accepts lower
observed query medians with uncertainty reported; that decision does not waive a
confirmed regression. All 22 full-suite intervals include no change or favor the
candidate. Higher point estimates remain uncertain rather than proven neutral.
The twelve full-suite and twelve target arms passed the recorded all-memory and
host-load gates, with zero measured physical I/O, spill or application swap.

This patch supplies a bounded component-work reduction and limited local SF10
query evidence. It does not close the umbrella's SF100/mixed-I/O requirements.
The implementation PR must report the final pre-push and review outcome.
