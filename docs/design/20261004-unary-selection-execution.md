# Unary selection execution consolidation

Owner issue: #29249. Local implementation series:
`codex/quality-29249-20261003`, based on main `37ba071297`.
Status: design approved before implementation; implementation and local validation
approved by independent gpt-6.1-sol xhigh review.

## Evidence and gate

At `16722dd388`, a private native probe selected all 21 unary template entries,
including three forwarding entries, across six constant-input states. Of 126
cells, 36 fail callback-count assertions in 19 entries; active, partial, NULL and
AllNull controls pass everywhere. The recently repaired two string/bytes-to-fixed
error templates pass all cells. Evidence: `29249-unary-inactive-family-20261004`.
A second probe selected all 294 constant/flat cells across seven states and
confirmed those 36 failures plus one flat flag/bitmap interpretation mismatch.
Failures are execution admission defects, not claimed SQL reproductions.

The design gate applies because this consolidates a hot execution responsibility
across the complete unary family. Production functions remain in one package;
there is no new worker, state machine, cache, protocol or external interface.

## Contract and owners

Within a nonnegative logical row domain, an empty input or a domain with no active
row must not invoke the scalar conversion. Source NULL also suppresses conversion.
An active constant invokes it once; a flat vector invokes it once per active,
non-NULL row. Partial masks preserve the row positions and NULLs. Short masks leave
unlisted rows active; masks beyond the logical length cannot affect the result.
Existing error propagation, NULL-on-error and result-NULL policies remain owned by
the current typed templates and callbacks.

FunctionSelectList owns the mask representation and its Contains/AnyNull/AllNull
semantics. The execution templates own publication to result NULLs and values.
Reuse these responsibilities. One package-private mask application helper belongs
in baseTemplate.go beside existing result-publication helpers; it accepts the
existing select list, result null bitmap and logical length. It applies bounded
partial mask NULLs and returns any-masked/all-masked observations. AllNull admission
returns immediately without a scan; callers retain their existing fixed/null-range
or variable/SetNullResult publication when every row is inactive.

The helper has no retained state, allocations or callbacks. It replaces existing
mask scans rather than adding a pre-scan. Unmasked admission is constant time.
Every actual unary owner returns for length zero before acquiring a parameter.
Forwarding entries inherit their owner's behavior and retain real call sites.
The special fixed-to-string NULL-on-error implementation keeps its existing flat
per-row loop: use the shared helper only for its constant branch, avoiding an
extra scan on its flat path. Its flat row admission reuses the existing
functionRowSkipped contract, which ignores stale bitmap contents when AnyNull
is false. Its constant result uses appendRepeatedBytesResult after common mask
publication; preserve the WithSelection helper for its four other consumers while avoiding
a second partial-mask pass in this constant branch. The retirement scope is
17 repeated common mask blocks and the special constant admission checks. No broader binary/ternary migration is authorized by
this stage. Other arities require independent inventory and evidence.

## Integration and retirement

Replace the repeated mask-publication blocks in the actual unary owners, including
the two recently repaired count loops. Retire their per-owner skip counters and
mask flag plumbing where replaced. Keep typed scalar evaluation, cached parameter
acquisition, result preallocation and error policy in their current owners.
Do not add a generic execution framework or adapt all callbacks through another
callback. Existing byte/string conversions and decimal prefix/binary admission
remain unchanged. Inspect all direct callers and the special row-index callbacks. Constant row-error
callbacks retain row zero; flat callbacks retain the actual row index, including
JSON binary provenance. OctString can publish NULLs in its callback, so common
mask application occurs before callbacks and never clears their NULL side effects.

## Validation and cost

Before implementation, extend the orthogonal probe to flat inputs and distinguish
healthy values, errors, NULL-on-error, source NULL, mask flags, long/short masks and
same-wrapper reuse. Consolidate permanent coverage into existing shared template
regressions; preserve an explicit old-to-new map. Avoid one fixture per template
or a large data cross-product. Use failing scalar callbacks to prove error
suppression and literal expectations to prove results and metadata.

Run focused native proof before complete function normal/race and incremental
static checks. Use the old owner overlay to prove new regression sensitivity.
Measure unmasked and partial-mask const/flat paths at the same binary/toolchain,
with prior owner code, alternating pairs and controlled setup/reset accounting.
No new allocations, second mask scan or material healthy-path regression is
acceptable without a concrete benefit and recorded decision. Review production,
test and documentation increments separately. Service SQL reachability/BVT remain
unverified and cannot be inferred from callback injection. Issue publication is
unavailable through the known integration403; local drafts remain drafts.

## Complete entry inventory

- `opUnaryFixedToFixed`
- `opUnaryBytesToFixed`
- `opUnaryStrToFixed`
- `opUnaryBytesToBytes`
- `opUnaryBytesToStr`
- `opUnaryStrToStr`
- `opUnaryFixedToStr`
- `opUnaryFixedToStrWithNullOnError`
- `opUnaryFixedToStrWithErrorCheck`
- `opUnaryStrToBytesWithErrorCheck` (forwarding entry)
- `opUnaryStrToBytesWithRowErrorCheck`
- `opUnaryBytesToBytesWithErrorCheck`
- `opUnaryBytesToBytesWithResultNull`
- `opUnaryBytesToBytesWithNullOnError`
- `opUnaryBytesToStrWithErrorCheck` (forwarding entry)
- `opUnaryBytesToStrWithRowErrorCheck`
- `opUnaryFixedToFixedWithErrorCheck`
- `opUnaryFixedToFixedWithNullOnError` (forwarding entry)
- `opUnaryFixedToFixedWithNullCheck`
- `opUnaryBytesToFixedWithErrorCheck`
- `opUnaryStrToFixedWithErrorCheck`

## Local verification

The focused contracts, complete function package in normal and race modes, vet,
and incremental lint passed. Molint retained two diagnostics in files identical
to the base. The prior owners fail the permanent callback, NULL and reuse oracles.
Representative fixed and string owners were compared in eight alternating pairs
for one-row controls and 1024-row unmasked/partially masked batches: no material
regression or allocation-median increase was measured. These are internal template
contracts and owner microbenchmarks; SQL reachability and service BVT remain open.
