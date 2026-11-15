# Fused two-column compact group lookup

Owner: #30841. Approved for implementation by the requester on 2026-11-16,
as PR 1 of the four independent TPC-H optimization PRs. Base: f70c37b16e.

## Contract and design

The existing compact table is a derived index of committed current-generation
groups. Preserve that ownership, the 257-value CHAR domain, allocation layout,
admission, generic insertion, spill and reset. No API, encoding, cache, group
creation or aggregate-state changes are authorized.

For two eligible flat CHAR(1) columns, resolve descriptors once and canonicalize
both values in one row loop. Compute a*257+b and write the existing code scratch
and the corresponding committed group ID. Do not clear codes first or traverse
them again. Keep one-column and selected-input routes unchanged.

Only publish eligibility after validating the entire chunk. A miss still checks
the suffix, then uses generic preview/commit; unsupported input leaves the
derived table ineligible. Generic publication uses completely initialized codes.
Empty/PAD SPACE values, embedded NUL, high bytes and non-inline/long values keep
the existing canonical identity and fallback. No scratch or ID is published
before its existing owner commits.

The status quo repeats descriptor/code traversals; direct-address group creation
would introduce a new publication protocol. The chosen change removes redundant
work without that protocol or any extra retained/temporary allocation.

## Validation and delivery

Extend existing tests for canonical tuples, misses, late unsupported values,
offsets/tails and new domains. Reuse the existing admission, spill and generation
tests. Benchmark actual 256-row full dispatch plus tiny/unsupported/miss controls.
Compare this branch alone against clean current main with all 22 TPC-H queries
and a Total row, locally in SF10 memory/disk and SF100 disk, without spill/swap.
One process/configuration loads once, excludes one cold suite and measures five
warm suites. Report wall time, instructions, allocations/copies and uncertainty;
scripts execute/drain only. Confirmed regressions block readiness and require a
reviewed fix, not a waived gate. A historical profile share is not a speedup claim.

R2: arithmetic/key-identity and publication boundaries are affected. Existing
ownership/lifecycle edges are unchanged. Focus owning tests/static checks and
self-review on those contracts; no new BVT is needed for this internal algorithm
with unchanged SQL semantics and existing real Group coverage. Preserve pending
dictionary work when resolving future source overlaps, without depending on it.
