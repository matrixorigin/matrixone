# Proportional Race And Timing Validation

Use this protocol only for a credible shared-state, lifecycle, synchronization,
background-work, or timing failure mode. Ordinary sequential logic uses focused
normal tests plus its owning package.

## Focused adaptive stress

1. Select each new/changed/directly affected concurrency test individually. An
   issue or CI failure naming `TestXxx` makes that exact test mandatory.
2. Prove the exact selection is non-empty.
3. Measure the test once under `-race` from its terminal `go test -json` event,
   excluding first-build time. With budget `B` and duration `T`, use
   `N = clamp(floor(B/T), 1, 100)`. Default `B=30s`; use `N=100` when `T` is
   absent/non-positive/below resolution. Override only to cover a known
   pre-fix occurrence window and record why.
4. Run each exact test separately:
   `GOWORK=off go test -mod=readonly -race -count=N -timeout 120s -run '^TestA$' ./pkg/path`.
   Keep repetitions in one process so leaked global/package state remains
   observable.

For production shared state/lifecycle code, then run each affected owning
package once under `-race`. For test-only local synchronization, the focused
stress is sufficient unless a shared fixture/helper/global or package-level
interaction creates a credible broader race; then run the owning package once.
Use `.agents/skills/mo-dev/scripts/mo-cgo-test` for CGo-direct/transitive
packages.

Every repeated stress command names one exact test. Never repeat-stress an
entire package/repository. Race success does not validate a timing, allocation,
coverage, or instrumentation-sensitive oracle; run the matching mode when that
property is the claim.

Measurement-only allocation/performance tests may use `//go:build !race` only
when race bookkeeping invalidates the measurement and a functionally equivalent
race-tested case remains. Never hide ordinary behavior or timing assertions
behind `!race`.

Tests must use observable phase barriers, durable oracles, immediate cleanup,
deterministic IDs/order/topology, and an outer deadline as a hang guard rather
than a scheduler. Sleeps, tiny deadlines, retries, or scheduler luck do not
become acceptable because repetition passes.

Reuse race evidence only when it satisfies the identity, selection, mode,
result, scope, and semantic-validity rules in
[validation-evidence.md](validation-evidence.md).
