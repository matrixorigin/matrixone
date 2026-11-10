# Remote expression send validation

Owner: SQL compilation (`pkg/sql/compile`). Tracking issue: [#29562](https://github.com/matrixorigin/matrixone/issues/29562). Implementation: the PR containing this document.

## Problem and invariant

A sender analyzes one serialized pipeline repeatedly and separately probes the same destination for each expression capability. Placement already shares analysis. Consolidating the sender must preserve every independent floor, diagnostic order, source validation, and receiver validation.

The first insufficient gate must still be reported when a worker satisfies some, but not all, floors. Reusing a Scope or Compile cannot reuse an earlier expression generation or worker observation. Cancellation and coordinator rollout changes remain live guards.

## Ownership and design

The sender computes `RequiredRemoteExpressionFeatures` once after the independent provenance fence, then uses the existing source fence and one ordered expression destination gate table. The receiver independently analyzes decoded data before using the same source fence. Provenance remains earlier because its errors have precedence over other expression errors.

The existing capability probe owns endpoint resolution and response release. Its single-worker portion returns the observed worker version. The placement/group wrapper retains its coordinator precheck, group-wide five-second budget, low-version early exit, and transient-error mapping to `parent.Err()`.

The sender performs, for each enabled gate:

1. Check destination presence, parent cancellation, and the current coordinator floor.
2. On the first enabled gate, resolve and observe the actual worker once with a five-second budget.
3. Compare that observation to this gate's floor, preserving the existing diagnostic.

Only the raw worker observation is reused within this call. The coordinator version is read again for each gate: a downgrade during the first successful RPC must reject a subsequent gate. An upgrade must not remain bounded by the earlier coordinator version. Every subsequent send resolves and probes again.

Unknown capability, missing endpoints/client, stale address or wrong identity, and transient probe errors fail closed under the existing policy. Responses, including responses accompanying errors, are released once. No new cache, worker, persistent field, wire field, configuration, or admission state is introduced.

## Boundaries retained

Bound-string validation deliberately inspects executable expressions rather than planner witnesses. Grouping checks multiple consumer endpoints, including dispatch consumers before the root dispatch is stripped. Vector transport confirms the actual stream protocol. These fences retain their owners and are not replaced by the expression destination observation.

Per-feature destination wrappers are removed. Shared version policy stays in the existing expression protocol owner. Direct component tests move to the common gate; original real sender/receiver tests retain their assertions.

## Validation and cost

The design was reviewed separately by gpt-6.1-sol / xhigh before implementation review. The review required live cancellation/coordinator checks and preservation of grouping/vector boundaries; the implementation includes those requirements.

Validation includes full compile normal/race tests, deterministic probe cancellation/rollout/unknown-response controls, repeated race tests, actual Scope reuse after expression/version changes, complete issues race comparison, multi-CN consumers, and incremental static checks. Four PITR subtest names contain runtime timestamps; equivalence normalizes only those timestamps.

Measure plain, mixed, and wide real send boundaries, including serialization and probe counts. Report complete package wall time, CPU, cumulative allocation, and peak RSS separately. Microbenchmarks do not establish CI-minute savings, and cumulative allocation reduction does not establish peak-memory reduction. Keep #29562 open until comparable CI evidence establishes its target.
