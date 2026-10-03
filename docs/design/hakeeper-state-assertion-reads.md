# HAKeeper state assertion reads

Related issue: https://github.com/matrixorigin/matrixone/issues/29562.
Implementation: the CI UT racing optimization PR containing this document.

## Problem and owner

`healthCheck` and `taskSchedule` each call `assertHAKeeperState` before and
after their work. Every assertion currently performs a linearizable
`StateQuery` and deep-copies the entire `CheckerState`, including every store's
configuration. The assertion consumes only `State`. Existing issues profiles
show configuration copying among material allocation costs; the amount owned
by assertions still needs separate measurement.

The state-machine lookup owns the snapshot. Eliminate unused snapshot work at
that owner, rather than caching configurations, weakening assertions, changing
the checker cadence, or bypassing the authoritative read.

## Contract and implementation

Add `StateOnly bool` to the existing local `StateQuery`. Its zero value and a
typed-nil `*StateQuery` retain the complete independent snapshot. When explicitly
requested, lookup returns a new `CheckerState` containing only the current
`State` enum. There are no aliased maps or other mutable state in that result.
The query is a local Dragonboat lookup input, not replicated or serialized
state or an RPC schema.

Only `assertHAKeeperState` requests this projection. Keep the full public and
private checker getters unchanged in meaning. Share the existing read/error
boundary with an explicit query parameter, keeping the existing timeout cause,
`SyncRead`, retry policy, error attachment and assertion behavior. Every
assertion still performs a separate authority read; there is no state reuse.

Public GET_CLUSTER_STATE, WrappedService, scheduling, truncation, recovery,
labels/workstate and configuration consumers retain full snapshots. In
particular, LogStore ConfigData contains dynamic WAL recovery status and must
remain complete on those paths. `ClusterDetailsQuery` is unaffected.

No persistence, protocol, worker, cache, synchronization, lifetime or cleanup
change is introduced. Lookup serialization remains owned by Dragonboat.

## Validation and cost gate

Retain the existing complete snapshot isolation tests. Extend their owning
fixture to cover typed-nil compatibility, state-only exact output, all state
enum values, fresh reads after transitions and independent returned values.
Use an existing real store fixture to compare full/state-only reads and preserve
cancellation/read-error behavior. Exercise the normal and race owning packages
and existing bootstrap, health, task, WAL recovery and two-CN restore consumers.
No new cluster solely for a duplicate oracle, sleep, skip, retry or removed case.

Measure real-config-sized full versus projected lookup work separately, then
measure the cumulative issues race workload with the same toolchain, CPU limit,
native artifacts and profile settings. Report CPU, allocations, RSS and wall
time, including any regression. Microbenchmarks do not establish CI minutes;
the final CI saving remains unconfirmed until its real run.
