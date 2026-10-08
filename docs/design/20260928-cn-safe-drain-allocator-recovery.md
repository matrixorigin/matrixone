# CN lock-service drain: allocator-loss recovery contract

This document describes the kernel side of the instance-bound drain protocol
introduced by PR #29291. It is a review contract, not evidence that an Operator
rollout, cloud eviction, or a full product acceptance test has passed.

## Identity and safety

`ServiceID` is the complete lock-service incarnation ID (start timestamp plus
CN UUID), not a UUID alias. `AttemptID` identifies one Operator drain attempt.
`BeginDrain` and `QueryDrain` are available only at MORPC v105 or later. A
successful `BeginDrain` returns the allocator ID and version. `QueryDrain`
returns `Safe=true` only for the same service, attempt, and allocator epoch after
the allocator has observed terminal `ServiceCanRestart`. Missing state,
ambiguous identity, another attempt, or another allocator epoch fails closed.

The CN stops normal new-lock admission on `ServiceLockWaiting`. Its local drain
can reach `ServiceUnLockSucc` only after pre-drain admissions, in-flight lock
operations, remote transaction holders, and binding references satisfy the
existing completion checks. An allocator's boolean response alone does not
make those local conditions true.

## Recovery after allocator state loss

The new allocator may receive `BeginDrain` before any `GetBind` for this CN.
It creates a **pending, non-admitting** bind and returns `OK=false`. A heartbeat
sent before the CN has observed the new allocator carries no matching epoch;
the pending bind rejects it, including a stale terminal heartbeat.

The rejection response contains the new allocator ID/version. The CN records
that epoch through its existing allocator-state observer, fencing stale binds,
and echoes it on its next `KeepLockTableBind` request. Only a heartbeat with the
matching epoch and exact service ID clears the pending flag. Its status may be
`Enable`, `Waiting`, `UnLockSucc`, or `CanRestart`; accepting a draining status
does **not** return the CN to `Enable`. A terminal heartbeat containing active
transaction IDs is rejected. `Waiting` remains unsafe until the CN subsequently
reports terminal completion. `UnLockSucc`/`CanRestart` are terminal only after
the CN's local completion checks and a matching-epoch heartbeat. A retry of the
same `BeginDrain` is then idempotent; another attempt is not adopted.

The sequence for an already-draining CN is:

1. Old allocator A authorizes drain; CN enters `Waiting` and still serves
   admitted remote transactions.
2. A loses its volatile state. A proof from A cannot pass `QueryDrain` on B.
3. `BeginDrain` on B creates a pending, non-admitting bind. The first old-epoch
   heartbeat is rejected; CN observes B's epoch.
4. CN echoes B's epoch while remaining in `Waiting`; B accepts this attempt
   but `QueryDrain` remains unsafe.
5. Remote holders release. CN reaches `UnLockSucc`, sends a matching-epoch
   heartbeat, and B publishes `CanRestart`. Only then can `QueryDrain` be safe.

An unobserved or stopped CN cannot manufacture step 4. A late A heartbeat
cannot echo B's epoch. If B itself restarts before step 5, the process repeats
with a new epoch; no prior allocator proof is reused.

## Recovery after negative timeout retirement

A transient validation connection failure can remove the bind without proving
safe completion. A later `BeginDrain` for the exact live incarnation discards
that negative retirement metadata and creates the same pending, non-admitting
`Waiting` bind used after allocator loss. It does not clear inactive-service or
commit fences. A positive retirement proof remains closed and idempotent.

With the same allocator, an epoch echo does not prove post-timeout freshness.
An earlier `Enable` or `Waiting` heartbeat can confirm observation but cannot
reopen admission or authorize completion. An authentic terminal heartbeat from
the same incarnation represents irreversible local drain completion; active
transactions in a terminal report remain rejected. The allocator lock publishes
the replacement bind atomically, and old-generation timeout cleanup cannot
retire that replacement. No new state machine, worker, or retry loop is added.

## Compatibility and rollout

The two new drain RPC methods are gated at MORPC v105. The two optional epoch
fields extend the existing keepalive request without changing old field
numbers. An older allocator ignores them; an older CN cannot satisfy a new
allocator's pending re-handshake after state loss, so normal eviction remains
blocked rather than silently falling back to the UUID/boolean restart RPC.
This release uses a coordinated downtime upgrade: stop the CN/TN services,
deploy compatible CN/TN lock-service binaries, restart them, then enable the
matching Operator client. Mixed-version and rolling upgrades are outside the
acceptance scope. A successful deployment workflow is not a safe-eviction proof.

## Validation ownership and remaining gates

- Kernel unit/integration: exact identity, wire round trip, predecessor-version
  rejection, old-epoch rejection, `Waiting` recovery, terminal recovery, and a
  real two-CN remote holder that keeps `QueryDrain` unsafe until release.
- Operator: bind the returned service/attempt/allocator proof to the current
  Pod incarnation, preserve protection on timeout or mismatch, and recheck
  identity before deletion or upgrade.
- Cloud control plane and QA: prove normal CNClaim, no-CNClaim, and Idle paths
  do not bypass PreDelete; validate the coordinated downtime upgrade, controlled
  eviction, timeout behavior, and the running main container in an isolated
  environment.

The deterministic kernel test models allocator state loss inside one test
topology; it does not replace a real process restart, Kruise lifecycle test,
or unit-agent/node-release validation. Those remain separate release gates.
