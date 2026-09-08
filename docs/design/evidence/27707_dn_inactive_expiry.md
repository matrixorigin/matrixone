# DN inactive transaction-state expiry

Base: `c51bb4ed868219af720cb5c019fb103bc1e7bcc7` (main, 2026-09-08).
Branch: `fix/27707-dn-inactive-expiry`.

## Contract and scope

This is the DN `GetActiveTxn` cleanup follow-up to issue #27707. It does not
change the CN remote-bind keeper fix in #28343. A repeated failure for the same
disconnected incarnation must not restart its retention window. Expiring that
window must not remove a recovered incarnation's state, a newer transaction
state, an in-flight Commit, or an unexpired persistent unknown-Commit fence.

The default disconnected-service retention remains 24 hours. The change fixes
eventual reclamation; it does not promise immediate cessation of retries or
change the retry budget, wire format, or public configuration.

Ordinary focused bug fix; no feature/major-refactor design gate. Risk is R3:
shared admission state, asynchronous cleanup, and CN replacement. The changed
closure is allocator snapshot -> RPC observation -> generation-checked cleanup,
with `Valid`, `AddCannotCommit`, `FinishCommit`, and `Resume` as consumers.

## State and ownership review

- `LoadOrStore` preserves the first disconnect time until Resume or expiry.
- Snapshot captures ctl identity, transaction generation, recovery epoch, and
  inactive timestamp before RPCs. It is local to one sweep, not retained state.
- Cleanup rechecks identity/epoch and inactive timestamp. Marker deletion and
  tombstone cleanup occur under `ctlMu -> inactiveMu -> commitCtl.mu`.
- Resume now holds `ctlMu.RLock` while acquiring its ctl and advancing its
  recovery epoch, so it cannot update a ctl concurrently detached by cleanup.
- RPCs and retry timers remain outside these locks and retain existing context
  bounds. Cancellation returns without applying an incomplete sweep.
- `commitCtl.clean` retains active transactions, in-flight commits and states
  newer than the snapshot. Persistent fences retain their own deadlines.
- Expiry applies even to an unknown/non-connection probe error or a negative
  response whose backend reset failed. Marker-only services also expire.

| Audit | Owner / termination |
|---|---|
| Q1 cleanup ownership | Allocator removes the exact ctl only when empty; stale snapshots cannot remove a replacement. |
| Q2 waits | No network operation under cleanup locks; existing three-attempt RPC budget and context cancellation remain. |
| Q3 retained state | Stable disconnect timestamp makes retention reachable; transaction and persistent-Commit protections keep their independent terminal conditions. Sweep maps are temporary and proportional to service count. |

## Deterministic evidence

The existing allocator fixture is reused with a one-hour background timer;
tests invoke individual cleaner sweeps and inject elapsed timestamps rather
than sleeping for retention. RPC callbacks explicitly place Resume, fresh
disconnects, newer tombstones, or ctl replacement between snapshot and cleanup.

All five new tests fail when compiled with the unchanged base allocator through
a Go source overlay, and pass with the fix:

- `TestCleanCommitStatePersistentDisconnectExpires`
- `TestCleanCommitStateExpiryDoesNotRequireSuccessfulProbe`
- `TestCleanCommitStateExpiryPreservesFreshRecoveryEpoch`
- `TestCleanCommitStateExpiryPreservesNewStateAndCommitSafety`
- `TestCleanCommitStateExpiryPreservesReplacementCtl`

The focused `TestCleanCommitState*` selection passed (3.036s). Each new test also
passed under race, then individually repeated under race with counts
45/90/90/100/100 (derived from the measured per-test duration and 30s budget).

Owning package passed with `-short -count=1` (99.310s), and with
`-short -race -count=1` (127.898s), using the repository CGo wrapper. Native
artifacts were built by `make -j8 cgo` in this worktree. `make build` passed.

The non-short package run encountered `TestIssue3288`'s 10-second deadline.
The exact test fails with the same deadline/error at the unchanged base; its
short-mode fixture and all other owning-package cases pass. No assertion was
weakened or test skipped by this change.

Repository-configured golangci-lint passed for `./pkg/lockservice/...` with
0 issues (7.258s). The earlier whole-repository SCA run was interrupted during
golangci analysis and is not reported as passing. The preceding custom
repository analyzer completed. `git diff --check` passed.

## Real-service validation

The harness uses four separate MO processes: LogService, DN, and two CNs, with
a clean shared disk store. CN2 holds an uncommitted SQL update and is killed;
a replacement CN starts with a new UUID and a different lockservice endpoint.
An additional cannot-commit tombstone for the departed CN's actual incarnation
is injected through the real DN `CannotCommit` RPC and confirmed by
`CheckOrphan`. This explicitly establishes the state missed by the previous
keeper-only reproduction; it is fault-injected, not a claim that the SQL update
alone necessarily creates this tombstone.

Both base and fixed binaries use the identical validation-only Go overlay
changing default retention from 24 hours to 20 seconds. The DN keep-bind timeout
is 2 seconds in both configurations (cleaner interval 4 seconds). The shipped
source retains the 24-hour default. Data checks verify the killed transaction's
uncommitted value is invisible and the replacement can commit a new update.

Both runs completed successfully on 2026-09-08; all harness-owned processes
were stopped afterward:

| Observation | Unchanged base | Fix |
|---|---|---|
| DN tombstone confirmed through RPC | yes | yes |
| Retry count across final 16-second observation | 24 -> 36 | 18 -> 18 |
| Tombstone after retention | still present | absent |
| Killed transaction's uncommitted value | invisible | invisible |
| Replacement CN's new Commit | succeeds | succeeds |

The fixed DN cleaned the tombstone and removed the inactive marker at
18:45:48 CST. The baseline continued retrying at 18:47:26 CST with the exact
`lockservice service ... is absent from cluster inventory` error seen in the
nightly environment. The two observations use independent clean deployments,
not an in-place binary swap.

Local artifacts are retained under
`/home/mo/worktrees/mo27707-build.AVUWzM/`: the normal, baseline and fixed
binaries; `dn-expiry-fixed-8bmfvojo` and `dn-expiry-baseline-g2dmkchm` service
logs/data; and `live-evidence` containing the RPC driver, harness, configurations,
source overlays and run summaries. The overlays and shortened timeout are
validation-only and are not part of the source change.

This accelerated, fault-injected test does not replace a production-default
24-hour stability acceptance run and does not claim the nightly deployment has
already been repaired.

Final self-review: every changed hunk is covered by the allocator closure and
the tests above; no remaining in-scope blocker found. No SQL/parser, wire,
storage-format, tenant, or index-plugin contract is changed. No new persistent
worker/cache/retry mechanism is introduced. The whole-repository interrupted
SCA run and the baseline non-short timeout remain explicitly distinguished from
passing checks.
