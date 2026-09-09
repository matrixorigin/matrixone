# Departed-owner probe quiescence, transaction-state expiry and route recovery

Base: `c51bb4ed868219af720cb5c019fb103bc1e7bcc7` (main, 2026-09-08).
Branch: `fix/27707-dn-inactive-expiry`.

## Contract and scope

This follow-up covers DN `GetActiveTxn` cleanup and the CN stale-route recovery
gap discovered during acceptance. It preserves the CN remote-bind keeper safety
fences in #28343. A repeated failure for the same
disconnected incarnation must not restart its retention window. Expiring that
window must not remove a recovered incarnation's state, a newer transaction
state, an in-flight Commit, or an unexpired persistent unknown-Commit fence.

The default disconnected-service retention remains 24 hours. Once local raw CN
inventory has no lock endpoint for the owner UUID, the cleaner stops network
probes, backend resets and repeated missing-owner error logs. Each sweep checks
the local periodically refreshed inventory again, so reappearance is not
permanently suppressed. This is an unknown observation, not an authoritative
negative response: admission fencing and cannot-commit state remain protected.
Generic connection errors for owners still in inventory retain the existing
three-attempt recovery budget. No wire format or public configuration changes.

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
- Missing-owner detection uses raw membership, not public SQL admission, so a
  pending-admission CN is still reachable for recovery. It neither refreshes
  discovery nor resets a connection. An existing bounded-label recovery counter
  gains `result="owner-absent"`; no per-owner series or growing cache is added.
- The absent observation follows the same ctl/epoch validation as failed RPCs.
  Resume racing this observation cannot be overwritten by stale re-fencing.

| Audit | Owner / termination |
|---|---|
| Q1 cleanup ownership | Allocator removes the exact ctl only when empty; stale snapshots cannot remove a replacement. |
| Q2 waits | No network operation under cleanup locks; missing endpoints skip RPC/reset immediately. Other owners retain the existing three-attempt RPC budget and context cancellation. |
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

The supplemental missing-owner regressions cover 45 deterministic sweeps
(equivalent to 15 minutes of default cadence): zero sends, zero resets, zero
ERROR logs, one initial state-transition INFO log, with the cannot-commit
tombstone still present and admission rejected. Membership reappearance allows
the very next sweep to send; Resume permits new commits. Subsequent absence
still allows local expiry. A separate callback interleaving verifies stale
absence cannot re-fence a resumed owner. The pending-admission raw-inventory
RPC regression also checks this preflight path.

With the supplement, the focused selection passed (2.740s), the owning package
passed `-short -count=1` (95.297s) and `-short -race -count=1` (101.878s).
The normal, unmodified-default binary built successfully. Focused repository
golangci-lint passed again with 0 issues (10.837s); this is not a full SCA pass.
The two supplemental regressions and pending-admission case passed focused race
(1.176s). Individual JSON terminal test measurements were 0.07/0.04/0.01s,
selecting count=100 for each under the 30-second per-test budget. Each exact test
then passed its separate race-stress command (5.099/4.753/1.867s including harness
overhead). A grouped 100-repeat run also passed (10.395s), but is supplemental
rather than a substitute for the individual selections.

## Real-service validation

The harness uses four separate MO processes: LogService, DN, and two CNs, with
a clean shared disk store. CN2 holds an uncommitted SQL update and is killed;
a replacement CN starts with a new UUID and a different lockservice endpoint.
An additional cannot-commit tombstone for the departed CN's actual incarnation
is injected through the real DN `CannotCommit` RPC and confirmed by
`CheckOrphan`. This explicitly establishes the state missed by the previous
keeper-only reproduction; it is fault-injected, not a claim that the SQL update
alone necessarily creates this tombstone.

For the initial expiry comparison, both base and fixed binaries use the identical validation-only Go overlay
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

This accelerated, fault-injected expiry test does not prove a production-default
24-hour soak and does not claim the nightly deployment has already been repaired.

### Supplemental real-service quiescence check

A normal build of the supplemented fix, without validation source overlays,
with the default 24-hour DN retention and default
10-second DN keep-bind timeout (20-second cleaner cadence), was run against the
same fault-injected topology. In `dn-expiry-quiesce-fisgxbxj`, the departed owner's
error count remained 6 -> 6 for a 65-second window while the `owner-absent` counter
increased 1 -> 4. Backend-reset count also remained 6. The DN tombstone was still
present. Replacement CN updates to another row committed throughout the window,
and a read of the killed transaction's row returned the original committed value.
The supplemental binary SHA-256 is
`421e390ee46573295083d976852d6e518092239bf4c427fd6e550d05a4145147`.
It was built from local `7b7a974ad5` plus the supplement (not an upstream image);
allocator/rpc source Git blobs are respectively
`77438a626efb25449a7be49931120dec4146db01` and
`d56b964c8a85599002d9248fdfc569eb12348d76`.

This run did **not** complete the two-CN acceptance test: the next attempted
update of the killed transaction's original row exceeded the SQL client's
20-second timeout. The unchanged CN `remote-lock-timeout` defaults to 10 minutes,
and `unlockTimeoutRemoteTxn` also scans on that interval. The run ended before
that independent orphan-lock recovery window; it cannot demonstrate either
permanent lock leakage or successful eventual recovery. This is a distinct
acceptance obligation from stopping DN missing-owner probes. Earlier harness
attempts also exposed a localhost HTTP proxy setup error and a surfaced 20702
bind-change error; neither is counted as a passing run. A subsequent harness
permits only 20702 retries (at most three attempts, 0.5 seconds apart), with all
other SQL failures still fatal.

The follow-up run `dn-expiry-quiesce-q1k7e4og` kept the normal binary, default
24-hour DN retention and 20-second cleaner cadence, but explicitly configured
all CNs with a 10-second remote-lock lease (CN bind heartbeats remained 500ms).
At 19:17:57 CST, CN1 logged the original orphan transaction's safe unlock with
its allocator fence timestamp. The first replacement again produced 6 -> 6
errors and 1 -> 4 absent observations across 65 seconds, with the tombstone
retained. Rewriting the original row on CN1 then succeeded before CN1 was killed
for the second replacement.

**End-to-end blocker:** after CN1 replacement, CN2new's workload update failed
with `20702 lock table bind changed` on all three bounded attempts. The normal
lock operator itself reported exhausted backend-availability retry budgets;
background task writes also reported bind-change failures. The final old-CN1
DN GetActiveTxn error was at 19:19:43.373 CST, with none afterward before harness
teardown at approximately 19:21:49 CST. Thus the missing-owner probe suppression
worked, but continuous-write acceptance did not. This run is FAILED, not a
two-CN validation pass. No retry budget or assertion was widened to hide it.
At that time its attribution to this supplement versus pre-existing bind
recovery was not established. The 2026-09-09 baseline comparison below now proves
that the same SQL failure is reachable without either local fix.
Do not publish this branch as an end-to-end resolution until that blocker is
explained and the workload passes. All harness-owned processes were stopped.

### 2026-09-09 baseline attribution verification

Verification only: no production source, retry budget, or assertion was changed
in this turn. The existing race/package/SCA evidence is reused because the
production and test source inputs are unchanged. The mo-dev evidence and test
contracts require a matching baseline failure before attributing a failure as
pre-existing; the real SQL comparison supplies that missing proof, not an
end-to-end acceptance pass.

Reused the same four-process/two-row harness, DN tombstone injection and bounded
three-attempt SQL oracle. The comparison mode uses identical 90-second settling
periods after replacement readiness and 65-second write windows in both builds,
rather than requiring the new `owner-absent` metric in a baseline that lacks it.
Only diagnostic log counts are recorded instead of asserted in this mode; SQL,
committed-value and retained-tombstone assertions are unchanged. Candidate
ports were shifted from 42xxx to 43xxx to isolate concurrent deployments;
configs, UUID roles and workload are otherwise identical, with fresh data dirs.
The 10-second test CN remote-lock lease, 500ms bind heartbeat, default DN
20-second cleaner cadence and default 24-hour retention are explicit as before.

The clean baseline is `c51bb4ed868219af720cb5c019fb103bc1e7bcc7`, before both the
committed expiry fix and uncommitted probe-suppression supplement. It was built
with the same Makefile/native artifacts using source overlays restoring the
only changed production files, allocator and rpc, to that object. Overlay files
differ from the corresponding Git blobs only by one trailing blank line each;
there are no validation timeout or behavior changes. Baseline binary SHA-256:
`56745599dbcac3acd8afac7dc832eabe878f23810d288eb1cb84e8786c79d286`.
Candidate binary SHA-256 remains the previously recorded `421e390e...145147`.

| Build / artifact directory | Actual `mo_tables` lock owner | Result |
|---|---|---|
| Clean c51 baseline / `dn-expiry-compare-ngcbg92x` | original CN1 | First replacement (CN2) passed 33 commits in 65s; after CN1 replacement, all three writes failed with 20702; exit 1. |
| Current candidate / `dn-expiry-compare-kaj2fs5h` | original CN2 | After CN2 replacement, all three writes failed with 20702; exit 1. |
| Saved pre-supplement build / `dn-expiry-compare-l4uhadml` | original CN2 | Same first-replacement failure; exit 1. This historical binary is supplementary evidence, not the clean-baseline attribution anchor. |

DN `bind created` records identify each owner for table ID 2 (`catalog.MO_TABLES_ID`).
Both the clean baseline and candidate have exactly three failed workload SQL
records with the same `lock table bind changed` signature. Bootstrap placement
explains why one fails on the first replacement and another on the second:
the failure follows replacing the catalog lock owner, not a specific replacement
ordinal. This is an observed trigger, not yet a complete stale-bind root cause.
The clean baseline's first 65-second window also shows the original DN issue:
GetActiveTxn errors grew 15 -> 24 despite 33 successful commits. The candidate's
short failed window remained 6 -> 6; it does not replace the prior successful
65-second quiescence observation or count as a 15-minute acceptance run.

There is an additional environment-sensitive recovery limitation. Both runs
logged `lock retry stopped under memory pressure`. The local system reported
approximately 16 GiB available but less than 1 GiB free. The unchanged lock
retry guard uses `system.MemoryUsed()/MemoryTotal()` with a 90% critical cutoff;
on this non-PID-1 Linux process, `MemoryUsed` returns gosigar `Mem.Used`, defined
as Total - Free rather than Total - MemAvailable. Thus reclaimable page cache
can trigger the critical stop. Some subsequent `budget exhausted` records even
precede their printed deadline because that same error return is logged as a
budget stop. This is verified source/log evidence for premature retry stopping,
not proof that it solely explains the September 8 failure or that fixing the
memory calculation alone resolves stale binding. No global cache drop or memory
policy change was made to obtain a pass.

Conclusion: this class of 20702 failure is not newly introduced by either local
DN fix. It is still reachable with the candidate, so #27707's end-to-end closure
gate remains FAILED. Next diagnostic scope is catalog lock-bind recovery after
owner replacement, separating the memory-pressure retry cutoff from persistent
stale-binding behavior. No GitHub state changed; all test-owned MO processes
were stopped. Harnesses, source overlays, binaries and per-process logs remain
under `/home/mo/worktrees/mo27707-build.AVUWzM/`; comparison summaries are retained
under `verification-20260909` there.

## Follow-up diagnosis

### Follow-up diagnostic contract (2026-09-09)

The follow-up is diagnosis, not a production implementation or GitHub update.
Two explicit hypotheses are separated: (A) cache-inclusive memory pressure
prematurely stops an otherwise recoverable retry; (B) a republished stale route
prevents fresh admission from ever consulting the converged allocator. Existing
production edits remain unchanged. All additional source changes are diagnostic
Go overlays under `pressure-isolation.uhUSOj`, not repository changes.

A deterministic extension of the existing
`TestKeepRemoteLockMissingOwnerFencesActiveTxn/route_present` fixture proves B:
after keeper invalidation, simulate one allocator reply still naming the old
bind, then advance the fake allocator to a healthy new generation. Three fresh
admissions all fail against the cached old bind, while the GetBind RPC count
does not increase. A keeper sweep also issues no refresh because the retained
ref is invalidated. Removing only that exact cached route makes the next GetBind
and fresh admission succeed, with the old invalidated transaction ref still
present. No sleeps or probabilistic scheduling are involved. The diagnostic
passed under race (1.035s); it asserts the observed broken behavior plus the
nearest recovery control, not a passing production regression. The first
diagnostic attempt omitted the fixture's allocation map; that setup panic was
corrected before obtaining this evidence.

Issue-ready finding draft (not posted): **A republished invalidated remote bind
can block fresh transactions after allocator convergence.** Reproduction state:
old transaction retains remote ref -> keeper fences/ref-invalidates and detaches
old route -> fresh GetBind before DN timeout republishes that exact old route ->
DN later provides a new bind -> fresh requests hit cache and fail admission
before reaching the remote RPC/error-refresh path. Keeper excludes the old ref,
so it cannot refresh either. Expected: reject unsafe old-generation admission
while permitting fresh route discovery. Actual: repeated ErrLockTableBindChanged
until another independent path removes the route or the old ref finally leaves.
This is a forward-progress failure, not a claim of an unbounded mutex wait.

| Audit layer | Owner / bound / diagnostic result |
|---|---|
| Q1 route and transaction ref | Route cache and ref-counted transaction protection have different owners. The recovery control detaches/closes only the matching route; old transaction cleanup still owns its ref. Remote table/proxy close is a local log-only close, not an RPC wait. |
| Q2 admission and discovery | Individual calls remain bounded, but cached rejection bypasses allocator discovery; keeper exclusion removes the other refresh path. The control restores discovery without extending the 10-second SQL retry budget. |
| Q3 retained state | No new cache, worker, queue or persistent diagnostic state. The old ref keeps its existing cleanup owner; removed route slices are temporary. |

Experimental recovery direction: after a remote admission rejection, release the
transaction/read locks, then under bind-change exclusion recheck the exact ref
is still invalidated and detach only the matching cached route. Preserve a newer
route or a ref that has already been released/reacquired validly; do not revive
old transactions or resume heartbeats for invalidated refs. This is currently a
diagnostic overlay only; completed real-process evidence follows below, while
formal implementation and production review remain outstanding.

The two real-process interventions retain the same SQL/data/fence assertions:
one forces only the lock retry pressure tier to normal (10-second deadline still
applies); the other retains the original pressure policy but adds the exact-route
detachment above. These are distinct diagnostic builds, neither a shipping
binary nor a reason to mark the current branch accepted. An initial pressure
run failed at startup because port 42322 collided with a client ephemeral port;
it is not counted as behavioral evidence. The reruns use isolated 22xxx/23xxx
ports, below this host's 32768-60999 ephemeral range, and fail immediately if a
test-owned service exits during readiness. No unrelated process was killed.

#### Intervention results

Both interventions have now completed, with terminal status and per-CN logs:

| Intervention | First write window | After replacing catalog lock owner | Terminal result |
|---|---|---|---|
| Pressure forced normal only / `dn-expiry-compare-sch0rzto` | 33 commits, DN errors 6 -> 6 | Three writes each exhaust the unchanged 10-second budget; zero successful window commits, DN errors [6,6] -> [6,6] | FAILED, exit 1, 20702 |
| Exact stale-route detachment only; original pressure policy / `dn-expiry-compare-h1b9hs7i` | 33 commits, DN errors 6 -> 6 | One transient 20702 followed by 30 committed writes; DN errors [6,6] -> [6,6] | PASSED, exit 0; final data/new-commit checks passed |

Each successful write window spans 65 seconds. Both old owners' synthetic
cannot-commit tombstones remain confirmed through the real DN CheckOrphan RPC.
After the second replacement, an additional cross-CN check rewrites the original
locked row from its original committed value 10 to 50, verifies 50 from the other
CN, restores 10, and verifies again. It passed without a retry. The main harness
then passed its final committed-value checks and printed `PROCESSES_STOPPED`.
All test-owned processes have exited; no final service-survival or data assertion
was removed. This is not a field 15-minute soak, a full default CN-lease test,
or a production readiness claim for the diagnostic helper.

Diagnostic binary SHA-256 identities:

- Pressure-only: `bad9c53e7f054456adeffd314db48e9c17cd5af8a4d7b9650de4f54398d664cf`.
- Route-only: `ac990aa4198a0850d904ba775edcabc2215275c879aedb5f7e619c79dc098a6d`.

The route-only binary contains the existing DN patch plus the diagnostic
`service.go` overlay; it does **not** contain the pressure override. The ordinary
worktree binary remains byte-identical to `421e390e...145147`, and production
source changes from before this diagnostic turn are unchanged.

Attribution is now narrower and stronger: cache invalidation/admission recovery
is a demonstrated cause of the persistent failure, while removing the pressure
early exit alone is insufficient. `git blame` identifies the rejecting
`acquireRemoteTxnBindRef` guard as part of `e201dc56d2` / #28343. Thus the earlier
clean-baseline comparison excludes the two **local DN** changes, not the earlier
#27707 PR series: that clean baseline already contains #28343. The missing
recovery arc belongs to the interaction between its valid safety fence and
republishing a stale route. Do not describe it as an unrelated old bug or remove
the fence to get progress.

Formal implementation should preserve the same constraints demonstrated by
the control: old consumers remain fenced, invalidated refs do not resume keeper
traffic, cleanup still owns their release, and only the exact stale route is
discarded so fresh requests can fetch a new bind. Required follow-up before
delivery: implement the narrow common owner path; add regression and races for
replacement-route preservation, ref release/reacquisition and cancellation;
run owning-package/race/SCA review and repeat real-service acceptance. No formal
production route fix, commit, PR or GitHub issue mutation was made in this turn.
Artifacts and summaries are preserved under `diagnosis-20260909` in the same
disk-backed artifact directory.

## Formal route recovery (2026-09-09)

Range: explicit investigation base/merge-base `c51bb4ed868219af720cb5c019fb103bc1e7bcc7`,
HEAD `7b7a974ad5ebf9ce02bedaccfbd1e36dd97e340b`, plus the unstaged DN supplement
and route fix. Remote main was freshly checked at `af965c3e4d86ec218de05b585dbafbedb10aa1ae`;
these results apply to the investigation branch, not that newer main. No rebase,
push, PR update or issue mutation is implied.

| Closure / risk | Owner and consumers | Required evidence |
|---|---|---|
| DN cleanup / R3 | allocator snapshots, raw membership preflight, Resume/Commit/expiry | existing deterministic expiry/absence controls, package/race, real DN RPC and log counters |
| Rejected remote route / R3 | service cache plus exact transaction refs; Lock, RemoteLock and ForwardLock admission | pre-fix negative control, fresh admission, exact-generation preservation, ref reuse, cancellation, cleanup exclusion; package/race and two-CN process replacement |
| Evidence / R0 | this record | delivery diff and explicit scope/validation limits |

Ordinary focused bug fix: no feature/major-refactor design gate. API/error codes,
wire/disk/catalog formats, authorization and index/planner behavior do not change.
Build/native and operational lenses apply; migration/tenant/algorithm-specific
lenses do not introduce new contracts. No retry budget or memory-pressure policy
is changed.

All three `acquireTxnBindRef` callers release transaction/read locks before the
common recovery helper. Under `bindChangeMu -> service.mu.RLock -> table holder`,
the helper rechecks and pins the invalidated exact ref through route detachment.
A valid ref reacquired for the same key is preserved. Lookup uses group/table
directly (constant-time, no all-table scan); exact-key comparison also preserves
different allocator/routing metadata that `Changed` deliberately ignores.
The holder's lookup-version changes only on actual removal. Close runs after all
mutation locks are released. No RPC, new worker, timer, retry, cache or metric
series is introduced, and successful lock admission has no extra work.

| Audit | Terminal ownership / control |
|---|---|
| Q1 route vs ref | Exactly one holder detaches a route and owns its close; transaction cleanup alone releases refs. Repeated rejection cannot double-close. Old transactions remain fenced and ineligible for keeper heartbeats. |
| Q2 rejection | No lock upgrade while holding txn; ref mutation and publication cannot cross detachment. All new critical sections contain local map operations only. Remote/proxy close is local and occurs outside locks; normal caller-bounded discovery owns the next fetch. |
| Q3 growth | No new retained state. Detachment removes a cache entry; rejected admission acquires no ref/intent. Existing transaction cleanup and DN retention bounds remain intact. |

Tests reuse the lightweight missing-owner keeper fixture and fake allocator,
without embedded services, sleeps or timing retries. They drive real `Lock`,
ForwardLock and RemoteLock-on-Shared-proxy handler rejection, then converged allocator discovery/admission while
the old transaction remains alive. Canceled admission creates no transaction or
cache mutation. Controls cover a new version, allocator incarnation, routing
metadata, another group, local owner, absent route/ref, last release and same-key
reacquisition. A test table verifies reference/publication exclusion during
detachment and exactly-once close outside locks. Each new/directly affected test
measures below JSON timing resolution under race, selecting 100 individual
repetitions from the 30-second adaptive budget.

Public-path proof uses the existing test-owned LogService + DN + two-CN harness.
Existing pessimistic transaction SQL cases cover lock waits but cannot kill and
replace two CN processes or seed/check DN fences, so no SQL-only BVT duplicate is
added. The two-row destructive topology test uses fresh data, real SQL and DN RPC;
it also asserts that DN log counts stay flat and rewrites the original locked
row after the second replacement, observing committed values from the other CN.
The focused six-test selection passed (0.062s). After adding the Shared-proxy
consumer, the final keeper selection passed under race (1.032s); the three route
tests passed separate 100-repeat race commands (1.300s / 1.196s / 1.128s). DN sentinel
construction was corrected from `errors.New` to `moerr.NewInternalErrorNoCtx`
after the repository-specific `make err-check` caught it; package golangci does
not cover that rule. The local singleton is still recognized only by identity
through `errors.Is`, never serialized as an authoritative GetActiveTxn reply.
The two affected DN tests were remeasured at 0.03s each, selecting 100 repeats.

Static checks on the final sources: `make err-check` passed;
repository-configured golangci for `./pkg/lockservice/...` passed with 0 issues
(6.200s). `go vet -vettool=molint` on the owning package exited 0 but emitted two
diagnostics in unchanged `test_helper.go` / `txn_test.go`. The identical command
at a clean detached `c51bb4...` worktree emitted byte-identical diagnostics after
normalizing the repository path. They are baseline diagnostics, not a claim of
zero molint findings. The disposable baseline worktree was removed; both logs
remain. Whole-repository SCA remains unverified.

Negative control: disabling only the new `Lock` rejection hook fails the fresh
admission regression at the stale-cache assertion (exit 1, 0.010s). The earlier
clean baseline real-process failure remains the independent external oracle.
Source overlay and log: `formal-validation.z6VSsC/before-route.json` and
`before-route.log` under the disk-backed artifact root.

The first formal live run (`dn-expiry-compare-71b3lnh4`) was intentionally
interrupted after the SCA correction so the acceptance binary would match the
final sources. It exited 130 and printed `PROCESSES_STOPPED`; it is not passing
evidence. The rebuilt, overlay-free binary has SHA-256
`e8bdcf2f3a400c0f914585b5cef9d26517154092639af61880e4b2075f271e8b`.

Final live acceptance: `dn-expiry-compare-q_w5u3km`, exit 0. The original CN1
owned `mo_tables` (TN table-ID-2 allocation log), so the second replacement
exercised the previously failing catalog-owner transition.

| Final formal binary observation | First CN replacement | Second CN replacement |
|---|---|---|
| 65-second write window | 33 commits | 33 commits; one transient 20702, then progress |
| DN errors for departed owners | [6] -> [6] | [6,6] -> [6,6] |
| Old-owner cannot-commit fences via DN CheckOrphan | retained | both retained |
| Killed transaction's uncommitted value | invisible | invisible |
| Original-row lock recovery | update succeeds before second replacement | 10 -> 50 -> 10 commits; values checked from other CN |

Final data assertion passed, no CN fatal/panic signature was found, and every
harness-owned process exited (`PROCESSES_STOPPED`). During the second window,
the live DN `owner-absent` metric increased 14 -> 18, independently confirming
that the cleaner still ran while missing-owner errors stopped. This binary has
no source overlay, shortened DN retention or memory-pressure override. DN
retention remains 24 hours and cleaner cadence 20 seconds; CN remote-lock lease
is explicitly 10 seconds for this topology test, not its 10-minute default.
The 90-second membership settling periods and two 65-second windows are not a
15-minute field soak or proof of uninterrupted zero-error rollout.

Final normal owning package passed `-short -count=1 -timeout=240s` (86.652s), and
`-short -race -count=1 -timeout=240s` passed (92.748s), both with exit 0.
All commands use the CGo
wrapper and disk-backed GOTMPDIR. Earlier expiry/Commit/Resume tests and their
baseline counterexamples remain valid; final package checks cover interaction
with route recovery. The documented non-short baseline timeout is unchanged.

Final evidence logs, harness/configs and negative-control overlay are archived
under `/home/mo/worktrees/mo27707-build.AVUWzM/formal-validation.z6VSsC/`;
the final process log/data directory is `dn-expiry-compare-q_w5u3km` in the same
artifact root. Production diff is limited to allocator/rpc/service admission
and exact route removal. No changes to lock retry pressure, wire, storage,
configuration defaults or generated files are included. No GitHub writes or
additional commits were made.

`mo-self-review` result: PASS for this local change/acceptance scope, with no
unresolved correctness or lifecycle finding in the mapped closures. The review
added the ForwardLock/Shared-proxy consumers, pinned ref lifetime through exact
detachment, and caught/fixed the repository error-construction rule. No behavior
assertion was weakened. This is not full-repository SCA certification or approval
to close the original issue; field-image acceptance below is still outstanding.

## Original issue closure gate

Keep #27707 open until the affected stability deployment runs an immutable image
verified to contain the complete fixes, and the following acceptance evidence is
attached. Local tests alone, or merely reducing log level, do not close it.

1. Run continuous read/write transactions with two CNs, including remote locks;
   replace both CNs sequentially with new incarnations. The rollout finishes,
   neither CN fatally exits/CrashLoops, and SQL keeps making progress without a
   sustained lockservice-unavailable/retry loop or stuck transactions.
2. Record when DN raw membership stops containing each departed owner. By its
   next completed cleaner sweep (normally a 20-second cadence, plus any bounded
   work already in progress), that owner's GetActiveTxn/reset/missing-owner
   error chain stops. For at least 15 minutes after replacement readiness and
   membership convergence, those per-owner log counters remain flat. Local
   membership checks and the bounded `owner-absent` counter are expected; roughly
   135 repeated errors per owner per 15 minutes is a failure, not acceptance.
3. Verify the previous CN remote-bind keeper fixes remain effective: no
   persistent keeper retry against departed incarnations and no recurrence of
   the original fatal failure. Committed values survive; killed transactions'
   uncommitted values remain invisible; replacement CNs commit successfully.
   Explicitly write the original locked rows again and verify recovery within
   the configured remote-lock lease/scan and RPC bounds. Writing unrelated rows
   alone does not establish orphan-lock recovery. Record those effective bounds
   and separately assess whether their operational delay is acceptable; a
   20-second client deadline is not a proof of permanent leakage under a
   10-minute lease.
4. The regression evidence must retain safety on temporary membership absence,
   recovery/Resume, concurrent new transaction state and unknown Commit fences.
   Preserve the default 24-hour retention. Deterministic boundary tests and the
   accelerated real-service expiry comparison establish expiry behavior; do
   not describe them as an actual 24-hour soak.

There is no claim yet that the affected nightly environment has passed this
gate. Before membership converges, finite connection-error retries can still
occur; this fix does not promise zero logs from the instant a CN is stopped.

The historical diagnostic failures above are retained rather than overwritten.
Formal delivery review and issue closure are separate decisions: a locally
passing fixed binary does not substitute for the field acceptance gate. The
whole-repository interrupted SCA run and baseline non-short timeout must remain
distinguished from passing checks.

## Delivery progression (2026-09-09)

The user requested continuation until the issue is closable. The latest issue
comment (`5595043017`, 02:52:47 UTC) describes a Chaos image preceding #28343;
it explicitly does not validate that keeper fix, let alone this unpublished
follow-up. Both #28115 and #28343 are merged; no existing PR carries this branch.

Fresh main was resolved to `f0c31cd4b8` for delivery. Lockservice, native sources,
Makefile, lint configuration and PR template are unchanged from the explicit
investigation base; main's dependency files and unrelated consumers have moved.
Local evidence remains scoped to this branch; PR merge CI must cover integration
with current main. No automatic rebase or unvalidated main-side claim is made.

Read-only IDC verification found all seven Pods Ready in each of
`mo-stb-main-845fff8-r33971015458-a2` and
`mo-chaos-main-061a343-202609060950`, using image digest
`sha256:5625b989db79bed057fd8f301f1bd82bd27fbd543f333148bae3900756a71ce1`.
This is inventory evidence, not fixed-image acceptance. Existing restart counts
are cumulative and are not attributed to this bug. No cluster mutation or fault
injection was performed. The investigation skill limits cluster access to
read-only operations; replacing the shared field image needs a separately
authorized deployment/test action.

The follow-up PR body uses the current repository template, references issue
#27707 without auto-closing it, and carries the explicit field acceptance gate.
Full-repository `make static-check-analysis GOLANGCI_LINT_CONCURRENCY=2` is being
run with bounded local Go memory/concurrency; terminal results and the PR/CI
identity will be recorded when available.
