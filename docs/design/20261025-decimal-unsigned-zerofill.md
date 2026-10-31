# DECIMAL UNSIGNED and ZEROFILL attributes

Catalog-upgrade routing follows [the current 4.0.7 design](20261028-catalog-upgrade-407.md); draft version-chain references below do not define additional supported upgrade sources or handlers.

Issue: [#30940](https://github.com/matrixorigin/matrixone/issues/30940)
(consolidated tracker, includes the independent report
[#30941](https://github.com/matrixorigin/matrixone/issues/30941)).

Revision: v36, 2026-10-29. Implementation PR: to be linked when opened. v1-v9
were each reviewed with REQUEST_CHANGES (four P2 findings per round in v1-v3,
two per round in v4-v8, one in v9); v10 passed design review (sha256
f9e881833b0940123ba82d50cc259f7df6d64bd31503693bb732ff7e9906fdc1) and its
implementation review returned one P2 finding (round-11 finding 1): the
steady-state COLUMNS reconciliation phase shipped with the implementation
exceeds v10's versioned-upgrade-entry-only compatibility machinery and its
"no new background work" promise. v11 (sha256
b2d539a2976f8d837de51e752ca0d483ab14772e1eb36f87f6c1650fc670092e) brought
that phase into the design explicitly and was itself reviewed with
REQUEST_CHANGES (round-12): finding 1 — the per-CN maintenance loops are
independent processes, so the "single repair writer" coordination promise
is unproven (two CNs can both observe the predecessor definition and enter
replacement); finding 2 — with `AutomaticUpgrade=false` no repair is
reachable for an already-final-version tenant (no maintenance worker, and
login-time `MaybeUpgradeTenant` returns immediately at the equal-version
branch); finding 3 — a persistent per-account repair error pins the cursor
page, starves later-page accounts, and aborts the independent
orphan-privileges phase. v12 (sha256
67bdcbbbb801bd1390766009048afd229e29d075488b44ce1c0eee459c0f7ffc)
redesigned the reconciliation lifecycle on those three axes and was
reviewed with REQUEST_CHANGES (round-13): finding 1 — the login-time
repair ignores the tenant completion cache
(`service_upgrade_tenant.go:45-49` returns for cached tenants and
:146-150 permanently caches every successful check), so a below-floor
login defers repair, caches completion, and never repairs after the
floor is published; finding 2 — the repair attempts carry no finite
deadline against a 24-hour owning-pass context while the recreate DDL
waits on the exclusive catalog-name lock (up to the 120-second default
lock wait), so one blocked account delays every later account and all
subsequent maintenance phases. v13 resolves both: repair completion is
redefined separately from version-check completion with a cache
admission/invalidation rule, and the pass gains finite per-attempt and
per-phase time budgets with timeout rollback/retry — under the same
Rollout subsection; behavior-changing consequences for the shipped
reconciler are design changes recorded here and land only after this
revision passes design review. Every other v10-v12 decision is
unchanged. v13 (sha256
caa96493d337c02eecb0b22a36866d368177dde601a0fb9e4775b1df08439f70) was
reviewed with REQUEST_CHANGES (round-14): finding 1 — an account
attempt's rollback runs synchronously, but ordinary unlock deliberately
retries its remote lock cleanup under the lockservice's own service
context, which the attempt context cannot cancel — so a timed-out
repair with stalled remote cleanup can hold the sequential pass beyond
both budgets; finding 2 — the pass drains the retry set before fresh
cursor work, so a handful of persistent 10-second attempt timeouts
exhausts every 60-second phase budget and a healthy account beyond the
cursor checkpoint is starved indefinitely. v14 resolves both: each
attempt runs supervised in a runner the pass awaits with a bounded
cleanup window, a pending-closure ledger keeps outstanding ownership at
one unfinished closure per account while the engine's service-context
retry preserves eventual lock release, and strict retry/cursor
alternation bounds the retry share of every pass so fresh cursor work
advances on every pass. The changes are again confined to the Rollout
subsection and land only after this revision passes design review.
Every other v10-v13 decision is unchanged. v14 (sha256
8c48aac0baca93912a80a730ecc8f91146bb203fddb3b07269ec2b0f0ee2f683) was
reviewed with REQUEST_CHANGES (round-15, one finding): its retry
ledger combined ordinary failures and pending closures under one
newest-kept eviction rule, so a page full of pending markers left the
next admission undefined — exceeding the bounded-outstanding-ownership
promise, or evicting a marker whose runner stays alive and
unreclaimable (ordinary unlock retries under the lockservice's service
context), after which cursor wrap could re-attempt the account. v15
resolves it with class-split ledger admission: reservations precede
spawned runners, ordinary retry entries alone are evictable, pending
markers are non-evictable until harvest, and a full non-evictable page
denies admission as logged backpressure with the cursor checkpoint
held — the fresh-progress guarantee is refined accordingly (ordinary
backlogs can never starve fresh work; only full non-evictable
saturation pauses fresh admissions, resuming on the first freed slot),
and validation gains a scoped-capacity saturation scenario. The
changes are again confined to the Rollout subsection and land only
after this revision passes design review. Every other v10-v14
decision is unchanged. v15 (sha256
103c4f182a609f03d98d6cffeb787176eec024c60a6f7aa9bfee43ba94b6e003) was
reviewed with REQUEST_CHANGES (round-16, one finding): it converted
every completed pending runner into an evictable retry entry, but a
runner can complete while cleanup is unfinished — ordinary unlock
returns deterministic cleanup errors while the closing transaction
stays registered and fenced, so eviction or a fresh repair txn could
open while the previous ownership persists. v16 separates runner
completion from cleanup completion: a nil rollback alone marks
cleanup complete; a cleanup error converts the entry to a
non-evictable CLEANUP-UNRESOLVED marker; completion ownership
transfers to the engine's verified orphan-unlock owners; and
re-attempt ownership is proven by a bounded lock probe at the
account's serialization point, never inferred from runner completion
or elapsed time. The changes are again confined to the Rollout
subsection and land only after this revision passes design review.
Every other v10-v15 decision is unchanged. v16 (sha256
2192cc1a2c4c7b1ac82941c058d1914460baa47d7be57a1ffa739a46253f1602) was
reviewed with REQUEST_CHANGES (round-17, three findings): finding 1 —
v16 split cleanup outcomes only through the rollback path, but a
successful commit deliberately reports success even when its deferred
lock release failed (the unlock error is recorded only in the txn
operator's private terminal outcome), so "on success the entry is
removed" discarded ownership tracking while the account's locks could
still be held by a fenced closing transaction; finding 2 — the named
engine orphan owners are unreachable for the reconciler's local-origin
transactions while the originating CN is healthy (the orphan scan
selects only remote-service registrations, the waiter paths treat
same-service transactions as valid, and the local-txn timeout check
runs only outside normal service status), and the name-lock probe
could not prove complete cleanup because unlock finalizes tables
individually with bind intents retained until the whole transaction
closes; finding 3 — a failed probe itself records an indeterminate
cleanup witness whose rollback can outlast the probe budget or retain
another closing transaction, and repeated probes could escape the
one-closure-per-account and page-capacity bounds. v17 resolves all
three by retiring the probe gate and the engine-owner transfer in
favor of a maintenance-owned closure discharge: transaction outcome
and cleanup outcome are tracked separately on BOTH the commit and the
rollback paths, a committed attempt is kept as a cleanup-unverified
CLOSURE-PENDING entry instead of vanishing, and each non-evictable
entry's account slot runs a bounded supervised re-drive of the
engine's own deferred `Unlock` — reachable in-process while the CN is
healthy — whose nil return is the authoritative, total closure proof
(the registry entry is deleted only after every table acknowledges).
The changes are again confined to the Rollout subsection and land
only after this revision passes design review. Every other v10-v16
decision is unchanged. v17 (sha256
1f3eecbd35f6c08ba8a6184d79f1823171266d89037419ef2b50eb4a72378a23)
was reviewed with REQUEST_CHANGES (round-18, two findings):
finding 1 — v17 still keyed the transaction outcome to
`ExecTxn`'s return, but a commit error returns WITHOUT rollback
and the committed-log visibility wait (enabled in the
maintenance options) fails only AFTER a durable commit, so a
committed repair could miss CLOSURE-PENDING or be misclassified,
while the unconditional error join on the failure path masks a
nil rollback so completely that no returned-error comparison can
observe rollback completion; finding 2 — the unknown-commit
corner was folded into the ordinary Unlock re-drive rule, but
the engine forbids releasing that transaction's locks before an
allocator fence proves it cannot still be committing, ordinary
Unlock performs no such check, the commit timestamp is assigned
only from a received response, the resolver admission's deadline
and sequence stay operator-private, and no interface seam
reports resolution completion. v18 replaces the terminal-outcome
protocol: the supervised runner ends every attempt with a
terminal-outcome RECORD (body result, `ExecTxn` return,
post-return operator snapshot, unknown-commit code test) and
classifies it COMMITTED / UNKNOWN / FAILED from the snapshot
status and error code alone; COMMITTED converts to
CLOSURE-PENDING even when the later visibility wait failed,
every FAILED conversion holds cleanup-unverified ownership
discharged by exactly one bounded confirmation re-drive (the
rollback-nil fast path is retired), and UNKNOWN converts to a
new non-evictable RESOLUTION-PENDING class whose alternation
slot runs one bounded supervised READ-ONLY content recheck per
pass — ordinary Unlock of the unknown generation is forbidden
unconditionally because fence eligibility is unobservable
outside the engine resolver — until observed content discharges
the account as converged (canonical definition) or re-repairable
(stale definition). The changes are again confined to the
Rollout subsection and land only after this revision passes
design review. Every other v10-v17 decision is unchanged.
v18 (sha256
fbc00f7fcf8732041ed356de9afa2c91d3f80b17d54da9d040a4e3e17ffe9bd8)
was reviewed with REQUEST_CHANGES (round-19, two findings):
finding 1 — v18 discharged RESOLUTION-PENDING entries on
observed view content, but the content recheck is an ordinary
read-only SELECT
(`pkg/bootstrap/versions/upgrade_strategy.go:401-407`) whose
min-committed-ts option buys snapshot freshness, never
transaction-resolution completion
(`pkg/util/executor/options.go:134-145`), and the engine
resolver explicitly skips cleanup while fencing fails or the
transaction is still committing
(`pkg/lockservice/unknown_commit.go:330-359`) — so a clean
stale read could convert the entry and readmit repairs while
the unknown transaction's ownership and fence-less locks
persisted, contradicting the recorded zero-re-attempt and
constant-occupancy scenarios; finding 2 — v18 assumed every
FAILED attempt's deferred cleanup had finished before its
confirmation re-drive, but a fatal SQL error schedules an
ASYNCHRONOUS deferred rollback before `ExecTxn` returns
(`pkg/sql/compile/sql_executor.go:385-393` drives
`operator.go:2116-2143`; the runner's own `Rollback` is
sealed out at `operator.go:2367-2380`; the workspace rollback
runs BEFORE the operator mutex at `operator.go:1216-1224`,
and `operator_test.go:2874-2915` demonstrates the
asynchronous boundary), so the metadata snapshot can stay
Active while the deferred owner still runs — an unobserved
nil re-drive then proves lock cleanup only, never workspace
closure. v19 resolves both: content observation is retired
as a discharge signal — the RESOLUTION-PENDING slot instead
runs ONE supervised closure-establishing finalizing attempt
per pass (a full repair-shaped transaction whose recreate is
authorized on BOTH content outcomes), because its commit must
acquire the account's exclusive catalog footprint, which a
conflicting predecessor must have released — the completion
condition independent of content equality — and successive
unknown generations are chain-accounted ({latest unknown id,
unknown count}, at most one ACTIONABLE generation per
account); and the FAILED confirmation re-drive gains a
snapshot-quiescence gate (an in-memory resample of the
retained operator handle: Active means the deferred owner
still runs; Aborted — published only under the mutex after
the workspace rollback — discharges the gate). The changes
are again confined to the Rollout subsection and land only
after this revision passes design review. Every other
v10-v18 decision is unchanged. v19 (sha256
3a21a284791bcded52e58789b1b3cbb57272e8b022243f8f78bc79d101abb993)
was reviewed with REQUEST_CHANGES (round-20, one finding):
v19's gate resamples the operator snapshot in the pass
BEFORE spawning the supervised re-drive, but that read
acquires the operator's read lock (`Txn()` ->
`getTxnMeta(false)` -> `tc.mu.RLock`,
`pkg/txn/client/operator.go:893-895`, `:1716-1721`), and
the deferred rollback owner holds the write lock ACROSS
its `tc.unlock(ctx)` (`rollbackOwned` registers the unlock
defer under the mutex chain, `operator.go:1224-1251`,
executed LIFO before `mu.Unlock`), which ordinary `Unlock`
runs under the service context with service-shutdown-only
cancellation (`pkg/lockservice/service.go:768-778`) — so
once the workspace-rollback latch releases and the owner
moves into a stuck remote unlock, a pass-synchronous
resample blocks the pass beyond both budgets, freezing
fresh-account progress and every later phase, and
`runningSQLWaitTimeout` cannot bound that leg. v20
resolves it by moving the sample INSIDE the supervised
closure action: one runner per entry runs
sample-then-maybe-redrive under the v14 attempt and
cleanup windows, with explicit ACTIVE / TERMINAL /
over-window outcomes (over-window converts to PENDING
with runner kind = closure re-drive, harvest-only, never
a duplicate observer or re-drive), so a blocked mutex can
pin at most one runner while the pass proceeds. The
changes are again confined to the Rollout subsection and
land only after this revision passes design review.
Every other v10-v19 decision is unchanged. v20 (sha256
156de487684a9274436857759ef5c19ba20f81386483279b982c8eccc8692769)
was reviewed with REQUEST_CHANGES (round-21, two findings):
finding 1 — v20 retained unfinished runners until recovery or
service close but specified no shutdown admission seal, ownership
transfer, or completion join: bootstrap `Close` stops only its
stopper (`pkg/bootstrap/service.go:447-449`), CN teardown closes
bootstrap BEFORE the engine and before the transaction client,
sender, and lockservice (`pkg/cnservice/server.go:577` in the
drain chain, `:607-612`, `stopRPCs` at `:830-845`), and a commit
blocked in ordinary unlock still owes workspace finalization after
the unlock returns (`pkg/txn/client/operator.go:1473-1490`,
`:1515-1525`) — a runner retained past `Close` therefore leaves
transaction and workspace work alive across dependency teardown,
while simply joining it at `Close` waits for the lockservice
cancellation that the same teardown sequence performs only later.
Finding 2 — the pass evaluates its gate and loads the account
page BEFORE any supervised slot with only the 24-hour outer bound
and a 60-second budget checked between account slots, but the
shared loader executes SQL
(`pkg/bootstrap/service_information_schema_views_maintenance.go:
177-188`), the executor passes its caller context into
`txnClient.New` (`pkg/sql/compile/sql_executor.go:284-295`), and
the snapshot acquisition can wait for logtail advancement until
that context ends or the client closes
(`pkg/txn/client/client.go:717-739`;
`pkg/txn/client/timestamp_waiter.go:293-300`) — a stalled initial
snapshot or page read bypasses both account budgets and delays
later phases beyond the claimed phase bound. v21 resolves both:
a shutdown protocol seals admission at service-context
cancellation, keeps dependency lifetime on the verified teardown
ordering, assigns completion ownership without any join at
`Close`, and records the blocked-cleanup shutdown disposition;
and the gate evaluation plus page load become ONE supervised
pre-attempt stage under bounded stage and cleanup windows with
cross-tick duplicate suppression, bounded outstanding work, and
an unchanged cursor. The changes are again confined to the
Rollout subsection and land only after this revision passes
design review. Every other v10-v20 decision is unchanged. v21 (sha256
0c4eaae041455cf671aeb268f4fafda8df8d22bd1d4e8ccf333f2eded8af5476)
was reviewed with REQUEST_CHANGES (round-22, one finding): v21
carried dependency lifetime on the teardown ORDER alone, but for
a runner blocked until lockservice shutdown the cited order works
the other way — the engine closes FIRST
(`pkg/cnservice/server.go:607`), then the transaction client and
sender (`:832-842`), and only then the lockservice (`:843-845`) —
while the commit runner still owes `closeLocked` and workspace
finalization after its unlock leg returns
(`pkg/txn/client/operator.go:1473-1490`, `:1515-1525`); no
completion barrier connected those obligations to dependency
retirement, and a Close-return openness check establishes nothing
about lifetime during later teardown. v22 replaces the ordering
argument with an enforceable protocol: a BOUNDED close drain
inside bootstrap `Close` whose full-drain shape certifies
completion BEFORE any dependency retirement and whose
partial-drain shape certifies the seal only, plus VERIFIED
post-drain completion independence — after the leg returns the
chain's whole remaining dependency set is process-lifetime
memory state (`closeLocked`'s default callbacks are memory-only
bookkeeping, `pkg/txn/client/client.go:1010-1025`, `:1222-1245`,
and executor-workspace finalization is CN-local,
`pkg/vm/engine/disttae/txn.go:3152-3157`, `:3159-3167`, `:3274`) —
with the lockservice-last cancellation cycle resolved explicitly.
The changes are again confined to the Rollout subsection and land
only after this revision passes design review. Every other
v10-v21 decision is unchanged. v22 (sha256
b2139a1c595de7de67653dff0f710ee5790d587717937f5f02bafecdf202164b)
was reviewed with REQUEST_CHANGES (round-23, one finding):
the bounded close drain still equated full-drain
certification with runner-channel completion alone, but two
outstanding obligations survive every completed runner — a
FAILED attempt or closure action may leave its detached
rollback owner ACTIVE with no reconcile-runner channel ever
covering it (the fatal-error path schedules that owner
before `ExecTxn` returns,
`pkg/sql/compile/sql_executor.go:385-393` driving
`pkg/txn/client/operator.go:2116-2143` through
`runSQLTracker.sealAndRunWhenDrained`, the tracker
dispatching it by `go callback()` at `:476-477`), so
Closing between passes could certify no remaining work
while that owner still ran; and a runner whose unlock leg
returns an error still COMPLETES while the transaction
stays registered and fenced — the lockservice skips
`deleteActiveTxn` and `tryCompleteDrain`
(`pkg/lockservice/service.go:830-839`) — with the closure
action or re-drive that would resume cleanup sealed off by
admission. Both v22 shutdown scenarios arranged an
outstanding runner and missed both states. v23 redefines
strong completion over every outstanding ledger and
deferred-owner obligation: full-drain certification
additionally requires no outstanding-owner entry and no
registration-retained marker, otherwise the seal-only
certification stands with an explicit per-class shutdown
lifetime contract, and two deterministic
Close-between-passes scenarios (an ACTIVE detached owner;
a completed unlock-error marker) cover the missed states.
The changes are again confined to the Rollout subsection
and land only after this revision passes design review.
Every other v10-v22 decision is unchanged. v23 (sha256
ef78e8d67a333375202dd8ed0802bc568fb9331499d5f23f813037b13683f302)
was reviewed with REQUEST_CHANGES (round-24, two findings).
Finding 1 — the v23 harvest record sourced the unlock-leg
disposition from the operator's recorded terminal outcome,
but that outcome is PRIVATE: the commit-path unlock failure
flips only `txnOperator.mu.terminalOutcome`
(`pkg/txn/client/operator.go:1515-1525`), the `TxnOperator`
interface exposes no accessor for it
(`pkg/txn/client/types.go:112-218`), and `Txn()` returns
only `txn.TxnMeta` (`operator.go:893-895`), so a clean
commit and a commit retaining locks expose the same nil
`ExecTxn` result and the same Committed snapshot — the
required distinction has no in-band source through the
specified interfaces. Finding 2 — the round-23 zero-runner
owner scenario latched the detached owner inside its
remote-unlock leg, but `rollbackOwned` holds the write
lock ACROSS that leg (`operator.go:1224-1225`, unlock
defer `:1241-1251`), so the required ACTIVE sample —
itself a read lock (`:1716-1721`) — would block instead
of completing, and releasing the latch before teardown
let the owner finish before any retirement, making the
lifetime assertion vacuous. v24 replaces the
private-outcome assumption with the already-designed
discharge oracle: every cleanup-unverified generation
stays marked outstanding until the supervised nil
`Unlock` re-drive establishes closure (the recorded nil
answer is the authoritative closure evidence), FULL DRAIN
derives from that evidence combined with owner quiescence
where required, and the zero-runner owner arm moves to
the round-19 pre-mutex workspace-rollback barrier
(`operator.go:1217-1222` runs BEFORE `:1224`), held
through `Close` and the relevant dependency retirements
and released only afterwards. Every other v10-v23
decision is unchanged. v24 (sha256
405dd5cd59e3355df7650ac97b18e360d5437dcdb4aad76d9ca6c01d177e0358)
was reviewed with REQUEST_CHANGES (round-25, one
finding). Finding — the pre-attempt stage's cleanup
obligation was bounded by its RUNNER: the stage block
licensed a fresh stage the pass after the stage channel
completed, and the shutdown obligations covered only
account-ledger classes; but the stage executes ordinary
SQL on the stage txn — the account-page SELECT through
the shared loader (`pkg/bootstrap/
service_information_schema_views_maintenance.go:177-188`)
— and a FATAL whole-txn error from that read
(`MustRollbackTxn`, `pkg/common/moerr/txn.go:17-22`,
`:33`) schedules a DETACHED rollback owner exactly as in
the account path (`pkg/sql/compile/
sql_executor.go:385-393`), while the executor's error
path (`:218-224`, `:895-899`) then fails fast against
the sealed terminal call
(`pkg/txn/client/operator.go:2367-2380`): the stage
CHANNEL completes with the detached owner still ACTIVE
and NO account-ledger entry ever minted, so a later
pass would replace the stage on channel completion
alone and Close could certify FULL DRAIN while
transaction and workspace cleanup remained. v25 gives
the stage slot a cleanup record INDEPENDENT of the
runner channel — {stage txn id, record class, owner
state} classified by the v18/v19 failure-class table —
with ONE supervised quiescence sample per pass (the v20
in-runner sample verbatim, no re-drive leg for the
read-only stage), replacement admission gated on the
record being quiescent with no unfinished slot runner,
FULL DRAIN requiring a quiescent record, and PARTIAL
DRAIN naming the stage slot in its owner-outstanding
line; the fatal-page-SELECT scenario latches the
detached owner at the existing pre-mutex
workspace-rollback barrier, drives passes and Close
with the owner held, and asserts bounded ownership
across passes and seal-only Close until closure is
verified. Every other v10-v24 decision is unchanged. v25 passed design
review (sha256 71b7e26eb04d778f36606f770c783ed6219cf7c42152fc8fdb64f79c5d0dbea3;
approval artifact state/artifacts/issue-30940/pre-pr-review-1792916297342.json)
and its implementation accumulated reviewed proof — including the
mandatory lifecycle race-validation gate, closed harness-only with no
design change. The pre-PR implementation review then returned
REQUEST_CHANGES with one P2 finding: current-main identifier allocations
invalidate D5 — main claimed MORPC protocol 131 (#31000, exact weighted
percentile remote dispatch) and 132 (#31191, decimal float rounding and
strict textual float conversions) and function IDs 605..615 (#31017),
616..618 (#31020's MRANK/CUMRANK/TMRANK renumber), and 619 (#31000's
re-issued QUANTILE_EXACT_WEIGHTED) — so the v25 fence value and the
implementation's 616-618 admission-function block collide with shipped
main identities, and a `>=131` gate would wrongly admit pre-fix main
peers advertising 131/132 with no attribute channel. v26 re-allocates
both on verified-unused values against origin/main 420786e2 (fence 133;
function block 620-622, FUNCTION_END_NUMBER 623), binds the
integrate-then-re-verify rule and the corrected <=132-vs-133
discrimination baseline into D5, and updates D7, the reconciler
floor-transition text, and the validation-plan fence row; every other
v10-v25 decision is unchanged. The design-only review of v26 returned
REQUEST_CHANGES with one P2 finding (round-27): v26's renumber inventory
described the receive-time classification as name-routed, but the
wire-time feature classifier `RequiredRemoteExpressionFeatures`
(`pkg/pb/plan/string_literal_form.go:1014`, with a sibling numeric
comment at `:405`) matches the admission family by raw numerics 616-618
— pb cannot import the registry (dependency cycle), and the compile-side
binding test that comment promises exists nowhere in the tree — while
the fixtures pin the same stale literals
(`pkg/pb/plan/unsigned_decimal_protocol_test.go:24,:46-48,:53`;
`pkg/sql/compile/unsigned_decimal_protocol_test.go:50`;
`pkg/sql/plan/unsigned_decimal_protocol_floor_test.go:48`) and the
registry identity table (`function_id_test.go:predefinedFunids`, whose
`Test_funids` fails any registered-but-unpinned function) never pinned
the admission family: a constant-only renumber would classify main's
MRANK/CUMRANK/TMRANK as DECIMAL admission functions, leave the real
620-622 channel unclassified, and keep the registry table red. v27
extends D5 with the numeric-identity half of the renumber contract —
pb-local named mirrors following the file's own MEDIUMINT precedent,
registry pins at 620-622 alongside main's preserved 605..618 pins, the
compile-side binding test closing the import-cycle promise,
registry-bound fixtures, and negative arms asserting main's rank family
stays floor-free — and corrects the disposition inventory; every other
v10-v26 decision is unchanged. v28 (2026-10-28) responds to the pre-PR
design-gate review of v27 (sha256 5835ce925a70e7c8dd8b2a47e335adf9408422be0511f14c006f8c1270bd76cb),
which returned REQUEST_CHANGES with one P2 finding: D5 conflicts with the
repository upgrade contract
(.agents/skills/mo-dev/references/upgrade-compatibility.md:3-27), which
permits shutdown upgrades only and prohibits new per-feature MORPC
versions, capability gates, and older-peer fallbacks unless RPC framing
or the handshake changes or a recorded project decision reintroduces
mixed-version support. Verified valid before resolving: the finding's
citations match this document and the tree exactly (the MORPCVersion133
constant and MORPCLatestVersion bump, the authoring-floor hook, the
destination-gate row, D5 itself, and the reconciliation rationale's
bootstrap and rolling-upgrade windows). v28 rewrites D5/D7 and the
COLUMNS repair design for shutdown-only upgrades: this change adds NO
protocol version, no capability gate, no floor comparison, no wire
feature classifier or pb-local mirrors, and no predecessor-template
fallback, while main's existing protocol line (floors <=132, the
tenant-init gate machinery, the v126 VIEWS maintenance phase) stays
untouched. The function block 620-622 with FUNCTION_END_NUMBER 623
stands as an ordinary registry addition under main's Test_funids
contract, and the v26 integrate-then-re-verify rule is unchanged. The
steady-state reconciliation phase and its whole lifecycle (the round-11
through round-24 machinery) are de-scoped: the reach gaps it closed
required the floor's deferred authoring, which no longer exists; the
remaining persisted-state requirement — pre-change tenants' permanent
undecorated COLUMNS definitions — is met by unconditional fresh-init
authorship plus the one-shot idempotent versioned upgrade entry. Every
other v10-v27 decision is unchanged. v29 (2026-10-28) responds to the design-only review of v28 (sha256
521e8d3f16a998c998af0761e0c45613f66fae9e41beb5b21a2d691b68ab9caf),
which returned REQUEST_CHANGES with two P2 findings (round 29), both
verified valid read-only before any edit. Finding 1 (upgrade
reachability): main's version chain ends at 4.0.17/1 (`upgrade.go`
handler tail; the handle's `VersionOffset` = 1 entry) and rev11 placed
the COLUMNS entry INSIDE existing version lists, so a catalog already
READY at 4.0.17/1 fails only the equality short-circuit
(`pkg/bootstrap/service_upgrade.go:214-215`) and never re-fires the
entry — the `MaybeUpgradeTenant` equal-version fast path
(`service_upgrade_tenant.go:66-69`) likewise skips; the existing
`columns_upgrade_test.go:37` starts at 4.0.16 and misses exactly this
production predecessor state. v29 registers the entry in a NEW forward
version (v4_0_18, next free at integration) with the canonical
constructor reverted to main verbatim (its UpgSql/CheckFunc resolve
the decorated text at runtime), names the version handle as the entry
owner and the persisted version advance + content-idempotent CheckFunc
as the completion marker, and binds the manual path to main's
synchronous login-goroutine tenant upgrade (version inequality drives
it; `AutomaticUpgrade` gates only the background maintenance worker).
Finding 2 (historical-template compatibility): recreating the current
COLUMNS definition strands pre-upgrade snapshots on the supported
snapshot read path — `view_columns_history.go:84-92` passes the stored
definition to `AdaptLegacyInformationSchemaColumnsDDL`
(`predefined.go:340-359`), which recognizes only V41/V46/V46Upgrade/
V58/V100/V109 and errors 'unsupported historical
information_schema.COLUMNS definition' for the immediate pre-change
current template (`view_columns_history_test.go:62-68`). v29 admits
every pre-change persistable template identity — the base-era current
text plus each upstream revision inherited through integration (#31461,
`ea4c68a04e`, is the known variant) — as byte-stable pass-through
historical arms (V109-arm shape), preserving snapshot projections and
ownership. Every other v10-v28 decision is unchanged. v30 (2026-10-28)
responds to the design-only review of v29 (sha256
b52bc539bb1b6fdd66bcfab3259af90e332f186a960c6367b1db848511782b9b),
which returned REQUEST_CHANGES with one P2 finding (round 30),
verified valid read-only before any edit: v29's AutomaticUpgrade=false
manual path ("the next login upgrades a version-behind tenant with no
operator step") is unsound — `pkg/cnservice/server.go:1453` gates the
ENTIRE `BootstrapUpgrade` call, route creation included, on
`AutomaticUpgrade`; `Bootstrap` fast-returns for an initialized
cluster (`pkg/bootstrap/service.go:200-208`); and the login path
itself requires cluster latest to equal the binary's final version
before executing tenant entries (the invalid-state gateway at
`service_upgrade_tenant.go:86-95`), so restarting READY-4.0.17/1 data
with the automatic worker off leaves cluster latest at 4.0.17 and the
tenant's first login RETURNS AN ERROR instead of upgrading. v30 binds
the manual path to main's existing privileged sequence: the
sys-account MoAdmin issues `UPGRADE ACCOUNT ALL` (platform-wide) or
`UPGRADE ACCOUNT '<name>' [WITH RETRY n]` (per tenant) — main's only
route creators when the automatic worker is off
(`pkg/bootstrap/custom_upgrade.go:31-74`: `UpgradePreCheck`
retained-route idiom, then `BootstrapUpgrade` or
`CheckAndUpgradeCluster` + `UpgradeOneTenant`); the login path still
upgrades version-behind tenants synchronously AFTER the route exists,
but never creates it. Corrections are
incorporated in the owning section below and summarized in
"Design-review disposition" at the end. v31 (2026-10-28) responds to the
design-only review of v30 (sha256
9fba8f9c95734dc81a8d4864676c19a34b39cf361cc22bfec31af24055e49b6e), which
returned REQUEST_CHANGES with two P2 findings (round 31), both verified
valid read-only before any edit. Finding 1: v30 required a sys-account
MoAdmin to issue `UPGRADE ACCOUNT` before any 4.0.18 route exists, but an
ordinary catalog-backed sys administrator cannot log in there — its
create_version is read from mo_account (`pkg/frontend/session.go:3522-3525`,
installed at `:3807`), `pkg/frontend/mysql_protocol.go:1767-1768` runs the
upgrade check before admitting queries, and the gateway at
`service_upgrade_tenant.go:86-95` rejects cluster latest 4.0.17 against
binary final 4.0.18; v30 never named the identity that CAN authenticate
pre-route. Resolution: the supported pre-route administrative login is
main's existing BOOTSTRAP SPECIAL USER — an in-memory registered
sys-tenant/moadmin-role identity (`pkg/frontend/authenticate.go:493-507`)
whose authentication returns early with no catalog access and no
create_version (`session.go:3454-3469`), so the upgrade check is skipped;
the operator-usable instance is the task-framework special user, registered
on each CN from the CreateTaskService heartbeat command
(`pkg/cnservice/server_task.go:100`), with deployment-provided credentials
from the logservice's `mo_admin_user`/`mo_admin_password` environment
(`pkg/logservice/store_hakeeper_check.go:673-692` — unset yields a random
UUID pair no operator can use, so setting them is a stated credential
prerequisite; main's own precedent connects real clients through exactly
this identity, `pkg/embed/upgrade_recovery_test.go:130-133`/`:230`). Finding
2: with the automatic worker off and only per-account commands/logins,
accounts advance while the global route remains `StateUpgradingTenant`
indefinitely — `CheckAndUpgradeCluster` starts only the cluster coordinator
(`custom_upgrade.go:65-75`), `UpgradeOneTenant` (`:79-180`) and login
upgrades perform no tenant-task accounting, and the route's final step
requires `TotalTenant == ReadyTenant` (`service_upgrade.go:528-529`)
advanced only by the `asyncUpgradeTenantTask` consumers that
`BootstrapUpgrade` alone starts (`service_upgrade.go:69-75`). Resolution:
distinguish per-account completion (version row + cache) from global route
finalization (task rows, counters, StateReady), and name
`UPGRADE ACCOUNT ALL` — the `BootstrapUpgrade` entry point — as the existing
finalizing operation: already-upgraded tenants are skipped
(`service_upgrade_tenant.go:211-218`) and the pass's end-reconciliation
snaps the counters (`:312-315`), so ALL after per-account work is a cheap
finalizing pass; validation item (iv) is split into ALL-first and
per-account-first scenarios with counters asserted before and after
finalization. Corrections are incorporated in the owning sections below and
summarized in "Design-review disposition" at the end. Every other v10-v30
decision is unchanged. v32 (2026-10-28) responds to the
design-only review of v31 (sha256
130e31a0202e4e77b8c5d2617e3e61caadebdd45be48ea84eb864ace8db00c24),
which returned REQUEST_CHANGES with one P2 finding (round 32),
verified valid read-only before any edit: v31's credential
prerequisite is imposed BEFORE FIRST BOOTSTRAP, but existing
clusters can legitimately hold the random-fallback credentials
(`pkg/logservice/store_hakeeper_check.go:447-457`), and those are
persisted ONE-SHOT in HAKeeper — `pkg/hakeeper/rsm.go:1383-1397`
accepts a task-table-user proposal only while the scheduler state
is `TaskSchedulerCreated` and then flips to `Running`, so
environment variables set at upgrade time can never replace them
and no rotation command exists (`GetTaskTableUserCmd`'s only
proposer is `registerTaskUser` -> `setTaskTableUser`,
`store_hakeeper_check.go:652-670`, which merely logs 'task user
info already set' on later attempts). Resolution: the rollout now
distinguishes the two predecessor states by credential knowledge —
KNOWN first-boot credentials ride v31's privileged statement path,
and UNKNOWN first-boot credentials ride main's other existing
route creator with zero credentials: the new binary's mandatory
shutdown restart starts the CNs with `auto-upgrade = true`
(`pkg/cnservice/types.go:327`; main's default is true,
`cmd/mo-service/config.go:171-173`), so startup's
`BootstrapUpgrade` performs route creation, all-tenant execution,
and global Ready entirely server-side with HAKeeper state
untouched; restoring the flag afterwards is optional and
behavior-neutral. Validation item (iv) gains Scenario C: a
predecessor bootstrapped WITHOUT either environment variable with
its HAKeeper state preserved across restart, the premise pinned
by a FAILED env-credential login, then the config-window
procedure reaching global Ready with ordinary logins restored.
Corrections are incorporated in the owning sections below and
summarized in "Design-review disposition" at the end. Every other
v10-v31 decision is unchanged. v33 (2026-10-28) responds to the
pre-PR implementation review of the completed v32 diff (v32 passed
design review, sha256
ce304c37a79b2c51d83e2dd72e56d3ba547d78bb32fce236ed295aa6047cd6f8),
which returned REQUEST_CHANGES with one P2 finding: upstream #31497
(`ea47345ff8`, on origin/main past this worktree's base) consolidated
the 4.0.8-4.0.17 catalog handlers into a FROZEN 4.0.7 terminal and
rewrote the bootstrap upgrade lifecycle, so v29's forward transition
(`pkg/bootstrap/versions/v4_0_18`, `MinUpgradeVersion` 4.0.17) is
unreachable from the chain's new READY 4.0.7/41 terminal state, the
two new test files import the removed v4_0_16/v4_0_17 packages, and
every predecessor/recovery fixture exercises the superseded chain.
Verified valid read-only against origin/main `9e29011e73` before any
edit (zero Go sources touched this revision): `pkg/bootstrap/versions/`
ends at `v4_0_7` — 4.0.18 stays free and is re-verified next-free at
integration under the D5 rule; `CanDirectUpgrade` admits a handle
whose `MinUpgradeVersion <= from` (`versions/version.go:45-47`); the
canonical COLUMNS constructor survived verbatim
(`versions/information_schema_columns.go`) and fires inside
consolidated 4.0.7 (`v4_0_7/tenant_upgrade_list.go:58-62`,
`upgradeDatabaseDefaultsColumns`); a 4.0.7 offset mismatch
hard-errors before any catalog mutation
(`pkg/bootstrap/service_upgrade.go:235-274` — "4.0.7 is frozen"),
matching upstream's own contract that later migrations take a NEW
semantic catalog version (`docs/design/20261028-catalog-upgrade-407.md`);
and the v30-v32 manual-SQL operator path is superseded by main's
startup-owned lifecycle: with `auto-upgrade = false` a CN FAILS TO
START against a catalog not READY at the binary's final version
(`pkg/cnservice/server.go:1495-1498` ->
`bootstrap.CheckManualUpgradeReadiness`,
`pkg/bootstrap/service_upgrade.go:75-99`), while
`auto-upgrade = true` drives the whole route to global Ready at
startup before any SQL admission (`server.go:1501-1548`,
`waitForBootstrapUpgrade` at `server.go:503`,
`pkg/cnservice/server_bootstrap_wait.go:20-50`). v33 re-bases D7's
forward transition onto the consolidated chain (predecessor READY
4.0.7/41), rewrites Rollout item 3 and validation item (iv) onto the
fail-fast/startup-owned contract — dissolving the v32
unknown-credentials case by construction, since no credential exists
anywhere in the supported path — re-bases the
predecessor/recovery fixtures, and records the integration merge
dispositions for the files #31497 rewrote. Every other v10-v32
decision is unchanged.

Design gate trigger for the review record: this change modifies persisted
catalog metadata (`plan.Type` proto plus the `mo_columns` attribute column),
changes the user-visible SQL compatibility contract (DDL, DML acceptance,
SHOW CREATE, CAST grammar, result presentation, LOAD behavior), adds versioned
bootstrap upgrade entries, and crosses the parser, planner, catalog,
expression-evaluation, LOAD, and frontend-output ownership boundaries. It is
therefore a feature under the design gate even though the estimated production
diff is well under 500 lines. The MEDIUMINT contract
([20261010-mysql-mediumint-semantics.md](20261010-mysql-mediumint-semantics.md),
PR #29818) is the direct precedent for the enforcement seams, DDL default
admission, and protocol rollout machinery reused below.

v34 (this revision) responds to the design-only review of v33, which
returned REQUEST_CHANGES with one P2 finding: the re-based transition
makes 4.0.7 an intermediate version, but main's legacy-CDC admission
preflight is gated on the final version BEING 4.0.7
(`pkg/bootstrap/service_upgrade.go:235-238` recheck path, `:277-280`
route-creation path), so appending 4.0.18 would let a released 4.2
source at 4.0.5/4.0.6 holding legacy CDC tasks mutate catalogs whose
safety contract (`pkg/bootstrap/service_upgrade_cdc.go:28-31`,
`docs/guides/upgrade-4.2-to-main.md:12-23`) requires terminal
rejection BEFORE any catalog mutation. The finding was verified valid
read-only against origin/main `9e29011e73` (the v33 review head) and
re-verified unchanged at `77679f178d` this revision. Resolution: both
call sites are re-gated to fire whenever 4.0.7 is crossed or resumed,
independently of the final target, keeping the existing owner
(`rejectLegacyCDCTasks`) and its pre-mutation position and editing no
frozen 4.0.7 entry; the validation plan gains the 4.0.5/4.0.6 and
interrupted-4.0.7 matrix with an empty-task control (Design item 2,
invariant I7, validation item (vi)); every other v10-v33 decision is
unchanged. Approval-chain provenance: the v32 design-only PASS (sha256
ce304c37a79b2c51d83e2dd72e56d3ba547d78bb32fce236ed295aa6047cd6f8) was
verified and explicitly reused by the pre-PR implementation review;
v33 (sha256
547120041139e12cd788222ece9324e4dceae8d46655691ecad1167ecf81b91b) was
reviewed with REQUEST_CHANGES as recorded here.

v35 (this revision) responds to the design-only review of v34 (sha256
1393d811a4dd7f944f32a9c67f3360531b497466542c345a18caa376b4b67751),
which returned REQUEST_CHANGES with two P2 findings on the same
upgrade-safety seam. Finding 1 (restrict CDC rejection to upgrades
that cross or resume 4.0.7): v34's route-creation re-gate keeps
`!v.IsReady()` as a disjunct, but a READY-4.0.7/41 source holding a
LEGITIMATE post-crossing CDC task that crashes mid-4.0.18-route comes
back with the route's persisted final row
(`versions.AddVersion(final..., StateCreated)`,
`pkg/bootstrap/service_upgrade.go:341`) as its latest version, so
`!v.IsReady()` fires the preflight against a valid task —
contradicting v34's own I7 clause that post-crossing tasks are never
re-rejected. Finding 2 (preserve the frozen 4.0.7 source-offset
rejection under the new final): v34 let main's offset guards
(`service_upgrade.go:255-276`, both `final.Version == "4.0.7"`-scoped)
lapse with static entry-list tests as the substitute, but a static
check cannot validate a PERSISTED source offset — under final 4.0.18 a
READY 4.0.7/1 catalog passes the lapsed guards and takes the direct
route (`:385-387`), skipping consolidated entries 2..41, and an
interrupted offset-only 4.0.7 route passes recovery validation
(`:302-312` -> `checkUpgradeRecovery`, `:305`), both violating
upstream's pre-mutation rejection contract
(`docs/design/20261028-catalog-upgrade-407.md:21`) and falsifying
main's regression oracles
(`service_upgrade_recovery_test.go:740`/`:767`) the moment 4.0.18
becomes final. Both findings verified valid read-only against
origin/main `77679f178d` and re-verified on the current head
`df6dff5305` (one commit past it, touching no bootstrap/upgrade/guide
machinery) — zero Go sources touched this revision. Resolution: the
cross-check preamble re-expresses ALL FOUR `final.Version == "4.0.7"`
safety conditions — both offset guards and both CDC call sites —
against `versions.Compare(final.Version, "4.0.7") >= 0` plus durable
source/route state read against the frozen 4.0.7 handler metadata
(`v4_0_7.Handler.Metadata().VersionOffset` = 41): an unfinished 4.0.7
CROSSING (`v.Version == "4.0.7" && !v.IsReady()`, or at recheck time
the durable 4.0.7/41 marker not READY) is distinguished from an
unfinished LATER upgrade (`v.Version` beyond 4.0.7), so legacy tasks
on crossing/resuming sources keep their terminal pre-mutation
rejection while valid post-crossing tasks on interrupted later routes
are never re-rejected; mismatched READY offsets and interrupted
offset-only routes keep their terminal rejection before any mutation
(invariant I7 re-expressed, I8 new). Validation gains the
task-survival positive control (READY 4.0.7/41 + valid task,
interrupted 4.0.18 route, restart completes with the task intact and
the preflight never queried), the recheck oracle qualified by the
same eligibility rule, mismatched READY offsets {1,1000} and the
interrupted offset-only route targeting 4.0.18 (terminal before
mutation) alongside the supported READY-4.0.7/41 control — and names
THREE upstream discriminating red/green oracles
(`TestLegacyCDCAdmissionBeforeUpgradeMutation`,
`TestV407RejectsDifferentOffsetBeforeCreatingRoute`,
`TestV407RejectsInterruptedOffsetOnlyRouteBeforeMutation`: each RED
on the naive integrated tree, GREEN UNCHANGED after the v35 re-gates
because under final 4.0.7 every re-gated condition reduces to main's
exact condition). Every other v10-v34 decision is unchanged.

v36 (this revision) responds to the design-only review of v35 (sha256
66f17482dbdd4f5867be70800fc18054c83cd5b8d709296d81cffd73f5428128),
which returned REQUEST_CHANGES with ONE P2 finding — derive CDC
crossing eligibility from actual persisted route progress. The v35
eligibility rule assumed that an unfinished 4.0.18 target implies the
4.0.7 crossing is complete and that `versions.GetVersionState("4.0.7",
41)` tracks intermediate-step completion; both premises are false on
main: `versions.AddVersion` has exactly ONE production call site (`pkg/bootstrap/service_upgrade.go:341`, the FINAL route row, StateCreated)
and `versions.UpdateVersionState` exactly one (`:695`, that same marker
flipped to Ready only after EVERY step of the route is Ready), so no
`mo_version` row ever records an intermediate route step. Intermediate
progress lives only in `mo_upgrade` step rows under the route key
(final version, final offset) — built by `addUpgradesToFinalVersion`
(`:364-397`) and advanced by `versions.UpdateVersionUpgradeState`
(`versions/version_upgrade.go:137-146`, from `doUpgrade` `:719`/`:732`).
A READY-4.0.6 source upgrading to 4.0.18 therefore persists ONLY the
4.0.18/1 marker plus steps [4.0.6 -> 4.0.7, 4.0.7 -> 4.0.18]: v35's
startup rule under-fired mid-crossing (latest row 4.0.18-Created was
classified SKIP while the 4.0.7 step was still pending, resuming the
crossing without re-checking legacy tasks where main re-checks), and
v35's recheck rule over-fired forever (under the integrated chain the
4.0.7/41 `mo_version` marker never comes to exist for a route created
by this binary, so the marker-missing-or-not-READY test kept rejecting
valid post-crossing tasks even after the 4.0.7 step durably committed).
The recheck lifecycle description is also corrected: `checkUpgrade(true)`
runs ONLY from `newUpgradePass` after a RETAINED historical (non-final)
target completes (`service_upgrade.go:530-534`, design comment
`:155-156`) — because another CN may have created or completed our
final route during that work — never after individual intermediate-step
commits. Resolution (verified valid read-only against origin/main
`77679f178d`, at the v35 review head `df6dff5305`, and re-verified
against the current head `9c8d405b1e`, whose one-commit delta touches
only the CSV-load machinery; zero Go sources touched this revision):
ONE shared step-based crossing-progress predicate, used consistently at
startup and at recheck — the preflight is required exactly while the
relevant persisted route still contains an uncommitted step with
`ToVersion == "4.0.7"`. At route creation the rule evaluates the latest
version row: below 4.0.7 (CHECK), 4.0.7 not READY (CHECK), beyond 4.0.7
(scan that row's own `mo_upgrade` route for a not-Ready 4.0.7 step) —
under final 4.0.7 the third arm is unreachable because no binary on
main's chain can persist a version row beyond its own final, so the
gate reduces to main's exact condition with ZERO added reads. At the
worker's recheck the rule evaluates the FINAL route's persisted steps:
under final 4.0.7 the non-READY final marker is itself the crossing
signal (every 4.0.7-final route's LAST step is the 4.0.7 step, steps
complete in order, and the marker flips Ready only after all steps
commit — `service_upgrade.go:695`), so firing stays byte-identical to
main with NO added read; under any later final one
`versions.GetUpgradeVersions(final.Version, final.VersionOffset)` scan
(`versions/version_upgrade.go:26`) checks the final route's steps for a
not-Ready 4.0.7 step. Validation gains the finding's required real-route
fixtures — a READY-4.0.6 source's actual persisted route targeting 4.0.18,
interrupted BEFORE the 4.0.7 step completes (restart terminally rejects
the legacy task before any mutation) and AFTER (restart skips the
preflight, resumes, completes) — with NO synthesized intermediate
`mo_version` marker, while the READY-4.0.7/41 valid-task survival
control, the frozen-offset rejection cases, and the three-oracle
red/green ladder are retained (I7 re-expressed; I8 and the offset
guards unchanged — the review supports them). Every other v10-v35
decision is unchanged.

## Problem and contract

### Evidence

Two independent reports confirm the same modifier-loss family against dev build
`3a1e42b430`, with MySQL 8.0.45 as the reference:

```sql
CREATE TABLE ud (x DECIMAL(10,2) UNSIGNED);   -- #30940
INSERT INTO ud VALUES (-1.5);
SELECT x FROM ud;
-- MatrixOne: -1.50  (negative silently accepted)
-- MySQL: ERROR 1264 (22003) Out of range value for column 'x' at row 1

CAST(-1.5 AS DECIMAL(10,2) UNSIGNED);
-- MatrixOne: -1.50 (spelling accepted, attribute ignored)
-- MySQL: ERROR 1064 (syntax error)

CREATE TABLE zf (x DECIMAL(10,2) ZEROFILL);   -- #30941
INSERT INTO zf VALUES (123.45);
SELECT x FROM zf;
-- MatrixOne: 123.45          MySQL: 00000123.45
SHOW CREATE TABLE zf;
-- MatrixOne: `x` decimal(10,2) DEFAULT NULL
-- MySQL:     `x` decimal(10,2) unsigned zerofill DEFAULT NULL
```

Reproduced live on `origin/main` `8fdc27559f9d2b82b0071ee02d40ca21ffbf3120`
(isolated instance, pymysql probe; evidence in the handler report): negative
INSERT/UPDATE accepted under both strict and non-strict `sql_mode` with zero
warnings; SHOW CREATE and `information_schema.COLUMNS.COLUMN_TYPE` drop the
modifiers; the CAST spelling is accepted and returns the signed value;
ZEROFILL rendering is unpadded; identical behavior at `DECIMAL(16,2)` /
`(17,2)` / `(39,2)` (all three engine widths).

Code inspection on the same base:

1. The parser records both modifiers for DECIMAL declarations:
   `column_type: numeric_type unsigned_opt zero_fill_opt` sets
   `InternalType.Unsigned` / `InternalType.Zerofill`
   (`pkg/sql/parsers/dialect/mysql/mysql_sql.y:15344`).
2. The planner drops them for DECIMAL:
   `getTypeFromAstWithoutCharset`
   (`pkg/sql/plan/build_util.go:386-392`) selects `T_decimal64/128/256` from
   precision alone; unlike the integer branches (and `INT24`'s
   `Unsigned || Zerofill` handling at line 276), the DECIMAL branch consumes
   neither flag.
3. `plan.Type` (`proto/plan.proto` message `Type`, fields 1-13) has no field
   that can carry either attribute, and the durable catalog record for a
   column is the `mo_columns` row: its `atttyp` blob encodes the runtime
   `types.Type` (`pkg/vm/engine/types.go:240-271` PlanColsToExeCols →
   `types.MustTypeFromPlan`; `pkg/catalog/tuplesGen.go:137-139` types.Encode),
   not `plan.Type`. disttae rebuilds `plan.Type` explicitly from the engine
   attribute (`pkg/vm/engine/disttae/txn_table.go:1735-1746`,
   `pkg/vm/engine/disttae/cache/catalog.go:880-921` genTableDefOfColumn →
   getTableDef), so a proto-only field cannot survive restart without an
   explicit durable carrier and conversion owners. (Correction of v1's
   incorrect automatic-inheritance assumption; review finding 1.)
4. SHOW CREATE renders `DECIMAL(M,D)` with no modifier suffix:
   `FormatColType` → `types.FormatSQLColumnType`
   (`pkg/sql/plan/build_show_util.go:1261`,
   `pkg/container/types/sql_type.go:57`).
5. CAST accepts a full `column_type` (`mo_cast_type: column_type`,
   `mysql_sql.y:13676`), so `CAST(x AS DECIMAL(10,2) UNSIGNED)` parses and is
   then converted with the attribute-free type. MySQL's restricted
   `cast_type` grammar does not admit attributes on DECIMAL CAST targets
   (1064).
6. The SQL-mode-aware DML assignment machinery exists:
   `cast_assign`/`cast_strict`/`cast_ignore`
   (`pkg/sql/plan/build_constraint_util.go`,
   `pkg/sql/plan/function/func_cast.go`) produce ERROR 1264 under strict modes
   and clip-plus-warning under non-strict modes for unsigned integers and
   MEDIUMINT; DECIMAL targets never carry the attribute that would activate an
   equivalent domain check, and same-type decimal-to-decimal assignments skip
   the cast entirely (`needsSameTypeAssignmentCast`,
   `build_constraint_util.go:1346`).

### MySQL 8.0.45 reference behavior (verified against source)

Fetched `mysql-server@mysql-8.0.45/sql/field.cc` (local evidence copy in the
handler artifacts): `Field_new_decimal::store_value` (lines 2836-2875) checks
`is_unsigned() && decimal_value->sign()` at lines 2849-2856 and substitutes
`decimal_zero` **before** `my_decimal2binary(..., precision, dec)` performs the
target-scale rounding. All source conversions are full-precision first:
`store(char*)` parses with `str2my_decimal` (line 2885, natural scale, no
target-scale rounding), `store(double)` uses `double2my_decimal` (line 2950),
`store(longlong)` uses `int2my_decimal`, and `store_decimal`
(line 2967-2970) delegates directly to `store_value`. Consequently:

- Any negative **nonzero** source value is out of domain for an unsigned
  DECIMAL target, including values that would round to zero at the target
  scale (`-0.004` into `DECIMAL(10,2) UNSIGNED` ⇒ strict error / non-strict
  clip-to-0 with warning 1264). This corrects v1's post-rounding acceptance
  (review finding 2).
- Exact zero values (`0`, `-0`, `-0.00`, `'-0.000'`) are in domain: a zero
  `my_decimal` carries no sign (zero controls are exercised in the validation
  plan; signed-zero edge vs. a live MySQL is an implementation-oracle item).
- ZEROFILL presentation: `val_str` pads the integer part to
  `precision - scale` digits when `zerofill` is set (field.cc `val_str`,
  `fixed_precision = zerofill ? precision : 0`).
- `sql_type` appends `unsigned`/`zerofill` to `decimal(p,s)` for metadata
  (`append_zerofill_and_unsigned`).

### Contract (target behavior, MySQL 8.0.17+ compatible)

| Declaration | Logical domain | Presentation | Metadata |
|---|---|---|---|
| `DECIMAL(p,s)` | signed, existing behavior | unchanged | unchanged |
| `DECIMAL(p,s) UNSIGNED` | `[0, max]`; negative **nonzero** sources out of domain | plain | `unsigned` round-trips (SHOW CREATE / I_S) |
| `DECIMAL(p,s) ZEROFILL` | implied unsigned (same domain rule) | integer part zero-padded to `p-s` digits | `unsigned zerofill` round-trips |
| `DECIMAL(p,s) UNSIGNED ZEROFILL` | unsigned | padded | `unsigned zerofill` round-trips |

The domain rule is evaluated on the source value **after its standard
full-precision conversion** (decimal source as-is; string source parsed at
natural scale; floating/int source converted to decimal) and **before**
rounding to the target scale:

- negative nonzero source → strict modes (`STRICT_TRANS_TABLES`,
  `STRICT_ALL_TABLES`, `TRADITIONAL`): ERROR 1264 (22003) "Out of range value
  for column '<label>' at row <n>", the statement stores nothing; non-strict
  modes: stored value is 0 (target scale) with one 1264 warning per row;
  `INSERT IGNORE`/`UPDATE IGNORE` take the clip+warning path;
- exact-zero sources (including signed zeros) are in domain;
- positive values follow the existing conversion/rounding/precision-range
  path unchanged.

CAST: `CAST(x AS DECIMAL...)`, `CAST(x AS SIGNED [INT])`, `CAST(x AS UNSIGNED
[INT])` keep working. `CAST(x AS DECIMAL(p[,s]) UNSIGNED [ZEROFILL])` and
`CAST(x AS DECIMAL ZEROFILL)` become syntax errors matching MySQL (1064),
replacing today's silent acceptance.

DDL defaults follow the MEDIUMINT contract: a constant negative (after the
same sign rule, so `DEFAULT -0.004` is rejected) DEFAULT on an unsigned
DECIMAL column is rejected at DDL authoring (ER_INVALID_DEFAULT, 1067),
independent of session `sql_mode`. Narrowing a column to gain UNSIGNED over
existing negative data fails the ALTER without replacing the source table.

Schema propagation: `CREATE TABLE ... LIKE` copies declarations including
attributes; CTAS keeps attributes **only** for direct single-source column
projections (matching MySQL 8.0.45; owner and evidence in "DDL propagation
of attributes" below); computed expressions produce plain
attribute-free columns.

Deprecation observability: authoring `DECIMAL ... UNSIGNED` appends warning
1681 and `... ZEROFILL` appends warning 3861 while still accepting and
enforcing the declaration, matching MySQL 8.0.17+.

`LOAD DATA` uses the same domain rule and mode matrix as DML (details and
format-specific staging in decision D9): strict ⇒ statement aborts with 1264;
non-strict ⇒ clip to 0 with per-row 1264 warnings; `IGNORE` ⇒ clip+warning;
`LOCAL` non-replace follows the existing lenient-assignment policy (clip+warning).

### Scope/non-goals

- In scope: `DECIMAL`/`DEC`/`NUMERIC` column declarations with `UNSIGNED`,
  `ZEROFILL`; their enforcement, metadata round-trip, display, LOAD behavior,
  CAST syntax alignment, and the issue's acceptance checklist (covers both
  #30940 and #30941 in one change).
- Out of scope (recorded, not deferred bugs): `FLOAT`/`DOUBLE` UNSIGNED/
  ZEROFILL (constantly dropped today at `build_util.go:290-293`; different
  semantics and deprecation trajectory), integer display width `INT(n)`, and
  MySQL's planned removal of ZEROFILL (9.x).
- No retroactive treatment of rows already stored in signed DECIMAL columns
  declared before this change: their catalogs carry no attribute, so reads and
  future writes keep current behavior.

## Invariants

- **I1 (metadata integrity).** A persisted `DECIMAL ... [UNSIGNED] [ZEROFILL]`
  declaration round-trips intact through parser → `plan.Type` → `mo_columns`
  durable record → engine attribute → rebuilt `plan.Type` → SHOW CREATE /
  `information_schema.COLUMNS.COLUMN_TYPE` / dump and restore, across a
  cluster restart. ZEROFILL surfaces its implied UNSIGNED exactly once
  (`unsigned zerofill`); the stored mask for ZEROFILL is normalized to include
  UNSIGNED. This version, `information_schema.COLUMNS.COLUMN_TYPE`
  round-trips the mask on base-table rows; view-over-attributed and
  subscription rows keep pre-fix `decimal(p,s)` rendering pending the
  gated follow-up template (see "Public metadata channels").
- **I2 (domain enforcement).** Every write path (INSERT, INSERT...SELECT,
  REPLACE, ODKU, UPDATE, LOAD CSV/parallel-CSV/Parquet/Arrow, generated
  columns, defaults materialization) admits an unsigned DECIMAL target only
  values within `[0, 10^(p-s)-1]` under the sign rule above, in the
  strict/non-strict/IGNORE modes defined above, with statement-level failure
  atomicity: a rejected statement stores no partial value.
- **I3 (display).** A direct column read of a ZEROFILL DECIMAL renders the
  integer part zero-padded to `p-s` digits in text protocol, binary protocol,
  and text exports; derived expressions are unaffected (attributes are column
  metadata). Physical exports (e.g. Parquet) carry the value unchanged with
  protocol flags preserved.
- **I4 (no collateral).** Signed DECIMAL behavior, `types.Type`/vector/
  objectio/TAE persisted formats, hot-path cost for targets without the
  attribute, and all existing decimal/information-schema/protocol contracts
  are unchanged.
- **I5 (mixed version, v28).** Mixed-version operation is unsupported
  (shutdown upgrade only, D5): attributed columns, persisted expressions
  containing attributed targets, and remote placement of expressions
  carrying attributed types exist only after every peer runs the new
  binary, so no peer ever meets an attributed type it cannot interpret —
  and no per-feature protocol floor is added to enforce it.
- **I6 (nullability/charset preservation).** `plan.Type.notNullable`,
  `types.Type.notNull`, charset and collation fields keep byte-for-byte
  current encodings; the attribute channel never reinterprets them.
  `SetNotNull`/`GetNotNull` semantics are untouched.
- **I7 (legacy-CDC admission, v35; eligibility re-derived in v36).**
  Whenever an upgrade crosses or
  resumes catalog version 4.0.7 — independently of the final
  version this change appends — a source catalog still holding
  pre-4.0.7 CDC tasks (any row in `mo_catalog.mo_cdc_task`,
  including paused/failed) is terminally rejected BEFORE any
  catalog mutation, by the existing owner (`rejectLegacyCDCTasks`,
  `pkg/bootstrap/service_upgrade_cdc.go:33`) at its existing call
  sites; an empty-task catalog proceeds through the route normally,
  and CDC tasks created after the 4.0.7 crossing are never
  re-rejected. v36 pins the ELIGIBILITY rule that makes the three
  clauses cohere — shared by the startup route-creation site and
  the worker's recheck, and derived from ACTUAL PERSISTED ROUTE
  PROGRESS (`mo_upgrade` step rows), never from a synthesized
  `mo_version` marker (no such marker can exist for intermediate
  steps: `versions.AddVersion`/`versions.UpdateVersionState` have
  exactly one production call site each —
  `service_upgrade.go:341`/`:695` — serving only the FINAL route
  marker). The preflight is required exactly while the relevant
  persisted route still contains an uncommitted step with
  `ToVersion == "4.0.7"`: at route creation, evaluated on the
  latest version row — `versions.Compare(v.Version, "4.0.7") < 0`
  (CHECK), `v.Version == "4.0.7" && !v.IsReady()` (CHECK), or
  `v.Version` beyond 4.0.7 with a not-Ready 4.0.7 step in that
  row's own `mo_upgrade` route (CHECK), otherwise SKIP; at the
  worker's recheck (invoked only after a retained historical
  target completes, `service_upgrade.go:530-534`), evaluated on
  the FINAL route — under final 4.0.7 the non-READY final marker
  is itself the crossing signal (the 4.0.7 step is every such
  route's last step, steps complete in order, and the marker flips
  Ready only after every step commits), byte-identical to main
  with no added read, and under any later final one
  `versions.GetUpgradeVersions(final.Version,
  final.VersionOffset)` scan for a not-Ready 4.0.7 step. An
  unfinished LATER route whose 4.0.7 step has durably committed
  and a READY-4.0.7/41 source (whose direct route contains no
  4.0.7 step) are never re-checked, so a valid task created after
  the crossing survives an interrupted 4.0.18 route and its
  restart; a route interrupted BEFORE its 4.0.7 step commits is
  re-checked exactly as main re-checks a mid-crossing restart.
- **I8 (frozen 4.0.7 source-offset rejection, v35).** Whenever the
  durable source or resumed route involves catalog version 4.0.7 —
  independently of the final target — a READY 4.0.7 source whose
  persisted offset differs from the frozen handler metadata
  (`v4_0_7.Handler.Metadata().VersionOffset`), or an interrupted
  route containing an offset-only 4.0.7 -> 4.0.7 step, is
  terminally rejected BEFORE any catalog mutation, by main's
  existing guards with their unchanged positions, scan, and error
  texts. A persisted source offset is validated against the frozen
  handler metadata at runtime; no frozen entry list is edited, and
  static list-shape tests stay only as complements, never as the
  offset-rejection mechanism.

## Runtime and durable representation (round-2 review finding 1 — resolved)

v1 deferred this choice to implementation and incorrectly assumed catalog rows
inherit a `plan.Type` field automatically. v2 picks the representation now:

**`types.Type` is frozen.** Both facts were re-verified on this base:

- Its native 16-byte layout is a persistence format: `EncodeType` performs an
  unsafe native copy (`pkg/container/types/encoding.go:100-110`) used by
  vector marshal (`pkg/container/vector/vector.go:4702,4923`;
  `vector/versions.go:79,108`), the TAE catalog schema blobs
  (`pkg/vm/engine/tae/catalog/schema.go:474,614`), objectio layouts
  (`pkg/objectio/legacy_column.go:246`), PK-filter persistence
  (`pkg/vm/engine/readutil/pk_filter_base.go:688`), aggregation serialization
  (`pkg/sql/colexec/aggexec/median2.go:546`), TAE batch envelopes
  (`pkg/vm/engine/tae/containers/batch.go`), and materialized views
  (`pkg/sql/internal/materialized/source.go:729`). Growing the struct to 20
  bytes changes `TSize` and invalidates every persisted buffer above.
- Packing bits into `notNull` breaks its contract twice over:
  `GetNotNull()` returns `t.notNull == 1` (`types.go:654`), so any value other
  than 0/1 flips every stored nullable column to "not null"; and the 20-byte
  catalog encoding's Unmarshal requires the `notNull` u16 at bytes [4:6] to be
  ≤ 1 (`types.go:318`). Overloading the reserved bytes [6:8] makes every old
  binary hard-fail (`"invalid type encoding header"`) when reading the
  catalog. Overloading `Charset` collides with `ValidateCollation` and the
  existing numeric-CAST private marker `Charset=255`
  (`types.go:334-346` keeps non-string collation revision fixed at 0).

**Chosen representation — plan-side channel with an explicit durable record:**

| Layer | Encoding |
|---|---|
| Plan | `plan.Type` gains one additive proto3 field `uint32 numeric_attributes = 14` (bit 0 = UNSIGNED, bit 1 = ZEROFILL). Legal only on `T_decimal64/128/256` OIDs; other OIDs must be 0 (validated at the same authoring admission that validates width/scale). Declared ZEROFILL is normalized at authoring to 3 (unsigned bit set), so bit 1 never appears without bit 0 in catalogs. |
| Durable catalog | `mo_catalog.mo_columns.att_is_unsigned` (existing `int8`, `pkg/catalog/types.go:446`, idx 15 of the tuples layout) widens from {0,1} to the same bitmask (0-3). `1` keeps today's exact meaning for unsigned integer columns; decimal columns use bit 0/1. **No catalog schema migration.** |
| Runtime mirror (in memory only) | `engine.Attribute` gains an in-memory `NumericAttributes uint8` field (not persisted; rebuilt from `mo_columns` on every read). `types.Type`, vectors, objectio, TAE catalog blobs, PK-filter payloads, and the 20-byte `types.Encode`/`types.Decode` format are untouched. |

**Conversion owners in both directions** (the v1 review required these to be
named):

| Direction | Owner | Mapping |
|---|---|---|
| Plan → engine (DDL write) | `PlanColsToExeCols` (`pkg/vm/engine/types.go:240`) | `plan.Type.numeric_attributes` → `Attribute.NumericAttributes` |
| Engine → `mo_columns` (DDL write) | `GenTuplesOfColumn` predicate (`pkg/catalog/tuplesGen.go:155-157`) | mask = (`Type.IsUInt()` ? 1 : 0) OR (`Attribute.NumericAttributes` for decimal OIDs) → `att_is_unsigned` |
| `mo_columns` → cache row (read, exists) | `ParseColumnsBatchAnd` (`pkg/vm/engine/disttae/cache/catalog.go:765`) | `att_is_unsigned` int8 → `Column.IsUnsigned` (unchanged code path) |
| cache row → engine attribute (read) | `genTableDefOfColumn` (`pkg/vm/engine/disttae/cache/catalog.go:880-921`) | for decimal OIDs: `Column.IsUnsigned` → `Attribute.NumericAttributes` |
| engine attribute → plan (read, two sites) | `getTableDef` (cache/catalog.go:923+) and `txnTable.GetTableDef` (`disttae/txn_table.go:1735-1746`) | `Attribute.NumericAttributes` → `plan.Type.numeric_attributes` |

Old/new codec behavior:

- Old binaries reading new query-plan protos skip proto3 field 14 silently
  (unknown-field skipping), and rebuild `plan.Type` from `types.Type` on
  restart paths, thereby losing the attribute exactly as today (documented
  exposure; the shutdown-only upgrade contract guarantees attributed columns
  are authored only when every CN runs the new binary, D5/I5). `att_is_unsigned` values 3 on old binaries are inert on
  the server side (the only server consumers treat the value as the legacy
  boolean; the one test asserts the writer's mapping) — with one offline
  exception: pre-change checkpointtool builds read `3` as not-unsigned and
  emit plain `decimal(p,s)` in reconstructed DDL for ZEROFILL decimals
  (documented limitation in "Public metadata channels"; old tools also emit
  no ZEROFILL today, so this is no regression).
- New binaries reading old catalogs: mask 0/1 → decimal columns always 0 ⇒
  signed; behavior identical to pre-upgrade.
- `types.Type` native and big-endian encodings: byte-identical to today for
  every input; no codec fence needed for storage formats.
- Nullability, charset, collation encoding paths are untouched end to end
  (`notNull` u16 stays 0/1; `GetNotNull()==1` unchanged; I6).

### Public metadata channels (round-3 findings 2 and 3 — resolved; round-4 finding 1 — resolved)

Review correctly showed that the plan channel alone cannot reach every
place the attributes must render: channels that today read only `atttyp`
have no mask input. One shared Go helper formats `decimal(p,s)` plus
suffixes from `(types.Type, attrs uint32)` (the `FormatSQLColumnType`
family); SHOW CREATE/dump and the checkpoint tool feed it the mask
directly, while the information_schema channel below derives the same
text in SQL for persisted-definition bind compatibility:

- **SHOW CREATE / dump / clone.** Renders from `plan.Type` field 14 via
  `FormatColType` (`build_show_util.go:1261` → `sql_type.go:57`); logical
  dump/restore round-trips through re-parsed DDL; physical backup copies
  `mo_columns` rows (mask carried); clone/snapshot rebuilds from the same
  records; subscription DDL re-derivation by parsing (unchanged from v2).
- **information_schema.COLUMNS.** The view is frozen, versioned DDL whose
  COLUMN_TYPE today is `mo_show_visible_bin(mc.atttyp,3)` on all row
  branches (`pkg/util/sysview/predefined.go:588`, enum branch adjacent),
  and the builtin's typWithLen mode decodes only `types.Type`
  (`func_builtin.go:614-634`) — the mask never enters. v3 proposed a
  three-argument mask-aware builtin in the `mo_show_visible_bin` family
  embedded in the recreated persisted definition; round-4 review
  disproved its downgrade safety: only the two-argument overload exists
  (`pkg/sql/plan/function/list_builtIn.go:15551-15568`), and a persisted
  definition referencing a new overload cannot bind on pre-change
  binaries at all (function-resolution failure, not degraded output).
  The recreated definition must therefore reference **only constructs
  present on pre-change binaries**. Resolution:
  1. no new builtin ships in any persisted system-view definition. The
     recreated COLUMNS template renders the local row-branch COLUMN_TYPE
     as plain SQL over the existing two-argument builtin, e.g.
     `concat(mo_show_visible_bin(mc.atttyp,3), case when
     upper(mo_show_visible_bin(mc.atttyp,2)) like 'DECIMAL%' then (case
     mc.att_is_unsigned when 3 then ' unsigned zerofill' when 1 then
     ' unsigned' else '' end) else '' end)` — `concat` (registered
     `function_id.go:1063`), `case`, `upper`, and the two-argument
     `mo_show_visible_bin` are all ancient, and this is exactly the
     idiom the frozen template already uses for its BOOL→TINYINT
     override (`upper(mo_show_visible_bin(mc.atttyp,2)) = 'BOOL'`,
     `predefined.go:577-579`) and its MEDIUMINT guard
     (`predefined.go:452-455`) — so the definition binds and evaluates
     identically on pre- and post-change binaries of the same version
     line, with mask values read from data at evaluation time.
     DATA_TYPE keeps the plain `decimal` spelling (MySQL omits
     attributes there; the `func_builtin.go:605-607` decimal special
     case and the existing `split_part(..., ' ', 1)` stripping stay).
     The decimal guard on the CASE keeps unsigned-integer columns
     (mask 1, already suffixed by typWithLen from the type itself) from
     gaining a duplicate suffix;
  2. the view branch (`cross apply mo_view_columns(mt.rel_id)`) and the
     subscription branch cannot express mask rendering in SQL that
     pre-change binaries bind, because the mask lives behind
     table-function output: a new `mo_view_columns` output field is
     rejected by pre-change CNs (`unknown View column metadata field`,
     `pkg/sql/colexec/table_function/view_columns.go:327-328`) and a
     persisted definition referencing it breaks those CNs outright.
     This PR therefore ships the producer extensions **in code only** —
     `mo_view_columns` gains an `att_is_unsigned` output field (column
     list `pkg/sql/plan/view_columns.go:67-75`, producer switch
     `table_function/view_columns.go:283-329`) sourced from the view's
     persisted plan column `Typ.numeric_attributes`, and
     `mo_subscription_columns()` gains the mask
     (`subscription_metadata.go:131-159` config plus its remote query
     at :794) sourced from the publisher's `mo_columns` — but **no
     shipped persisted definition references them this version**.
     COLUMN_TYPE for view-over-attributed-column and subscription rows
     keeps rendering plain `decimal(p,s)` this version (identical to
     pre-fix behavior; documented limitation, no regression). Mask
     rendering on those two branches lands in a later template
     generation once the producer fields are universal (shutdown-upgrade
     ubiquity, not another per-feature floor);
  3. compatibility machinery (mechanism extended in v11, round-11
     finding 1): a frozen pre-attribute template discriminator plus the
     canonical versioned tenant upgrade entry
     (`versions.InformationSchemaColumnsUpgradeEntry`,
     `pkg/bootstrap/versions/information_schema_columns.go:28-47` —
     main's own file, kept verbatim) recreates persisted
     COLUMNS definitions on existing tenants/clusters at version
     transitions (the V46 upgrade pattern: `UpgSql` +
     canonical-definition `CheckFunc`,
     `pkg/bootstrap/versions/v2_0_2/tenant_upgrade_list.go:35-63`);
     fresh initialization authors the new template unconditionally (v28:
     the D5 floor and its deferred-authoring windows are removed under
     the shutdown-only upgrade contract, so a new binary never persists
     the predecessor template and no login-time or steady-state repair
     path exists); the one-shot idempotent versioned entry above —
     delegated from main's v4_0_14/v4_0_17 handles and from the NEW
     v4_0_18 handle this change registers (v29) — recreates every
     pre-change persisted definition at the shutdown upgrade, as
     designed under "COLUMNS initialization and one-shot upgrade
     repair" in the Rollout section. Old binaries keep
     evaluating the persisted old view until the entry recreates it; after
     recreation they still can (the new SQL uses only ancient constructs
     — the property item 1 guarantees);
  4. rollback and downgrade barrier (round-4 finding 1): cross-version
     downgrade against an upgraded catalog is already rejected by the
     upgrade framework itself — when the persisted latest version sorts
     after the binary's final version, upgrade resume reports
     invalid state and refuses
     (`pkg/bootstrap/service_upgrade.go:374`, `canResumeUpgrade` at
     :302-313), so "old binary over a newer catalog" is never a
     supported configuration and needs no extra fence. Same-version-line
     rollback (the supported downgrade shape: rebuilt pre-change binary
     over a catalog at the same version) is safe by construction for
     this change: the recreated COLUMNS definition evaluates on it
     (item 1), mask-bearing `mo_columns` rows are inert to it (server
     consumers treat the value as the legacy boolean), and D7's
     operator procedure strips column attributes first so its reads
     match pre-fix semantics exactly. The unsupported full-rendering
     alternative — recreate all three branches with mask references
     plus a mandatory pre-downgrade template restore — is recorded
     under Alternatives and rejected in favor of the repair-free
     template above.
- **Checkpoint-tool DDL reconstruction (mask consumer 6).**
  v2 claimed no consumer interprets `att_is_unsigned` bits beyond
  writer+cache copy; review disproved that:
  `pkg/tools/checkpointtool/table_dump.go:3953-3954` reads the value
  through `isTruthyCatalogValue` (`:5194-5196`), which accepts only `1` or
  `true` — mask `3` currently yields `Unsigned=false` — and its renderer
  (`:500-508`, with the existing SET-leak guard) can only append a boolean
  `UNSIGNED` suffix from the unchanged `atttyp` blob. The tool gains a
  bit-aware reader (integer mask parse; legacy `1`/`true`/`0`/`false`
  inputs behave identically) and renders ` unsigned zerofill` per the
  bits for decimal columns. Pre-change tool builds reading mask 3 degrade
  to today's lossy output for ZEROFILL decimals only (mask 1 decimal
  unsigned renders fine) — a documented tool-version limitation and a
  no-regression statement relative to pre-fix main.

### DDL propagation of attributes (round-3 finding 4 — corrected)

v2's oracle claimed CTAS discards attributes on the new table; that is
wrong for direct column projections and would silently strip the
non-negative domain. Verified against MySQL 8.0.45 sources: CTAS builds
its column definitions via `generate_create_field` (`sql/field.cc:10437`),
whose direct-field branch constructs `Create_field(tmp_table_field,
table_field)` copying `flags(old_field->all_flags())`
(`sql/create_field.cc:59-66`); `prepare_pack_create_field` then promotes
`flags & ZEROFILL_FLAG/UNSIGNED_FLAG` into `is_zerofill`/`is_unsigned`
including `MYSQL_TYPE_NEWDECIMAL` (`sql/sql_table.cc:4093-4094`); and
`make_field` passes both into `Field_new_decimal`
(`sql/field.cc:9533-9542` → `:9444-9449`).

- A CTAS **direct single-source column** projection preserves the
  attributes: `CREATE TABLE dst AS SELECT x FROM src` keeps `x`'s
  unsigned/zerofill domain, rendering, and enforcement. Propagation
  owner: `genAsSelectCols` (`pkg/sql/plan/build_ddl.go:1683+`), whose
  existing `outputColumnProvenance`/`ProvenanceSingleSource` mechanism
  already identifies direct columns (the enum/set override at
  `:1688-1692` is the in-tree precedent); attributes copy from
  `provenance.Source.Metadata.Typ` into the target `plan.Type` only under
  `ProvenanceSingleSource`.
- Computed expressions (`x+1`, `-x`, aggregates, explicit casts) do **not**
  propagate attributes: the result `plan.Type` carries mask 0 and the
  column stays signed. MySQL's unsigned-arithmetic result typing for
  decimal operands is out of scope (MO expression typing never emits
  unsigned decimals today).
- `CREATE TABLE ... LIKE` resolves and reuses the source `tableDef`
  (`build_ddl.go:2756+`), so attributes propagate automatically through
  `plan.Type` field 14 — recorded here only to close the propagation map;
  no LIKE-specific owner is needed.

### Set-operation output attributes (round-4 finding 3 — resolved)

Round-4 review showed v3 left set operations undefined: with field 14 on
`plan.Type`, `SELECT x FROM zf UNION ALL SELECT x FROM zf` would keep the
ZEROFILL bit and pad the outer display, contradicting MySQL's documented
rule "The ZEROFILL attribute is ignored for columns involved in
expressions or UNION queries." (MySQL 8.0 Reference Manual, Numeric Type
Attributes — fetched copy retained in handler artifacts.)
Verified owner and current behavior on this base:

- `setOperationOutputType` (`pkg/sql/plan/query_builder.go:693-707`) is the
  single shared output-type owner for every set operation; it returns the
  **left branch's** `plan.Type`, adjusting only medium-int width and
  `NotNullable` (UNION/UNION ALL AND-merge nullability; INTERSECT/MINUS
  deliberately retain the left contract). All set-op builders flow through
  it — the project-list builders (`query_builder.go:1385`, `:4999`), the
  pushdown rewrite (`pushdown.go:1275`), recursive/replace
  (`bind_replace.go:424`), multi-value/multi-insert unions
  (`bind_insert.go:6249`, `bind_multi_insert.go:1173`), and the
  prepared-statement resolver pair (`utils.go:7611`, `:7982`).
- Same-storage-type branches elide casts (`query_builder.go:4936-4943`),
  so branch values reach the output unmodified; the output `plan.Type` —
  and therefore its attribute bits — is exactly what
  `setOperationOutputType` returns.

Decided rule, at that single owner (one edit covers every caller):

- **UNION / UNION ALL:** clear the ZEROFILL bit; keep UNSIGNED only when
  every branch carries it (AND-merge, the same shape as the existing
  `NotNullable` merge at `:705`). Set-op outputs are derived rows; UNSIGNED
  on them is packet-metadata only (flags/display length) — domain
  admission never keys on it, because enforcement happens at attributed
  **target** writes, and a set-op output is not
  `ProvenanceSingleSource`, so CTAS over a UNION already falls under the
  computed-expression rule above and lands mask 0 / signed.
- **INTERSECT / EXCEPT (MINUS):** same clearing, applied uniformly —
  these outputs are likewise materialized derived results, not column
  reads; MySQL documents only the UNION case and MO has no countervailing
  contract, so the uniform rule avoids per-operation divergence.
- Non-decimal branches and unattributed decimals are unaffected (I4); the
  clearing touches only `numeric_attributes` and leaves width/scale/
  nullability/collation decisions byte-identical to today.

Consequences recorded for the validation plan: UNION and UNION ALL in
both branch orders over signed/unsigned/zerofill mixtures render
unpadded and report no ZEROFILL flag; CTAS over a set operation produces
plain `decimal(p,s)` and accepts negatives; `INSERT INTO attributed SELECT
... UNION ...` still enforces 1264 through the target's admission.

## Domain enforcement mechanism (round-2 review finding 2 — corrected; round-6 findings 1 and 2 — resolved; round-7 findings 1-4 — resolved; round-8 findings 1 and 2 — resolved; round-9 finding 2 — resolved in the strict-validation boundary of the same DEFAULT bullet; round-10 finding 1 — resolved in the persisted-expression policy replay boundary)

The sql-mode semantics reuse the existing assignment-cast machinery, but the
sign admission is staged **before** scale rounding, matching MySQL's
store_value order:

- A new plan-composed admission step runs in front of the existing assignment
  cast chain whenever the target column carries the unsigned DECIMAL
  attribute. Plan-wise: DML builders already wrap target values in
  `cast_assign`/`cast_strict`/`cast_ignore`
  (`build_constraint_util.go` `forceCastExpr2WithProcess`,
  `forceAssignmentCastExprWithProcess`, `makePlan2AssignmentCastExpr`
  `make.go:969`); the same sites compose the admission step for attributed
  decimal targets, in insertion order `admission → cast_assign`. The
  admission operand is the unmodified source expression, so even a plan-time
  constant fold of the composition evaluates admission on the unrounded
  value before the cast:
  1. Admission evaluates, per row, on the source value:
     - decimal source: sign directly;
     - string source: classify with the shared assignment token grammar
       defined below (round-6 finding 2), never with an independent
       raw-string prefix scan and never at target scale;
     - float32/64 and signed int sources: sign of the converted value
       (matches MySQL's `double2my_decimal`/`int2my_decimal` full-precision
       conversion feeding `store_value`);
     - unsigned-int source: always in domain.
  2. If negative (nonzero): strict ⇒ ERROR 1264 (statement aborts before any
     store; no partial writes — the same per-statement atomicity the strict
     assignment errors already rely on); non-strict/IGNORE ⇒ substitute 0 at
     target scale and append one warning 1264 via the existing warning
     sink (`appendDecimalCastRangeWarning` pattern).
  3. Otherwise pass through; `cast_assign` then performs the ordinary
     scale/precision conversion and its existing range checks on positives.
- String classification (round-6 finding 2 — corrected in round-7 findings
  2-4; round-8 finding 2 — classification boundary extended to the complete
  conversion outcome). Admission classifies the value the assignment cast
  will actually **store**, derived through the same shared logic the cast
  runs before target-scale quantization — never an independent grammar —
  over the **complete pre-quantization conversion outcome** (success, or
  failure at tokenization, or failure after tokenization when complete
  consumption is validated), following the source sub-paths of
  `strToDecimal64/128/256` (`func_cast.go:8562+`, `:8903+`, `:8999+`):
  - text varlena, conversion succeeds (token accepted by
    `parseCastNumericToken`, `:7446-7479`, AND complete consumption
    validated — `decimalCastCoefficient` rejects any scanned prefix that
    does not consume the whole expansion, `:8656-8664`): expand radix
    digits exactly as the cast does
    (`prefixedDigitsToDecimalString`, `:7482-7491` — `big.Int.SetString`
    keeps a sign embedded after a radix prefix, and the token grammar only
    rejects a **leading** second sign at `:7450-7452`) and scan the
    expansion exactly as `decimalCastCoefficient` does
    (`scanDecimalPrefix` facts, `:8010-8085`, via `:8656-8667`). The
    effective sign is the cast's own combine rule — the stored value is
    negated iff the scanned sign differs from the token sign (`negative !=
    token.negative`, `:8697` and the `:8864`/`:8897` analogues) — so the
    violation test is: effective negative iff `scanned != token`, AND the
    scanned mantissa contains a nonzero digit (`facts.first >= 0`;
    exponents never affect zeroness). `'0x-1'` stores `-1.00` and is a
    violation; `'-0x-1'` stores `1.00` and is admitted; `'0x-0'` stores
    `0.00`. The radix expansion is the cast's own bounded,
    content-proportional allocation, shared with classification rather
    than duplicated (round-6's "no coefficient materialized" claim is
    corrected: base-10, the dominant case, needs no expansion; radix forms
    reuse the conversion's single expansion). Radix strings remain an
    existing MatrixOne cast extension this contract neither widens nor
    narrows.
  - text varlena, conversion failure (round-7 finding 2 — corrected in
    round-8 finding 2 to cover BOTH failure stages): the value the cast
    retains decides, and retention fires on the complete conversion error,
    not only on tokenizer rejection. IGNORE retention is triggered by
    `err != nil` from `parseDecimal64*CastString` (`:8606-8622`); a string
    can be token-**accepted** yet fail afterwards: after the sign split a
    body like `'1junk'` has no leading second sign and no whitespace, so
    the token is admitted (`:7446-7479`), then `decimalCastCoefficient`
    rejects it because the scanned prefix does not consume the whole
    expansion (`:8656-8664`) — and IGNORE retains `'-1'` and stores
    `-1.00`. This later-failure stage is the exact path the existing
    `'12.34tail'` contract pins (`func_cast_ignore_test.go:297-303`). The
    classifier therefore applies at the same boundary the cast applies it:
    - INSERT IGNORE / LOAD IGNORE (`castModeAssignmentIgnore` via
      `NewAssignIgnoreCast`, `:1018-1020`), non-special syntax
      (`isAssignmentSpecialNumericSyntax`, `:7977-7993` — radix-prefixed
      bodies and `nan`/`inf`/`infinity` skip retention and keep the
      existing error): the cast retains the numeric prefix
      (`assignmentDecimalPrefix`, `:7794-7799`, the same
      `scanDecimalPrefix` scan) and stores it converted with a truncation
      warning (`:8606-8614`, `:8948-8956`, `:9044-9052`). Admission
      classifies that **retained prefix before its rounding** (pure
      decimal grammar: sign plus nonzero digit): tokenizer-rejected
      `'-1 junk'`/`'-0.004 junk'` and consumption-rejected `'-1junk'`,
      `'-0.004junk'`, `'-1e'` (incomplete exponent — the prefix scan
      rolls the end back to the `e` without digits, `:8081-8083`, full
      consumption fails, IGNORE retains `'-1'`) all retain negative
      nonzero prefixes and are violations; `'1 junk'`/`'1junk'` store
      positive prefixes with only the truncation warning; `'0junk'`/
      `'-0 junk'`/`'-0junk'` retain zero prefixes — truncation warning
      only, no 1264. A retained prefix whose own conversion overflows
      (prefixed range, `'999...9tail'`) keeps the cast's range error —
      the retention re-parse fails and the cast returns its original
      error (`:8609-8613` falls through; contract
      `func_cast_ignore_test.go:305-311`) — nothing is stored and
      admission never runs.
    - IGNORE, no retained prefix at either stage (`'abc'`): the cast
      stores the zero default with its invalid-input warning
      (`:8615-8619`) — always non-negative, no violation possible.
    - strict and non-strict non-IGNORE (`castModeStrictStringWidth` /
      `castModeAssignment`, selected by `NewAssignCast` `:1009-1015`): a
      conversion failure at EITHER stage is the cast's own lexical or
      range error, returned unchanged — admission never runs and
      diagnostics are byte-identical to today (executed fact:
      `parseDecimal64/128/256CastString` reject `'-1junk'`,
      `'-0.004junk'`, `'-1e'`, `'12.34tail'` with errors).
  - IsBin varlena (`types.ParseDecimal64FromByte` branch, `:8630`;
    round-7 finding 3): the cast accumulates an **unsigned big-endian byte
    magnitude** (`y = y*256 + byte`, `decimal.go:2163-2181`; the
    decimal128/256 analogues `:2650-2689`), so every IsBin value is
    non-negative by construction — bytes `0x2d 0x31` (ASCII `'-1'`) store
    `115.69`, and a textual sign classifier would clip a positive
    assignment. IsBin sources are always admitted; empty input and width
    overflow keep the cast's existing errors (`decimal.go:2164-2166` and
    each family's `Mul*`/`Add*` overflow path).
  - charset-255 prefix branch (`:8572-8580`): `parseMySQLDecimal*Prefix`
    (`:8777-8791`) classifies by its own scan facts over the prefix —
    violation iff its negative flag AND `facts.first >= 0`, before its
    clamping.
  One shared helper family (exposing the cast's pre-quantization facts:
  token sign, scanned sign, nonzero-digit fact, retained-prefix decision
  covering BOTH failure stages) serves DML and LOAD, because LOAD's staged
  text (below) is always text varlena in the same source kind.
- Diagnostic ownership and ordering for string sources (round-7 finding 2;
  round-8 finding 2 — same ownership across both conversion-failure
  stages). Lexical and range diagnostics stay owned by whichever component
  performs the
  effective conversion; the domain diagnostic (1264) is owned by admission:
  - full-consumption negatives: no lexical diagnostic exists; admission
    emits the single 1264 (strict error, or lenient clip warning).
  - IGNORE retained-prefix negatives at EITHER failure stage (token
    rejected, or token accepted but consumption-invalid): admission
    substitutes `0` before the
    cast runs, so it also emits the retained-prefix truncation warning
    through the existing appender (`appendTruncatedAssignmentConversionWarning`
    pattern — same `WARN_DATA_TRUNCATED` code and message) FIRST and the
    1264 clip warning SECOND, matching MySQL's `store()` order (parse
    diagnosis before range adjustment) and today's IGNORE diagnostic
    contract with no double emission.
  - IGNORE no-prefix invalid input, IGNORE retained-prefix overflow
    (prefixed range), and all non-IGNORE conversion failures (lexical or
    range, at either stage): admission abstains; the cast's existing
    invalid warning (plus zero default) or its returned error stands,
    unchanged.
- VALUES literal materialization boundary (round-6 finding 1 — corrected in
  round-7 finding 1). Both VALUES builders call `MakeInsertValueConstExpr`
  (`build_constraint_util.go:1964`) first and continue on success — the
  insert value scan (`bind_insert.go:5865-5872`) and `buildValueScan`
  (`build_constraint_util.go:2266-2276`) — and its decimal branches
  (`:2071-2084`) parse the literal text directly at `typ.Width, typ.Scale`
  (`util.SetInsertValueDecimal64/128`, `pkg/sql/util/eval_expr_util.go:1033-1062`,
  `:1086-1115`): a plan-time target-scale quantization that would turn
  `VALUES ('-0.004')` into a const `0.00` before admission could recover
  its sign. v6 assumed skipping that fast path reaches natural-scale
  binding; round-7 finding 1 showed the generic route is also
  target-aware: both builders construct their literal binders with the
  column type (`NewDefaultBinder(..., col.Typ, ...)`, `bind_insert.go:5826`;
  `sourceType := col.Typ`, `build_constraint_util.go:2234`), and
  `bindNumVal` receives that type via `defaultValueBindType`
  (`base_binder.go:13060-13071`), converting numeric literals against the
  destination type at bind time: the `P_decimal` branch parses directly at
  target width/scale (`:11477-11517`), while plain fractional literals
  (`1.5`/`0.004`) and scientific forms are `P_float64` in this grammar
  (executed DEFAULT probe: `ValType=6`) and take the target-typed
  `returnDecimalExpr` path (`:11586-11590`)
  producing `cast(lit '.004' -> decimal(10,2))`. For a
  decimal64/128/256 target whose column `plan.Type` carries a nonzero
  attribute mask — in scope as `col.Typ` at both sites
  (`bind_insert.go:5794-5795`, `build_constraint_util.go:2208-2209`) — the
  builder BOTH skips `MakeInsertValueConstExpr` AND binds the literal with
  a **target-free** binder, through the suppression machinery that already
  exists: `defaultValueBindType` returns an empty `plan.Type` under
  `integerArgumentSourceContext`/`suppressDefaultValueBindType`
  (`base_binder.go:13061-13069`), and both builders already select a
  target-free binder for integer targets (the insert value scan builds
  `funcBinder` with `plan.Type{}` at `bind_insert.go:5835` and selects it
  at `:5882-5884`; `buildValueScan` resets `sourceType` to `plan.Type{}`
  at `build_constraint_util.go:2234-2239`). The design extends that same
  predicate to attributed decimal targets: with an empty bind type,
  `bindNumVal`'s natural-typing branches run — decimal literals through
  `makePlan2DecimalExprWithType` (`make.go:68-110`, width/scale derived
  from the literal text, sign preserved), scientific literals keep their
  float path (`base_binder.go:11591-11597`, classified on the double
  value), quoted literals stay varchar consts under the string rule — and
  the composed admission then sees the unrounded source on every literal
  form. Unattributed targets keep the fast path and the target-aware
  binder byte-identical (I4), and the INSERT IGNORE special-type handler
  (`makeInsertIgnoreMySQLSpecialTypeConstExpr`, `bind_insert.go:5854-5863`,
  `build_constraint_util.go:2254-2264`) is untouched — IGNORE literals on
  the target-free route take admission's IGNORE clause.
- DDL DEFAULT source-preservation boundary (round-8 finding 1). Every DDL
  DEFAULT binding owner constructs a `DefaultBinder` that carries the
  destination `plan.Type` and binds the source expression BEFORE the
  admission/assignment wrapper runs: `buildDefaultExprWithColumnsObserved`
  (binder at `build_util.go:995-1001`, wrapper `makePlan2AssignmentCastExpr`
  at `:1029`), `buildOnUpdate` (`build_util.go:1141-1154`), CTAS default
  reconstruction `buildCTASDefaultFromOrigin` (`build_ddl.go:1961-1980`,
  including the `finalizeCTASDefaults` rebind at `:2020`), and dump/restore
  re-derivation (`table_dump_binding.go:332`, `:355`). Numeric literals
  reach `bindNumVal` with that destination type (`defaultValueBindType`,
  `base_binder.go:13060-13071` via the dispatch at `:75`) and are converted
  against target width/scale AT BIND TIME: an executed probe confirms
  `DEFAULT -0.004` and `DEFAULT -4e-3` on `DECIMAL(10,2)` bind to
  `unary_minus(cast(lit '.004' / '4e-3' -> decimal(10,2)))` — an inner
  target-typed cast whose scale-2 evaluation rounds any |v| < 0.005 to
  zero before admission ever sees the sign (`types.ParseDecimal64`
  rounds and discards the scale error, `decimal.go:2138-2139`). Quoted
  literals are unaffected: `bindNumVal`'s `P_char` branch returns a raw
  varchar const (`base_binder.go:11625-11627`). The same predicate the
  VALUES contract
  defines therefore extends to these DEFAULT binders: for a
  decimal64/128/256 destination whose column `plan.Type` carries a nonzero
  attribute mask, the binder takes the target-free route (empty
  `defaultValueBindType` under the existing suppression knobs,
  `base_binder.go:13061-13069`), so decimal literals take natural typing
  with their natural width/scale (executed probe control: the untyped
  binder yields `unary_minus(cast('.004' -> decimal(3,3)))` — sign and
  magnitude preserved), scientific literals keep the float path, and
  quoted literals keep today's raw-const behavior; unattributed
  destinations bind byte-identically to today (I4). Strict
  constant-validation boundary (round-9 finding 2 — constant detection
  on the original bound source, before composition): DDL authoring
  validates constant defaults regardless of session sql_mode, following
  the MEDIUMINT precedent (`build_util.go:1043-1068`) — with a node
  correction that the precedent's own shape makes necessary. Under
  MEDIUMINT the composition is `cast_assign(source, target)`, so the
  precedent's source check (`function.IsConstant(cast.Args[0])`,
  `:1049-1052`) reads the raw source. This design's composition is
  `cast_assign(admission(source), target)`, and admission is
  sql-mode-dependent — it must resolve sql_mode at execution time for
  plan-cache/prepared-statement correctness, exactly like `cast_assign`,
  whose overload is flagged volatile for that reason
  (`list_operator.go:2741-2754`, `volatile: true` with the
  prepared-statement comment); `function.IsConstant` rejects any
  function node that is volatile or carries row-scoped diagnostics
  (`function.go:420-441`). Reading `cast.Args[0]` here would therefore
  read the admission node, which is never constant, and the strict path
  would never fire (round-9 executed probe: the composed gate is false
  while `IsConstant` on the raw source is true). Detection instead runs
  on the RAW bound source BEFORE composition: each owner captures the
  target-free source expression `src` produced by its binder predicate
  above, and constant means `function.IsConstant(src, false)`. The
  validation itself also needs both layers strict: a separate
  authoring-time STRICT validation composition is built from `src` —
  the admission family's strict entry-point applied to `src` (selected
  by name at plan time on the `cast_strict` vs `cast_assign` precedent,
  used only inside this validation composition), wrapped in the strict
  target cast via `forceAssignmentCastExprWithName(bindCtx, ..., typ,
  "cast_strict")` (`:1053-1057` precedent) — so a non-strict authoring
  session cannot route the inner admission through its clip+warning
  mode branch; that composition is force-evaluated in an isolated
  executor (`:1058-1063` precedent), a domain violation from the strict
  admission (the out-of-range class) maps to ER_INVALID_DEFAULT (1067)
  with the existing mapping (`:1066-1069`, `mapDDLAssignmentCastError`
  `:1268-1274`), and every other error keeps the existing
  `mapDDLAssignmentCastError` outcome. The STORED default expression
  remains the mode-sensitive `cast_assign(admission(src), target)`
  composition — strict validation adds no behavior to DML-time default
  evaluation, and at DML replay the persisted boundary is re-policed by
  the persisted-expression policy replay boundary below (round-10
  finding 1), which switches BOTH layers to the current statement
  policy (I4 for everything it does not match). One shared validation helper owns capture, constant
  detection, strict-composition build, evaluation, and error mapping,
  and every DDL DEFAULT owner above uses it — the CREATE/ALTER site
  (`build_util.go:995-1068`), ON UPDATE (`:1141-1167`), CTAS default
  reconstruction (`build_ddl.go:1961-1993`, `finalizeCTASDefaults`
  `:2020`), and dump/restore re-derivation (`table_dump_binding.go:332`,
  `:355`) — so a reparsed definition validates identically to the
  original authoring. Every literal class is rejected for negative constants —
  unquoted `-1`/`-0.004`, scientific `-4e-3`, quoted `'-0.004'`/`'-1.5'`
  (the quoted source classifies through the string rule at admission) —
  while `DEFAULT 0`/`DEFAULT -0.00`, positive constants, and `DEFAULT
  NULL` pass, and volatile defaults (`now()`, `uuid()`, `rand()` — not
  constant) are never evaluated early and are enforced per use by the
  same admission at DML under the replayed statement policy (round-10
  finding 1 below). CTAS reconstruction and dump/restore
  re-derivation inherit the identical binder predicate and strict
  validation (CTAS `ConstantFold` at `build_ddl.go:1984-1988`; ON UPDATE
  evaluation at `build_util.go:1158-1167`).
- Runtime mode is read at execution (`isStrictSqlMode(proc)`), so prepared
  statements and plan caches keep session-mode semantics, exactly like the
  existing assignment casts. Admission never runs outside DML/LOAD assignment
  contexts, so explicit `CAST` and comparison/planning paths are untouched
  (I4).
- NULL inputs: pass through unchanged; admission produces no warnings;
  clipping to 0 never manufactures NULL; `plan.Type.notNullable` propagation
  is unchanged (I6).

This keeps the eval type system untouched (no `types.Type` change): the
admission step is a plan-level function whose input is the raw source
expression and whose shape parameters come from the target `plan.Type` —
all information available at the existing DML/LOAD build sites.

`needsSameTypeAssignmentCast` (`build_constraint_util.go:1346`) is extended so
same-type decimal→decimal assignments still execute the admission path for
attributed targets (otherwise `INSERT INTO t SELECT x FROM t`-style same-type
writes would skip enforcement).

- **Persisted-expression policy replay boundary (round-10 finding 1).**
  Persisted assignment boundaries are composed at DDL with the
  mode-resolved admission entry and `cast_assign` (DEFAULT/ON UPDATE per
  the bullet above; generated columns by their authored wrapper over the
  user expression), but DML replay reaches them through three consumer
  classes that never switch the INNER admission to the statement policy:
  `getDefaultExprForAssignment` returns every non-BLOB/TEXT default —
  DECIMAL included — unchanged (`build_util.go:2590-2607`, early return
  at `:2595-2596`; the BLOB/TEXT branch at `:2600-2606`, which strips the
  persisted wrapper and re-forces it under the statement's `ignore`
  flag, is the strip-and-rebuild precedent this mechanism generalizes);
  `applyGeneratedColumnAssignmentCast` replaces only the OUTER wrapper
  by name and rebuilds from `f.Args[0]` (`:2423-2456`), so an IGNORE swap
  to `cast_ignore` would still wrap the persisted mode-resolved
  admission node; and the ON UPDATE copies are used verbatim
  (`bind_update.go:833`, `bind_insert.go:2897` — the user-written ODKU
  assignment path at `bind_insert.go:2860` receives the statement-policy
  wrapper while the copied ON UPDATE expression does not). Under strict
  sql_mode a negative value produced by a persisted expression —
  `DEFAULT (-1-RAND())`, a generated unsigned DECIMAL reading a
  negative signed column, or an ON UPDATE expression evaluating
  negative — would reach the inner mode-resolved admission and ERROR
  with 1264 where MySQL's IGNORE adjusts with one warning, because
  INSERT IGNORE policy never reaches that inner node. One shared
  mechanism restores the COMPLETE assignment boundary at every replay
  site: a typed boundary rewriter owned by the plan builder (homed
  beside `forceAssignmentCastExprWithProcess`,
  `build_constraint_util.go:1470-1482`, on the same build-time policy
  axis as `assignmentCastFunctionName`, `:1441-1461`) that recognizes
  the persisted admission composition by typed shape — an
  assignment-cast-family outer node (the name set the existing
  consumers already match, `build_util.go:2432`,`:2601`) whose arg0 is
  the plan-owned admission node for an attributed decimal target — and
  rebuilds BOTH layers under the current statement policy: IGNORE ⇒
  `cast_ignore` + the admission IGNORE entry (the same entry the DML
  VALUES and LOAD builders pick when `isInsertIgnore`/
  `loadAssignmentIgnore` holds), non-IGNORE ⇒ `cast_assign` + the
  admission mode-resolved entry. Runtime sql_mode strictness stays
  resolved inside the volatile functions (strictness is the
  execution-time axis; IGNORE is the build-time axis), and the
  unmodified source `src` passes through untouched, including every
  user-written subexpression and explicit CAST inside it — the
  recognizer matches only the plan-owned admission node at the boundary
  position, following the existing guard comments
  (`build_util.go:2430-2435`, `:2603-2605`); explicit user CAST
  expressions and non-assignment evaluation paths are never matched
  (I4). Consumers migrated: `getDefaultExprForAssignment` gains an
  attributed-composition branch before its unchanged return — every
  caller inherits it (`bind_insert.go:2808`, `:5474`, `:5804`, `:5877`,
  `:6036`, `bind_multi_insert.go:1112`, `bind_replace.go:1474`,
  `bind_update.go:201`, `build_constraint_util.go:936`, `:1049`) while
  unattributed defaults keep today's path byte-identically;
  `applyGeneratedColumnAssignmentCast` delegates to the shared rewriter
  when the boundary is the admission composition (its legacy
  `cast_strict` upgrade, array, and BLOB/TEXT branches unchanged); and
  the two ON UPDATE copies route the copied expression through the
  shared rewriter with the statement's ignore flag (the `ignore`
  parameter threaded through the update binder,
  `bind_update.go:195-228`,`:243`,`:318`, and `builder.isInsertIgnore`
  on the ODKU update side, `bind_insert.go:2860`,`:2948`; REPLACE keeps
  its existing `false` policy). Strict + INSERT IGNORE then clips the
  negative to `0` at target scale with EXACTLY one 1264 admission
  warning per adjusted row (the already-clipped zero passes
  `cast_ignore`'s range checks without a second domain diagnostic; a
  truncation warning from the source conversion, when one legitimately
  exists, still precedes it per the diagnostics bullet), and the
  statement commits; strict non-IGNORE keeps the v9 error with full
  statement atomicity; non-strict keeps the v9 clip+warning. The
  round-9 DDL strict-validation composition is untouched (it
  force-evaluates the raw source once at authoring; replay uses the
  mode/IGNORE entries at DML). The rewriter is idempotent by contract:
  re-applying it under the same policy is identity, replaying a
  boundary it rebuilt once changes nothing further, and an IGNORE →
  non-IGNORE sequence reproduces the DDL-authored boundary; anything
  outside the boundary shape passes through byte-identically (I4).

## LOAD DATA (round-2 finding 3; round-3 finding 1 — resolved; round-4 finding 2 — resolved; round-5 findings 1 and 2 — resolved; round-6 finding 2 — resolved via the shared string classifier; round-7 findings 2-4 — resolved in the same classifier contract; round-8 finding 2 — resolved in the complete-outcome extension of the same classifier; round-9 finding 1 — resolved in the raw-field preservation boundary of the same CSV staging contract)

v2 promised LOAD mode behavior with a shared plan owner and per-format
staging but left the staging representation undecided. Review correctly
showed why that is not enough: a per-row **natural-scale decimal vector**
cannot exist — one vector carries exactly one `types.Type` (one width, one
scale; `pkg/container/vector/vector.go:101-105`), while one CSV column mixes
natural scales (`1.2`, `1.23`, `-0.004`), and source precision can exceed
the target decimal width before rounding. Round-5 review closed the last
degree of freedom: scan output vectors are allocated **once** in `Prepare`
from the plan column types (`pkg/sql/colexec/external/external.go:251-278`,
guarded by `external.ctr.buf == nil`), and the compiled projection's
function executors bind their declared types at `Init` and reset the result
type before every evaluation
(`pkg/sql/colexec/evalExpression.go:1115-1148`, `:1906-1909`) — so the
staging type per attributed column must be **one fixed type for the whole
statement**, not a per-file choice. The staging representation is decided
below against the seams that actually exist:

- Modern and legacy LOAD projections exclude DECIMAL targets from assignment
  casts (`pkg/sql/plan/bind_load.go:338-355` applyLoadAssignmentCasts and
  `pkg/sql/plan/build_load.go:786-796` gate on BLOB/TEXT/TIME/MEDIUMINT only).
- Serial CSV converts the raw field straight to the **target scale** inside
  the scanner (`pkg/sql/colexec/external/external.go:2435-2471`,
  `ParseDecimal64/128/256(field.Val, width, targetScale)`), destroying the
  source sign before any plan predicate could see it.
- Parquet rescales into the target precision/scale during page decode
  (`parquet.go:3812+` `parquetDecimalValueToDecimal64(proc.Ctx, st, v,
  precision, scale)`; exact raw copy only when `canUseRawDecimalMapping
  (st, precision, scale)` holds at `:2129/:2190/:2247/:3605`).
- The Arrow MEDIUMINT validator returns InvalidInput with no clipping or
  warning channel (`pkg/container/arrowbridge/budget.go:121-150`).

Resolved design:

- **Shared assignment owner.** `applyLoadAssignmentCasts` (bind_load.go) and
  the legacy projection loop (build_load.go) extend their predicates to cover
  attributed DECIMAL targets and wrap the projected values in the same
  admission → `cast_assign` composition as DML (one owner for mode semantics,
  warnings, and atomicity); on the parallel path this overrides the generic
  `makeCastExpr` slot (`build_load.go:783`, same override shape as the
  existing MEDIUMINT/TIME/BLOB cases). `loadAssignmentIgnore`
  (`bind_load.go:324-336`) keeps its mapping: `IGNORE` ⇒ ignore-assignment;
  `LOCAL` non-replace ⇒ ignore-assignment; `LOCAL ... REPLACE` and remote
  LOAD follow session sql_mode.
- **Staging representation — decided (round-5: one uniform type).** For an
  attributed decimal target the staged value is **text in a `T_varchar`
  staging vector on every format and every file of the statement**. This
  single type contract replaces v4's per-file choices (round-5 finding 1:
  Prepare-allocated scan vectors and compiled-executor type binding make
  per-file staging types unimplementable); admission's LOAD input is
  therefore always the string source kind, and `cast_assign`'s
  string→decimal conversion is the only conversion — the decimal/float/int
  source-kind branches still serve DML, unchanged.
  - CSV and JSONline (file readers funnel through `CsvReader`,
    `reader_csv.go:42`, into `materializeOneRow` → `getColData`; Kafka
    JSONLINE rows land in the same `getColData` per
    `external.go:1859-1895`, though LOAD DATA itself uses only the file
    readers): the staged value is the **raw field text**, typed
    `types.New(types.T_varchar, 0, 0)`. This reuses the scanner's existing
    text-staging seam rather than inventing a new one: `makeType`
    (`external.go:1343-1348`) allocates VARCHAR when the staging flag
    holds, and `getColData` appends the un-parsed field bytes and returns
    early (`external.go:2026-2031`), skipping the target-scale decimal
    cases at `:2435-2471`. The opt-in predicate `loadTimeTextInput`
    (`external.go:1904-1908`) is generalized from TIME-only to
    TIME-or-attributed-decimal for `ExternType_LOAD`; parallel LOAD
    already VARCHAR-stages every non-array column
    (`flag = param.ParallelLoad`, `external.go:259-266`), so new scanner
    work touches only the serial path. **Raw-field preservation
    boundary (round-9 finding 1):** "raw field text" must reach staging
    unmodified, but `getColData` first applies the LOCAL non-strict CSV
    numeric-prefix rewrite to UNQUOTED decimal fields
    (`shouldApplyLoadDataNonStrictAdjustments` at
    `external.go:1510-1516` — LOAD + `Extern.Local` + CSV +
    non-strict session — gating `isLoadNumericAdjustedValueType`
    `:1578-1588`, which includes all three decimal families, and the
    rewrite at `:2002-2009`, executed before the staging branch at
    `:2026-2031` on the single code path that BOTH readers share:
    serial `materializeOneRow` → `getColData`,
    `reader_csv.go:243`→`external.go:1859-1895`, and the parallel
    varchar staging under `param.ParallelLoad`). Executed premise probe
    of `loadDataNonStrictNumericPrefix` (`:1590-1631`): `'-0x1'` ->
    `'-0'`, `'-0b1'`/`'-0o7'` -> `'-0'`, `'0x-1'` -> `'0'`,
    `'-0x-1'` -> `'-0'`, `'0x123'` -> `'0'`, `'-1junk'` ->
    `'-1'`, `'-1 junk'` -> `'-1'`, `'-0.004junk'`/`'-0.004 junk'`
    -> `'-0.004'`, `'-1e'` -> `'-1'`, `'12.34tail'` -> `'12.34'`,
    `'abc'`/`''` -> `'0'`, while complete exponents survive
    (`'-1e2'` -> `'-1e2'`) — so admission would see signed zero for
    radix forms that must classify negative-nonzero (`'-0x1'`
    requires the 1264 warning; `'0x123'` must store `291.00`), would
    lose every retained-prefix truncation diagnostic (`'-1junk'`,
    `'-1e'`), and would lose the invalid-input diagnostic for
    `'abc'`, contradicting this section's own CSV radix and IGNORE
    matrices under both readers. The rewrite branch therefore gains an
    attributed-decimal exception: when the target column's
    `plan.Type` (the same `col.Typ` the staging decisions already read
    at `external.go:268`/`:1343-1348`) carries a nonzero attribute
    mask, the raw trimmed field bytes stage unmodified; unattributed
    decimal targets keep today's rewrite byte-identically (I4). One
    gate inside `getColData` covers serial and parallel readers
    because both funnel through it. Preserved policies (unchanged for
    every target, attributed or not): whitespace trimming
    (`:1976-1979`), NULL mapping (`field.IsNull`/`mappedNull`/
    trimmed-empty -> null stage, `:1990-1999`), the empty-field `"0"`
    fill (`emptyNumericField`, `:1992-1995` — admission receives
    `"0"` and admits it), the quoted-field exemption from the rewrite
    (`field.HasStringQuote`, `:2005-2008` — quoted forms were always
    staged raw and classify through the same string rule),
    error-tolerant row rollback (`getOneRowData`/`appendErrorRow`,
    `:1766-1832`), the strict CHAR oversize check (`:2021-2025`), and
    the temporal/`char` non-strict normalizations for their own
    types. Unattributed targets keep today's
    parse (I4).
  - Parquet: for attributed targets the mapper stages **text rendered
    from the decoded value**, never a target-rescaled decimal — the
    staging decision keeps the round-4 representability bound (a
    `DECIMAL(pf,sf)` staging vector cannot hold file precision above the
    widest engine decimal, width 76,
    `pkg/container/types/types.go:869-871`) and adds the round-5
    allocation bound (finding 2: `parquetDecimalScale` reads the file's
    declared int32 scale unvalidated, `parquet.go:4790-4795`, so
    fixed-point rendering at an arbitrary declared scale could amplify a
    tiny field into O(scale) bytes; today's importer is immune because
    `roundScaledDecimalBigInt` returns zero for oversized scales without
    expanding, `parquet.go:3886-3895`, and the batch byte-budget check
    runs only after mapper materialization,
    `parquet.go:5143-5151`). Encoding rule, exact value
    `coeff × 10^-sf` with `coeff` the unbounded `big.Int` from the
    existing decoder (`parquetDecimalValueToBigInt`/
    `decimalBytesToBigInt`, `parquet.go:3750-3771`):
    - well-formed fixed-point domain `0 <= sf <= 76` and `pf <= 76`:
      source-scale fixed-point text (the CSV-shaped form, e.g. `-0.004`,
      `1.00`); rendered length <= `pf + 2` bytes — bounded by the
      schema's own sane bound;
    - otherwise (`pf > 76`, `sf > 76`, `sf < 0`, or `sf > pf` —
      malformed or extreme schemas): **bounded scientific text**
      `coeff-digits` + `e` + `-sf` (e.g. a `DECIMAL(80,79)` holding
      `1.0` renders coefficient `10^79` as `1` + 79 zeros + `e-79`; a malformed
      `DECIMAL(80, 2147483647)` field containing coefficient `1`
      renders `1e-2147483647` — under 16 bytes, versus gigabytes of
      fixed-point expansion). Rendered length is
      `digits(coeff)` + ~13 bytes, and `digits(coeff)` is bounded by the
      physical content actually read (<= ceil(physical bytes * 2.41)), so no schema
      claim can trigger an allocation beyond the byte budget already
      spent decoding the value. The existing parse path consumes this
      form: `ParseDecimal64/128/256` accept `e`/`E` with an int32-bounded
      exponent (`pkg/container/types/decimal.go:2053-2065`, `:2255-2267`,
      `:2422-2434` — the round-4 exponent admission cases already
      relied on this), and scale application is allocation-free for
      extreme exponents (`Decimal64.Scale` returns 0 for `n < -19`,
      `decimal.go:360-366`; the `Decimal128` analogue at `:440-446`),
      so `1e-2147483647` into `DECIMAL(10,2)` rounds to `0.00` stored
      exactly like today's zero-from-rescale, and `-1e-2147483647`
      fails admission (negative nonzero mantissa — the exponent cannot
      turn a nonzero mantissa zero), preserving negative-near-zero
      enforcement at any magnitude;
    - STRING-physical decimals yield the raw field text; int32/int64
      physicals render exact integer text; float/double physicals render
      shortest round-trip text (both sign-faithful); NaN/Inf inputs to a
      decimal target keep today's failure outcome (parse error on the
      rendered token; recorded as an implementation-verified path).
  - Arrow: `arrowbridge` keeps shape/nullability validation; attributed
    targets render the Arrow value to text into the scan-owned staging
    vector — borrowed Arrow buffers are never mutated or padded in
    place; clipped values exist only downstream of admission. The
    MEDIUMINT reject-only validator is explicitly not reused for this
    contract.
- **Staging-to-projection type contract (round-5 finding 1 — decided).**
  The staging vector for an attributed decimal column is allocated once
  per statement in `Prepare` through the existing `makeType` staging
  seam (`external.go:251-278`; with the staging predicate true the
  allocated type is `T_varchar(0,0)`, identical on serial and parallel
  paths). Every reader **appends** into those pre-allocated vectors —
  the Parquet mapper takes and fills `bat.Vecs[colIdx]`
  (`parquet.go:4997-5052`, `:5283`; page rollback reuses the same
  vectors, `:5317`) — so **no code path replaces a scan vector with a
  differently typed one**: vector shape and ownership are invariant for
  the statement. The compiled LOAD projection is therefore compiled
  once against `T_varchar` inputs and the target decimal output;
  function executors bind that contract at `Init` and re-assert it each
  evaluation (`evalExpression.go:1115-1148`, `:1906-1909`), the
  admission/expression overloads are uniform, and a differing-schema
  multi-file load (e.g. `DECIMAL(40,..)` then `DECIMAL(80,..)` files)
  runs through the **same compiled plan** — the union of the per-file
  encoding rules above always yields text. NULL values stage as null
  entries (Parquet nulls are recorded, never rendered); empty pages
  produce zero-row batches, which the existing row-count paths already
  tolerate. Default-value handling (`appendLoadDefaultProjections`,
  `build_load.go:806`) is untouched (defaults go through D9 admission).
- **Precision bounds.** Textual admission is **lexical** and shares the DML
  string classifier defined above (round-6 finding 2, corrected in round-7
  findings 2-4): staged fields are text varlena, so classification uses the
  token-accepted and retained-prefix branches — on the trimmed field
  (`getColData` trims non-string fields at `external.go:1973-1979`;
  mapper-rendered text needs no trim; the classifier's own `TrimSpace` is
  the same one the cast applies) it determines the effective sign (token
  sign combined with the scanned sign exactly as the cast does, so a
  staged `'-1 junk'` under LOAD IGNORE classifies on its retained `-1`
  prefix and `'0x-1'` classifies negative) and whether the mantissa digit
  run contains a nonzero digit. Base-10 fields — including every mapper
  rendering form above — need no coefficient; radix fields reuse the
  cast's single bounded, content-proportional expansion. Mixed natural
  scales, exponent forms (`-2.5e-3`, `1e2`, and the scientific staging
  forms above — the exponent cannot turn a zero mantissa nonzero), quoted
  vs unquoted fields, and literals of arbitrary length impose no
  representability requirement on admission; `cast_assign` then performs
  the ordinary width/scale conversion with its existing overflow/truncation
  semantics, whose exponent/scale handling is verified allocation-free
  above. Staging and admission together add no allocation beyond the
  conversion the load already performs.
- **Buffer ownership.** Scanner staging copies field bytes or renders
  decoded values into the scan-owned pre-allocated vector
  (`vector.AppendBytes(..., false, mp)`); the projection allocates its own
  output vectors; the staging vector is never mutated in place and Arrow
  borrowed buffers are never written to. On strict-mode abort the
  existing page/batch lifecycle cleans up (`closePagesOnError`,
  `parquet.go:5146`; page rollback at `:5317`), the scan batch is freed
  with the operator, and LOAD publishes its staged output only on
  statement success, so a strict-mode abort leaves zero rows committed
  (existing LOAD atomicity preserved).
- **Warning ownership.** Only the shared plan-composed admission/
  `cast_assign` appends 1264 warnings through the process warning sink;
  scanner staging never emits domain diagnostics for attributed decimals
  (no double-counting).
- **Mode matrix (public contract).** Strict: 1264 error, statement aborts,
  zero rows committed. Non-strict: row stored as 0 with a per-row 1264
  warning; other rows unaffected. IGNORE: clip+warning. LOCAL non-replace:
  clip+warning per the existing lenient policy; the "LOCAL must not relax
  strict parsing of unrelated input column types" rule is preserved (only
  the assignment attribute path becomes lenient).

## Remaining decisions

- **D4 (ZEROFILL display).** Attributes are column metadata: direct column
  reads of ZEROFILL DECIMAL pad the integer part to `p-s` digits (matching
  MySQL `val_str`). Protocol metadata: result `MysqlColumn` flags set
  `UNSIGNED_FLAG` (and `ZEROFILL_FLAG`) for attributed columns; the
  plan-driven result-metadata builder (`setMysqlColumnTypeInfo` /
  `convertEngineColumnTypeToMysqlType`, `pkg/frontend/util.go:2101+`, and its
  plan-type callers) gains the attribute from the output `plan.Type`.
  `ColumnDefinition41` display length for attributed decimals follows MySQL
  (`mysqlDecimalDisplayLength(precision, scale, signed=false)`,
  `pkg/frontend/util.go:2063`). Padding is one shared helper applied at
  **every** serialization site whose cell text reaches a user — round-4
  finding 4 enumerated the independent decimal formatters that a
  text-row-only change would leave unpadded (verified on this base):
  1. text protocol rows — `appendResultSetTextRow`
     (`pkg/frontend/mysql_protocol.go:2821`) and
     `appendResultSetTextRow2` (`:3424`);
  2. binary protocol rows — `appendResultSetBinaryRow` (`:2572`, decimal
     branch `:2734-2743` via `mrs.GetString`) and
     `appendResultSetBinaryRow2` (`:3123`, decimal branch `:3309-3316`
     via `GetDecimal` over `ColumnSlices`); both transmit decimals as
     length-encoded strings, so the same pad helper applies to content;
  3. text exports — CSV/OUTFILE `constructByte`
     (`pkg/frontend/export.go:551`, decimal branches `:705-716` calling
     `Decimal.Format(scale)`) and JSONLINE export `vectorValueToJSON`
     (`export.go:1647`, decimal branches `:1736-1746`).
  Padding is gated on the result column's ZEROFILL flag at each site
  (`MysqlColumn` flags are available to the protocol serializers; the
  export paths receive the same metadata through their `MysqlResultSet`),
  so derived expressions — which carry mask 0 per the propagation and
  set-operation rules — keep plain rendering, and NULLs stay NULL.
  Physical exports (e.g. Parquet OUTFILE) carry the value unchanged.
- **D5 (no per-feature protocol fence; identifier allocation v26, gate
  removal v28).** The repository upgrade contract
  (`.agents/skills/mo-dev/references/upgrade-compatibility.md:3-27`)
  permits shutdown upgrades only — no rolling upgrade, no mixed-version
  operation, no downgrade — and prohibits new per-feature
  `MORPCVersion` values, `MORPCLatestVersion` bumps, capability gates on
  `AllCNsSupportProtocol`/heartbeat-advertised protocol versions, and
  legacy/older-peer fallback paths, unless RPC framing or the handshake
  itself changes or a recorded project decision reintroduces
  mixed-version support. Neither exception applies here: the admission
  functions are ordinary registry functions and the mask-bearing
  channels are ordinary function/proto outputs, which the contract
  explicitly exempts from gating (`:21-23`). v28 therefore removes
  everything the v25-v27 fence carried: (a) the `MORPCVersion133`
  constant and `MORPCLatestVersion` bump (`MORPCLatestVersion` stays
  main's `MORPCVersion132`); (b) the authoring floor in
  `build_constraint_util.go` (the MEDIUMINT-era
  `RequirePersistedProtocolVersionForAuthoring` machinery is main's and
  stays for its own callers); (c) the remote-expression feature
  classifier and its pb-local numeric mirrors, the compile-side binding
  test, and the destination/placement/send/receive/persisted floors
  (`RequiredRemoteExpressionFeatures` admission case in
  `pkg/pb/plan/string_literal_form.go`,
  `expression_destination_protocol.go`, `expression_protocol.go`,
  `remoterun.go`, `persisted_ip.go`) — with no gate there is nothing to
  classify, so round-27's rank-family collision concern dissolves:
  main's MRANK/CUMRANK/TMRANK at 616-618 are never inspected by this
  change; (d) the bootstrap floor aggregation and the pre-attributes
  COLUMNS template fallback — fresh init authors the current template
  unconditionally (the D5-related branch of
  `InitInformationSchemaSysTablesForProtocol` is never added by this
  change); (e) the 131/132 peer-discrimination contract at every
  removed gate. Main's existing protocol line (floors <=132 and the
  tenant-init gate machinery) is untouched. The mask-bearing metadata
  channels of this change — the `mo_view_columns` `att_is_unsigned`
  output field and the `mo_subscription_columns()` mask — are
  new-binary outputs only: pre-change binaries persist no definition
  that references them, post-upgrade every peer runs the new binary,
  and same-version-line rollback keeps them present-but-unused (their
  persisted-definition reference rule in "Public metadata channels" is
  unchanged). Allocation (unchanged from v26, verified against
  origin/main `420786e21566e5b9d5875e7d6a7f3102fd0761af`): main's
  function-ID line runs **605..615 = #31017 indicator kernels**,
  **616..618 = MRANK/CUMRANK/TMRANK (#31020)**, **619 =
  QUANTILE_EXACT_WEIGHTED (#31000's re-issue)** with
  `FUNCTION_END_NUMBER = 620`; the admission family takes
  `UNSIGNED_DECIMAL_ADMISSION = 620`,
  `UNSIGNED_DECIMAL_ADMISSION_STRICT = 621`,
  `UNSIGNED_DECIMAL_ADMISSION_IGNORE = 622`,
  `FUNCTION_END_NUMBER = 623`, each verified unused on that main blob —
  an ordinary registry addition pinned in `function_id_test.go`'s
  `predefinedFunids` alongside main's preserved 605..618 pins
  (`Test_funids` fails any registered-but-unpinned function; executed
  probe cited in the v26 -> v27 disposition). Integration rule
  (unchanged): the implementation turn first integrates origin/main
  (420786e2 or its then-current descendant — the identifier
  prerequisite that licenses the base update), records the integrated
  commit and new merge-base, and re-runs the unusedness check; if any
  allocation has meanwhile been claimed, the next free values are
  allocated and this design returns for traceable re-approval before
  any implementation submission. Old-catalog readability: pre-change
  binaries never wrote non-zero decimal masks, so old catalog rows read
  back as plain signed with no upgrade logic; mask codes are written
  only by the new binary after the shutdown upgrade and read only by
  new binaries (no downgrade support, per the contract; D7 owns the
  supported rollback shape).
- **D6 (CAST parity).** Reject `UNSIGNED`/`ZEROFILL` modifiers on DECIMAL
  CAST targets with a syntax error (1064), matching MySQL's restricted
  `cast_type`; integer `SIGNED`/`UNSIGNED` forms unchanged.
- **D7 (upgrade/rollback, v28).** No data migration. Upgrade shape:
  shutdown upgrade per the repository contract — the cluster stops on
  the old binary and restarts on the new one, and the upgrade framework
  advances every tenant through the versioned entry list; attributed
  DDL/DML is available on every CN from that point with no ordering
  gate, because mixed-version operation is unsupported (D5). Pre-change
  tenants' persisted COLUMNS definitions are recreated by the versioned
  upgrade entry during that same upgrade, registered in a NEW
  forward catalog version (v29; "COLUMNS initialization and
  one-shot upgrade repair" in Rollout; re-based onto main's
  consolidated chain in v33): the integration-time final version is
  main's FROZEN consolidated terminal (READY 4.0.7/41; upstream
  #31497 removed the 4.0.8-4.0.17 handlers; its in-progress/READY
  offset hard-errors `pkg/bootstrap/service_upgrade.go:255-276` are
  gated on the final version being 4.0.7, so v35 re-gates them
  against the frozen 4.0.7 handler metadata — Design item 2,
  invariant I8 — preserving both terminal pre-mutation rejections
  under any final target), so an edit-in-place
  entry never re-fires for already-ready catalogs and editing the
  frozen lists is itself forbidden — the entry rides the next free
  semantic version (v4_0_18, `MinUpgradeVersion` 4.0.7; the number
  verified free on origin/main at `9e29011e73` and re-verified at
  integration) and every pre-change tenant crosses a real
  transition. Below-4.0.7 sources (released 4.2 at 4.0.5/4.0.6) and
  interrupted-4.0.7 resumptions keep main's legacy-CDC admission
  preflight BEFORE any catalog mutation — v35 re-gates both
  `rejectLegacyCDCTasks` call sites from the `final.Version ==
  "4.0.7"` equality to an eligibility condition read from durable
  source/route state, re-derived in v36 from ACTUAL PERSISTED ROUTE
  PROGRESS (the `mo_upgrade` step rows of the relevant route, not a
  synthesized `mo_version` marker — no such marker exists for
  intermediate steps, `service_upgrade.go:341`/`:695` being the
  single writer sites for the final marker only): any source
  version below 4.0.7, a 4.0.7-headed latest row not READY, or a
  later route still holding an uncommitted `ToVersion == "4.0.7"`
  step is checked — while a route whose 4.0.7 step has durably
  committed and a READY-4.0.7/41 source (direct route, no 4.0.7
  step) are not — independently of the final
  target (Design item 2; invariant I7;
  validation item (vi)). The operator path is main's startup-owned contract
  (v33; supersedes the v30-v32 manual-SQL sequence): the shutdown
  upgrade starts the CNs with `auto-upgrade = true` — main's
  default (`cmd/mo-service/config.go:173`) and #31497's own
  documented prerequisite — so startup's `BootstrapUpgrade`
  performs route creation, all-tenant execution, and global Ready
  ENTIRELY server-side before any SQL admission
  (`pkg/cnservice/server.go:1501-1548`, gated at `:503` by
  `waitForBootstrapUpgrade`, `server_bootstrap_wait.go:20-50`;
  frontend admission follows at `:556`); with
  `auto-upgrade = false` the CN instead FAILS TO START against the
  not-yet-final catalog (`pkg/cnservice/server.go:1495-1498` ->
  `CheckManualUpgradeReadiness`,
  `pkg/bootstrap/service_upgrade.go:75-99`, 'catalog version x/y is
  not ready for X/Y; enable auto-upgrade for the stopped upgrade
  and restart'), so no credential, special-user login, or SQL
  statement participates in the transition in ANY deployment state
  — the v32 unknown-credentials case is dissolved by construction.
  Restoring `auto-upgrade = false` at a later restart is optional
  and succeeds (readiness passes on the READY-final catalog;
  completed route leaves no work). Main's `UPGRADE ACCOUNT` /
  login-gateway machinery stays main's: under the fail-fast gate it
  can no longer CREATE this transition, and its post-completion
  compensation channel for late or snapshot-restored accounts is
  unaffected and unused by this design. Cross-version downgrade
  against an upgraded catalog is refused by the
  upgrade framework itself (`pkg/bootstrap/service_upgrade.go:418`
  `canResumeUpgrade`), so the only supported downgrade shape is
  same-version-line rollback. Documented operator procedure before that
  rollback while attributed columns exist: (1) enumerate them from
  `mo_catalog.mo_columns`, filtering decimal `atttyp` with non-zero
  `att_is_unsigned` (implementation finalizes the decoding predicate);
  (2) `ALTER TABLE ... MODIFY` each back to plain `DECIMAL`, which
  re-derives `att_is_unsigned = 0`; (3) verify the enumeration is
  empty. The recreated COLUMNS definition needs **no** restore step (it
  references only pre-existing constructs — "Public metadata channels"
  items 1 and 4), and SHOW CREATE on the rolled-back binary renders
  plain `decimal(p,s)` for the stripped columns, matching pre-fix
  behavior exactly. Skipping the procedure leaves old binaries treating
  att-masked decimal columns as signed (same silent-loss exposure as
  pre-fix main, documented here; reads never hard-fail since codecs and
  the persisted view text are unchanged in capability requirements).
- **D8 (observability).** Authoring appends deprecation warnings 1681
  (UNSIGNED on decimal) and 3861 (ZEROFILL). 1264 strict/non-strict
  diagnostics reuse the existing labels/sink. No new metrics.
- **D9 (DDL admission).** `DEFAULT` constant admission
  (`makePlan2AssignmentCastExpr`, `make.go:969`) extends to attributed
  decimals so the strict DDL check (ER_INVALID_DEFAULT, 1067) runs through
  the same admission expression; the binding-owner and strict
  constant-validation contract is the "DDL DEFAULT source-preservation
  boundary" bullet in "Domain enforcement mechanism" (round-8 finding 1,
  constant-detection node corrected in round-9 finding 2): target-free
  binding at every DEFAULT binder owner (`build_util.go:995-1029`,
  `:1141-1154`, `build_ddl.go:1961-1980`, `table_dump_binding.go:332,355`)
  keeps the source unrounded, constant detection runs on that raw bound
  source BEFORE the admission composition (the admission node itself is
  volatile and can never satisfy `function.IsConstant`,
  `function.go:420-441` with `list_operator.go:2741-2754`), and one
  shared helper builds a separate explicit-strict validation composition
  from the raw source (strict admission entry + `cast_strict`, on the
  MEDIUMINT precedent `build_util.go:1043-1068`), maps negative constant
  defaults in any literal class to 1067 at authoring time regardless of
  session sql_mode, and leaves volatile and NULL defaults unevaluated
  early; narrowing ALTER validates existing rows
  before replacement via the existing ALTER copy/narrowing validation path,
  treating gained-UNSIGNED as a domain-narrowing change.

## Validation plan

Red/green protocol: for each behavior, the identical SQL/UT must fail on
`8fdc27559f` (assigned base) and pass after the change. The v1 baseline probe
(SQL evidence in the handler artifacts) is the base-red record; the same
probe becomes the post-fix green oracle.

### Unit tests (each maps to one owner)

- Parser/plan type mapping: `DECIMAL(p,s)[ UNSIGNED][ ZEROFILL]` across
  decimal64/128/256 precision boundaries yields the right OID plus masks
  (unsigned=1, zerofill=3 normalized); CAST attribute spelling rejected
  (1064); tree stringification round-trip.
- `plan.Type` proto round-trip of field 14; old-proto (no field) decode ⇒ 0.
- Conversion owners: PlanColsToExeCols ↔ tuplesGen mask computation
  (unsigned int still exactly 1; decimal unsigned 1; decimal zerofill 3;
  plain 0); genTableDefOfColumn and both rebuild sites map mask → proto field.
- Admission function: strict 1264 vs non-strict/IGNORE clip+warning for
  `-0.004` **and** `-0.005`, `-0.5`, `-1`, exact `0`, `-0`, `-0.00`,
  `'-0.004'`, `'-0.00'`, `'  -1 '`, `'abc'`-style zero coercions
  (documented as zero ⇒ in domain only where existing lexical conversion
  treats them), each from decimal, string, float32/64, and signed-int
  sources; NULL passes through silent; positives pass unchanged including
  existing precision-overflow behavior; same-type decimal→decimal assignment
  still executes admission (needsSameTypeAssignmentCast). String-source
  token controls (round-6 finding 2, corrected in round-7 finding 4):
  `'-0x1'`, `'-0b1'`, `'-0o7'` classify negative nonzero (strict 1264;
  clip+warning otherwise) with stored values proven identical to the
  unadmitted signed-decimal conversion (`-1.00`/`-1.00`/`-7.00`);
  effective-sign pairs through the full radix grammar: `'0x-1'` stores
  `-1.00` and violates, `'-0x-1'` stores `1.00` and is admitted, `'0b-1'`
  violates, `'-0o-7'` stores `7.00` admitted, `'0x-0'` stores `0.00`
  admitted — each asserted per mode with the identical outcome across
  decimal64/128/256; signed-zero radix forms `'-0x0'`, `'-0b0'` and
  whitespace-wrapped `'  -0x1  '` classify consistently (`strings.TrimSpace`
  grammar); positive radix `'0x123'` stores `291.00` admitted. IGNORE
  retained-prefix controls (round-7 finding 2, extended in round-8
  finding 2 to the post-tokenization failure stage): `'-1 junk'` and
  `'-0.004 junk'` store `0.00` with the truncation warning FIRST and the
  1264 clip warning SECOND; the consumption-rejected forms `'-1junk'`,
  `'-0.004junk'`, and incomplete-exponent `'-1e'` produce the identical
  stored value and the identical warning ORDER (executed probe facts for
  the unadmitted IGNORE cast: `'-1junk'`/`'-1e'` -> `-1.00` with the
  truncation warning, `'-0.004junk'` -> `0.00` from a pre-round-negative
  prefix, across decimal64/128/256); positive-prefix `'1 junk'`/`'1junk'`
  store `1.00` with the truncation warning only; zero-prefix `'0junk'`/
  `'-0 junk'`/`'-0junk'` store `0.00` with the truncation warning only
  (no 1264); prefixed-range `'999...9tail'` keeps the cast's range error
  even under IGNORE (nothing stored, no admission diagnostic, contract
  pinned at `func_cast_ignore_test.go:305-311`); `'abc'` keeps the
  existing invalid warning plus zero default;
  special-syntax guards `'-0x'`/`'nan'` keep the existing error untouched;
  strict and non-strict non-IGNORE keep today's lexical error for every
  conversion failure at EITHER stage (executed probe facts:
  `parseDecimal64/128/256CastString` reject `'-1junk'`, `'-0.004junk'`,
  `'-1e'`, and `'12.34tail'`) with no admission diagnostic. IsBin controls
  (round-7 finding 3): bytes `0x2d 0x31` (ASCII `'-1'`) store the positive
  magnitude `115.69` admitted across all three families; `0x00` stores
  `0.00`; `0xff`/`0x01 0x00` magnitudes accepted; empty input keeps the
  cast's InvalidInput error; oversized magnitudes keep each family's
  overflow error. charset-255 branch controls classify negative prefix
  facts (`'-1 junk'` negative, `'-0x1'` zero under its own prefix grammar —
  that branch stores `0.00`).
- VALUES literal route (round-6 finding 1, corrected in round-7 finding 1):
  for an attributed decimal target both VALUES builders skip
  `MakeInsertValueConstExpr` AND switch to the target-free binder (the
  integer-target predicate extended; plan-shape assertions on
  `EXPLAIN`-inspected expressions prove no target-scale const is folded)
  for unquoted decimal `-0.004`, scientific `-4e-3` (float path), quoted
  `'-0.004'`, and positive/zero controls; behavior pairs for each under
  strict (1264, nothing stored), non-strict (`0.00` + 1264 warning), and
  IGNORE (`0.00` + warning); unattributed targets keep a byte-identical
  fast-path const and target-aware binder (I4 regression guard), and
  `buildValueScan` (ODKU context) exercises the same bypass.
- DDL DEFAULT binding route (round-8 finding 1): for an attributed
  decimal64/128/256 destination, each owner (`build_util.go`
  `buildDefaultExprWithColumnsObserved`/`buildOnUpdate`, `build_ddl.go`
  `buildCTASDefaultFromOrigin`, `table_dump_binding.go` re-derivation)
  binds the DEFAULT source with the target-free predicate — plan-shape
  assertions prove unquoted `-0.004`, scientific `-4e-3`, and quoted
  `'-0.004'` sources carry sign and natural scale through binding with
  no target-scale inner cast — plus positive `1.5`, zero `0`/`-0.00`,
  and quoted `'-1.5'`/`'1.5'` source-shape assertions. Strict
  constant-validation cases (gate node corrected in round-9 finding 2):
  `DEFAULT -1`, `DEFAULT -0.004`, `DEFAULT
  -4e-3`, `DEFAULT '-0.004'`, `DEFAULT '-1.5'` each fail ER_INVALID_DEFAULT
  (1067) under EMPTY sql_mode at DDL authoring; `DEFAULT 0`, `DEFAULT
  -0.00`, `DEFAULT '1.5'`, positive constants, and `DEFAULT NULL` are
  accepted; a volatile default (`rand()`-based) is accepted without
  early evaluation. Gate-shape assertions pin the detection node: the
  stored composition `cast_assign(admission(src), target)` reports
  `function.IsConstant == false` at its outer node AND at the admission
  node (execution-time sql_mode resolution), while `IsConstant(src)` on
  the raw bound source is true for every constant literal class —
  proving detection runs BEFORE composition, since the volatile
  admission node can never satisfy the precedent's `cast.Args[0]`
  reading (`function.go:420-441`). Validation-shape assertions prove
  the separate authoring composition is built FROM the raw source with
  BOTH layers strict (strict admission entry-point + `cast_strict`),
  so an empty-session sql_mode cannot route the validation through a
  clip+warning branch; volatile defaults (`rand()`, `now()`, `uuid()`)
  reach no early evaluation at all (no executor built, no diagnostics
  captured) and their stored expression retains the mode-sensitive
  shape. Unattributed destinations bind byte-identically to
  today (I4 regression guard at every owner), and a CTAS inheriting an
  attributed column's valid default round-trips the origin text
  unchanged through `buildCTASDefaultFromOrigin`.
- Persisted-expression policy replay (round-10 finding 1): typed
  assertions over the shared boundary rewriter for every persisted
  shape — the DDL-authored `cast_assign(admission_mode(src), typ)`, the
  legacy `cast_strict(admission_mode(src), typ)` upgrade shape, and each
  rebuilt variant — proving: IGNORE ⇒ `cast_ignore` + admission IGNORE
  entry with `src` structurally unchanged; non-IGNORE ⇒ `cast_assign` +
  admission mode-resolved entry; **idempotence** — applying the
  rewriter twice under the same policy returns the once-rewritten
  expression unchanged for both policies, and an IGNORE →
  non-IGNORE sequence reproduces the DDL-authored boundary; a
  user-written explicit CAST or subexpression inside `src` is untouched
  (recognizer fires only at the boundary position); unattributed
  decimal and non-composition shapes pass through byte-identically
  (I4 guards). Consumer-shape assertions pin each migration point:
  `getDefaultExprForAssignment` returns the re-policed composition for
  an attributed DECIMAL default under `ignore=true`/`false` while
  returning unattributed defaults unchanged;
  `applyGeneratedColumnAssignmentCast` switches BOTH layers for an
  attributed generated expression under `isIgnore` (not only the outer
  wrapper); the two ON UPDATE copies (`bind_update.go:833`,
  `bind_insert.go:2897`) emit the statement-policy boundary rather than
  the DDL-authored one under an ignore statement and the DDL-authored
  one otherwise.
- Format/SHOW-CREATE: `FormatColType`/`FormatSQLColumnType` renders
  `decimal(p,s)[ unsigned][ zerofill]`; the recreated COLUMNS template's
  CASE wrapper renders masks 0/1/3 for decimal rows and leaves
  DATA_TYPE plain `decimal`, while an unsigned-integer row (mask 1,
  non-decimal `atttyp`) gains no duplicate suffix; the template-text
  allowlist assertion — every persisted-definition identifier resolves
  to constructs present on pre-change binaries (parser-level `case`,
  `concat`, two-argument `mo_show_visible_bin`, existing catalog
  columns), and the text must not reference the new producer fields or
  any new builtin; the code-only
  `mo_view_columns`/`mo_subscription_columns` mask outputs verified at
  the producer level; the checkpoint-tool renderer's bit-aware
  suffixes; the protocol flag/display-length mapping for attributed
  decimals.
- COLUMNS initialization and upgrade entry (reshaped in v28; transition
  cases added in v29): fresh tenant initialization persists the
  decorated COLUMNS definition with NO protocol-floor branch (the
  v25-v27 `WaitsForProtocol` / `AuthoringProtocolReady` gate tests are
  deleted with the gate); the canonical constructor reverts to main
  verbatim (protocol 109, no 133); the entry's `CheckFunc` no-ops on
  the canonical definition and recreates missing, stale, and
  predecessor definitions in both catalog spellings
  (`TransitionsLegacyDefinition`, `RepairsMissingAndStaleDefinitions`
  reused where gate-independent; executed evidence
  `ut-bootstrap-columns-maint.log`, `ut-bootstrap-all.log`); entry
  idempotence — a second execution issues zero writes. v29 round-29
  transition UT rows, re-based onto main's consolidated chain in v33
  (pre-PR finding after the v32 PASS; upstream #31497): the new
  v4_0_18 package pins handler metadata (4.0.18, `MinUpgradeVersion`
  4.0.7, `RequiredProtocolVersion` 126 — inherited from the frozen
  chain terminal, the entry itself needing only its own 109 gate —
  offset = entry count = 1) and the `CanDirectUpgrade` chain
  boundary (4.0.6 -> 4.0.7 -> 4.0.18: direct from READY 4.0.7; via
  4.0.7 from below); the exact-predecessor chain fixture lives in
  that package's UT — cluster and tenant READY at 4.0.7/41 ⇒ new
  single-step route fires the entry once, tenant advances to 4.0.18,
  canonical replay issues zero writes — because #31497 rewrote
  `columns_upgrade_test.go` into entry-level admission/idempotence
  coverage, which integration PRESERVES unchanged (its
  'already current' case compares against the same
  `sysview.InformationSchemaColumnsDDL` symbol the decoration
  extends); `service_statistics_upgrade_test.go` hop expectations
  are re-derived from the consolidated chain (versions <= 4.0.6
  route 4.0.7 -> 4.0.18; == 4.0.7 routes 4.0.18 direct); crash
  replay keeps its v29 shape (interrupted tenant upgrade transaction
  replays the entry to completion; version marker advances exactly
  once); and the operator-path rows are re-specified onto main's
  startup-owned contract (v33, superseding the v30-v32
  special-user/`UPGRADE ACCOUNT` scenarios, whose machinery the
  fail-fast gate makes unreachable as a creator path):
  `auto-upgrade = false` startup against READY-4.0.7/41 must FAIL
  with the exact readiness error and expose no SQL surface, and
  `auto-upgrade = true` startup must create the 4.0.7 -> 4.0.18
  route, execute every tenant, and reach global Ready BEFORE
  frontend admission — no credential exists anywhere in the path
  to test, which closes the v32 unknown-credentials predecessor by
  construction. Legacy-CDC admission and frozen-offset rows (v35;
  eligibility fixtures re-based onto actual persisted route
  progress in v36): the bootstrap package suite pins the re-gated
  preflight at BOTH call sites by the I7 eligibility rule —
  creation-time fixtures {READY 4.0.5, READY 4.0.6, in-progress
  4.0.7} x {one legacy
  task, exercised across active/paused/failed states} targeting
  final 4.0.18 return the terminalUpgradeError naming
  account/task/state BEFORE any catalog-mutation SQL
  (mem-executor default-reject oracle, main's own pattern), the
  empty-task control forms its route; the REAL-ROUTE interruption
  pair (v36, replacing v35's marker pair — the v35 review's
  finding): a READY-4.0.6 source's actual persisted route
  targeting 4.0.18 — latest version row 4.0.18/1 (Created or
  UpgradingTenant) plus `mo_upgrade` steps [4.0.6 -> 4.0.7, 4.0.7
  -> 4.0.18] under key (4.0.18, 1), exactly the persistence shape
  `addUpgradesToFinalVersion` writes, with NO 4.0.7/41
  `mo_version` row ever synthesized — interrupted (a) BEFORE the
  4.0.7 step completes (that step Created or UpgradingTenant):
  the restart through `doCheckUpgrade` terminally rejects the
  seeded legacy task BEFORE any catalog-mutation SQL (the
  preflight's `mo_cdc_task` read fires; the default-reject mock
  proves no `mo_version` 4.0.7-marker read is ever issued — v35's
  eligibility read is gone), and (b) AFTER the 4.0.7 step
  completes (that step Ready, the 4.0.7 -> 4.0.18 step pending):
  the restart issues NO `mo_cdc_task` read (default-reject as
  the no-query oracle), resumes the in-flight route, and
  completes with the seeded task row intact; the recheck arm is
  pinned by driving the same two persisted shapes through
  `newUpgradePass`'s retained-target completion path
  (`service_upgrade.go:530-534`) with a concurrently-created
  non-READY final route — mid-crossing (4.0.7 step not Ready)
  rejects, post-crossing (4.0.7 step Ready) skips — the
  consistent-eligibility clause; the
  task-survival positive control (READY 4.0.7/41 + one VALID
  post-crossing task, route interrupted mid-4.0.18) restarts
  through `doCheckUpgrade`, NEVER issues the preflight's
  `mo_cdc_task` query (the default-reject mock trips if it does),
  resumes the in-flight route, and leaves the task row intact at
  READY 4.0.18; the frozen-offset rows (I8) pin READY-4.0.7
  sources at persisted offsets {1, 1000} and an interrupted
  offset-only 4.0.7 -> 4.0.7 route, each terminal BEFORE any
  mutation SQL toward final 4.0.18 with main's unchanged error
  texts, with the supported READY-4.0.7/41 control forming its
  direct route; main's preserved `service_upgrade_cdc_test.go`
  AND `service_upgrade_recovery_test.go` stay green under the
  integrated final (under final 4.0.7 the third eligibility arm
  is unreachable at creation and the recheck arm's first disjunct
  IS main's exact condition, so both re-gated sites perform ZERO
  added reads and the fixtures' mocked SQL sequences are
  unchanged; the one read-count assertion,
  `routeReads == 1` for the retained-4.0.6 fixture, stays 1
  because the below-4.0.7 arm short-circuits without any
  `mo_upgrade` scan).
- Historical-template recognition (new in v29, round-29 finding 2;
  variant set finalized in v33): `AdaptLegacyInformationSchemaColumnsDDL`
  recognizes each pinned pre-change template variant byte-exactly —
  TWO verified variants: the base-era current text (V109 lineage plus
  the MEDIUMINT DATA_TYPE arm, per `view_columns_history_test.go:62-68`
  and the frozen constructor in `predefined.go`) AND the #31461
  (`ea4c68a04e`) selector rewrite, which is present on origin/main at
  `9e29011e73` and ABSENT from this worktree's base, so integration
  pins both frozen texts; #31497 is verified (diff inspection this
  revision, zero COLUMNS-DDL-text hunks) not to change the current
  template, so the integrated current template is the #31461 text and
  the decorated template is that text plus this change's CASE arm
  composed through `informationSchemaCurrentColumnsDDL()` —,
  normalizes the lowercase view name, and returns byte-stable
  pass-through output; unknown or mutated texts (trailing space) keep
  the hard error; history-adapter
  rows {current decorated, definition pre-change variant => adapt}
  extend the existing V41/V46/V58/V100/V109 matrix; pre-upgrade
  snapshot definitions never re-derive against the decorated
  template.
- ZEROFILL payload per channel (round-4 finding 4): one UT per
  serialization site named in D4 — text rows (both serializers),
  binary rows (both serializers, asserting the emitted length-encoded
  payload bytes), CSV/OUTFILE row, JSONLINE export row — each
  asserting the exact padded bytes `00000123.45` from a
  `DECIMAL(10,2) ZEROFILL` holding `123.45`, `00000000.00`-style zero
  rendering, NULL staying NULL, a plain signed `decimal(10,2)` control
  staying `123.45`, and a derived-expression control staying unpadded;
  column-definition packets assert `UNSIGNED_FLAG`/`ZEROFILL_FLAG` and
  the MySQL display length.
- Set-operation typing (round-4 finding 3): `setOperationOutputType`
  clears ZEROFILL and AND-merges UNSIGNED for UNION/UNION ALL in both
  argument orders, clears both bits for INTERSECT/MINUS, and preserves
  existing width/nullability/collation outputs byte-identically
  (regression guard against collateral type changes).
- LOAD staging and type contract (round-5 revision; staging-byte
  assertions added in round-9 finding 1): the scan vector for
  an attributed decimal column is `T_varchar` on **every** format
  (CSV/JSONline serial and parallel raw text via the generalized
  staging-predicate + `makeType` + `getColData` early-return; Parquet and
  Arrow rendered text) — asserted as one allocated type per statement at
  Prepare, with the projection compiled once against it; lexical
  admission covers mixed natural scales in one column (`1.2` / `1.23` /
  `-0.004`), exponent forms (`-2.5e-3`, `1e2`, `-1e-30` into
  `DECIMAL(10,2)`), excess source precision (80-digit text), and quoted
  vs unquoted fields; **staging-byte assertions (round-9 finding 1)**:
  under LOCAL non-strict CSV the staged bytes equal the raw trimmed
  field bytes for an attributed target in BOTH the serial and parallel
  readers — radix `'-0x1'` stages `'-0x1'`, `'0x123'` stages
  `'0x123'`, retained-prefix `'-1junk'`/`'-1e'` stage unchanged,
  invalid `'abc'` stages `'abc'` (the invalid input must reach
  admission, whose classification owns the zero-coercion diagnostic),
  signed decimals `'-0.004'`/`'-1.5'` stage unchanged, quoted forms
  remain exempt from rewriting as today, an empty unquoted field still
  stages `"0"` (`emptyNumericField` policy preserved), and NULL
  markers still stage NULL — while an unattributed decimal control
  keeps today's rewritten bytes (`'-0'`, `'0'`, `'-1'`) byte-identical
  (I4); round-6 radix staged-field cases (`-0x1`
  errors/clips per mode and stores what DML would, `-0x0` stores `0.00`,
  `0x123` stores `291.00`); Parquet rendering asserts `DECIMAL(40,10)` /
  `DECIMAL(70,30)` produce fixed-point source-scale text,
  `DECIMAL(80,79)` holding `1.0` produces the bounded scientific form
  (`10^79` coefficient + `e-79`) whose admission passes and whose
  `cast_assign` stores `1.00` into `DECIMAL(10,2)`, and its negative
  variant errors/clips per mode; bounded-failure malformed-schema cases:
  a crafted `DECIMAL(80, 2147483647)` field with coefficient `1` renders
  `1e-2147483647` (bounded bytes, no O(declared-scale) allocation; memory
  observed content-proportional), converts to `0.00` stored, and the
  `-1` coefficient variant fails/clips admission per mode (preserving
  negative-near-zero enforcement through scientific forms); Parquet
  NULLs stage as null entries (never rendered), int64/double physicals
  render sign-faithful text, and Arrow staging leaves its borrowed
  buffers unmodified; a differing-schema multi-file load (`pf` 40 then
  `pf` 80, plus an int-physical file) runs through the same compiled
  LOAD plan and stores the same values as single-file loads; strict-mode
  abort mid-file frees page/batch buffers (cleanup assertion).
- Function identity (v28; replaces the v25-v27 "Protocol fence" row):
  the admission family pins at 620/621/622 with `FUNCTION_END_NUMBER`
  623 in `function_id_test.go`'s `predefinedFunids` (main's ordinary
  registry-identity contract, `Test_funids`), alongside main's
  preserved 605..618 pins; no protocol gate, classifier, floor, or
  peer-discrimination row remains — with no gate there is nothing to
  classify, so round-27's rank-family negative arms retire with the
  classifier.

### Public SQL tests (BVT / issue integration)

- Both issue reproductions verbatim plus: `DECIMAL(16,2)` d64 / `(17,2)` d128
  / `(39,2)` d256 negative inserts under strict (error) and non-strict
  (clip+warning, SHOW WARNINGS content); `-0.004` rejected/clipped with
  warning, `-0`/`-0.00`/`0` accepted; UPDATE, REPLACE, ODKU, INSERT...SELECT,
  prepared-stmt negative binds; max-value boundary accepted, `(max+ulp)`
  rejected by existing precision checks; genuine zero controls accepted.
  Negative-near-zero VALUES mode matrix (round-6 finding 1, extended in
  round-7 finding 1): `INSERT INTO ud VALUES ('-0.004')` (quoted),
  `VALUES (-0.004)` (unquoted decimal), and `VALUES (-4e-3)` (scientific)
  each assert ERROR 1264 with an empty table under strict, `0.00` stored
  with a 1264 warning under non-strict, and `0.00` stored with a warning
  under IGNORE, executed through both the insert value scan and
  `buildValueScan` forms; multi-row `VALUES` including one violating row
  asserts statement atomicity under strict.
  VARCHAR-source radix DML matrix (round-6 finding 2, extended in round-7
  finding 4): `INSERT INTO ud SELECT v`/`UPDATE` from a VARCHAR column
  holding `'-0x1'` (strict 1264; clip+warning with `0.00` stored
  otherwise), `'0x-1'` (strict 1264; clip+warning), `'-0x-1'` (admitted,
  `1.00` stored, no warning), `'0b-1'` (strict 1264; clip), `'-0o-7'`
  (admitted, `7.00`), `'-0x0'` (admitted, `0.00` stored, no warning),
  `'0x123'` (admitted, `291.00` stored), and the tokenizer-rejected
  control `'-0x'` (existing lexical diagnostic path unchanged, no
  admission warning).
  IGNORE retained-prefix matrix (round-7 finding 2): `INSERT IGNORE INTO ud
  SELECT v` with `'-1 junk'` and `'-0.004 junk'` stores `0.00` and asserts
  SHOW WARNINGS contains the truncation warning before the 1264 clip
  warning; the consumption-rejected controls `'-1junk'`, `'-0.004junk'`,
  and incomplete-exponent `'-1e'` round (round-8 finding 2) store `0.00`
  with the identical truncation-first-1264-second warning order;
  `'1 junk'`/`'1junk'` store `1.00` with the truncation warning only;
  `'0junk'`/`'-0junk'` store `0.00` with the truncation warning only;
  `'abc'` keeps the existing invalid warning with `0.00` stored;
  strict INSERT of each negative control fails 1264 with the table empty;
  CSV `LOAD DATA ... IGNORE` rows carrying the same fields assert the
  identical stored values and warning order.
  IsBin DML controls (round-7 finding 3): `INSERT INTO ud VALUES
  (x'2d31')` — bytes spelling `'-1'` in ASCII — stores the positive
  magnitude `115.69` admitted with no warning; `x'00'` stores `0.00`;
  the same controls through a BINARY source column.
- Metadata round-trip: SHOW CREATE and I_S COLUMN_TYPE for unsigned/zerofill
  combos on the base-table COLUMNS row branch; view-over-attributed-column
  and subscription rows assert the documented this-version limitation
  (plain `decimal(p,s)`; subscription CI-only if the publish/subscribe
  harness is unavailable locally); `mo_columns.att_is_unsigned` masks
  asserted via SQL; dump/restore round-trip (SHOW CREATE re-parse);
  `CREATE TABLE ... LIKE` preserves attributes; CTAS asserts: direct
  column projection ⇒ SHOW CREATE shows `unsigned` / `unsigned zerofill`,
  a ZEROFILL read from the CTAS table renders padded, and a strict
  negative write into the CTAS table fails 1264, while a
  computed-expression CTAS column carries no attribute and accepts
  negatives; upgraded-catalog case: a persisted pre-attribute COLUMNS view
  definition recreated by the versioned upgrade entry ⇒ COLUMN_TYPE
  asserts for masks 0/1/3 on the base-table branch, then the same
  unchanged definition text is re-evaluated from a fresh catalog handle
  to demonstrate the same-line rollback property (constructs bind
  without the change's code additions — full old-binary evaluation is
  the CI remainder below); entry idempotence case: re-running the entry
  against the canonical definition issues zero recreate DDL, and an
  interrupted upgrade replays the entry to completion (framework
  resume); relaunched existing-data case (v28; predecessor pinned in v29,
  re-based in v33): an existing-data cluster READY at main's
  consolidated terminal 4.0.7/41 is relaunched on the new binary and
  converges through the NEW version's entry — main completes the
  route server-side before SQL admission, so the first admitted query
  already observes the 4.0.18/1 marker and decorated COLUMN_TYPE
  (public BVT still waits on the marker with `-- @wait_expect(5, 120)`
  as belt-and-braces; executed evidence re-collected on the integrated
  source in the implementation turn), and an already-decorated cluster
  issues zero recreate DDL; snapshot-compat case (new in v29, round-29 finding
  2): an account snapshot taken BEFORE the tenant upgrade recreates
  COLUMNS stays queryable after the recreate —
  `information_schema.columns {snapshot=...}` returns the pre-change
  projection with no 'unsupported historical
  information_schema.COLUMNS definition' error, while CURRENT COLUMNS
  renders decorated metadata in the same script;
  checkpoint-derived DDL round-trip for masks 0/1/3 with
  unsigned-integer controls, including applying the reconstructed DDL and
  asserting strict negative rejection on the restored table; narrowing
  ALTER over existing negative rows fails with schema and data proven
  unchanged (SHOW CREATE identical; negative rows intact); legal ALTER
  controls succeed.
- Set operations (round-4 finding 3): ZEROFILL-decimal unioned with signed
  decimal, and the reverse branch order, under UNION and UNION ALL render
  unpadded
  with no ZEROFILL flag on the result column-definition; all-unsigned
  unions keep the UNSIGNED flag, mixed signed/unsigned unions drop it;
  INTERSECT/EXCEPT outputs carry neither bit; CTAS over a UNION produces
  plain `decimal(p,s)` and accepts negative writes; `INSERT INTO
  attributed ... SELECT ... UNION ALL SELECT` still fails 1264 under
  strict on a negative branch value (target admission, unchanged).
- Per-channel presentation (round-4 finding 4): wire-level assertions via
  the pymysql probe — plain-query (text protocol) `SELECT x FROM zf`
  returns the padded literal; `PREPARE`/`EXECUTE` (binary protocol)
  returns the identical padded value; `SELECT ... INTO OUTFILE` CSV and
  JSONLINE export files contain the padded bytes; each with NULL, plain
  signed-decimal, and derived-expression (`-x`) controls and column
  flags/display length from the result metadata.
- DDL default admission (round-8 finding 1): negative defaults rejected with
  1067 even under empty sql_mode across every literal class — unquoted
  `DEFAULT -1` and `DEFAULT -0.004`, scientific `DEFAULT -4e-3`, quoted
  `DEFAULT '-0.004'` and `DEFAULT '-1.5'`; `DEFAULT 0`, `DEFAULT -0.00`,
  `DEFAULT NULL`, quoted positive `DEFAULT '1.5'`, and `DEFAULT 1.5`
  accepted (controls); a volatile default (`rand()`-based) not rejected
  early; a CTAS inheriting a valid attributed default creates successfully
  and a strict negative write into the CTAS table fails 1264.
- Persisted-expression policy replay (round-10 finding 1) — negatives
  produced BY PERSISTED EXPRESSIONS under explicit sql_modes:
  a volatile default written to produce negatives (e.g.
  `DEFAULT (-1-RAND())`) on an attributed decimal column: **strict +
  INSERT IGNORE** inserts the row with `0.00` stored and SHOW WARNINGS
  showing EXACTLY one 1264 admission warning per adjusted row (no
  duplicate domain warning from the re-policed boundary; a legitimate
  conversion-truncation warning, where present, precedes it);
  **strict + INSERT** fails 1264 with zero rows committed (failure
  atomicity); **empty sql_mode + INSERT** stores `0.00` with the 1264
  warning; a generated unsigned DECIMAL column reading a negative
  signed source column replays identically per mode
  (strict+IGNORE ⇒ clip+warn, strict ⇒ error with atomic rollback,
  non-strict ⇒ clip+warn); **strict + INSERT IGNORE ... ON DUPLICATE
  KEY UPDATE** with an ON UPDATE attributed expression evaluating
  negative clips+warns on the update side while the plain non-IGNORE
  duplicate update errors 1264; `UPDATE ... SET col = DEFAULT`
  replays the same policy through `getDefaultExprForAssignment`;
  positive/zero-producing persisted expressions store unchanged with
  no admission warning in every mode; unattributed signed DECIMAL
  defaults/generated/ON UPDATE keep today's behavior byte-identically
  (mechanism inert off the attributed boundary, I4).
- Statement atomicity (expected outcomes by mode): multi-row INSERT with a
  mid-batch negative row under strict ⇒ ERROR and post-statement row count =
  0 (no prefix committed); INSERT...SELECT with one negative source row ⇒ no
  rows; UPDATE rendering one row negative ⇒ error with all rows unchanged;
  under non-strict the same statements succeed with negative rows stored as 0
  and warnings present.
- LOAD: CSV serial+parallel, JSONline, Parquet, Arrow public cases
  asserting stored values and SHOW WARNINGS for
  strict/non-strict/IGNORE/LOCAL non-replace (negative nonzero field → 0 +
  1264 warning; strict → error, table empty); negative-near-zero fields
  behave like DML; CSV radix fields (`-0x1`, `0x-1`, `-0x-1`, `-0x0`,
  `0x123`) assert the same per-mode outcomes as the VARCHAR-source radix
  DML matrix, **specifically including a LOCAL CSV file loaded under an
  EMPTY sql_mode (round-9 finding 1 — the scenario that activates the
  scanner's non-strict prefix rewrite, `external.go:1510-1516`) with
  unquoted AND quoted variants of the radix forms, retained-prefix rows
  (`-1junk`, `-0.004junk`, `-1e`), an invalid-zero row (`abc` → `0.00`
  with the conversion's own invalid diagnostic and no admission 1264),
  and signed-decimal controls (`-1.5` clipped with 1264, `-0.004`
  clipped with 1264, `-0` and `1.5` admitted without warnings) —
  asserting stored values and SHOW WARNINGS content so the attributed
  bypass of the prefix rewrite is proven at the SQL surface in both
  serial and parallel load configurations**; and LOAD IGNORE / LOCAL
  non-replace rows carrying
  `-1 junk` / `-0.004 junk` / `-1junk` / `-0.004junk` / `-1e` / `1junk` /
  `0junk` assert the IGNORE retained-prefix matrix (round-8 finding 2);
  mixed-scale, exponent-form, and excess-source-precision
  inputs per the staging UT list above; Parquet round-4/5 cases: a
  generated `DECIMAL(80,79)` file with positive fitting values stores the
  MySQL-equivalent rounded result, its negative variant errors/clips per
  mode, the malformed oversized-scale file loads with content-bounded
  memory (positive → rounded stored value, negative → per-mode
  error/clip), and a differing-schema multi-file load (pf 40 + pf 80)
  completes through the same compiled plan; unattributed decimal targets
  unchanged (I4).
- Engine/catalog round-trip across restart (issue-integration test pattern
  like `TestIssue29723MediumIntSemantics`): create attributed columns,
  assert masks, restart the cluster, re-verify SHOW CREATE, metadata, and
  strict rejection; a pre-fix-signed catalog keeps signed behavior.
- Counterexamples: derived expressions (`SELECT -x`, `x+0`, aggregates)
  render unpadded and carry no attribute; signed DECIMAL tables unaffected;
  `CAST(x AS UNSIGNED)`/`SIGNED` integer forms pass; `CAST` to DECIMAL from
  string/decimal unaffected; subscription DDL re-derivation documented.

### Evidence reuse / CI-only remainder

Focused red/green pairs above run locally; the full distributed BVT matrix,
backup/clone matrix, publish/subscribe metadata
harness, upgrade-scenario matrix, a true pre-change-binary evaluation of
the upgraded catalog's recreated COLUMNS definition (requires a released
old image), and MySQL 8.0.45 cross-checks of signed-zero edge cases and
UNION-of-unsigned column-definition flags are recorded as CI work, not
silently skipped.

## Alternatives and trade-offs

| Option | Benefit | Cost / decision |
|---|---|---|
| Plan-side attribute + `att_is_unsigned` mask (chosen) | Zero catalog migration; codecs byte-stable; old binaries never hard-fail; enforcement owned once in plan composition | Two-channel mapping (proto + mask) — mitigated by a single shared mask constant and five named conversion owners; mask column name is legacy (`att_is_unsigned` holds a 2-bit mask for decimals) |
| `types.Type` bit-packing / struct growth | Single channel | Breaks native 16-byte persistence (vector/objectio/TAE/agg) or `GetNotNull==1`/Unmarshal validation; old binaries hard-fail reads — rejected with verified evidence |
| New decimal OIDs (`T_udecimal*`) | Explicit type identity | Touches every type switch, codec, dispatch, protocol mapping — rejected (also in MEDIUMINT design) |
| Dedicated `att_zerofill` catalog column via the upgrade framework (`pkg/bootstrap/versions/v4_0_6` precedent exists for `att_is_unsigned`) | Clean naming | Requires bootstrap DDL change, version-upgrade entry, reader fallbacks (see checkpointtool fallback for `att_is_unsigned`), and dump/protocol enumeration changes — rejected: the bitmask carries both bits with zero migration and identical outcomes |
| DDL rejection of the modifiers | Tiny diff | Deviates from MySQL (accepts+enforces); breaks MySQL dump ingestion; issue's expected behavior prefers enforcement — fallback only if enforcement is found infeasible |
| Validator-only LOAD reuse (v1) | Minimal diff | Cannot express conversion order/clipping/diagnostics (review finding 3) — rejected |
| COLUMNS template referencing new mask builtin/table-function fields on all branches + mandated pre-downgrade template restore (v3 shape) | Full mask rendering on view/subscription branches immediately | Persisted definitions become un-bindable on pre-change binaries (round-4 finding 1 evidence: single 2-arg overload; unknown table-function field error), making same-line rollback depend on an extra mandatory operational step — rejected in favor of the repair-free CASE template with deferred branch rendering |
| Per-feature MORPC protocol fence (v25-v27 shape) | Mixed-version negotiation of the attribute channel with pre-fix peers | Rejected (round-28 finding; D5 v28): the repository upgrade contract permits shutdown upgrades only and prohibits new per-feature protocol versions, capability gates, and older-peer fallbacks; this feature changes neither RPC framing nor the handshake and no mixed-version project decision is recorded |

## Rollout, compatibility, and operations

Mixed-version and downgrade rules are D5/D7. Operational notes: no new
configuration; failure blast radius for statement work is statement-scoped
(strict admission aborts the statement, existing transaction machinery rolls
back); no hot-path cost for unattributed targets (one attribute test on the
plan type at build time; admission absent from the eval DAG). v28 ships NO
steady-state background work — the v11-v27 bounded COLUMNS
reconciliation phase is de-scoped with the protocol floor that made its
reach gaps possible (below), restoring v10's "no new background work"
promise; the only lifecycle additions are main-idiomatic versioned
bootstrap upgrade entries. Performance model: admission adds one
row-scan over the staged batch for attributed targets only — same shape
as the existing MEDIUMINT bounds pass; no extra allocations for
non-negative values; the upgrade entry runs once per tenant at the
shutdown upgrade and no-ops on the canonical definition thereafter.

### Catalog initialization and upgrade repair

New accounts install the current COLUMNS definition in their account transaction.
A stopped released 4.1/4.2 source upgrades to catalog **4.0.7/41** through the
existing bootstrap owner. The consolidated tenant entry installs the decorated
COLUMNS definition; its check is idempotent and preserves logical identity and
grants. The former unreleased 4.0.18 repair handler is retired. The published
4.0.6/5 handler remains unchanged; current metadata belongs to 4.0.7.

The authoritative routing, source boundaries, CDC and snapshot admission,
cancellation, rollback and restart contracts are in
[catalog upgrade design](20261028-catalog-upgrade-407.md). Prior draft references
to frozen main-era 4.0.7 or forward 4.0.18/19 routes are superseded: only released
sources are supported, and 4.0.7 becomes immutable when it is released.
Historical stored-view identity readers remain independent of handler retirement.

Validation uses real SQL for plain and attributed DECIMAL COLUMNS results on
upgraded and fresh tenants, entry failure/idempotence tests, caller-owned rollback,
and restart/CDC admission controls. Historical evidence is not current-code proof.

## Design-review disposition

### v1 → v2

| Review finding | Resolution |
|---|---|
| 1. Choose runtime/durable representation; define encodings, conversions, nullability, validation, old/new codec behavior; add round-trip + restart validation | Resolved by the representation section: `types.Type` freeze proved from code; plan.Type proto field 14 + `att_is_unsigned` bitmask chosen; five named conversion owners; I6 nullability clause; old/new codec table; restart round-trip test added to the plan |
| 2. Correct sign-check order and negative-near-zero oracle for decimal/string/float sources; retain zero controls | Corrected contract and D-mechanism verify MySQL 8.0.45 (`field.cc:2836-2875,2885,2950,2967`); admission staged pre-round; oracle matrix includes `-0.004`/`-0.005` from all source kinds and exact-zero controls |
| 3. Define mode-aware LOAD assignment, raw staging, format/mode matrix, warning and Arrow buffer ownership; plan public LOAD tests | LOAD section rewritten: shared plan owner, mode matrix incl. LOCAL semantics, single warning ownership; public LOAD tests added (staging representation finalized in v3 below) |
| 4. Add explicit validation for DDL rejection and failure atomicity with mode-expected outcomes | Default-admission, narrowing-ALTER preservation, and multi-row atomicity oracles added to the plan with strict/non-strict expected outcomes |

### v2 → v3

| Review finding | Resolution |
|---|---|
| 1. Choose a representable LOAD staging format; define scan/project type agreement, precision bounds and buffer ownership; add mixed-scale, exponent and excess-source-precision cases | LOAD section rewritten: textual formats stage **raw field text** as `T_varchar` via the existing `makeType`/`loadTimeTextInput`/`getColData`-early-return seam (`external.go:1343-1348, 1904-1908, 2026-2031`; serial path only — parallel already stages every non-array column); Parquet stages source-faithfully (file-declared `DECIMAL(pf,sf)`, STRING→text, natural numerics); Arrow keeps owned buffers; admission is lexical (sign + nonzero mantissa) so no coefficient ever needs a common scale; scan/project type agreement, precision bounds, and buffer ownership stated explicitly; mixed-scale/exponent/excess-precision/quoting cases added to UT and public plans |
| 2. Close the separate information_schema metadata channel; shared mask-aware interface, local/view/subscription producers, compatibility for persisted view definitions, upgraded-catalog validation | "Public metadata channels" section: shared `(types.Type, attrs)` helper; mask-aware builtin in the `mo_show_visible_bin` family; all three COLUMNS branches gain the mask — local via `mc.att_is_unsigned`, view via a new `mo_view_columns` output column (`view_columns.go:67-75` + `table_function/view_columns.go:283-329`), subscription via `mo_subscription_columns()` (`subscription_metadata.go:131-159,:794`); compatibility via frozen pre-attribute template + versioned upgrade entry (V46 pattern) + D5-floor gating (`sysview.go:155-186`); upgraded-catalog and per-branch validation added |
| 3. Account for checkpoint dump's boolean unsigned reader; bit-aware preservation, older-tool limits, checkpoint DDL round-trip for masks 0/1/3 | Same section, consumer 6: checkpointtool's `isTruthyCatalogValue` (`table_dump.go:5194-5196`) confirmed to drop mask 3; the tool gains an integer-mask reader and bit-aware ` unsigned zerofill` rendering next to the SET-leak guard (`:500-508`); older-tool degradation documented as a no-regression limitation; checkpoint DDL round-trip oracle for masks 0/1/3 plus unsigned-integer controls added to the plan |
| 4. Correct the direct-column CTAS contract; identify the propagation owner; distinguish computed expressions; plan SHOW CREATE/presentation/strict-write assertions | v2's discard-oracle was wrong. Verified MySQL 8.0.45 chain — `generate_create_field` (`field.cc:10437`) → `Create_field(Field*)` copies `all_flags()` (`create_field.cc:59-66`) → `prepare_pack_create_field` promotes flags for NEWDECIMAL (`sql_table.cc:4093-4094`) → `make_field` → `Field_new_decimal` (`field.cc:9533-9542,:9444-9449`). Contract corrected: direct single-source projections propagate attributes (owner: `genAsSelectCols`, `build_ddl.go:1683+`, via existing `ProvenanceSingleSource`; enum/set precedent `:1688-1692`); computed expressions do not; `CREATE TABLE LIKE` propagates automatically; CTAS SHOW CREATE/padded-read/strict-1264 and computed-column controls added to the plan |

### v3 → v4

| Review finding | Resolution |
|---|---|
| 1. Close rollback compatibility for the persisted COLUMNS definition (define an enforceable downgrade barrier or a restore procedure; include the new metadata-function contracts in protocol admission; plan an upgraded-catalog rollback test) | Verified valid before resolving: only a two-argument `mo_show_visible_bin` overload exists (`list_builtIn.go:15551-15568`), pre-change CNs reject a new `mo_view_columns` output field (`table_function/view_columns.go:327-328`), and the upgrade framework itself refuses cross-version downgrade (`service_upgrade.go:374`, `canResumeUpgrade:302-313`). Resolution ("Public metadata channels"): no new builtin ships in any persisted definition; the recreated COLUMNS template renders the base-table COLUMN_TYPE as plain `concat`/`case` SQL over the ancient two-argument builtin so it binds and evaluates identically on pre- and post-change binaries; view/subscription producer mask fields ship code-only, unreferenced by shipped definitions, with their rendering deferred to a later gated template generation (documented no-regression limitation); both fields join D5's admission list; D7 documents the same-line rollback attribute-stripping procedure with no template restore step; the validation plan gains the template-construct allowlist UT, upgraded-catalog COLUMN_TYPE asserts, and the CI old-image evaluation |
| 2. Choose Parquet staging that remains representable above 76 digits (lossless staging with explicit bounds and scan/project agreement; positive-fitting and negative >76-digit cases; differing source schemas) | Verified valid before resolving: widest staged decimal vector is width 76 (`types.go:869-871`) while today's importer already loads e.g. `DECIMAL(80,79)` `1.0` via unbounded `big.Int` decode + rescale (`parquet.go:3750-3771`, `:3771-3826`). Resolution (LOAD section): explicit bound — `pf <= 76` stages an exact `(pf,sf)` decimal vector in the smallest engine family; `pf > 76` stages exact source-scale decimal **text** (VARCHAR) rendered from the big.Int coefficient, feeding the lexical admission used by CSV, so positive-fitting values keep today's stored results (1.0 ⇒ `1.00`) and negatives error/clip per mode; per-file staging per batch keeps differing-schema multi-file loads defined; UT and public cases added (positive-fitting and negative 80-digit files; pf 40/70/80 mixed-file load) |
| 3. Define attribute propagation through UNION (merge/clearing at the shared output-type owner, ZEROFILL suppression; UNION/UNION ALL both orders, mixed signed inputs, CTAS consequence) | Verified valid before resolving: `setOperationOutputType` (`query_builder.go:693-707`) is the single owner for all set-op builders (callers `:1385`,`:4999`, `pushdown.go:1275`, `bind_replace.go:424`, `bind_insert.go:6249`, `bind_multi_insert.go:1173`, `utils.go:7611`,`:7982`) and returns the left branch's `plan.Type`, with same-type branches eliding casts (`:4936-4943`). MySQL doc verified by fetch: "The ZEROFILL attribute is ignored for columns involved in expressions or UNION queries." Resolution (new "Set-operation output attributes" section): at that owner, UNION/UNION ALL clear ZEROFILL and AND-merge UNSIGNED; INTERSECT/EXCEPT clear both (uniform derived-result rule); CTAS over set ops stays mask 0 via the existing provenance rule; target admission for `INSERT INTO attributed SELECT ... UNION` is unchanged; UT plus UNION/UNION ALL both-orders, mixed-sign, flag, and CTAS public cases added |
| 4. Plan executable proof for every promised ZEROFILL output channel (deterministic payload assertions for both binary-row serializers and text-export paths alongside text-row checks; padded bytes, flags/display length, NULL and plain/derived controls) | Verified valid before resolving: six independent serialization sites format decimals — text rows `appendResultSetTextRow`/`2` (`mysql_protocol.go:2821`,`:3424`), binary rows `appendResultSetBinaryRow`/`2` (`:2572` decimal `:2734-2743` via `mrs.GetString`; `:3123` decimal `:3309-3316` via `GetDecimal`), CSV export `constructByte` (`export.go:551`,`:705-716`), JSONLINE export `vectorValueToJSON` (`export.go:1647`,`:1736-1746`). Resolution: D4 enumerates all six sites behind one shared pad helper keyed on the result column's ZEROFILL flag; the validation plan gains one UT per site asserting exact padded bytes (`00000123.45`, zero rendering, NULL, plain signed control, derived control) plus protocol flags/display length, and wire-level public cases: text (plain query) vs binary (`PREPARE`/`EXECUTE`) identical padding and CSV/JSONLINE export file bytes |

### v4 → v5

| Review finding | Resolution |
|---|---|
| 1. Close the LOAD staging-to-projection type contract (stable staging/admission output type or a normalization owner; vector replacement, ownership, NULL batches; retain a differing-schema test through the same compiled plan) | Verified valid before resolving: scan output vectors are allocated once in `Prepare` from plan column types (`external.go:251-278`, `ctr.buf == nil` guard) and function executors bind/reset declared result types (`evalExpression.go:1115-1148`, `:1906-1909`), and the Parquet mapper only appends into the pre-allocated vectors (`parquet.go:4997-5052`, `:5283`, rollback `:5317`) — so v4's per-file staging types (decimal `pf<=76` / text `pf>76` / natural numerics) had no implementable contract. Resolution (LOAD section): one uniform staging type — `T_varchar` text on every format and every file — so the compiled LOAD plan has one stable input/output contract, no vector replacement exists by construction, admission's LOAD input is uniformly the string source kind, NULLs stage as null entries (never rendered), and the promised `pf40`/`pf80` mixed-file case runs through the same compiled plan (UT and public cases updated) |
| 2. Bound high-scale Parquet text materialization before allocation (bounded lossless encoding, or schema validation + allocation bound pre-render; oversized-scale and malformed-schema cases proving bounded failure or conversion, cleanup, and negative-near-zero preservation) | Verified valid before resolving: `parquetDecimalScale` returns the file's int32 scale unvalidated (`parquet.go:4790-4795`); fixed-point rendering at a forged huge scale would amplify a tiny field, while today's target rescale returns zero without expansion (`roundScaledDecimalBigInt`, `:3886-3895`) and the batch byte-budget check runs only after mapper materialization (`:5143-5151`). Resolution (LOAD section): bounded lossless scientific encoding — fixed-point source-scale text only for well-formed `0<=sf<=76, pf<=76` schemas (<= pf+2 bytes), otherwise `<coeff>e<-sf>` rendered from the decoded `big.Int` (length bounded by decoded content + ~13 bytes, never by declared schema); parse path verified: `ParseDecimal64/128/256` accept `e`/`E` with int32-bounded exponents (`decimal.go:2053-2065`, `:2255-2267`, `:2422-2434`) and scale application is allocation-free for extreme exponents (`Decimal64.Scale` `n<-19` ⇒ 0, `:360-366`; Decimal128 `:440-446`), so `1e-2147483647` into `DECIMAL(10,2)` stores `0.00` like today's zero-from-rescale and `-1e-2147483647` fails admission (negative nonzero mantissa); malformed-scale positive/negative, cleanup (existing `closePagesOnError` `:5146` / rollback `:5317`), and content-proportional-memory cases added to the plan |

### v5 → v6

| Review finding | Resolution |
|---|---|
| 1. Preserve source values before VALUES literal materialization (define a shared source-preserving route for attributed targets in both VALUES builders; carry the plan attribute metadata to that boundary; admission before target-scale conversion; quoted negative-near-zero VALUES cases under strict/non-strict/IGNORE) | Verified valid before resolving: both VALUES builders call `MakeInsertValueConstExpr` first and continue on success (`bind_insert.go:5865-5872`, `build_constraint_util.go:2266-2276`; function at `:1964`), and its decimal branches (`:2071-2084`) call `util.SetInsertValueDecimal64/128`, which parse the literal text directly at `typ.Width, typ.Scale` (`eval_expr_util.go:1033-1062`, `:1086-1115`) — a plan-time target-scale quantization that erases the sign of `'-0.004'` before admission. Resolution (Domain enforcement section): for decimal64/128/256 targets with a nonzero attribute mask — read from `col.Typ`, in scope at both call sites (`bind_insert.go:5794-5795`, `build_constraint_util.go:2208-2209`) — the builders skip the fast path and bind the literal through the generic route (`bindNumVal` `base_binder.go:11396` + `makePlan2DecimalExprWithType` `make.go:68-110`, natural typing with sign preserved) into the existing admission → `cast_assign` composition, whose operand is the unmodified source expression so even constant folding admits before rounding; unattributed targets keep the fast path byte-identical (I4); the INSERT IGNORE special-type handler is untouched; UT plan-shape assertions plus strict/non-strict/IGNORE diagnostics-and-stored-value pairs added through both builders |
| 2. Align lexical admission with the actual assignment grammar (shared assignment token semantics incl. signed radix forms and whitespace; preserve pre-rounding sign checks and invalid-input diagnostics; radix controls through VARCHAR-source DML and LOAD) | Verified valid before resolving: the text-varlena assignment cast parses via `parseCastNumericToken` (`func_cast.go:7446-7479`; sign split `:7411-7423` on `strings.TrimSpace`; radix bases `0b/0o/0x` with decimal expansion `:7482-7491`; sign applied after target-scale quantization `:8656-8667`,`:8696-8699`), and existing tests prove signed radix conversion (`func_cast_test.go:1009-1017`, `'-0x123'` ⇒ `-291.00`), while v5's raw-string prefix scan (`assignmentDecimalPrefix` `:7794-7799`) consumes only `-0` from `'-0x1'` and classifies it zero — a stored `-1.00` would pass admission. Resolution (Domain enforcement section): the string classifier consumes the same token the cast consumes, per `strToDecimal64/128/256` sub-path (`:8562+`,`:8903+`,`:8999+` — token/radix grammar for text varlena, byte-parser decimal grammar for IsBin, prefix grammar for the charset-255 branch); violation iff negative sign AND a nonzero mantissa digit (base-10 scan facts or any nonzero radix digit; exponents never affect zeroness; no coefficient materialized); tokenizer rejection abstains so lexical diagnostics and the IGNORE prefix-fallback (`:8606-8622`) stay authoritative; LOAD staged text is text varlena and inherits the identical classifier; UT radix/sign-zero/whitespace/abstention controls and public VARCHAR-source DML plus CSV radix matrices added |

### v6 → v7

| Review finding | Resolution |
|---|---|
| 1. Remove target-scale binding before VALUES admission (specify target-free source binding for attributed targets in both VALUES builders including decimal and float literal branches; extend validation to unquoted decimals and scientific literals) | Verified valid before resolving: both builders bind VALUES literals with the column type (`NewDefaultBinder(..., col.Typ, ...)`, `bind_insert.go:5826`; `sourceType := col.Typ`, `build_constraint_util.go:2234`), `bindNumVal` receives it via `defaultValueBindType` (`base_binder.go:13060-13071`) and parses decimal literals at target width/scale (`:11477-11517`) with float literals routed to the same target-scale path (`:11586-11590`) — so v6's skip alone still reached target-scale materialization. Resolution (VALUES boundary bullet): the builders ALSO bind the literal with a target-free binder through the existing suppression machinery (`defaultValueBindType` empty under `integerArgumentSourceContext`/`suppressDefaultValueBindType`, `:13061-13069`; the integer-target binder selection at `bind_insert.go:5835`,`:5882-5884` and the `sourceType` reset at `build_constraint_util.go:2234-2239` extended to attributed decimals), so decimal literals take natural typing (`make.go:68-110`) and scientific literals keep the float path (`base_binder.go:11591-11597`); UT plan-shape and per-mode behavior cases extended to unquoted decimals and scientific literals plus the public VALUES matrix |
| 2. Apply admission to numeric prefixes retained by IGNORE (define admission over the effective prefix selected by IGNORE before rounding; specify lexical/range diagnostic ownership and ordering; DML and LOAD cases for '-1 junk' and '-0.004 junk' with positive-prefix and invalid-zero controls) | Verified valid before resolving: under `castModeAssignmentIgnore` the cast retains a numeric prefix for a tokenizer-rejected string and stores it converted with a truncation warning (`func_cast.go:8606-8614`,`:8948-8956`,`:9044-9052`; contract pinned by `func_cast_ignore_test.go:297-305`, `'12.34tail'` ⇒ `12.34` + `WARN_DATA_TRUNCATED`), so v6's abstain-on-rejection lets an IGNORE-retained `-1` into an unsigned target. Resolution (classification + diagnostics bullets): admission classifies the value the cast retains — retained-prefix negatives (`'-1 junk'`, `'-0.004 junk'`) violate before rounding; diagnostic ownership: admission owns the 1264 and replays the retained-prefix truncation warning through the existing appender (truncation FIRST, 1264 SECOND, matching MySQL `store()` order; no double emission), no-prefix invalid input and non-IGNORE rejection keep the cast's existing diagnostics unchanged; special-syntax guards (`isAssignmentSpecialNumericSyntax`, `:7977-7993`) preserve the existing error; UT controls and public DML/LOAD matrices added |
| 3. Treat IsBin values as byte magnitudes rather than decimal text (correct the binary-source contract to the existing byte conversion while preserving its errors; IsBin controls: negative-text bytes, magnitudes, zero, empty, overflow) | Verified valid before resolving (code read plus executed probe): `types.ParseDecimal64FromByte` accumulates `y = y*256 + byte` — an unsigned big-endian byte magnitude (`decimal.go:2163-2181`; decimal128/256 `:2650-2689`), so bytes `0x2d 0x31` (ASCII `'-1'`) store positive `115.69` and empty input keeps InvalidInput. Resolution (classification bullet): IsBin sources are always admitted (non-negative by construction); empty/overflow errors stay with the cast; UT controls across families plus public `x'2d31'`/`x'00'` cases added |
| 4. Include the effective sign produced by radix conversion (derive effective sign/nonzero from the complete radix grammar through shared logic; plan '0x-1'/'-0x-1' plus binary/octal controls without changing signed-decimal semantics) | Verified valid before resolving (code read plus executed probe): the token grammar rejects only a leading second sign (`func_cast.go:7450-7452`), `big.Int.SetString` keeps an embedded sign after a radix prefix (`:7482-7490`), and all three families negate the result iff scanned sign differs from token sign (`:8697`,`:8864`,`:8897`) — so `'0x-1'` stores `-1.00` while `token.negative` is false and `'-0x-1'` stores `1.00`. Resolution (classification bullet): the violation test uses the cast's own combine rule — effective negative iff `scanned != token`, AND nonzero scanned mantissa; the radix expansion is the cast's bounded shared allocation (round-6's no-materialization claim corrected); UT effective-sign pairs across families plus VARCHAR-source DML (`'0x-1'`, `'-0x-1'`, `'0b-1'`, `'-0o-7'`, `'0x-0'`) and CSV LOAD matrices added |

### v7 → v8

| Review finding | Resolution |
|---|---|
| 1. Preserve DEFAULT source precision before admission (extend the source-preservation contract to DDL DEFAULT binding and relevant reconstruction paths before assignment conversion; specify the binding owner and strict constant-validation boundary; source-shape and negative-near-zero DEFAULT cases for unquoted/scientific/quoted forms plus positive/zero/volatile controls) | Verified valid before resolving (code read plus executed probe): every DDL DEFAULT binder owner constructs a destination-carrying `DefaultBinder` and binds BEFORE the admission wrapper (`build_util.go:995-1001` vs `:1029`, `:1141-1154`; `build_ddl.go:1961-1980`; `table_dump_binding.go:332,355`), and numeric literals are converted against target width/scale at bind time — probe: `DEFAULT -0.004` / `DEFAULT -4e-3` on `DECIMAL(10,2)` bind to `unary_minus(cast(lit '.004'/'4e-3' -> decimal(10,2)))` (`ValType=6` = `P_float64`, target-typed `returnDecimalExpr`, `base_binder.go:11586-11590`,`:11524`), so |v| < 0.005 loses its sign before admission; quoted literals bind as raw consts (`:11625-11627`) and remain faithful. Resolution ("DDL DEFAULT source-preservation boundary" bullet in Domain enforcement + D9): the VALUES target-free predicate extends to all four DEFAULT binder owners (empty `defaultValueBindType` under the existing suppression knobs, `:13061-13069`; natural typing verified by the untyped-binder probe control `decimal(3,3)`); the strict constant-validation boundary reuses the MEDIUMINT precedent (`build_util.go:1043-1068` — strict force-evaluation of a constant default source including admission, mapped to 1067 via `:1066-1069` / `mapDDLAssignmentCastError :1268-1274`, regardless of session sql_mode), with volatile/NULL defaults excluded from early evaluation; UT source-shape assertions and public 1067 cases added for unquoted `-0.004`, scientific `-4e-3`, quoted `'-0.004'`/`'-1.5'` plus zero/positive/NULL/volatile controls and a CTAS-inherited-default round-trip |
| 2. Handle retained prefixes after complete conversion validation fails (define classification over the complete pre-quantization conversion outcome incl. post-tokenization failures; apply IGNORE retention at the same boundary as the cast; preserve non-IGNORE diagnostics; add '-1junk', '-0.004junk', incomplete-exponent, positive-prefix, and zero-prefix controls through DML and LOAD with values and warning order) | Verified valid before resolving (code read plus executed probe): `parseCastNumericToken` ACCEPTS `'-1junk'`/`'-0.004junk'` (after sign split the body has no leading second sign and no whitespace, `func_cast.go:7446-7479`) while `decimalCastCoefficient` rejects them on complete consumption (`:8656-8664`) and IGNORE retains the negative prefix (`:8606-8614`) — probe: `'-1junk'`/`'-1e'` -> `-1.00`, `'-0.004junk'` -> `0.00` (pre-round negative), each with the truncation warning, identical across decimal64/128/256, and non-IGNORE parses reject all of them; the `'12.34tail'` contract (`func_cast_ignore_test.go:297-303`) pins exactly this later-failure stage. Resolution (classifier restructure in the string-classification and diagnostics bullets): admission classifies over the complete conversion outcome — succeed / fail-at-tokenization / fail-after-tokenization-at-consumption — applying IGNORE retention at the same boundary the cast applies it (`err != nil` from `parseDecimal64*CastString` feeds `assignmentDecimalPrefix`), so consumption-rejected negatives violate identically with the same truncation-first-1264-second ownership; IGNORE retained-prefix overflow (prefixed range) and every non-IGNORE failure keep the cast's own error; UT controls (`'-1junk'`, `'-0.004junk'`, incomplete-exponent `'-1e'`, positive-prefix `'1junk'`, zero-prefix `'0junk'`/`'-0junk'`, prefixed-range `'999...9tail'`) and the public DML + LOAD IGNORE matrices assert stored values and SHOW WARNINGS order |

### v8 → v9

| Review finding | Resolution |
|---|---|
| 1. Preserve LOCAL CSV input before numeric-prefix rewriting (attributed-decimal bypass of the destructive numeric-prefix rewriting before staging, covering serial and parallel readers; preserve NULL, empty-field, and trimming policies; staging-byte assertions and LOCAL CSV cases under empty sql_mode for unquoted/quoted radix, retained-prefix, invalid-zero, and signed-decimal controls) | Verified valid before resolving (code read plus executed probe): the rewrite runs inside `getColData` at `external.go:2002-2009` BEFORE the text-staging branch at `:2026-2031`, on the single code path both readers share (serial `reader_csv.go:243` → `getOneRowData`/`materializeOneRow` → `getColData` `external.go:1859-1895`; parallel staging via `param.ParallelLoad` at `:2026`), gated by `shouldApplyLoadDataNonStrictAdjustments` (`:1510-1516` — LOAD + `Extern.Local` + CSV + non-strict, independent of serial/parallel) and the decimal-inclusive family predicate (`:1578-1588`). Executed probe of `loadDataNonStrictNumericPrefix` (`:1590-1631`, temporary in-package test, deleted; log `state/artifacts/issue-30940/probe-r9-load-prefix.log`): `'-0x1'`->`'-0'` (loses the radix form's required 1264), `'0x123'`->`'0'` (loses `291.00` entirely), `'0x-1'`->`'0'`, `'-0x-1'`->`'-0'`, `'-1junk'`/`'-1 junk'`->`'-1'` and `'-1e'`->`'-1'` (lose the 1265 truncation diagnostic), `'12.34tail'`->`'12.34'`, `'abc'`/`''`->`'0'` (lose the invalid diagnostic), complete exponents preserved (`'-1e2'`), whitespace trimmed first (`'  -0x1  '`->`'-0'`) — contradicting v8's own CSV radix and IGNORE matrices under both readers. Resolution ("Raw-field preservation boundary" in the LOAD CSV staging bullet): the rewrite branch gains an attributed-decimal exception reading the target column mask from the same `col.Typ` the staging decisions already read (`external.go:268`/`:1343-1348`) so the raw trimmed bytes stage unmodified; one gate covers both readers because both funnel through `getColData`; NULL mapping (`:1990-1999`), empty-field `"0"` fill (`:1992-1995`), trimming (`:1976-1979`), the quoted-field exemption (`:2005-2008`), error-mode rollback (`:1766-1832`), the strict CHAR check (`:2021-2025`), and the temporal/`char` normalizations are explicitly preserved; unattributed targets keep the rewrite byte-identically (I4). Staging-byte UT assertions and LOCAL CSV public cases under empty sql_mode (unquoted and quoted radix, retained-prefix `-1junk`/`-0.004junk`/`-1e`, invalid-zero `abc`, signed-decimal `-1.5`/`-0.004`/`-0`/`1.5`) added to the validation plan |
| 2. Detect constant DEFAULTs beneath the admission composition (constant detection on the original bound source before composition; a separate strict validation composition from that source with explicitly strict admission and 1067 domain mapping; reuse across DEFAULT reconstruction owners; assertions proving constant negatives reach validation while volatile defaults stay unevaluated) | Verified valid before resolving (code read plus executed probe): the MEDIUMINT precedent's source check reads `cast.Args[0]` under a `cast_assign(source)` composition (`build_util.go:1044-1052`), but this design composes `cast_assign(admission(source), target)`, and admission must be volatile to resolve sql_mode at execution time — the same reason `cast_assign`/`cast_ignore` are flagged volatile (`list_operator.go:2741-2754`,`:2773-2783`); `function.IsConstant` rejects volatile or diagnostics-bearing nodes (`function.go:420-441`). Executed probe (temporary in-package test, deleted; log `state/artifacts/issue-30940/probe-r9-constant-gate.log`): `IsConstant(raw source)` = true, `IsConstant(admission-like volatile node(source))` = false, `IsConstant(cast_assign composition)` = false, and v8's gate `IsConstant(cast.Args[0])` = **false** — the strict path would never fire for `DEFAULT -0.004` — while controls `cast_strict(source)` and plain `cast(source)` are true, so detection on the raw source is sound. Resolution (strict constant-validation boundary rewritten in the DEFAULT bullet + D9): each owner captures the target-free raw bound source `src` BEFORE composition and detects `function.IsConstant(src, false)`; a separate authoring-time STRICT validation composition is built from `src` with BOTH layers strict (strict admission entry-point by name on the `cast_strict` precedent + `forceAssignmentCastExprWithName` `cast_strict` to the target) so a non-strict authoring session cannot route the inner admission through clip+warning — answering the finding's second half — and a domain violation maps to ER_INVALID_DEFAULT via the existing `:1066-1069`/`:1268-1274` precedent while other errors keep `mapDDLAssignmentCastError`; the stored expression stays the mode-sensitive composition (I4); one shared helper owns capture/detection/build/evaluate/map and serves all four DEFAULT owners (CREATE/ALTER, ON UPDATE, CTAS reconstruction incl. `finalizeCTASDefaults`, dump/restore re-derivation); UT gate-shape assertions pin constant-vs-volatile detection on the raw source, the both-layers-strict validation shape, volatile bypass with no early evaluation, and unattributed destinations byte-identical |

### v9 → v10

| Review finding | Resolution |
|---|---|
| 1. Define policy replacement for persisted admission compositions (one shared mechanism restoring the complete assignment boundary under the current statement policy, switching both admission and cast while preserving user-written expressions and explicit CASTs; cover defaults, generated columns, and ON UPDATE consumers; typed idempotence assertions and public strict/non-strict/IGNORE cases proving clipping, warning counts, and failure atomicity for negatives produced by persisted expressions) | Verified valid before resolving (code read against every cited site): `getDefaultExprForAssignment` returns every non-BLOB/TEXT default — DECIMAL included — unchanged (`build_util.go:2590-2607`, early return `:2595-2596`; the BLOB/TEXT branch `:2600-2606` strips the persisted wrapper and re-forces it under the statement `ignore` flag — the precedent); `applyGeneratedColumnAssignmentCast` replaces only the OUTER wrapper by name (`cast`/`cast_assign`/`cast_strict`/`cast_ignore`) and rebuilds from `f.Args[0]` (`build_util.go:2423-2456`, the swap at `:2446-2450`), so a persisted admission node inside `f.Args[0]` keeps the mode-resolved entry during IGNORE execution; the ON UPDATE copies are used verbatim (`bind_update.go:833`; `bind_insert.go:2897`) while user-written ODKU assignments receive the statement-policy wrapper (`bind_insert.go:2860` with `builder.isInsertIgnore`); the update binder already threads an `ignore` parameter (`bind_update.go:195-228`,`:243`,`:318`). With v9's persisted `cast_assign(admission(src), target)` composition and volatile defaults permitted, strict sql_mode + INSERT IGNORE would error 1264 at the inner admission where MySQL clips with one warning — under defaults, generated columns, and ON UPDATE consumers alike. Resolution (new "Persisted-expression policy replay boundary" bullet in Domain enforcement, plus DEFAULT-bullet cross-references): one typed boundary rewriter owned by the plan builder (home beside `forceAssignmentCastExprWithProcess`, `build_constraint_util.go:1470-1482`; same build-time policy axis as `assignmentCastFunctionName`, `:1441-1461`) recognizes the persisted admission composition by typed shape (assignment-cast-family outer whose arg0 is the plan-owned admission node — never user-written CASTs/subexpressions inside `src`, following the existing guard comments `:2430-2435`,`:2603-2605`) and rebuilds BOTH layers: IGNORE ⇒ `cast_ignore` + admission IGNORE entry; non-IGNORE ⇒ `cast_assign` + admission mode-resolved entry; sql_mode strictness stays execution-time; explicit CASTs untouched (I4). Consumers migrated: `getDefaultExprForAssignment` (attributed-composition branch; all ten callers inherit; unattributed byte-identical), `applyGeneratedColumnAssignmentCast` (delegates to the rewriter for the admission composition; legacy/array/blob branches unchanged), both ON UPDATE copies (statement ignore flag; REPLACE keeps `false`). Strict+IGNORE ⇒ clip to `0` + exactly one 1264 admission warning per adjusted row and statement commits; strict ⇒ 1264 error with atomicity; non-strict ⇒ v9 clip+warn; round-9 authoring validation untouched. UT: typed idempotence (double application under one policy returns the once-rewritten expression per policy; IGNORE → non-IGNORE reproduces the DDL boundary), recognizer abstention on non-boundary shapes and user CASTs, unattributed byte-identity, and per-consumer shape assertions; public: volatile `DEFAULT (-1-RAND())` and generated-unsigned-over-negative-signed and ON UPDATE negatives under strict+INSERT IGNORE (clip+single warning, committed), strict (1264, atomic zero rows), empty sql_mode (clip+warn), warning-count and failure-atomicity assertions, `UPDATE ... SET col = DEFAULT`, positive/zero controls, unattributed byte-identity; rewriter naming added to remaining-open |

### v10 → v11

| Review finding | Resolution |
|---|---|
| 1. Obtain design review for the new persistent COLUMNS reconciliation phase — explain why ongoing reconciliation is necessary, evaluate initialization/upgrade alternatives, and define coordination across CNs, scan/DDL budgets, cancellation, retry/rollback, failure containment, and corresponding validation; the approved v10 design specified versioned tenant upgrades for repair and explicitly promised no new background work or synchronization (implementation-review P2) | Verified valid before resolving: v10 item 3 promised only the frozen pre-attribute template plus the versioned upgrade entry plus floor-gated authoring ("unchanged from v3"), and Rollout stated "no new background work, caches, or queues ... no new synchronization"; the v10-scoped implementation added `pkg/bootstrap/service_information_schema_columns_maintenance.go:37-144` (single-flight guard, account cursor, repeated tenant scans, transactional view replacement) wired into recurring maintenance at `pkg/bootstrap/service_upgrade_tenant.go:369` with errors terminating the shared pass — a lifecycle, operational-cost, and failure-scope change beyond the approved design. Resolution (new "Persistent COLUMNS reconciliation" subsection under Rollout; amendments to item 3 and the Rollout operational notes; validation-plan UT and live bullets extended): necessity argued from the verified bootstrap-window/rolling-upgrade reach gap plus the live existing-data-cluster observation; alternatives table (entry-only, per-CN one-shot init repair, on-read rendering, periodic full-list replay) resolves to entries + init ladder + bounded reconciliation sharing one canonical entry; coordination via the single upgrade-service writer, the in-process single-flight guard, and the one-txn content-driven recreate; budgets pinned to 32-account pages on the existing 10-second cadence (worst case 32 recreate DDL pairs, read-only steady state, no new timers or wide scans); cancellation via the service context with a closed gate degrading to a write-free skip; retry/rollback anchored on the persisted definition as the durable marker with a process-local commit-gated cursor that resets to 0 on restart; failure containment classifies the lone gate failure (swallowed after rollback, retried next tick) against all other errors (abort later maintenance phases for one tick, logged, retried; statement and upgrade paths unaffected), with the inherited VIEWS-precedent delay caveat recorded; validation assigned — executed maintenance UTs (`ut-bootstrap-columns-maint.log`, `ut-bootstrap-all.log`) and live/BVT evidence (`probe1_core.log`, `bvt-run2.log`) cited as reusable, multi-CN matrices remain the CI remainder. The revision changes design text only; the preserved implementation already realizes exactly this contract |

### v11 → v12

| Review finding | Resolution |
|---|---|
| 1. Define actual cross-CN reconciliation coordination — either cluster-wide ownership or a sound multi-writer transaction protocol with the serialization point, stale-read/conflict handling, retries, and CN-scaled budgets; validate with two independent services observing the same stale definition and competing to commit | Verified valid before resolving: each CN constructs its own bootstrap service (`pkg/cnservice/server.go:1431-1439`), `BootstrapUpgrade` starts per-service tenant workers (`pkg/bootstrap/service_upgrade.go:71-76`), and maintenance ownership is process-local (`pkg/bootstrap/service_upgrade_tenant.go:402-411`), so v11's "sole writer" claim covers one process only, and v11 specified no concurrent-transaction behavior. Resolution (subsection rewritten): multi-writer transaction protocol — the per-account repair txn is the serialization point; in-txn content re-check handles staleness; three safe race outcomes (peer commit observed ⇒ no writes; identical DDL ⇒ one commit + retryable-class rejection (`ErrTxnWWConflict`/`ErrTxnNeedRetry`/`ErrTxnNeedRetryWithDefChanged`, precedent `isSystemViewRetryableError` `service.go:516-536`) or harmless byte-identical double commit; loser rollback with nothing partial persisted); conflict classes are convergence noise — logged, swallowed after rollback, resolved by next pass's zero-write content check; other errors go to failure isolation; deadlock precluded (single-object txn); CN-scaled budgets (C-1 rolled-back short txns worst case, read-only steady state); cluster-wide election rejected. Validation plan gains the two-service race test over one shared catalog stub plus loser conflict-class injection; multi-CN deployment variant → CI remainder |
| 2. Close the repair lifecycle when automatic upgrade is disabled — a reachable repair lifecycle or an enforced prerequisite before attributed declarations are admitted, covering bootstrap and restart, with validation scenarios asserting the resulting COLUMNS metadata in these modes | Verified valid before resolving: the maintenance worker starts only under `s.cfg.AutomaticUpgrade` (`server.go:1453-1469`); login-time repair returns immediately for equal versions (`service_upgrade_tenant.go:66-69` — inside the universal login path `mysql_protocol.go:1736-1746` → `Session.MaybeUpgradeTenant` → `CheckTenantUpgrade` `server.go:768-785` → `bootstrap.MaybeUpgradeTenant`); `InitSystemViews` repairs only sys-account TABLES/tracing (`service.go:478-514`). Resolution (new "Repair lifecycle when automatic upgrade is disabled" block): extend the equal-version branch to a content-driven check for THIS tenant inside the already-open login txn — floor published + definition diverged ⇒ gate recheck + canonical recreate in-txn (transition-path contract, `columns_upgrade_test.go` precedent); converged ⇒ one read, zero writes; floor unpublished ⇒ nil skip (presentation-only gap: domain enforcement and SHOW CREATE are catalog-driven regardless); errors propagate retryably like the transition path. Coverage: account-scoped information_schema means every account with observers logs in and self-heals; reconciler remains the bulk path; init ladder and versioned entries unchanged. Rejected: declaration gating (statement-path coupling beyond the gap), `InitSystemViews` extension (sys account only), config-note lifecycle. Validation gains equal-version repair UT scenarios (stale/converged/floor-unpublished/error) plus an integration scenario; deployment-variant CI remainder |
| 3. Prevent persistent repair errors from starving unrelated maintenance — failure isolation preserving atomic replacement and visible diagnostics while healthy accounts and independent phases progress, failed accounts retained for retry; validate persistent injected failure across repeated passes, a healthy later-page account, and orphan-privilege maintenance still executing | Verified valid before resolving: v11/v10-shaped code runs the whole page in one txn and any non-gone, non-gate error returns from `maintainInformationSchemaColumns` without cursor advance (`service_information_schema_columns_maintenance.go:88-144`), and the wiring aborts before orphan-privilege maintenance on that error (`service_upgrade_tenant.go:369-374`); the cited DDL-error test clears its injected error after one occurrence (`service_information_schema_columns_maintenance_test.go`), so only transient recovery was proven. Resolution (Mechanism/Retry/Failure-isolation blocks rewritten): per-account txns replace the page txn — atomic replacement preserved per account; the cursor checkpoints after each attempted account so healthy same-page and later-page accounts progress in the same tick; failed accounts enter a bounded in-memory retry set (≤ one page, overflow counted) drained first on the next pass, with the persisted stale definition as the durable marker; every attempt logs account id, error, and ordinal; the wiring becomes phase-independent — a failed columns phase no longer skips orphan-privileges maintenance in the same tick. Validation gains: deterministic persistent failure across repeated passes asserting neighbor convergence, cursor progress, once-per-pass retry, and per-attempt diagnostics; a phase-independence test; the two page-atomicity tests reshaped and rerun |

### v12 → v13

| Review finding | Resolution |
|---|---|
| 1. Account for the tenant cache in login-time repair — define repair completion separately from version-check completion including cache admission/invalidation on floor change; a skipped or rolled-back repair must remain reachable; add a same-service scenario logging in below the floor, publishing floor 131, reconnecting, and asserting canonical COLUMNS metadata without restart or worker | Verified valid before resolving: `MaybeUpgradeTenant` consults the completion cache before opening its txn (`service_upgrade_tenant.go:45-49`) and admits every successful caller-owned check permanently (`:146-150`), so v12's below-floor nil-skip would have been cached and would strand the tenant past floor publication — with no reconciler under AutomaticUpgrade=false the gap is permanent. Resolution (new "Tenant completion cache and floor transitions" block in the login-repair design): repair completion redefined — cache admission only for a durable complete state under the current floor (canonical-verified converged no-op or committed recreate); a below-floor deferral returns nil for the login but is NEVER cached; errors stay uncached per the existing "failed admission must remain retryable" contract. Invalidation is admission-time exclusion: nothing incomplete is ever admitted, so floor publication needs no hook — the next login re-enters the branch, re-reads the definition, and repairs in the login txn. Reachability of deferred/rolled-back repairs follows (uncached ⇒ retried on next login). Steady-state cost bounded: after the first converged login, later logins take the pre-txn cache hit. Validation gains the demanded same-service scenario (below-floor login ⇒ deferral + no cache + zero writes; publish floor in-process ⇒ next login repairs in-txn and information_schema.COLUMNS serves canonical metadata, no restart/worker; third login cache hit) in the login-repair harness |
| 2. Bound blocking repair attempts before claiming failure isolation — finite account and phase time budgets, timeout rollback/retry behavior, healthy-account and subsequent-phase execution time retained; validate with a deterministically blocked repair proving timeout cleanup, retained retry work, and healthy-account/independent-phase progress before the blocker releases | Verified valid before resolving: the recreate DDL waits on the exclusive catalog-name lock (`pkg/sql/compile/ddl.go:3181`, `lockMoTable(..., LockMode_Exclusive)`) with the 120-second default wait (`pkg/defines/const.go:179`), the owning pass's context allows 24 hours (`service_upgrade_tenant.go:171-174`), and v12 specified no finite deadline, so one blocked account serialized every later account and, through the tick chain, the orphan-privileges phase ahead of any isolation; v12's planned tests injected immediate errors only. Resolution (new "Attempt and phase time budgets" block plus Mechanism/Cancellation/Failure-isolation amendments): per-attempt timeout context (10 seconds — one tick; above a healthy committed-read + gate + DDL pair, below the 120-second engine lock wait so our deadline terminates a blocked wait deterministically) aborting the account's txn via context rollback, classified as a distinct "attempt timeout" retryable outcome (logged with account id and ordinal, retry-set membership retained); per-pass 60-second phase budget checked between attempts, stopping cleanly at the next attempt boundary with the cursor at its last checkpoint and the retry set intact; healthy accounts converge in the SAME pass before the blocker clears and subsequent phases keep their tick time. Engine-lock-wait-only reliance rejected (one order higher, per-lock inside an unbounded pass). Validation gains: deterministically blocked recreate past the attempt budget (rollback + timeout class + same-pass healthy convergence + orphan-privileges execution before release; post-release convergence) and phase-budget exhaustion (clean stop at an attempt boundary, resume next tick); the budgets' constant names join remaining-open |

### v13 → v14

| Review finding | Resolution |
|---|---|
| 1. Include synchronous cleanup in the attempt-bound design — define how maintenance progress remains bounded through transaction cleanup while preserving eventual lock release and bounded outstanding ownership; add deterministic blocked-cleanup validation proving later-account and independent-phase execution before cleanup is released, followed by complete cleanup without leaked transactions or locks | Verified valid before resolving: `ExecTxn` rolls back synchronously on error (`pkg/sql/compile/sql_executor.go:218-224`, `exec.rollback` at `:895-899`), `txnOperator.Rollback` defers the unlock (`pkg/txn/client/operator.go:1240-1251`) into `lockService.Unlock` (`operator.go:2013`), ordinary `service.Unlock` replaces the caller context with the service context — its comment states remote cleanup retries until completion with service shutdown as the one terminal cancellation owner (`pkg/lockservice/service.go:769-774`) — and the remote cleanup loop retries under that context (`pkg/lockservice/lock_table_remote.go:549-604`); the v13 attempt context therefore bounded transactional work only, and the planned DDL-latch test never entered cleanup. Resolution (new "Attempt cleanup and pending-closure ledger" block plus budgets/Cancellation/Retry/Failure-isolation amendments): each attempt runs supervised in a pass-owned runner (`ExecTxn` including its synchronous rollback); the pass awaits it with the 10-second attempt window then a 10-second cleanup window (20-second worst case per fully blocked attempt); a runner still incomplete at the cleanup deadline classifies the account CLEANUP-PENDING with a pending-closure marker holding the runner's channel (one log line; the pass proceeds to the next account or phase); no second attempt opens while a marker is pending (at most one unfinished closure per account, ledger-bound pass-wide); harvest at pass start and between-items checks classifies finished runners (attempt timeout / original error) and returns the account to ordinary retry; eventual lock release is preserved by the engine's service-context retry, with a one-hour escalation log for aged markers; engine unlock semantics unchanged, inline cleanup wait rejected as the v13 gap, closure abandonment rejected for breaking single ownership; the login path keeps its per-connection contract. Validation gains the demanded blocked-cleanup scenario: rollback held on a harness latch past the cleanup window — next-account convergence and orphan-privileges execution before release, single-ownership during pending, post-release harvest classification and convergence, closed-txn and empty-lock registries |
| 2. Prevent retry work from exhausting every scan budget — specify fair scheduling between retries and fresh cursor work so persistent timeouts cannot consume every pass; extend deterministic validation with enough blocked retries to exhaust the phase budget repeatedly, asserting that a healthy account beyond the checkpoint still converges while failed accounts remain reachable for retry | Verified valid before resolving: v13's Mechanism drained the retry set before the cursor walk while the 60-second per-pass budget stopped passes with the retry set retained, so six persistent 10-second attempt timeouts (three at the v14 20-second worst case) would consume every subsequent pass before any fresh cursor attempt ran, and the round-13 mass-blockage scenario asserted clean stop/resume without progress — the v13 later-pages guarantee was false for accounts beyond the checkpoint. Resolution ("Scheduling (new in v14)" under "Retry and rollback" plus Mechanism/budgets/Failure-isolation amendments): strict retry/cursor alternation — one retry-ledger attempt then one fresh cursor attempt, round-robin, empty sides proceeding alone; the retry share is bounded at every other attempt slot and the first fresh attempt starts no later than the second slot (≤ 20 seconds in), so a healthy account beyond the checkpoint converges within a bounded pass count under any retry backlog while a healing backlog still drains at every other slot; cleanup-pending markers consume no attempt slots, only non-blocking harvest checks; drain-first-plus-quota-timer (a second interacting timer) and retry-only-on-cursor-wrap (heals only after a full walk) rejected. Validation gains the repeated-exhaustion scenario: twelve deterministically stalled retries exhaust the phase budget every pass — the healthy account beyond the checkpoint converges in pass 1's second attempt slot, all twelve remain retry-ledger members after every pass, the retry share never exceeds every other slot, and all twelve converge within a bounded pass count after the stalls clear; the round-13 mass-blockage clean-stop and resume-next-tick assertions are retained |

### v14 → v15

| Review finding | Resolution |
|---|---|
| 1. Define admission and eviction when pending closures fill the ledger — specify capacity reservation before spawning runners, non-evictable pending ownership across passes, and full-capacity backpressure with explicit cursor/retry reachability; reconcile with the unconditional fresh-progress guarantee; extend deterministic validation using a scoped small capacity and capacity-plus-one accounts: hold cleanup across multiple passes, mix ordinary failures with pending entries, cross cursor wrap, assert bounded outstanding runners and no duplicate account attempt, preserve independent-phase execution, then release cleanup and prove convergence | Verified valid before resolving: v14's Mechanism combined ordinary retries and pending closures in one 32-entry ledger with "newest kept" eviction while the v14 block required pending markers to remain and claimed the page bound caps outstanding closures — across repeated passes stalled cleanup can fill all 32 entries while fresh attempts continue, so the 33rd pending closure exceeds the resource bound, and evicting a marker leaves its runner alive (unreclaimable, because Unlock retries under the service context, `pkg/lockservice/service.go:769-774`) and removes the guard against another attempt on cursor wrap; the round-14 validation covered one marker and twelve retries, never saturation. Resolution (new "Ledger admission, eviction, and backpressure" block plus Mechanism/item-2/Scheduling/Failure-isolation amendments): class-split ledger keyed by account id — a RESERVATION is created at spawn and converts to an evictable RETRY entry (newest kept, eviction counted once per pass) or a non-evictable PENDING marker that leaves only through harvest; admission is strictly reservation-before-spawn (existing entries reused; new accounts admitted on free space or by evicting the oldest RETRY entry only; DENIED with no runner spawned when every entry is non-evictable), so live runners = RESERVATION+PENDING ≤ one page by construction and a capacity-plus-one pending closure is impossible; backpressure on denial logs once per pass, holds the cursor checkpoint, keeps alternation for existing RETRY entries and harvest, idles the columns phase cheaply, and leaves orphan-privileges untouched; the progress guarantee is reconciled — ordinary backlogs never block fresh work (RETRY eviction always yields a slot), ledgered accounts are skipped on cursor wrap, fresh admissions pause only under full non-evictable saturation (a fleet-wide lock-service stall) and resume on the first freed slot; evicting live markers (breaks single ownership), cancelling pending cleanup (impossible for service-context retries), and unbounded runner growth (the finding itself) rejected. Validation gains the scoped-capacity scenario: capacity 4 with eight groomed accounts (2 ordinary failures, 5 cleanup-latching = capacity plus one, 1 healthy probe) — ordinary-only eviction counted, DENIED admissions spawn no runner during all-pending saturation, at most 4 runners and one open txn per account across wrapped passes with no duplicate attempt, saturation line per pass, orphan-privileges every tick, and after latch release the checkpointed walk resumes and all eight converge with registries clean |

### v15 → v16

| Review finding | Resolution |
|---|---|
| 1. Separate runner completion from completed cleanup ownership — define a cleanup-complete or explicit ownership-transfer condition independently of runner completion; retain non-evictable ownership and a reachable cleanup owner when cleanup returns an error; prevent a new account attempt until that ownership terminates; extend validation with a returned cleanup error that leaves lockservice state retained, proving bounded ownership, no duplicate repair, and eventual cleanup after recovery | Verified valid before resolving: remote unlock returns deterministic cleanup errors (`pkg/lockservice/lock_table_remote.go:569-588`), ordinary `service.Unlock` returns them while retaining the closing transaction registered and fenced (`pkg/lockservice/service.go:830-839`), rollback propagates the error through the deferred unlock while the operator closes (`pkg/txn/client/operator.go:1236-1251`), and `ExecTxn` returns the joined error (`pkg/sql/compile/sql_executor.go:895-899`) — a completed runner can therefore leave the account's locks held, and v15's unconditional conversion of completed runners to evictable RETRY entries permitted eviction or a fresh repair txn while ownership was unfinished. Resolution (new "Cleanup errors and ownership termination" block plus harvest/lifecycle/guard/Failure-isolation amendments): harvest split — rollback-nil completions are cleanup-complete (evictable RETRY), non-nil rollbacks convert to the non-evictable CLEANUP-UNRESOLVED marker (ordinal, txn id, error text); cleanup completion ownership transfers to the named engine owners (periodic orphan unlock `pkg/lockservice/service_remote.go:1292-1336` with the deliberate fail-closed retention rationale `service.go:1314-1316`, waiter orphan checks `waiter_events.go:481-498`, `checkTxnTimeout`), never re-driven by the maintenance loop; ownership termination is proven independently of runner completion by the alternation-slot probe whose first action acquires the account's exclusive catalog-name lock (`pkg/sql/compile/ddl.go:3181`) under a 1-second probe wait — failed acquisition keeps the marker with no writes, success is the termination proof and the same txn runs the normal content check plus recreate; markers count toward non-evictable capacity, probe cost bounded at ~1s per pass per unresolved account, one-hour escalation retained; rejected: completion-based conversion, liveness pings without the serialization point, re-driving `Unlock` from our layer, time-based eviction. Validation gains the returned-cleanup-error scenario: retained lock yields CLEANUP-UNRESOLVED and is never evicted, probe-only slots perform zero writes and no duplicate repair while fenced, DENIED backpressure under mixed saturation, and post-release the SAME probe slot converges the account with the ownership bound one and clean registries |

### v16 → v17

| Review finding | Resolution |
|---|---|
| 1. Track cleanup independently of successful commit — define separately observable transaction and cleanup outcomes for both commit and rollback, retain bounded ownership after a successful commit with unresolved unlock while preserving commit-success semantics, specify how the runner obtains cleanup status, and add validation for committed repair with unlock failure and for repair failure with clean rollback | Verified valid before resolving: the commit-write path defers unlock and converts a failed unlock into success, keeping the failure only in the private `tc.mu.terminalOutcome` (`pkg/txn/client/operator.go:1515-1524` — an unlock failure must not turn a successful commit into an error; the flag gates only `claimRestart` at `:2330-2337`), `ExecTxn` then returns nil (`pkg/sql/compile/sql_executor.go:226-229`), and `service.Unlock` retains the closing transaction registered and fenced on error (`pkg/lockservice/service.go:830-839`) — while the design's success transition removed the ledger entry unconditionally (v15 item 1) and the harvest split distinguished only rollback outcomes (v16 item 1); the round-16 scenario exercised only a rollback error. Resolution (v17 block items 1/3 plus v15 item-1 and harvest amendments): transaction and cleanup outcomes are tracked separately on BOTH paths — rollback-nil remains the one synchronous cleanup-complete proof (failure path, with the unknown-commit resolver-transfer corner cited, `operator.go:1948-1990`), and EVERY committed attempt is cleanup-unverified BY RULE, converting to the new non-evictable CLOSURE-PENDING class that holds the captured closure identity (`executor.TxnExecutor.Txn()` `pkg/util/executor/types.go:49` → `TxnOperator.Txn()` `pkg/txn/client/types.go:121` → `TxnMeta.ID`/`CommitTS` `pkg/pb/txn/txn.pb.go:199-211`, replayed exactly as the engine's own deferred unlock consumed them); the runner obtains cleanup status by RE-ESTABLISHING closure itself — the supervised re-drive's nil is the only accepted proof — and discharge removes the CLOSURE-PENDING entry with no re-repair (commit-success semantics: the definition is already canonical). Validation gains the demanded pair: committed repair with unlock failure (entry kept and non-evictable, no second repair txn, discharge on recovery, clean registries) and repair failure with clean rollback (evictable RETRY, zero re-drive calls, converges — the over-conservation control) |
| 2. Provide reachable cleanup for retained originating-CN ownership — identify an effective cleanup owner reachable while the originating CN remains normally operational, require authoritative completion of retained ownership (or preserve it within an enforced bound after name-lock release), and extend validation to the actual recovery eligibility and a partial-unlock case where the name lock is available but originating transaction state or other references remain | Verified valid before resolving: the reconciler's local-origin transactions register with `remoteService == ""` (`pkg/lockservice/service.go:534-535`); the periodic orphan scan builds its timeout set only from remote-service registrations (`service.go:3238-3252`, the dequeue fed only for `remoteService != ""` at `:3036-3046`, entry selection at `:3270-3280`); the waiter paths treat same-service transactions as valid and ineligible for remote unlock (`service.go:3333-3335`, `:3360-3362`); the local-txn check returns immediately under normal service status (`pkg/lockservice/service_remote.go:1338-1340`, `canUnlockLocalTxn` at `:1372-1410` — drain-only); unlock finalizes tables individually and holds bind intents until the whole transaction closes (`pkg/lockservice/txn.go:1239-1246`, `:1295-1297`), so the v16 name-lock probe could succeed while other references remained; and the round-16 scenario's manual harness release simulated a recovery owner that does not exist for this class. Resolution (v17 block item 2, with the v16/v14 "ours-side Unlock" rejection explicitly reversed on this evidence): the maintenance loop owns finalization by re-driving `Unlock` through `lockservice.GetLockServiceByServiceID(s.sid)` (`pkg/lockservice/types.go:396-399`; registered before bootstrap runs, `pkg/cnservice/server.go:1162-1175` vs `:468`; the bootstrap sid is that same CN UUID, `server.go:1432-1439`) — reachable in-process under every service status; a nil re-drive is the AUTHORITATIVE total completion (resume semantics `:830-833`, idempotent absent-nil via the create=false lookup `:2846-2862` with the fallback `:803-807`, registry delete only after every table acknowledges `:836-845`), strictly subsuming the retired probe's serialization-point proof. Validation gains the demanded cases: actual recovery eligibility with no engine orphan machinery and no drain transition present (the re-drive is the sole `Unlock` caller for the recorded txn id across every pass), and the partial-unlock intermediate — the name lock's table released while a second table's unlock fails: the marker remains despite the free serialization point, the re-drive resumes only the unacknowledged table (no duplicate unlock), and discharge completes only with the registration deleted |
| 3. Account for unfinished cleanup created by failed probes — define probe reservation, supervision, and cleanup-error transitions that preserve every outstanding generation and enforce the aggregate resource bound; prevent another probe while its predecessor remains unresolved; reconcile probe cleanup latency with the phase budget; add deterministic failed-probe scenarios with blocked rollback and returned cleanup errors across repeated passes, followed by recovery and registry-cleanliness assertions | Verified valid before resolving: a failed remote `Lock` records an unconfirmed cleanup witness (`pkg/lockservice/lock_table_remote.go:462-477` — transport failures are indeterminate; `ErrRemoteLockWaitTimeout` returns only after the witness is recorded); the witness's rollback unlock runs under the service context, never the probe's one-second deadline (`pkg/lockservice/service.go:769-774`), and can itself return a cleanup error retaining another closing transaction (`service.go:830-833`); v16 ran probes outside the supervision and admission machinery on a ~1-second budget line with a marker holding only the original attempt's identity, so each pass could mint an unaccounted generation past the one-closure-per-account and page-capacity guarantees. Resolution (v17 block items 1/3/4): the probe mechanism is RETIRED wholesale — the closure discharge performs no `Lock` at all, so the probe's witness class cannot recur — and every closure action that CAN block joins the v14 supervised runner machinery (the same attempt and cleanup windows; PENDING with the new runner-kind field on overrun; one runner per account entry, so a later re-drive is impossible while its predecessor is unresolved — the prevention the finding demands, applied to the only remaining closure action); a re-drive creates no transaction and no new obligation, preserving every outstanding generation (per account at most one open maintenance transaction, one recorded unfinished generation, one unfinished runner) with the aggregate resource bound unchanged (the non-evictable classes share the v15 capacity; re-drive cost is charged to the retry share already bounded at every other slot — the phase-budget reconciliation). Validation gains the demanded deterministic scenarios, translated from probes to re-drives: a blocked re-drive held past the supervision windows across repeated passes (PENDING kind=closure, no second re-drive, constant generation set and ledger occupancy, one escalation line per pass, fresh admissions unaffected, harvest-discharge after release) and returned re-drive errors across N passes (one re-drive per account per pass, zero account txns of any kind in the harness registry, then nil → discharge → clean closed-txn and empty-lock registries) |

### v17 → v18

| Review finding | Resolution |
|---|---|
| 1. Define observable outcomes for every ExecTxn terminal branch — specify an explicit terminal-outcome record and its observation owner; distinguish body failure, rollback completion, known commit outcome and post-commit visibility failure, preserving committed semantics and cleanup ownership independently of the returned error; extend validation with successful commit plus failed visibility wait and failed unlock, commit-error branches, and independent rollback-result observation | Verified valid before resolving: `ExecTxn` rolls back only the attempt body's failure before returning, returns commit errors WITHOUT rollback (`pkg/sql/compile/sql_executor.go:217-229`), and can return a committed-log visibility-wait error AFTER a durable commit (`:230-246`; the maintenance's committed-read options enable that wait — `WithWaitCommittedLogApplied` in the Mechanism options), so v17's three return-keyed conversions could miss CLOSURE-PENDING or misclassify; and the failure path joins rollback unconditionally (`:895-899` — a joined error never equals its non-nil input), so no returned-error comparison can establish rollback-nil and v16/v17's "synchronous cleanup-complete proof" had no code signal; the txn operator hides a failed post-commit unlock (`pkg/txn/client/operator.go:1515-1524`), leaving response-published status (`:1786-1788`, `:1556`) as the only commit-truth signal. Resolution (v18 block item 1, with v14 harvest and v15 item-1 classification amendments): the supervised runner samples a terminal-outcome RECORD (body result, ExecTxn return, post-return operator snapshot, unknown-commit code test) classifying COMMITTED / UNKNOWN / FAILED from snapshot status and error code alone — committed semantics and cleanup ownership tracked independently of the returned error; the rollback-nil fast path retires and every FAILED entry pays exactly one bounded absent-nil confirmation re-drive in its own slot before becoming evictable. Validation gains scenario 1a (commit + failed visibility wait + failed unlock), 1b (definitive commit rejection; pre-acquire busy) and 1c (harness-split hidden rollback outcomes observed identically) |
| 2. Separate unknown-commit resolution from ordinary Unlock re-drive — define an explicit unknown-outcome transition covering successful and failed resolver admission; preserve accounted ownership until authoritative resolver completion, forbid ordinary Unlock before resolution eligibility, and specify timestamp selection and subsequent content recheck; plan deterministic tests for both admission outcomes, unresolved repeated passes, absence of premature unlock or duplicate attempts, and eventual fenced cleanup | Verified valid before resolving: `ErrTxnUnknown` is reachable at `pkg/txn/client/operator.go:1587-1594`; the deferred cleanup then schedules `ResolveCommitUnknown` with operator-private deadline/sequence instead of ordinary unlock (`:1946-1990`, admission pre-acquired at `:1446-1462`) and scheduling can fail; the resolver contract forbids releasing locks before allocator proof (`pkg/lockservice/types.go:158-170`) and obtains proof plus its own fence timestamp (`pkg/lockservice/unknown_commit.go:443-468`; fenced unlock `pkg/lockservice/service.go:1278-1350` — "the allocator fence has already made a later Commit impossible"); ordinary `Unlock` performs no such check (`service.go:789-828`); the commit timestamp is assigned only from a received response (`operator.go:1786-1788`), re-admission rejects a zero deadline (`unknown_commit.go:213-215`), resolution completion is decided privately by active-transaction absence (`:361-399`), and the public interface carries no resolution query (`types.go:100-136`) — a successful scheduling return is therefore not cleanup completion, and v17's generic re-drive rule cannot safely cover unknown outcomes. Resolution (v18 block items 1-4): UNKNOWN classifies to the new non-evictable RESOLUTION-PENDING class; ordinary `Unlock` of that generation is forbidden unconditionally (eligibility is unobservable from this layer, so the pre-fence ban never lifts); the maintenance selects NO timestamp (fence selection is resolver-exclusive); the alternation slot runs one supervised bounded READ-ONLY content recheck per pass — canonical content removes the entry as converged (fenced residue stays with the engine resolver), a clean stale read converts to evictable RETRY for alternation re-repair (writes serialize against residual locks inside the attempt window; the fence guarantee plus content idempotence preclude double-apply), and a read error retains the entry with escalation; the admission-failure corner follows the same ledger lifecycle with stranded-lock recovery deferred to engine orphan machinery (recorded residual risk). Validation gains scenarios 2a (successful admission in both terminal arms, zero-Unlock assertion, fence-timestamp provenance), 2b (scheduling failure, horizon escalation, recovery convergence) and 2c (duplicate suppression and per-pass bounds) |

### v18 → v19

| Review finding | Resolution |
|---|---|
| 1. Close UNKNOWN ownership before retiring its ledger entry — define an observable completion or accounted ownership-transfer condition independent of content equality; if overlapping generations are intended, specify their complete ownership and capacity model instead of superseding unresolved identity; extend validation with successful stale and canonical reads before resolver completion, covering both successful and failed resolver admission; verify retained accounting, admission bounds and eventual cleanup | Verified valid before resolving: the recheck (`versions.CheckInformationSchemaColumnsDefinition`) is an ordinary read-only SELECT (`pkg/bootstrap/versions/upgrade_strategy.go:401-407`); `WithMinCommittedTS` only moves the txn snapshot (`pkg/util/executor/options.go:134-145` — freshness, not resolution completion); and `resolvePending` skips the entry without cleanup exactly when its fence RPC fails or the txn is still committing (`pkg/lockservice/unknown_commit.go:330-359`), so v18's stale-read discharge could retire the marker and readmit repairs while the unknown generation's locks still stood. Resolution (item 3(b) redesigned): content observation is retired as the discharge oracle; the RESOLUTION-PENDING slot runs one supervised closure-establishing FINALIZING ATTEMPT per pass — a full repair-shaped transaction (attempt + cleanup windows, runner kind = finalizing attempt) whose recreate is authorized on BOTH content outcomes (a byte-identical atomic recreate on canonical content — the scoped exception to never rewriting a converged account). Its COMMIT is the content-independent completion condition: committing requires the account's exclusive catalog footprint (`pkg/sql/compile/ddl.go:3181`, `lockMoTable(..., LockMode_Exclusive)`), which cannot grant while the unknown generation's locks stand — so COMMITTED proves every recorded unknown generation released the footprint and retires the unknown marker as ACCOUNTED (residual fenced cleanup stays with the engine resolver), converting the entry to CLOSURE-PENDING holding the finalizing generation for the ordinary re-drive discharge. Overlapping unknown generations get the demanded complete model by chain induction: each later generation passed the footprint only after every earlier one released it, and release follows settle-and-fence (`unknown_commit.go:330-359`), so at most THE LATEST can be unresolved; the O(1) marker {latest unknown txn id, unknown count} is diagnostics-only, never actionable, never re-driven — at most one ACTIONABLE generation, one open maintenance txn, one unfinished runner and ONE ledger slot per account (v15 capacity untouched, unknown count never multiplies occupancy); a finalizing attempt ending UNKNOWN becomes the new latest; FAILED (blocked-on-footprint timeout included) keeps the entry RESOLUTION-PENDING with the attempt's generation discharged under the item-3(a) quiescence discipline into a cleared record — never an ordinary RETRY conversion. Termination: the resolver settles the latest unknown in bounded retries (`unknown_commit.go:27`, `:330-359`), after which the footprint acquisition closes; the admission-failure corner keeps the same lifecycle with orphan recovery as residual risk; a restart re-derives through a fresh ordinary attempt whose commit establishes the same closure. Validation: round-18 scenarios 2a-2c reshaped to finalizing-attempt assertions, plus the new round-19 matrix — stale and canonical reads BEFORE resolver completion across both admission outcomes (marker retained, ZERO `Unlock` calls, ZERO fresh admissions, occupancy and slot constant, no RETRY conversion), chain accounting for successive UNKNOWN attempts, and eventual closure with clean registries |
| 2. Distinguish deferred rollback from a completed transaction outcome — add an explicit transition for an active deferred terminal owner; establish that owner's completion before treating a lockservice nil result as total transaction closure or admitting another repair; specify the observation mechanism and deterministic validation using a fatal SQL error with workspace rollback held on a latch; assert no premature discharge, duplicate repair or unaccounted cleanup goroutine | Verified valid before resolving: a fatal SQL error schedules an ASYNCHRONOUS deferred rollback before `Exec` returns (`pkg/sql/compile/sql_executor.go:385-393` → `client.DeferredTxnRollback.RollbackAfterRunningSQL` → `pkg/txn/client/operator.go:2116-2143`); the cleanup launches without blocking on SQL exit (`sealAndRunWhenDrained`, `operator.go:496`), as `operator_test.go:2874-2915` exercises; the runner's follow-up public `Rollback` is rejected by the sealed terminal gate (`claimTerminalCall`, `operator.go:2367-2380`); and the workspace rollback runs BEFORE the operator mutex (`operator.go:1216-1224`), while the Aborted status publishes only under it (`:2051-2054`, `closeAsAborted :2066-2086`) — so the item-1 record can classify FAILED with the snapshot still ACTIVE, and a nil re-drive then would prove only the lock-idempotent side, never the deferred owner's workspace closure. Resolution (item 3(a) quiescence gate): a CLEANUP-UNRESOLVED slot FIRST resamples the operator snapshot through the attempt's retained handle — an in-memory, mutex-published read opening no transaction and no RPC. ACTIVE ⇒ deferred terminal owner still running ⇒ NO re-drive, NO repair admission, one "deferred owner active" line per pass with one-hour escalation, entry and occupancy unchanged (the engine bounds the owner itself: `runningSQLWaitTimeout`, `operator.go:78`); ABORTED ⇒ the owner's PRE-unlock work finished (workspace rollback precedes the mutex; Aborted publishes only under it), the remaining obligation is exactly the deferred unlock the re-drive duels idempotently, and the v17 re-drive proceeds — nil then discharges total closure per class; CLOSURE-PENDING needs no gate (a published Committed admits no later rollback owner). Validation: the round-19 fatal-error scenario with the workspace rollback held on a latch (`blockingRollbackWorkspace` shape) — while latched, zero re-drive `Unlock` calls, zero admissions, constant occupancy, one "deferred owner active" line per pass; after release, ABORTED observed, exactly one re-drive returning absent-nil, discharge to evictable RETRY, convergence with no unaccounted cleanup goroutine or lockservice effect in the gated window |

### v19 → v20

| Review finding | Resolution |
|---|---|
| 1. Supervise snapshot acquisition as potentially blocking work — put snapshot acquisition inside the bounded supervised closure action, with explicit ACTIVE, completed and pending outcomes and one retained runner per entry; extend the deterministic scenario: capture ACTIVE, release workspace rollback, hold remote unlock while the operator mutex is owned, and verify fresh-account progress and orphan-privileges execution before releasing unlock, with bounded occupancy and no duplicate observer or re-drive | Verified valid before resolving: `Txn()` -> `getTxnMeta(false)` -> `tc.mu.RLock` (`pkg/txn/client/operator.go:893-895`, `:1716-1721`); `rollbackOwned` acquires `tc.mu.Lock()` at `:1224` and its LIFO defer chain runs `tc.unlock(ctx)` (`:1241-1251`) BEFORE `mu.Unlock` (`:1225`) and before the Aborted publication (`:1236-1239`); `tc.unlock` (`:1943`) hands ordinary cleanup to lockservice `Unlock`, which swaps the caller deadline for the service context with service shutdown as the one terminal cancellation owner (`pkg/lockservice/service.go:768-778`) — so a stuck remote unlock holds the write lock indefinitely, a pass-synchronous resample blocks the pass beyond both budgets, and `runningSQLWaitTimeout` cannot bound that leg. Resolution (item 3(a) restructured): the CLEANUP-UNRESOLVED slot runs ONE supervised CLOSURE ACTION per entry — the snapshot sample executes INSIDE the v14 runner under the attempt and cleanup windows, never pass-synchronously; outcomes are explicit: ACTIVE (no `Unlock` issued, no admission, one "deferred owner active" line per pass, entry and occupancy unchanged), TERMINAL (Aborted implies terminal work finished — workspace rollback precedes the mutex `:1216-1224`, deferred-path Aborted publishes only after the unlock leg and under the write lock `:1241-1251`/`:1236-1239`, other set points `:2051-2054`/`:2066-2086` — the in-runner re-drive proceeds and its nil/error answer discharges per class), and OVER-WINDOW (blocked sample or re-drive, indistinguishable to the pass: entry converts to PENDING with runner kind = closure re-drive, harvest-only, exactly one runner retained, never a duplicate observer or re-drive, occupancy constant, one escalation line per pass, pass progress preserved by alternation; the stuck leg is the accepted v14 unreclaimable service-context cleanup class and unblocks on remote recovery or service close). The v19 "the engine itself bounds that owner" claim is corrected, item 4 counts the fully blocked closure action instead of a cost-free resample, and the rejected alternatives gain the pass-synchronous resample. Validation: the round-19 fatal-error scenario is re-voiced to in-runner sampling, and the new round-20 blocked-sample scenario captures ACTIVE, releases the workspace-rollback latch, holds remote unlock while the write lock is owned, and asserts PENDING conversion, exactly one runner, no duplicate observer or re-drive, constant occupancy, the 30-second timeout NOT bounding the wait, fresh-account convergence and orphan-privileges execution BEFORE the unlock releases, then release-to-converge with clean registries and no unaccounted goroutine |

### v20 → v21

| Review finding | Resolution |
|---|---|
| 1. Define shutdown ownership for retained runners — an enforceable shutdown protocol (admission sealing, dependency lifetime, completion ownership, blocked-cleanup disposition) plus deterministic close-with-unlock-held scenarios proving shutdown never certifies dependency retirement while owned work remains and eventual release leaves nothing behind | Verified valid before resolving: bootstrap `Close` stops only its stopper (`pkg/bootstrap/service.go:447-449`, current tree); CN teardown closes bootstrap BEFORE the engine and the transaction dependencies — `closeBootstrapService` in the drain chain at `pkg/cnservice/server.go:577` (definition `:720-733`), `s.closeStoreEngine` at `:607`, `closeCNServiceSteps`/`stopRPCs` at `:611-612` retiring `_txnClient` (`:832-834`), `_txnSender` (`:840-842`), and `lockService` (`:843-845`), server.go outside the worktree diff; and the commit path's LIFO defer chain owes workspace finalization after the unlock leg returns — the workspace-finalization defer is registered FIRST (`pkg/txn/client/operator.go:1473-1490`, `workspacePrepared = true` at `:1472`, `FinalizeCommitWithUnknownResult` `:1480`, `FinalizeCommit` `:1490`) and the unlock defer LAST (`:1515-1525`), running first under `tc.mu`, operator.go outside the diff; the unbounded unlock leg itself is the round-20 verified service-context retry (`pkg/lockservice/service.go:768-778`). Resolution (new "Shutdown ownership for retained runners" block plus the OVER-WINDOW pointer): admission sealing at service-context cancellation checked at the existing harvest points; dependency lifetime carried by the verified teardown ordering — never by waiting — with the lockservice retiring last among the runner's dependencies; completion ownership WITHOUT a join at `Close` (the join would park teardown on the later lockservice step), the operator's defer chain completing workspace finalization inside its own goroutine against still-open dependencies; blocked-cleanup disposition unchanged per marker with the ledger evaporating at process exit and the persisted-definition restart rule re-deriving accounts; `Close` certifies admission sealing only, never dependency retirement. Rejected: joining at `Close`, early dependency or service-context cancellation from the maintenance, discharge-on-close certification, shutdown-drain forced unlocks (resolver fence contract). Validation gains the close-with-unlock-held scenario with both terminal arms |
| 2. Bound the gate and page-loading stage before account attempts — an explicit finite wait and cleanup-ownership protocol with bounded outstanding work and duplicate suppression across ticks, plus blocked page-transaction validation asserting bounded phase return, unchanged cursor, and subsequent-phase execution before release | Verified valid before resolving: the shared loader executes SQL (`pkg/bootstrap/service_information_schema_views_maintenance.go:177-188`), the executor passes its CALLER context into `txnClient.New` (`pkg/sql/compile/sql_executor.go:284-295`), and `New`'s snapshot acquisition can wait for logtail advancement until that context ends or the client closes (`pkg/txn/client/client.go:717-739` with `updateSnapshotWithClose` at `:739`; the wait's arms are caller-context done, client-closed, and logtail notification at `pkg/txn/client/timestamp_waiter.go:293-300`), all four files outside the worktree diff — while v20 loaded the page BEFORE any supervised slot with only the 24-hour outer bound (`service_upgrade_tenant.go:171-174`) and a 60-second budget checked only between account slots, so a stalled initial snapshot or page read bypassed both account budgets and delayed later phases past the claimed bound. Resolution (new budget item 3 plus Mechanism/Cancellation amendments): the gate evaluation and 32-account page load run as ONE supervised pre-attempt stage in the v14 runner machinery under a stage context (child of the 24-hour pass context) bounded by a 10-second stage window plus the 10-second cleanup window; expiry cancels the stage context — deterministically ending the snapshot wait through its caller-context arm — then one cleanup window is awaited; a still-unfinished stage runner is RETAINED with the pass performing harvest only and returning with an unchanged cursor (checkpoints follow attempted accounts only) while the orphan-privileges phase runs in the same tick; at most ONE stage runner is ever outstanding (no duplicate spawn while unfinished, one "page stage unfinished" line per pass, stale page DISCARDED on stranded completion); cleanup ownership is the v14 service-context class for the read-only stage txn (no catalog footprint), terminating at remote recovery, client close, or lockservice close with nothing persisted; the phase-return bound becomes the stage windows plus the slot arithmetic. Rejected: outer-bound-only continuation, duplicate spawn without a dedup oracle, engine-surface instrumentation of `txnClient.New` or the timestamp waiter, skip-harvest. Validation gains the blocked page-transaction stage scenario and its post-cancellation rollback variant |

### v21 → v22

| Review finding | Resolution |
|---|---|
| 1. Preserve dependency lifetime through retained-runner completion — replace the ordering-only argument with an enforceable shutdown protocol (bounded drain, completion barriers, explicit lockservice-last cycle resolution), and extend validation through complete CN teardown with a commit runner blocked in unlock while workspace finalization is still owed | Verified valid before resolving: the leg's deterministic release is lockservice cancellation, the LAST teardown step (`pkg/cnservice/server.go:843-845`), retiring AFTER the engine (`:607`) and the transaction client/sender (`:832-842`) — so a runner blocked until lockservice shutdown still owes `closeLocked` and workspace finalization (commit-path LIFO defers, `pkg/txn/client/operator.go:1473-1490`, `:1515-1525`) against components that already retired; v21's Close-return openness check established nothing about later teardown and no completion barrier existed. Resolution (shutdown block parts 2-3 redesigned): a BOUNDED close drain (20-second window, finite by construction) whose FULL shape certifies completion of every defer chain BEFORE the first dependency retirement (`:607`) and whose PARTIAL shape certifies only the seal and retained count; plus VERIFIED completion independence for the remainder — `closeLocked`'s only externally visible work is two immutable default closed callbacks (`pkg/txn/client/operator_events.go:128-144`; `client.go:473-476`; the executor path appends none — only incrservice appends), both memory-only (`updateLastCommitTS` atomic CAS `:1010-1025`; `closeTxn` signal/map/histogram `:1222-1245`); `txnClient.Close()` touches no active operator, map, or atomic (`:858-898`); the executor workspace IS attached (`sql_executor.go:349-353` -> `pkg/vm/engine/disttae/engine.go:888-909`, `AddWorkspace` :909) and its finalization is CN-local (`disttae/txn.go` `FinalizeCommit` :3152-3157, `FinalizeCommitWithUnknownResult` :3159-3167, `Rollback` :3247-3272, `delTransaction` :3274+), so the post-leg dependency set is process-lifetime memory state that no mid-teardown close retires — all cited files outside the worktree diff — resolving the lockservice-last cycle without claiming completion for post-drain runners. Rejected: unbounded join, ordering-only lifetime, engine-context cancellation or teardown reorder, discharge-on-close certification, forced-unlock drain, completion wait inside lockservice close. Validation: the round-21 close arm re-voiced to the bounded drain (strong certification, completion before engine close) plus the round-22 partial-drain scenario through complete CN teardown with a dependency-call watchdog asserting zero runner calls into retired components and a terminal no-leftovers check |

### v22 → v23

| Review finding | Resolution |
|---|---|
| 1. Include outstanding cleanup obligations in the full-drain predicate — define strong completion over every outstanding ledger and deferred-owner obligation, require authoritative quiescence/closure evidence before FULL DRAIN else retain seal-only certification with an explicit shutdown lifetime contract, and add deterministic Close-between-passes scenarios for an ACTIVE detached owner and a completed unlock-error marker | Verified valid before resolving: the v22 FULL DRAIN predicate awaited retained-runner completion channels only, while the design itself permits a FAILED attempt or closure action to end with the detached rollback owner still ACTIVE (design lines 2596-2603, 2685-2696) — the owner is scheduled before `ExecTxn` returns (`pkg/sql/compile/sql_executor.go:385-393` calling `RollbackAfterRunningSQL`; body `pkg/txn/client/operator.go:2116-2143`, detached through `runSQLTracker.sealAndRunWhenDrained` at `:2128` and dispatched `go callback()` at `:476-477`, so no reconcile-runner channel ever covers it) — and a completed runner can end with an unlock-leg error that leaves the transaction registered and fenced (`pkg/lockservice/service.go:830-833`, skipping `deleteActiveTxn` `:835` and `tryCompleteDrain` `:839`), the admission seal then preventing the closure action or re-drive that would resume cleanup; Closing between passes can therefore find no unfinished runner while both obligation shapes persist, and both v22 scenarios arrange an outstanding runner and miss them. Resolution (shutdown block parts 2-4 amended, certification boundary only): strong completion is redefined over channels AND obligations — the harvest record gains the unlock-leg disposition from the existing seams (commit-path terminal-outcome flip `pkg/txn/client/operator.go:1515-1525`; synchronous-rollback joined error `:1241-1251`), the unresolved classes gain the owner-state field (verified-terminal / active / over-window / unverified — an owner-side unlock error is observable only in the owner's own log `operator.go:2131-2137`, so UNVERIFIED never discharges at close), FULL DRAIN additionally requires no outstanding-owner entry and no registration-retained marker as of the last recorded harvest (drain-time harvest may still discharge an entry and clear its marker), and PARTIAL DRAIN logs one class-tagged outstanding-obligation line per marked entry while certifying only the seal and counts, with an explicit per-class shutdown lifetime contract (owner: bounded detached context `operator.go:2129`, error-only externals after each retirement, CN-local workspace epilogue, remnant owned by lockservice close `pkg/lockservice/service.go:761-765`; registration: component-owned from the leg error, no post-seal maintenance reference, re-derived on restart from the durable pending-work marker). Rejected: in-drain owner waits (no exposed completion channel; the round-20 blocked-resample hazard; the rejected unbounded join renamed), a final in-drain closure action (seal and v20 sample boundary), runner-channel-only FULL (the round-23 fallacy). Validation gains two deterministic Close-between-passes arms (ACTIVE detached owner; completed unlock-error marker) asserting seal-only certification, class-tagged lines, bounded return, zero post-seal admissions, successful-call watchdog silence, and component-owned remnant retirement, partitioning the {runner unfinished} × {obligation marked} state space with the v22 arms |

### v23 → v24

| Review finding | Resolution |
|---|---|
| 1. Base cleanup certification on observable evidence — keep every cleanup-unverified generation marked outstanding until the already-designed supervised nil Unlock re-drive establishes closure, derive FULL DRAIN from that evidence combined with owner quiescence where required, and remove the unsupported private-outcome observation assumption | Verified valid before resolving: v23's part 2 sourced the commit-path unlock-leg disposition from the operator's recorded terminal outcome, but that field is PRIVATE — the commit-path unlock failure flips only `txnOperator.mu.terminalOutcome` (`pkg/txn/client/operator.go:1515-1525`), the `TxnOperator` interface exposes no accessor (`pkg/txn/client/types.go:112-218`, read this turn), and `Txn()` returns only `txn.TxnMeta` (`pkg/txn/client/operator.go:893-895`) — so a clean commit and a lock-retaining commit present the same nil `ExecTxn` result and the same Committed snapshot, and the required distinction is unobtainable through the specified interfaces. Resolution (shutdown part 2 obligation (ii) re-grounded): the discharge oracle is the already-designed supervised nil `Unlock` re-drive (the v17 rule — CLOSURE-PENDING re-drives directly, CLEANUP-UNRESOLVED re-drives after a TERMINAL sample, RESOLUTION-PENDING finalizes); every cleanup-unverified generation stays marked outstanding at admission and at close until its recorded nil answer discharges it, FULL DRAIN derives from the recorded nil re-drive answers plus owner quiescence where an owner can exist (drain-time harvest of a latent re-drive completing inside the window still discharges its entry), an error keeps the entry for the next supervised slot, and no unlock-leg disposition field is harvested — visible attempt errors (including the synchronous rollback's joined unlock error `:1241-1251`) inform state but never discharge. Rejected: private-outcome observation, a new synchronous lockservice residue probe at `Close`. Validation: the cleanup-unverified Close-between-passes arm asserts the sealed-off re-drive, the class-tagged line, zero post-seal admissions, and component-close ownership of any factual remnant |
| 2. Use a reachable barrier for the zero-runner ACTIVE-owner scenario — obtain ACTIVE with every reconcile runner completed via the existing pre-mutex workspace-rollback barrier, keep the owner blocked through Close and the relevant dependency retirements, then release it and verify the actual post-retirement cleanup path | Verified valid before resolving: `rollbackOwned` acquires the write lock at `pkg/txn/client/operator.go:1224-1225` and its LIFO unlock defer (`:1241-1251`) runs `tc.unlock` BEFORE `mu.Unlock`, so an owner latched inside its remote-unlock leg holds the write lock while the closure action's snapshot sample needs the read lock (`getTxnMeta` `:1716-1721`) — the sample would block (OVER-WINDOW, a retained runner), never the required zero-runner ACTIVE state; and releasing that latch before teardown (v23 scenario) let the owner finish before any retirement, making the lifetime assertion vacuous. The workspace rollback runs BEFORE the mutex (`operator.go:1217-1222`), so the round-19 pre-mutex barrier holds the owner ACTIVE without the write lock. Resolution (Round-23 owner arm re-voiced): latch the detached owner at the existing pre-mutex workspace-rollback barrier, run ONE closure action that samples ACTIVE and completes (zero outstanding channels), drive `Close` between passes (seal, one owner-outstanding line, no full certification, bounded return), keep the barrier through the relevant retirements with the successful-call watchdog asserting zero successful calls into retired components, then release and verify the actual post-retirement cleanup path — closed-client error from the retired sender, process-lifetime memory epilogue under the bounded detached context (`operator.go:2129`), and lockservice-close cancellation of any unlock retry with component ownership of any remnant (`pkg/lockservice/service.go:761-765`) — including the held-through-lockservice-close variant |

### v24 → v25

| Review finding | Resolution |
|---|---|
| 1. Account for detached rollback owners from the pre-attempt stage — retain the stage's cleanup obligation independently of runner completion; define supervised quiescence/discharge, replacement admission and Close certification for that stage slot, reusing the existing ownership machinery; add a deterministic fatal-page-SELECT scenario with the detached owner held at the pre-mutex workspace barrier; assert bounded ownership across passes and seal-only Close until closure is verified | Verified valid before resolving: the v24 stage block bounded outstanding stage work by its runner ("at most ONE stage runner is ever outstanding"), licensed a fresh stage the pass after the stage channel completed, and confined stage cleanup to the v14 retained-runner class, while the shutdown obligations (design part 2) covered only account-ledger classes — but the stage executes ordinary SQL on the stage txn through the shared loader (`pkg/bootstrap/service_information_schema_views_maintenance.go:177-188`, read this turn), a FATAL whole-txn error from that read (`MustRollbackTxn` membership in `wholeTxnRollbackCodes`, `pkg/common/moerr/txn.go:17-22`, loop at `:33`, read this turn) schedules a DETACHED rollback owner exactly as in the account path (`pkg/sql/compile/sql_executor.go:385-393`), and the executor's error path (`:218-224`, `:895-899`, read this turn) then fails fast against the sealed terminal call (`pkg/txn/client/operator.go:2367-2380`, CAS failure -> `NewTxnClosedNoCtx`, read this turn) — so the stage CHANNEL completes with the owner still ACTIVE and NO account-ledger entry ever minted, escaping every certification input. Resolution (stage block, shutdown part 2, certification text, and a new scenario block): the stage slot carries a cleanup record INDEPENDENT of the runner channel — {stage txn id, the v18 record class, owner state none / active / over-window / unverified / verified-terminal} — classified at stage completion by the v18/v19 failure-class table and bounded to AT MOST ONE outstanding stage-lifecycle owner (stage load runner, stage quiescence runner, or detached rollback owner); ONE supervised quiescence sample per pass (the v20 in-runner sample verbatim, OVER-WINDOW converting to the slot's one retained runner of kind stage quiescence) discharges it with NO re-drive leg — TERMINAL flips the record to verified-terminal and frees the slot, ACTIVE keeps it with one "stage owner active" line per pass; replacement admission requires a quiescent record plus no unfinished slot runner, else the pass performs harvest only with the cursor untouched and the orphan-privileges phase in the SAME tick; obligation (i) and FULL DRAIN additionally require no unquiescent stage-slot record, PARTIAL DRAIN logs one owner-outstanding line naming the stage slot, and the detached-owner class follows the v23/v24 owner contract. Validation: the new Round-25 fatal-page-SELECT scenario — a steady-state arm asserting channel completion with ZERO ledger entries, exactly one outstanding stage-lifecycle owner, no replacement admission across passes, then TERMINAL discharge and convergence; and a Close-between-passes arm asserting the owner-outstanding slot line, seal-only certification, the held-through-retirement watchdog, and post-release cleanup ownership, with a held-through-lockservice-close variant |

Remaining open (non-blocking, owner: implementation turn): the exact function
name/signature of the admission expression (including its strict evaluation
entry-point used only by authoring-time DEFAULT validation, and its IGNORE
entry used by IGNORE statement builds and the replay rewriter), of the shared
strict-validation helper (raw-source capture, constant detection, strict
composition build, isolated evaluation, error mapping) used by the four DDL
DEFAULT owners, of the shared persisted-boundary rewriter (typed recognizer
of the admission composition at the boundary position plus the both-layers
rebuild under the statement policy) used by `getDefaultExprForAssignment`,
`applyGeneratedColumnAssignmentCast`, and the two ON UPDATE copies, of the
shared SHOW CREATE
formatting helper, and of the CASE-wrapped COLUMNS COLUMN_TYPE expression
text; the exact name/signature of the shared pre-quantization facts helper
(token sign, scanned sign, nonzero fact, retained-prefix decision covering
both failure stages — one
family over the `strToDecimal*` sub-paths), of the VALUES fast-path skip
plus target-free binder predicate (now spanning the four DDL DEFAULT
binder owners alongside both VALUES builders), and of the attributed-decimal
attribute-mask gate inside `getColData`'s LOCAL non-strict rewrite branch;
the signed-zero display text (`0.00` vs
`-0.00`) for zero-valued inserts — to be cross-checked against MySQL 8.0.45
during implementation; the `MORPCVersion131` constant name/comment wording
chosen at implementation; the Parquet/Arrow numeric→text rendering helpers
(fixed-point renderer and shortest round-trip float formatting) inside the
mapper; the D7 enumeration predicate over `mo_columns`; the exact names
of the v13 time-budget constants (per-attempt repair timeout, per-pass
phase budget) and of the login-repair cache-admission predicate (durable
complete state under the current floor); the names of the v14
cleanup-window and pending-escalation constants, of the attempt-runner
supervision seam (runner completion channel, pending-closure marker,
harvest points), and of the strict-alternation slot scheduler; the
v15 ledger keying and eviction/saturated log-line wording (capacity
constant = one maintenance page, admission predicate and class
conversions per the v15 block); and the v17 closure-discharge seam
names — the PENDING runner-kind field, the closure re-drive's slot
action, the lockservice-handle absence log line, the "closure
pending"/"cleanup unresolved" per-pass wording — plus the marker
fields already fixed in the v17 block (attempt ordinal, txn id,
commit timestamp, committed flag, error text); v18 adds the
terminal-outcome record's field names (body result, ExecTxn
return, operator snapshot, unknown-commit code test) with the
visibility-failure diagnostic text, the RESOLUTION-PENDING
marker wording (a "resolution pending" per-pass line) and
its O(1) {latest unknown txn id, unknown count} fields, the
closure-establishing finalizing attempt's slot action name
and recreate-authorization flag, the supervised closure
action's two-step body (in-runner sample then re-drive)
with its ACTIVE / TERMINAL outcome names and its "deferred
owner active" per-pass wording, and the runner-kind enum's
domain (repair attempt, closure re-drive — reused by the
closure action — and finalizing attempt) — the v14 attempt
and cleanup window constants are reused unchanged, with
closure actions and finalizing attempts each awaited under
the full supervision windows. v21 adds the pre-attempt stage's
seam names (stage context, stage runner, the "page stage
unfinished" per-pass line, the stale-page discard path), the
admission-seal predicate with its harvest-point checks, and the
`Close`-path certification wording; the stage reuses the v13
attempt-window and v14 cleanup-window constants unchanged.
v22 corrects the certification seam names — the full-drain
certification text (replacing the Close-return openness
assertion), the drain-window constant, the retained-runner
per-entry drain line, and the dependency-call watchdog hooks
of the partial-drain scenario. v23 re-scopes the full-drain
certification text and adds the obligation seam names — the
unlock-leg disposition field (clean / retained-with-error)
sourced from the operator terminal outcome and the joined
rollback error, the owner-state field (verified-terminal /
active / over-window / unverified) on the unresolved
classes, the registration-retained marker, the two
class-tagged outstanding-obligation drain lines, and the
watchdog's successful-call assertion for the owner arm.
v24 corrects the closure-evidence seams: NO unlock-leg
disposition field is added (finding 1) — closure evidence
is the recorded supervised nil re-drive answer (the
existing discharge seam), the outstanding class tag
becomes cleanup-unverified, and the owner arm's barrier
(finding 2) reuses the round-19 pre-mutex
workspace-rollback latch held through `Close` and the
relevant dependency retirements.
v25 adds the stage-slot cleanup-record seams — the
owner-state field domain for the stage slot (none /
active / over-window / unverified / verified-terminal),
the stage quiescence sample (the v20 in-runner sample
reused, no re-drive leg) with its OVER-WINDOW retained
runner of kind stage quiescence, the "stage owner
active" per-pass line, the replacement-admission
predicate (record quiescent plus no unfinished slot
runner), and the owner-outstanding drain line naming
the stage slot.

### v25 → v26

| Review finding | Resolution |
|---|---|
| 1. Allocate protocol and function identities that are unused on current main — integrate current main, allocate fresh protocol and function IDs, update every authoring/placement/send/receive/bootstrap gate and hard-coded classifier, and revise D5 with traceable approval; revalidate compatibility against pre-fix main peers advertising 131/132 and rerun affected implementation proof | Verified valid before resolving: origin/main `420786e21566e5b9d5875e7d6a7f3102fd0761af` (blobs read in-repo) assigns protocol 131 to #31000's exact weighted percentile remote dispatch (`9a7e12a933`) and 132 to #31191's decimal float rounding and strict textual float conversions (`861a2c4cb6`) with `MORPCLatestVersion = MORPCVersion132`, and function IDs 605..615 to #31017's indicator kernels, 616..618 to #31020's MRANK/CUMRANK/TMRANK renumber (`ac4ec31587`), and 619 to #31000's re-issued QUANTILE_EXACT_WEIGHTED with `FUNCTION_END_NUMBER = 620` — while this worktree's diff declares `MORPCVersion131` for the attribute channel (`pkg/defines/const.go:169`) and `UNSIGNED_DECIMAL_ADMISSION{,_STRICT,_IGNORE} = 616/617/618` (`function_id.go:922-924`), so the >=131 gates admit pre-fix main peers advertising 131/132 with NO attribute channel, and serialized plan fragments decode the admission functions as rank functions on a main peer; tests comparing only protocol 130 against the worktree's 131 could never expose that class. Resolution: D5 is rewritten to bind the fence to `MORPCVersion133` and the function block to 620/621/622 (`FUNCTION_END_NUMBER = 623`) against those verified main blobs, with the integrate-then-re-verify-and-escalate rule (integrate origin/main first as the identifier prerequisite; record commit and new merge-base; re-run the unusedness query on both files; on a fresh claim allocate the next free values and return for traceable re-approval before submission); D7's rollout precondition and the reconciler floor-transition scenarios now read 133; the validation-plan fence row gains the demanded <=132-peer discrimination rows; historical disposition mentions of "floor 131" (round-12 onward) record the pre-reallocation numbering and are left as the review record. Gate/classifier inventory for the renumber (name-grep over the worktree: 27 references across 15 Go files, every one name-routed): `pkg/defines/const.go` (allocation + Latest); `pkg/bootstrap/versions/information_schema_columns.go` (`RequiredProtocolVersion` + floor check) with `pkg/bootstrap/versions/v4_0_14/upgrade_test.go`'s expectation; `pkg/bootstrap/columns_upgrade_test.go` peer values; `pkg/bootstrap/service_information_schema_columns_maintenance{,_test}.go` authoring-floor sets; `pkg/util/sysview/sysview.go` template selection + `pkg/util/sysview/predefined_test.go`; `pkg/sql/plan/build_constraint_util.go` + `pkg/sql/plan/persisted_ip.go` floors + `pkg/sql/plan/unsigned_decimal_protocol_floor_test.go`; `pkg/sql/compile/expression_protocol.go` / `expression_destination_protocol.go` / `remoterun.go` (authoring floor, destination classifier, send gate and its one raw error-text literal) + `pkg/sql/compile/unsigned_decimal_protocol_test.go`; function registration in `pkg/sql/plan/function/function_id.go` (with `list_operator.go` and `pkg/pb/plan/string_literal_form.go`) stays name-routed, so no admission call site moves; no raw 131/133 comparators or 616-618 numerics exist elsewhere (the numeric lookalikes in `mysql_sql.go` are goyacc table states, not IDs). Revalidation after integration (validation-evidence contract — rerun exactly what changed inputs touch): (i) every UT binding the fence value or registration identity (plan protocol-floor UT; compile unsigned_decimal_protocol UT; bootstrap columns_upgrade / v4_0_14 / maintenance floor assertions; sysview predefined selection; function registration and string-literal identity); (ii) NEW discriminating rows for peers advertising exactly 131 and 132 without the attribute channel at every gate — the counterexample class that would have let the old >=131 gate pass while the defect stood; (iii) baseline red/green repro, BVT 140/140, restart persistence, and the mandatory lifecycle race gate re-collected once on the integrated tree (binary identity changes; the rev7 race archive remains the shape precedent); (iv) SQL-semantics evidence whose inputs never referenced the numeric fence (repro oracles, domain math, display renderers) remains reusable |

v26 also resolves the v25 "Remaining open" item that left the
`MORPCVersion131` constant's name/comment wording to implementation:
the bound name is `MORPCVersion133` carrying the attribute-channel
comment from D5; every other v25 open item is unaffected.

### v26 → v27

| Review finding | Resolution |
|---|---|
| 1. Include numeric feature classification in the renumber design — the renumber design incorrectly excludes a numeric feature classifier and its stale test identities: `pkg/pb/plan/string_literal_form.go:1014` explicitly matches numeric 616-618, so a constant-only renumber leaves main's MRANK/CUMRANK/TMRANK classified as DECIMAL admission functions and misses execution-identity detection for 620-622; `pkg/pb/plan/unsigned_decimal_protocol_test.go:46-48` expects 616-618 to activate the feature and `pkg/sql/compile/unsigned_decimal_protocol_test.go:50` encodes literal 617. Revise D5 and the disposition inventory to include the numeric classifier and literal test identities; require registry-bound assertions that 620-622 activate the DECIMAL feature after wire round-trip, while plain MRANK/CUMRANK/TMRANK at 616-618 do not acquire its protocol floor | Verified valid before resolving, and broader than stated (read-only verification this round; zero Go sources touched). (a) The classifier is exactly as described: `string_literal_form.go:1014` is `if id == 616 \|\| id == 617 \|\| id == 618` with the import-cycle comment at `:1009-1010` ("The planner depends on this package, so registry IDs cannot be imported here. Compile tests bind these IDs to the registry.") — and that promised binding test exists nowhere in the tree. Its sibling feature comment at `:405` is stale on BOTH identities: "UnsignedDecimalAttributes requires MORPC v131 … admission functions 616..618". (b) The stale fixture set beyond the two the finding cites: pb round-trip rows `pkg/pb/plan/unsigned_decimal_protocol_test.go:46-48` (616/617/618 expected true), the carrier row `:50` (615), the negative row `:53` (`617-3` = 614), and the test's own header comment `:23-26` ("v131", "ids 616..618"); plus the plan-floor fixture `pkg/sql/plan/unsigned_decimal_protocol_floor_test.go:48` (`Obj: int64(617) << 32`). (c) The registry identity table never pinned the family: `function_id_test.go`'s `predefinedFunids` ends `CUMPOSITIVESTREAK: 615` / `FUNCTION_END_NUMBER: 616` while `function_id.go:976-978` registers the three admission names — probed this round against the CURRENT tree (read-only, archived log state/artifacts/issue-30940/funids-probe-rev9.log): `go test ./pkg/sql/plan/function/ -run Test_funids -count=1` exits 1 with `require.Failf` at `function_id_test.go:878` — "no functionid in 'predefinedFunids'" / "put funtion id 616 into 'predefinedFunids'". (d) pb-layer identity is purely numeric (`int32(fn.Func.Obj >> 32)` at `string_literal_form.go:1908`; the pb test helper's `ObjName` is decorative at that layer), so the wire contract is the number alone. (e) The renumber target is clean on origin/main `420786e21566e5b9d5875e7d6a7f3102fd0761af`: its `string_literal_form.go` blob has no 620/621/622 numerics and no `UnsignedDecimalAttributes` case, and the chosen mirror names (`unsignedDecimalAdmissionFunctionID`, `unsignedDecimalAdmissionStrictFunctionID`, `unsignedDecimalAdmissionIgnoreFunctionID`) match nothing in `pkg/pb/plan/`. Resolution (bound in D5, v27): (i) pb-local named mirrors following this file's own "mirrored from pkg/sql/plan/function to avoid a dependency cycle" precedent (`mediumIntStrictCastFunctionID = 541` block): `unsignedDecimalAdmissionFunctionID = 620`, `unsignedDecimalAdmissionStrictFunctionID = 621`, `unsignedDecimalAdmissionIgnoreFunctionID = 622`, replacing the `:1014` literals and refreshing the `:405` and `:1009-1010` comments (fence wording v131 → v133, functions 616..618 → the bound names); (ii) registry pins added to `predefinedFunids` — `UNSIGNED_DECIMAL_ADMISSION{,_STRICT,_IGNORE}: 620/621/622` and `FUNCTION_END_NUMBER: 623` — added alongside main's preserved 605..618 pins (SMA..CUMPOSITIVESTREAK, MRANK/CUMRANK/TMRANK), no existing pin moves, turning the probed red green; (iii) a NEW compile-side binding test honors the `:1009-1010` promise: each pb mirror is asserted equal to its registry constant (`function.UNSIGNED_DECIMAL_ADMISSION*`), so any one-sided future renumber fails a focused test deterministically; (iv) every fixture rebinds to the registry — compile placement becomes `function.EncodeOverloadID(function.UNSIGNED_DECIMAL_ADMISSION_STRICT, 0)` (replacing literal 617), the plan-floor fixture takes the same constant (replacing `int64(617) << 32`), the pb round-trip rows are built from the pinned IDs (replacing 616/617/618, the 615 carrier, and the `617-3` negative), and all header comments name the bound fence 133; (v) the demanded registry-bound negative arms: main's rank family MRANK/CUMRANK/TMRANK at 616/617/618 — under their real ObjNames AND under a spoofed `unsigned_decimal_admission` ObjName — sets `UnsignedDecimalAttributes` on NO owner (expr, plan, or wire-round-tripped plan after Marshal/Unmarshal), acquires no 133 floor in placement or send classification, and stays dispatchable by main's own rules, while the 620-622 wrappers DO activate the feature after the same round-trip. Record correction: v26's disposition sentence "function registration in `pkg/sql/plan/function/function_id.go` (with `list_operator.go` and `pkg/pb/plan/string_literal_form.go`) stays name-routed, so no admission call site moves" is wrong for the numeric classifier — `string_literal_form.go` is NOT name-routed (it matches raw numerics by design); and v26's scan claim "no raw 131/133 comparators or 616-618 numerics exist elsewhere" missed the `:405` v131 comment and the fixture literals above (the v26 table row is preserved verbatim as the review record). Consequently the v25 → v26 revalidation item "(i) … function registration and string-literal identity" is made concrete as items (ii)+(iii)+(iv) here, with (v) added to the NEW discriminating rows of that plan. Every other v10-v26 decision is unchanged. |

### v27 → v28

| Review finding | Resolution |
|---|---|
| 1. Revise the design to follow the shutdown-only upgrade contract: revise D5/D7 and the dependent initialization/repair design for shutdown upgrades, remove this change's new feature-level protocol bump, gates and predecessor-template fallback while preserving unrelated existing gates, reassess the reconciler against the remaining persisted-state requirements, and obtain approval for the revised design before completing implementation review — `.agents/skills/mo-dev/references/upgrade-compatibility.md:3-27` explicitly prohibits new per-feature MORPC versions, capability gates and older-peer fallbacks, yet D5 requires `MORPCVersion133` (`pkg/defines/const.go:171-172`), the authoring gate (`pkg/sql/plan/build_constraint_util.go:1431-1435`), the destination gate (`pkg/sql/compile/expression_destination_protocol.go:57-59`), and a reconciliation rationale depending on the new floor | Verified valid before resolving (read-only design turn; zero Go sources touched). (a) The contract is exactly as cited: shutdown upgrade only — no rolling upgrade, no mixed-version operation, no downgrade — with explicit prohibition of new per-feature `MORPCVersion` values, `MORPCLatestVersion` bumps, `AllCNsSupportProtocol`/heartbeat gates, and older-peer fallback paths unless RPC framing or the handshake changes or a recorded project decision reintroduces mixed-version support (`upgrade-compatibility.md:14-40`). (b) This feature meets neither exception: it adds ordinary registry functions and proto/function outputs, which the contract explicitly exempts from gating (`:21-23`), and no mixed-version project decision is recorded. (c) Every cited site matches the tree and this document: the 133 constant and Latest bump, `requireUnsignedDecimalProtocolForAuthoring` at the cited lines, the destination-gate row at the cited lines, the send/persisted floors, the pb classifier and mirrors, and the reconciliation rationale's bootstrap and rolling-upgrade windows. (d) The reconciler's own "why" text names a rolling-upgrade window the contract declares unsupported — so the gate removal also removes the machinery's raison d'être. Resolution: D5/D7 rewritten for shutdown-only upgrades (no protocol version, no gates, no floors, no classifier/mirrors/binding test, no predecessor-template fallback; main's 605..618 pins, <=132 floors, tenant-init machinery, and the v126 VIEWS maintenance phase explicitly preserved); "Persistent COLUMNS reconciliation" re-assessed against the remaining persisted-state requirements and replaced by "COLUMNS initialization and one-shot upgrade repair": of v11's three reach-gap mechanisms only persisted-definition permanence survives, met by unconditional fresh-init authorship plus the canonical versioned entry (content-idempotent `CheckFunc`, framework replay for crash survival, no protocol predicate); the steady-state reconciler, login-time repair, cache/floor-transition rules, coordination/budget/ledger/shutdown-ownership machinery and their validation rows are de-scoped with rationale recorded in the Rollout section; the Alternatives table gains the rejected-fence row. Implementation-turn removal list (exercised only after this revision passes design review; inventory by name-grep, all name-routed): `pkg/defines/const.go` (drop 133, restore Latest to main's 132); `pkg/sql/plan/build_constraint_util.go` (authoring hook + call); `pkg/sql/plan/persisted_ip.go` (floor arm); `pkg/sql/plan/unsigned_decimal_protocol_floor_test.go` (delete); `pkg/sql/compile/expression_protocol.go`, `expression_destination_protocol.go`, `remoterun.go` (feature/gate rows), `pkg/sql/compile/unsigned_decimal_protocol_test.go` and `unsigned_decimal_id_binding_test.go` (delete); `pkg/pb/plan/string_literal_form.go` (classifier case, pb mirrors, stale comments), `pkg/pb/plan/unsigned_decimal_protocol_test.go` (delete); `pkg/util/sysview/sysview.go` (133 template branch), `pkg/util/sysview/predefined_test.go` (fence-era expectations re-derived); `pkg/bootstrap/versions/information_schema_columns.go` and `v4_0_14/upgrade_test.go` (protocol predicates removed; entry kept), `pkg/bootstrap/columns_upgrade_test.go` (floor assertions removed), `pkg/bootstrap/service_information_schema_columns_maintenance{,_test}.go` (reconciler deleted); `pkg/frontend/authenticate_test.go` (`preAttributesColumnsStmt` and the two fence-era gate expectations re-derived — main's 126 machinery untouched); BVT `decimal_unsigned_zerofill.test/.result` reconcile-wait blocks moved to version-marker waits. Kept: the SQL-semantics core (2-bit mask propagation through `getTypeFromAstWithoutCharset`, 1264/22003 strict rejection with column+row context, non-strict saturate+warning, CAST 1064 parse-admission rejection, zerofill rendering at every serialization site), catalog mask persistence and its checkpoint-tool reader, function registration 620/621/622 + registry pins + `FUNCTION_END_NUMBER = 623` (ordinary `Test_funids` addition contract), and the `cast(... as varchar(65535))` COLUMN_TYPE header pin. Revalidation plan under the evidence contract: rerun only checks whose inputs touched the removed fence — the deletion-driven compile of the owning packages, the sysview/frontend template expectations, the bootstrap entry UTs, the entry-driven relaunched existing-data case, and a build plus focused-BVT green run; SQL-semantics evidence whose inputs never referenced the fence (baseline red vs green repro oracles, 1264/1064 outcomes, domain math, renderers, dtype attribution) remains reusable, citing revision/mode/terminal result per the repository validation-evidence contract. Every other v10-v27 decision is unchanged. |

v28 scope note: the removal list above ships only after this revision
passes design review; the worktree's rev11 implementation and its
archived proof stay untouched until then.

### v28 → v29

| Review finding | Resolution |
|---|---|
| 1. Define a new upgrade transition for already-final-version catalogs — specify a forward catalog transition reachable from the current ready 4.0.17/1 state with an explicit entry owner and completion marker; extend the validation plan to cover that exact predecessor state, crash replay and the documented manual-upgrade path; prove entry execution and decorated metadata without depending on background maintenance | Verified valid before resolving (read-only design turn; zero Go sources touched). (a) Main's chain ends at 4.0.17/1: `pkg/bootstrap/upgrade.go:79-83` appends handlers through `v4_0_17`, and `v4_0_17/upgrade.go:30-37` declares `Version: "4.0.17"`, `MinUpgradeVersion: "4.0.16"`, `VersionOffset = len(tenantUpgEntries)` = 1 — its sole entry is the canonical `InformationSchemaColumnsUpgradeEntry()` (file provenance: upstream #30362, `git log` on HEAD). (b) The equality short-circuit is exactly as cited: `pkg/bootstrap/service_upgrade.go:214-215` returns true when `v.Version == final.Version && v.VersionOffset >= final.VersionOffset`, and main's `MaybeUpgradeTenant` returns at the equal-version branch (`service_upgrade_tenant.go:66-69`) — so rev11's edit-in-place placements (v4_0_15 list splice + constructor fence) never re-fire for a catalog READY at 4.0.17/1. (c) The manual path is main's own: `service_upgrade_tenant.go:110-145` upgrades version-behind tenants synchronously in the login goroutine (per-handle `HandleTenantUpgrade` + `UpgradeTenantVersion`), and `AutomaticUpgrade` gates only the maintenance worker (`pkg/cnservice/server.go:1453`). (d) `columns_upgrade_test.go:37` indeed starts at 4.0.16. Resolution: D7 and the rollout section register the entry in a NEW forward version package (v4_0_18; `MinUpgradeVersion` 4.0.17; number re-verified next-free at integration under the D5 rule), constructor reverted to main verbatim (protocol 109; UpgSql/CheckFunc resolve the decorated text at runtime), rev11's edit-in-place placements reverted (v4_0_15 list, information_schema_columns.go incl. the license-comment mangling); entry owner = the version handle/tenant-upgrade framework; completion marker = content-idempotent CheckFunc + persisted version advance (cluster route `4.0.18,1` Created->Ready; tenant version row advances atomically with the entry's transaction); manual path = main's synchronous login-goroutine upgrade, no added operator step; coverage matrix recorded: tenants below 4.0.17 cross main's own v4_0_14/v4_0_17 handles with the decorated text, exactly-4.0.17/1 tenants cross the new version, fresh tenants get the decorated init template. Validation-plan additions (i)-(iv) cover the exact 4.0.17/1 predecessor state, zero-write canonical replay, crash replay with exactly-once marker advance, and the maintenance-disabled manual path; the relaunched existing-data service case is re-based to start READY at 4.0.17/1 waiting on the 4.0.18/1 marker. #31461 (`ea4c68a04e`) acknowledged as upstream periodic maintenance on origin/main past base — preserved at integration, not depended on. |
| 2. Preserve recognition of the pre-change COLUMNS template — add the exact pre-change template identities to the persisted-state compatibility design and existing historical-template adapter contract, preserving historical projections and snapshot ownership; plan focused recognition tests and a public sequence that creates a snapshot before upgrade, recreates current COLUMNS, then successfully queries the old snapshot; include the relevant template variant inherited from main | Verified valid before resolving (read-only; zero Go sources touched). (a) The adapter is exactly as cited: `pkg/util/sysview/predefined.go:340-359` (`AdaptLegacyInformationSchemaColumnsDDL`) recognizes only V100/V109 byte-stable arms and V41/V46/V46Upgrade/V58 re-derived arms, returning `("", false)` otherwise; the pre-change current template passes today solely via the "already current" identity row (`pkg/sql/plan/view_columns_history_test.go:62-68` region). (b) The error path is exactly as cited: `pkg/sql/plan/view_columns_history.go:84-92` gates on `IsInformationSchemaColumnsMigrationDDL`, then anything unrecognized yields `moerr.NewNotSupported(..., "unsupported historical information_schema.COLUMNS definition")`. (c) The public path is real: `test/distributed/cases/view/view_metadata_on_demand.sql:88-93` queries `information_schema.columns {snapshot='view_metadata_restore'}`. (d) Our rev11 diff never extended the recognition list (adapter diff shows only the decoration helper). (e) The inherited variant is identified: #31461 (`ea4c68a04e`, on origin/main past base, verified NOT an ancestor of HEAD) rewrites the current template's charset/collation selectors and touches `predefined.go`/`predefined_test.go`/`view_columns_history_test.go`. Resolution (new "Historical-template compatibility" block in the rollout section): every template identity a pre-change main build can persist becomes a recognized historical identity the same version the decorated template becomes current — (i) the base-era current text plus (ii) integration-time-enumerated upstream variants (#31461 the known one) — adapted as BYTE-STABLE pass-through arms after the existing lowercase view-name normalization (V109-arm shape): no re-derivation against the decorated template, no mask-CASE injection, no selector changes; unknown/mutated texts keep the hard error; the decorated template's own current-identity recognition is unchanged from the v10-v28 metadata design. Validation: focused recognition UTs per variant (byte identity, lowercase normalization, pass-through byte stability, trailing-space negative) + history-adapter rows {current decorated, definition pre-change variant => adapt}, and a public BVT sequence creating an account snapshot before the tenant upgrade, letting the entry recreate current COLUMNS, then asserting the snapshot query succeeds with the pre-change projection while current COLUMNS renders decorated metadata. Implementation-turn surface (with the round-28 list): NEW `pkg/bootstrap/versions/v4_0_18/` package + `pkg/bootstrap/upgrade.go` handler append; revert `pkg/bootstrap/versions/information_schema_columns.go` and `pkg/bootstrap/versions/v4_0_15/tenant_upgrade_list.go` to main verbatim; revert `pkg/bootstrap/versions/v4_0_14/upgrade_test.go` fence-era edits; extend `pkg/bootstrap/columns_upgrade_test.go` per rows (ii)-(iv); adapter arms in `pkg/util/sysview/predefined.go` + rows in `predefined_test.go` and `pkg/sql/plan/view_columns_history_test.go`; BVT snapshot-compat case in `test/distributed/cases/dtype/decimal_unsigned_zerofill.{test,result}`. Every other v10-v28 decision is unchanged. |

v29 scope note: the registration/revert/recognition list above ships
only after this revision passes design review; the worktree's rev11
implementation and its archived proof stay untouched until then.

### v29 → v30

| Review finding | Resolution |
|---|---|
| 1. Define the missing cluster-upgrade prerequisite for AutomaticUpgrade=false — specify the actual manual cluster/tenant upgrade sequence, its accessible privileged entry point and readiness prerequisites; re-specify validation item (iv) to start READY-4.0.17/1 with automatic upgrade disabled and no pre-created 4.0.18 route, invoke that supported sequence, then verify entry execution, version advancement and decorated metadata; remove the unsupported promise that login alone creates the upgrade transition | Verified valid before resolving (read-only design turn; zero Go sources touched). (a) The startup gate is exactly as cited: `pkg/cnservice/server.go:1453` guards the whole `BootstrapUpgrade` state machine (call at `:1469`) on `s.cfg.AutomaticUpgrade`, and `pkg/bootstrap/service.go:200-208` fast-returns 'mo already bootstrapped' for an initialized cluster — so with the automatic worker off, NO route to 4.0.18 is created at startup (the finding cites `:1453-1469` and `:196-198` on HEAD; the worktree's +4 line drift at service.go comes from rev11's init additions and is the same code). (b) The login gateway is exactly as cited: `pkg/bootstrap/service_upgrade_tenant.go:86-95` (worktree; HEAD `:79-86`) returns `NewInvalidStateNoCtxf` 'tenant upgrade requires current cn version %s to match cluster latest version %s' whenever cluster latest trails the binary's final version — with no route, a version-behind tenant's first login (tenant 4.0.17 < final 4.0.18) reads latest 4.0.17 and ERRORS before any handle runs, so v29's 'its next login runs the 4.0.18 handle' was unsound (rev11's equal-version hook above the gateway is the login-repair already slated for revert under v28/v29 and is irrelevant to it). (c) The manual route creator is exactly as cited: `pkg/bootstrap/custom_upgrade.go:31-74` — `UpgradeTenant` runs `UpgradePreCheck` (`:232-275`: retained unready route ⇒ start `BootstrapUpgrade`, return the retry-later error), then `BootstrapUpgrade` (`pkg/bootstrap/service_upgrade.go:40`, `UPGRADE ACCOUNT ALL` form) or `CheckAndUpgradeCluster` (`:64-74`: `adjustUpgrade` + `doCheckUpgrade` route creation + async worker) + `CheckUpgradeAccount` + retry-tasked `UpgradeOneTenant` (`:79-172`), whose own gateway (`:115-123`) re-asserts cluster latest == final and waits for the route's tenant phase before running handles. (d) The accessible privileged entry point verified: grammar `UPGRADE ACCOUNT target [WITH RETRY n]`, target = STRING \| ALL (`pkg/sql/parsers/dialect/mysql/mysql_sql.y:4058-4095`); dispatch `pkg/frontend/self_handle.go:642` → `handleExecUpgrade` (`mysql_cmd_executor.go:8238-8247`) → `Session.UpgradeTenant` (`session.go:3889`); privilege — ALL form = sys `specialTagAdmin` (`authenticate.go:6510-6513`) with the objectTypeNone MoAdmin guard (`:10570-10571`), single-account form = `PrivilegeTypeUpgradeAccount` (`:6514`) seeded at system init (`system_initialize.go:186`); `SHOW ACCOUNT UPGRADE` (`:10568`) is the companion status read. Resolution: D7 tail, the observed-live-gap paragraph, item 2's cross-reference, and the UT validation row's manual clause corrected to attribute route creation to the startup worker or the `UPGRADE ACCOUNT` statement (login qualified as post-route only); item 3 rewritten as the verified manual sequence — the gating fact, the Bootstrap fast-return, the login gateway's invalid-state error, both privileged statement forms with their exact privileges and dispatch chain, the `UpgradePreCheck` retry-later idiom, readiness prerequisites, item 2's unchanged completion/idempotence/crash marker, post-route mixed command+login convergence, and the `SHOW ACCOUNT UPGRADE` status read; validation item (iv) re-specified exactly per the demanded sequence — start READY-4.0.17/1 with maintenance disabled and no pre-created route, assert the red state (login returns the gateway error), invoke `UPGRADE ACCOUNT ALL` plus the per-account form, then verify route Created→Ready, exactly-once version advancement with zero-write replay, decorated `information_schema.COLUMN_TYPE`, clean post-upgrade logins, no-op replay, and the retained-route pre-check leg; the rollout Validation title now reads '(v28; extended in v29 and v30)'; the UT validation row and the header gain the corrected manual-path text. Implementation-turn surface: UNCHANGED from v29 — the corrected sequence uses only main's existing `UPGRADE ACCOUNT` machinery, and the already-listed test rows (`columns_upgrade_test.go` rows (ii)-(iv), the BVT manual-path leg) are re-derived against this sequence as scheduled. Every other v10-v29 decision is unchanged. |

v30 scope note: the correction above ships only after this revision passes
design review; the worktree's rev11 implementation and its archived proof
stay untouched until then, and v29's registration/revert/recognition list
stands as scheduled by the v29 scope note.

### v30 → v31

| Review finding | Resolution |
|---|---|
| 1. Specify an administrator login that works before route creation — define a supported pre-route administrative login, including special-user registration and operator credential/readiness prerequisites if using that existing mechanism; extend the validation plan to establish that real SQL connection from the stated predecessor state, assert ordinary administrator login rejection, then execute UPGRADE ACCOUNT without pre-creating the route or bypassing authentication | Verified valid before resolving (read-only design turn; zero Go sources touched). (a) The ordinary-admin rejection chain is exactly as cited: a catalog-backed session reads create_version from mo_account (`pkg/frontend/session.go:3522-3525`, installed by `SetCreateVersion` at `:3807`); `pkg/frontend/mysql_protocol.go:1767-1768` runs `ses.MaybeUpgradeTenant` for every session with a non-empty create_version before queries are admitted; the gateway at `service_upgrade_tenant.go:86-95` rejects cluster latest 4.0.17 against binary final 4.0.18 — so a sys administrator authenticated from the catalog fails login pre-route exactly like any version-behind tenant, and MoAdmin privilege alone does not make the documented command accessible. (b) The special-user early return is exactly as cited: `session.go:3454-3469` — `isSpecialUser` + `specialAccount.IsMoAdminRole()` classify the bootstrap-special user, whose authentication returns early after `SetTenantInfo(specialAccount)` with NO catalog access and NO create_version (external special users still receive the snapshot boundary per the code comments at `:3456-3459`); `mysql_protocol.go:1767` is therefore skipped (empty create_version) and the gateway never runs for this identity. (c) Registration and credentials verified: `authenticate.go:476-507` — the in-memory `specialUsers` map, `SetSpecialUser` building the sys-tenant/moadmin-default-role `TenantInfo`, and the `:491` name-must-not-contain-':' note; the task-framework user is registered on each CN at task-framework creation from the CreateTaskService heartbeat command (`pkg/cnservice/server_task.go:100`; the same command registers `mo_logger` at `:131` and the proxy user at `:192`); credentials originate at the logservice's `registerTaskUser` (`pkg/logservice/store_hakeeper_check.go:446-457`, TODO 'rename TaskTableUser to moadmin' at `:453`) from the `mo_admin_user`/`mo_admin_password` environment variables (`:673-692`) with a random-UUID fallback (`randomUser`) when unset — operator-unusable, so setting them is a stated credential prerequisite; in-repo precedent verified: main's `TestCrossTargetUpgradeRecovery` sets both variables and connects real SQL clients through this identity for the manual subcases (`pkg/embed/upgrade_recovery_test.go:106`/`:130-133`/`:230`/`:259`/`:303`, unchanged on HEAD). Resolution: Rollout item 3 gains the "Pre-route administrative login" block naming the bootstrap special user as the supported identity with its mechanism proof, privilege-gate satisfaction, the task-framework instance, and the operator credential (env vars set before first bootstrap) and readiness (CN consumed CreateTaskService) prerequisites; the D7 tail and the UT validation row now attribute the statement to this login; validation item (iv) opens from the READY-4.0.17/1 no-route state by asserting the ordinary catalog sys administrator's login rejection, then establishing a REAL special-user SQL connection, and only then executing `UPGRADE ACCOUNT` — no pre-created route, no authentication bypass. Implementation-turn surface: UNCHANGED — still zero new code sites; the already-listed test rows (`columns_upgrade_test.go` manual-path row, the BVT manual-path leg) are re-derived against this login as scheduled. |
| 2. Separate per-account completion from global route finalization — specify the existing operation that starts tenant-task consumption and finalizes the route, such as UPGRADE ACCOUNT ALL; distinguish per-account completion markers from global Ready; plan separate initial-state scenarios for ALL and per-account commands, asserting task counters and route state before and after the required finalization step | Verified valid before resolving (read-only design turn; zero Go sources touched). (a) `CheckAndUpgradeCluster` starts only `asyncUpgradeTask` (`custom_upgrade.go:65-75`, the cluster coordinator) — verified: no `asyncUpgradeTenantTask` reference exists anywhere in `custom_upgrade.go`. (b) `UpgradeOneTenant` (`:79-180`) runs the handles, calls `UpgradeTenantVersion`, and flips `s.mu.tenants[tenantID]` (`:180`) with NO tenant-task state or `ReadyTenant` accounting; login-driven upgrades (`MaybeUpgradeTenant`) likewise omit it. (c) The route's final step requires `TotalTenant == ReadyTenant` (`service_upgrade.go:528-529`, with the UpgradingTenant wait at `:536-538`); the coordinator creates the task rows and accumulates `TotalTenant` as it advances (`:568-577`, `versions.AddUpgradeTenantTask`), and `ReadyTenant` advances only via per-batch task completion (`service_upgrade_tenant.go:373`/`:391`) inside `newTenantUpgradePass`, run solely by `asyncUpgradeTenantTask` (`:440`) — which `BootstrapUpgrade` alone starts (`service_upgrade.go:69-75`). (d) Consequence confirmed by construction: AutomaticUpgrade=false + only per-account commands/logins ⇒ every account can reach 4.0.18 while the route stays `StateUpgradingTenant` indefinitely (no consumer advances `ReadyTenant`), and running ALL first in validation would conceal this gap; mitigating facts also verified — `shouldRunTenantUpgrade` (`service_upgrade_tenant.go:211-218`) skips already-upgraded tenants, and when no unready task rows remain the pass snaps `ReadyTenant = TotalTenant` (`:312-315`), so the finalizing ALL is a cheap pass. Resolution: Rollout item 3 gains "Per-account completion vs global route finalization" — the two markers distinguished, the consumers' sole start site named, the indefinite-UpgradingTenant consequence stated, `UPGRADE ACCOUNT ALL` (its `BootstrapUpgrade` path) named as the existing finalizing operation, and the recommended operator sequence (ALL alone, or per-account forms followed by ALL); validation item (iv) is split into Scenario A (ALL from the predecessor state; `TotalTenant`/rising `ReadyTenant` counters asserted through Created → UpgradingTenant → Ready) and Scenario B (per-account command FIRST — account-local marker asserted while the route stays `StateUpgradingTenant` with pending task rows and `ReadyTenant` below `TotalTenant`, then the required ALL finalization step with the counter flip asserted), explicitly forbidding an ALL-first ordering in Scenario B because it would conceal this exact gap. Implementation-turn surface: UNCHANGED — no new code; the `columns_upgrade_test.go` manual-path row and the BVT manual-path leg are re-derived as the two assertive scenarios as scheduled. Every other v10-v30 decision is unchanged. |

v31 scope note: the corrections above ship only after this revision passes
design review; the worktree's rev11 implementation and its archived proof
stay untouched until then, and v29's registration/revert/recognition list
stands as scheduled by the v29 scope note.

### v31 → v32

| Review finding | Resolution |
|---|---|
| 1. Provide an upgrade procedure for existing clusters with random task credentials — specify a supported, executable upgrade procedure for already-initialized clusters whose task credentials are unknown, including how the operator reaches the upgrade entry point; extend the validation plan with a predecessor cluster bootstrapped without either environment variable, preserve its HAKeeper state across restart, and verify the specified procedure reaches global Ready and restores ordinary logins; a prerequisite imposed before historical bootstrap does not close this rollout case | Verified valid before resolving (read-only design turn; zero Go sources touched). (a) The random fallback is exactly as cited: `registerTaskUser` (`pkg/logservice/store_hakeeper_check.go:447-457`) takes `getTaskTableUserFromEnv` (`:677-690`) and substitutes `randomUser()` (`:692-695`) when the variables are unset — a legitimate pre-existing deployment state. (b) The one-shot persistence is exactly as cited: `pkg/hakeeper/rsm.go:1383-1397` (`handleTaskTableUserCmd`) returns early unless `TaskSchedulerState == TaskSchedulerCreated`, then stores the user and flips to `Running`; the proposer `setTaskTableUser` (`store_hakeeper_check.go:652-670`) reads the pre-update state and merely logs 'task user info already set' when it is not `TaskSchedulerCreated` — so neither env-set nor new random proposals on later restarts ever replace the persisted first-boot credentials, the state rides the HAKeeper RSM across restarts, and `GetTaskTableUserCmd`'s only caller in the tree is that proposer (no rotation command exists). (c) With `AutomaticUpgrade=false`, the v30/v31 ordering facts still hold: startup creates no route (`pkg/cnservice/server.go:1453` / `:1469`) and ordinary catalog logins are gateway-rejected, so from this predecessor state neither the v31 special-user login (unknown credentials) nor any catalog identity can reach `UPGRADE ACCOUNT` — the rollout case is genuinely unclosed by the before-first-bootstrap prerequisite. (d) The supported credential-free entry point verified: `auto-upgrade` is a plain CN config key (`pkg/cnservice/types.go:327`, 'Whether to automatically upgrade when system startup') whose main default is true (`cmd/mo-service/config.go:171-173`); enabling it for the SAME mandatory shutdown restart that deploys the new binary makes startup's `BootstrapUpgrade` perform route creation, per-tenant execution, and global Ready entirely server-side — no SQL login, no credential, HAKeeper state untouched — and restoring the flag afterwards changes nothing (config read; completed route leaves no work). Resolution: Rollout item 3's credential prerequisite re-scoped to the special-user variant with the one-shot fact stated; a new 'Existing clusters whose first-boot credentials are UNKNOWN' block specifies the config-window procedure (shutdown restart + `auto-upgrade = true`; whole transition server-side; ordinary logins resume at Ready; optional, behavior-neutral restore); the recommended operator sequence is split by credential knowledge (known: v31 statement path; unknown: config window, no SQL entry point); the D7 tail and UT validation row gain the unknown-credentials clause; validation item (iv) gains Scenario C exactly per the demanded shape — predecessor bootstrapped WITHOUT either environment variable, HAKeeper state preserved across restarts, the premise pinned by a FAILED env-credential login plus gateway-rejected ordinary logins, then `auto-upgrade = true` on the new binary driving the route Created → UpgradingTenant (counters `TotalTenant`/`ReadyTenant`) → Ready with exactly-once version advancement and decorated metadata, ordinary logins (tenant and sys administrator) RESTORED, and the restore-false leg asserting steady state. Implementation-turn surface: UNCHANGED — zero new code sites; the already-listed test rows (columns_upgrade_test manual-path row, the BVT manual-path leg) gain the Scenario C leg as scheduled. Every other v10-v31 decision is unchanged. |

v32 scope note: the correction above ships only after this revision passes
design review; the worktree's rev11 implementation and its archived proof
stay untouched until then, and v29's registration/revert/recognition list
stands as scheduled by the v29 scope note.

### v32 → v33

| Review finding | Resolution |
|---|---|
| 1. Update the upgrade design for main's consolidated 4.0.7 chain (pre-PR review of the completed v32 diff, one P2): — integrate current main, revise D7 and obtain approval for the updated design revision; define a reachable forward transition using the current canonical COLUMNS entry, replace the predecessor/recovery fixtures, and verify READY-4.0.7 upgrade, manual/automatic completion, restart and historical metadata compatibility on the integrated source. Evidence as cited: `pkg/bootstrap/versions/v4_0_18/upgrade.go:35-36` requires predecessor 4.0.17, but origin/main at `72d99c8575` includes #31497 (`ea47345ff8`) removing handlers 4.0.8-4.0.17 and ending the chain at 4.0.7, so the route builder cannot construct a route from READY 4.0.7; the new `upgrade_test.go:25-26` and `pkg/embed/upgrade_columns_4_0_18_test.go:28-29` import the removed packages; existing recovery receipts exercise the superseded 4.0.16/4.0.17 chain | Verified valid read-only against origin/main `9e29011e73` before any edit (design turn; zero Go sources or tests touched; the completed v32 implementation and its archived proof stay untouched until this revision passes). (a) Consolidation confirmed: `pkg/bootstrap/versions/` on main ends at `v4_0_7`; `ea47345ff8` is an ancestor of origin/main; its file list removes v4_0_8..v4_0_17 and rewrites the bootstrap lifecycle (`service_upgrade.go`, `service_upgrade_tenant.go`, `custom_upgrade.go`, `service_upgrade_tenant_v407.go`, `columns_upgrade_test.go`, the COLUMNS/TABLES maintenance, cnservice startup, embed operator/start control, disttae cache). (b) Unreachability confirmed: `versions.CanDirectUpgrade` admits a handle whose `MinUpgradeVersion <= from` (`version.go:45-47`), so a 4.0.18 handle requiring 4.0.17 can never chain from READY 4.0.7; the walk in `addUpgradesToFinalVersion` (`service_upgrade.go:340-397`) confirms no route forms. (c) The new terminal confirmed: consolidated 4.0.7 fires the same canonical COLUMNS constructor (`v4_0_7/tenant_upgrade_list.go:58-62`, `upgradeDatabaseDefaultsColumns`); final offset 41 (27 tenant + 14 cluster entries; pinned at `service_upgrade_tenant_v407_test.go:55`, `service_upgrade_snapshot_test.go:123,147`); 4.0.7 is FROZEN — an offset mismatch hard-errors before any catalog mutation (`service_upgrade.go:235-274`) and upstream's contract assigns later migrations a new semantic version (`docs/design/20261028-catalog-upgrade-407.md`); the version number 4.0.18 stays free on `9e29011e73`. (d) Operator-path supersession confirmed: with `auto-upgrade = false`, CN startup fails against a not-READY-final catalog (`pkg/cnservice/server.go:1495-1498` -> `CheckManualUpgradeReadiness`, `service_upgrade.go:75-99`, 'enable auto-upgrade for the stopped upgrade and restart'); with it true, the route completes before SQL admission (`server.go:1501-1548`, `waitForBootstrapUpgrade` at `:503`, frontend admission at `:556`, serialization at `:1539-1543`); the special-user/`UPGRADE ACCOUNT` machinery materially cannot create the transition on main, and no credential exists in the supported path. (e) Merge surface mapped: the both-diff intersection is `pkg/bootstrap/upgrade.go`, `pkg/util/sysview/predefined.go`, `pkg/bootstrap/columns_upgrade_test.go`, `pkg/bootstrap/service_statistics_upgrade_test.go`, `pkg/util/sysview/view_columns_history_test.go`, `pkg/sql/plan/view_columns_history_test.go`, `pkg/vm/engine/disttae/cache/catalog.go`; #31497 rewrote `genTableDefOfColumn` as a wrapper over `decodeColumnAttribute` (the mask mapping re-sites there); it did NOT change the COLUMNS DDL text (zero matching hunks), while #31461 (`ea4c68a04e`, on main, ABSENT from base) is the one additional current-template variant to pin. Resolution: D7 re-based — the forward transition is `MinUpgradeVersion` 4.0.7 onto READY 4.0.7/41 with the unchanged canonical constructor, `RequiredProtocolVersion` 126 inherited from the frozen terminal, a stated frozen-chain invariant (no edits to consolidated lists), and merge dispositions (a)-(f) recorded in Design item 2; Rollout item 3 rewritten onto the startup-owned lifecycle (fail-fast manual mode; startup-owned automatic completion; credential-free by construction, dissolving the v32 unknown-credentials case); fixtures replaced — the v4_0_18 package UT (READY-4.0.7/41 chain fixture plus `v4_0_7` boundary pins), the embed recovery suite re-staged on {v4_0_7}/{v4_0_7,v4_0_18} handle lists with fail-fast/startup-completion/restart assertions, statistics-hop expectations re-derived; historical-template arms pin BOTH the T0 base-era and the #31461-T1 texts; validation item (iv) re-specified to verify READY-4.0.7 upgrade, manual (fail-fast) and automatic completion, restart/crash resume, and historical metadata compatibility ON THE INTEGRATED SOURCE, with the superseded-chain receipts explicitly re-collected; the D7 decision bullet, the UT validation row, the service rollout row, the historical-template block, and the alternatives table amended in place. Every other v10-v32 decision is unchanged. |

v33 scope note: the D7 corrections above ship only after this revision passes design review; the worktree's completed v32 implementation and its archived proof stay untouched until then. The integration itself — the rebase onto current main, the merge dispositions, re-collecting the stale-chain evidence, and re-running the affected validation ladder — is implementation-turn work after that PASS.

### v33 → v34

| Review finding | Resolution |
|---|---|
| 1. Preserve legacy CDC admission when 4.0.7 becomes an intermediate step (design-only review of v33, one P2): — revise D7 to preserve the existing CDC preflight whenever crossing or resuming 4.0.7, independently of the final target, using its existing owner without editing frozen entries; extend the validation plan with 4.0.6/5 and interrupted-4.0.7 sources targeting 4.0.18: remaining legacy tasks must produce terminal rejection before further catalog mutation; an empty-task control must complete. Evidence as cited: both `rejectLegacyCDCTasks` calls are conditional on `final.Version == "4.0.7"` (`pkg/bootstrap/service_upgrade.go:235-238`, `:277-280`), so appending 4.0.18 disables them; the safety contract (`service_upgrade_cdc.go:28-31`, `docs/guides/upgrade-4.2-to-main.md:12-23`) requires rejection because even paused tasks lack the identity needed for safe continuation; the proposed READY-4.0.7 fixtures cannot detect the regression | Verified valid read-only against origin/main `9e29011e73` (the v33 review head) and re-verified byte-identical on the current head `77679f178d` before any edit; design turn — zero Go sources or tests touched; the completed v32 implementation and its archived proof stay untouched until this revision passes. (a) Both gates confirmed final-scoped at `:235` (recheck) and `:277` (creation), each comparing `final.Version == "4.0.7"`; `final` is `s.getFinalVersionHandle().Metadata()` (`:171`), the LAST appended handle, which becomes 4.0.18 precisely when this change appends `v4_0_18` in `pkg/bootstrap/upgrade.go`. (b) Safety contract confirmed: owner at `service_upgrade_cdc.go:33` (rationale `:28-31`) rejects ANY `mo_catalog.mo_cdc_task` row — paused/failed included — with a terminalUpgradeError listing up to 32 tasks, and the guide documents the new-CN startup rejection as the last safety check preventing subsequent work rather than undoing it. (c) Pre-mutation position confirmed: the creation site runs before `EnsureViewMetadataCatalogs` (`:286-289`) and before the route-retention/recovery block (`:302-312`) and its `canResumeUpgrade` resume (`:418-426`), all inside the version-table-locked transaction; the 4.0.7 step's 41 entries and any view-metadata catalog DDL come strictly after. (d) Fixture-blindness confirmed: v33's READY-4.0.7/41 fixtures start POST-crossing (their preflight already passed), so only below-4.0.7 or interrupted-4.0.7 sources discriminate; main's own `TestLegacyCDCAdmissionBeforeUpgradeMutation` covers exactly those source classes for final 4.0.7 and goes RED on the naive integrated tree (its default-reject mock trips on the first mutation SQL), the cheapest discriminating red/green. (e) Precedent and adjacent guards confirmed: upstream itself gates `EnsureViewMetadataCatalogs` with `versions.Compare(final.Version, "4.0.7") >= 0` (`:286`) — the final-agnostic pattern this resolution mirrors — while the in-progress/READY offset hard-errors (`:255-275`) are likewise final-4.0.7-scoped and are DELIBERATELY NOT widened by this change (v34 scope correction recorded in Design item 2); `versions.Compare` at `versions/version.go:197`. Resolution: D7 Design item 2 gains the v34 paragraph re-gating the creation site to `created && Compare(final,"4.0.7") >= 0 && (Compare(v,"4.0.7") < 0 || !v.IsReady())` and the recheck site to `state != StateReady && Compare(final,"4.0.7") >= 0` — behavior-identical under final 4.0.7, firing for below-4.0.7 and not-ready sources under any later final; the owner function, query, 32-task diagnostic cap, and error text are unchanged, NO frozen v4_0_7 entry is touched; new invariant I7; merge notes gain (g) the re-gate disposition with preserved upstream CDC tests and (h) the post-review-head delta (five textual intersections, decimal-semantics seams only); the D7 decision bullet, UT validation row, and alternatives table amended; validation item (vi) covers {READY 4.0.5, READY 4.0.6, interrupted 4.0.7} x {legacy task incl. paused/failed; empty control} plus the recheck case and a staged-catalog embed leg asserting rejection before the 4.0.7 step commits. Every other v10-v33 decision is unchanged. |

v34 scope note: the correction above ships only after this revision passes design review; the completed v32 implementation, the v33 re-base work, and all archived proof stay untouched until then. The re-gate itself, the integration merge (including dispositions (a)-(h)), the new final-4.0.18 CDC matrix, and the re-collection of the affected evidence are implementation-turn work after that PASS.

### v34 → v35

| Review finding | Resolution |
|---|---|
| 1. Restrict CDC rejection to upgrades that cross or resume 4.0.7 (design-only review of v34, one P2): — define eligibility at both call sites from the durable source/route state, distinguishing an unfinished 4.0.7 crossing from an unfinished later upgrade; preserve pre-mutation rejection for legacy sources; add validation starting from READY 4.0.7 with a valid CDC task, interrupting the 4.0.18 route and proving restart completes without deleting the task; qualify the recheck oracle by the same eligibility rule. Evidence as cited: v34's condition at `:2655-2664` treats any unfinished later version as requiring legacy-CDC rejection — READY 4.0.7/41 with a legitimate task created after that upgrade: initial admission skips the check, and `service_upgrade.go:340-341` persists 4.0.18/1 as Created; after a crash `GetLatestVersion` returns that unfinished target, so the `!v.IsReady()` condition invokes `rejectLegacyCDCTasks`, whose unchanged query rejects every task row (`service_upgrade_cdc.go:35-67`), aborting startup despite the task being valid; contradicts I7; the validation plan omitted this positive control | Verified valid read-only against origin/main `77679f178d`, re-verified on the current head `df6dff5305` (the one-commit delta touches none of the bootstrap/cnservice/guide machinery) before any edit; design turn — zero Go sources or tests touched; the completed v32 implementation, the v33 re-base work, and all archived proof stay untouched until this revision passes. (a) Route persistence confirmed: `addUpgradesToFinalVersion` writes `versions.AddVersion(final.Version, final.VersionOffset, versions.StateCreated, txn)` at `service_upgrade.go:341` inside the checker's route-creation path (`:324-345`), so a crash after route creation leaves the newest durable row at 4.0.18/1-Created and `GetLatestVersion` (`versions/version.go:85`, the newest-row read via `LatestVersionSQL` at `:80-83`) returns it — under v34's bare `!v.IsReady()` disjunct the preflight then fires, and the owner rejects ANY `mo_cdc_task` row, including the valid post-crossing one. (b) Supported states enumerated against the latest-row state alone: pre-4.0.7 sources (released 4.2 at 4.0.5/4.0.6), an unfinished 4.0.7 CROSSING (latest row 4.0.7/41 non-READY), a READY 4.0.7/41 source, and an unfinished LATER route (latest row 4.0.18/1 non-READY) — only the first two require the preflight, and the latest-row state distinguishes all four with no new read at route creation. (c) Recheck path confirmed: the worker's post-commit recheck reads the FINAL route marker (`versions.GetVersionState(final.Version, final.VersionOffset, txn, false)`, `service_upgrade.go:233`), which under final 4.0.18 is not the crossing marker — so eligibility there must read the durable 4.0.7/41 state (`versions.GetVersionState`, `version.go:132`); under final 4.0.7 the row just read IS the crossing marker (the final handle being the v4_0_7 handler, `pkg/bootstrap/upgrade.go:63`), and its non-READY state — the branch's own condition — reproduces main's firing byte-exactly with no added query; a failed eligibility read propagates as retryable, matching main's treatment of the adjacent version-state reads. (d) Byte-identity under final 4.0.7 confirmed: the creation condition reduces to main's `v.Version == final.Version && !v.IsReady()` (the literal "4.0.7" is the same string on that final), the shape mirroring main's own adjacent `EnsureViewMetadataCatalogs` gate at `:286-288` (`versions.Compare(final.Version, "4.0.7") >= 0` plus the same literal); preserved upstream fixtures (`service_upgrade_cdc_test.go:75-114` — READY 4.0.6/5 and 4.0.7/41-Created with one paused task, mocked `mo_upgrade` scan carrying a NON-offset-only 4.0.6 -> 4.0.7 step, default-reject beyond) therefore drive byte-identical mocked SQL sequences and stay GREEN UNCHANGED. (e) Parity window confirmed: mid-crossing rechecks still fire because the 4.0.7 step's marker stays non-READY until that step commits — preserving main's re-verification guarantee exactly where main provides it — while after the crossing durably commits, later-created tasks are post-crossing and survive, matching the guide's "prevents subsequent work rather than undoing it". Resolution: Design item 2's CDC block re-gates the creation site to `created && versions.Compare(final.Version, "4.0.7") >= 0 && (versions.Compare(v.Version, "4.0.7") < 0 || (v.Version == "4.0.7" && !v.IsReady()))` and the recheck site to `state != versions.StateReady && versions.Compare(final.Version, "4.0.7") >= 0` PLUS the crossing-marker eligibility (same-row reuse under final 4.0.7; one `GetVersionState("4.0.7", v4_0_7.Handler.Metadata().VersionOffset)` read under later finals); I7 re-expressed around the eligibility rule; the UT validation row gains the mid-crossing/post-crossing recheck pair and the task-survival positive control (READY 4.0.7/41 + valid task, interrupted 4.0.18 route, restart completing with the task row intact and the preflight query never issued — the default-reject mock as the no-query oracle); validation item (vi) gains the same plus the task-survival embed leg; merge note (g) and the alternatives table amended. Every other v10-v34 decision is unchanged. |
| 2. Preserve frozen 4.0.7 source-offset rejection under the new final version (design-only review of v34, one P2): — preserve both rejection contracts independently of the final target, validating source 4.0.7 against its frozen handler metadata without editing its entry lists; extend the plan with mismatched READY offsets and interrupted offset-only routes targeting 4.0.18, requiring terminal rejection before mutation, alongside the supported READY-4.0.7/41 control. Evidence as cited: v34 at `:2618-2624` explicitly allows the guards to stop firing, substituting unchanged entry lists and static list-length tests; on origin/main `service_upgrade.go:255-276` rejects mismatched READY offsets and interrupted offset-only routes only when 4.0.7 is final; with final 4.0.18 a READY 4.0.7/1 catalog passes these guards and takes the direct route at `:385-387`, skipping the remaining consolidated effects; an interrupted 4.0.7 -> 4.0.7 route can likewise pass recovery validation at `:418-459`; upstream's contract (`docs/design/20261028-catalog-upgrade-407.md:21`) requires pre-mutation rejection; `service_upgrade_recovery_test.go:740`/`:767` preserve those regression oracles; static entry-list checks cannot validate persisted source offsets | Verified valid read-only against origin/main `77679f178d`, re-verified on `df6dff5305` before any edit (same design turn, zero Go sources touched). (a) Both guards confirmed final-scoped: the interrupted offset-only guard at `:255-265` (conditions `!v.IsReady() && final.Version == "4.0.7" && v.Version == final.Version && v.VersionOffset == final.VersionOffset`, inner `GetUpgradeVersions` scan rejecting a From == To step with the "unsupported in-progress 4.0.7 offset-only upgrade" text) and the READY-source drift guard at `:272-276` ("catalog 4.0.7 offset %d differs from binary offset %d; 4.0.7 is frozen") both lapse under final 4.0.18. (b) Consequences confirmed on the naive integrated tree: a READY 4.0.7/1 source passes to `addUpgradesToFinalVersion`, where `final.CanDirectUpgrade(latest)` (`:386`) is true (`MinUpgradeVersion` 4.0.7) and appends the single 4.0.7 -> 4.0.18 step — consolidated entries 2..41 never run; and an interrupted offset-only route (latest 4.0.7/41-Created, a From == To == 4.0.7 step) reaches the retention/recovery block (`:302-312` -> `checkUpgradeRecovery` at `:305`), whose contiguity/order/support validation passes the offset-only step. (c) Upstream contract confirmed verbatim: the frozen-4.0.7 design requires rejecting an offset mismatch BEFORE any catalog mutation and assigns later migrations a new semantic version (`docs/design/20261028-catalog-upgrade-407.md:21`); a persisted source offset is durable catalog state, so only a runtime guard can validate it — static list-shape tests cannot. (d) Oracle identity confirmed: both recovery fixtures (`:740` READY offsets {1, 1000}; `:767` 4.0.7/41-Created + offset-only step) mock exactly the reads preceding the rejection (mo_tables, `LatestVersionSQL`, and `:767` additionally `from mo_upgrade`) with a default-reject "unexpected SQL before offset rejection" — on the naive integrated tree the lapsed guards let the flow reach unmocked SQL (RED), and re-gating restores the early terminal error over the same mocked reads (GREEN UNCHANGED), giving alongside finding 1's CDC oracle a three-test red/green ladder discriminating this exact seam. (e) Byte-identity under final 4.0.7 confirmed: on that final the `>= 0` compare holds exactly as main's equality does and `final.VersionOffset` IS `v4_0_7.Handler.Metadata().VersionOffset` (41 = 27 tenant + 14 cluster, `v4_0_7/upgrade.go:37-43`) — each re-gated condition and compared value reduces to main's, positions and ordering unchanged (`:255-276`, before the CDC gate `:277-280`, before `EnsureViewMetadataCatalogs` `:286-289`, before the retention/recovery block `:302-312`), no frozen entry touched; a legitimate interrupted CROSSING (a 4.0.6 -> 4.0.7 step, no offset-only step) still passes guard (a)'s scan to CDC eligibility and recovery exactly as under main. Resolution: the D7 bullet's offset-guard clause and Design item 2's scope-correction paragraph are rewritten (v34's lapse-plus-static-tests stance replaced by runtime preservation); Design item 2 gains the "Frozen 4.0.7 source-offset rejection (v35)" block re-gating guard (a) to `!v.IsReady() && versions.Compare(final.Version, "4.0.7") >= 0 && v.Version == "4.0.7" && v.VersionOffset == v4_0_7.Handler.Metadata().VersionOffset` with the unchanged scan and text, and guard (b) to `v.IsReady() && versions.Compare(final.Version, "4.0.7") >= 0 && v.Version == "4.0.7" && v.VersionOffset != v4_0_7.Handler.Metadata().VersionOffset` with the unchanged text; new invariant I8; validation item (vi) gains the offset matrix {READY-4.0.7 sources at persisted offsets {1, 1000} -> terminal before mutation; interrupted offset-only route -> terminal; READY-4.0.7/41 control -> direct route completes}; merge note (g) expanded to all four re-gates with both preserved suites; the alternatives table gains the rejected lapse/static-test row. Every other v10-v34 decision is unchanged. |

v35 scope note: the corrections above ship only after this revision passes design review; the completed v32 implementation, the v33 re-base work, and all archived proof stay untouched until then. The re-gates themselves, the integration merge (including dispositions (a)-(h)), the new final-4.0.18 CDC/offset matrices, and the re-collection of the affected evidence are implementation-turn work after that PASS.

### v35 → v36

| Review finding | Resolution |
|---|---|
| 1. Derive CDC crossing eligibility from actual persisted route progress (design-only review of v35, one P2): — revise the design to distinguish crossing progress using the persisted source and actual mo_upgrade steps, with consistent eligibility at startup and recheck; correct the recheck lifecycle description; extend the validation plan with a real route created from READY 4.0.6 targeting 4.0.18, interrupted before and after the 4.0.7 step completes, without synthesizing an intermediate mo_version marker; retain the READY-4.0.7 valid-task survival control and frozen-offset rejection cases. Evidence as cited: v35's eligibility assumed an unfinished 4.0.18 target means the 4.0.7 crossing is complete and that `GetVersionState("4.0.7", 41)` tracks intermediate-step completion; on main, `service_upgrade.go:341` inserts only the final mo_version row, `:364-397` create intermediate steps in mo_upgrade, completing the 4.0.7 step updates mo_upgrade (`versions/version_upgrade.go:137-146`), and `:695` updates only the final version marker — so a direct 4.0.6 -> 4.0.7 -> 4.0.18 route already has latest version 4.0.18 before crossing completion and never acquires the presumed 4.0.7/41 marker, meaning the v35 startup rule takes SKIP before crossing completion while the v35 marker read stays missing even after completion; additionally `checkUpgrade(true)` runs after completing a retained historical target (`service_upgrade.go:530-533`), not after each intermediate-step commit, and the marker-based validation pair does not establish behavior for this real route | Verified valid read-only against origin/main `77679f178d`, at the v35 review head `df6dff5305`, and re-verified against the current head `9c8d405b1e` (whose one-commit delta `df6dff5305..9c8d405b1e` touches only `pkg/sql/colexec/external` CSV-load machinery and `pkg/tests/sqlintegration` — no bootstrap/cnservice/design surface) before any edit; design turn — zero Go sources or tests touched; the completed v32 implementation, the v33 re-base work, and all archived proof stay untouched until this revision passes. (a) Single-writer pins CONFIRMED: `versions.AddVersion(` has exactly one production call site (`service_upgrade.go:341`, the FINAL route row, StateCreated; definition `versions/version.go:58`) and `versions.UpdateVersionState(` exactly one (`service_upgrade.go:695`, that marker flipped to Ready only after every step is Ready; definition `version.go:163`) — no `mo_version` row ever records intermediate-step progress. (b) Step persistence CONFIRMED: `addUpgradesToFinalVersion` (`:340-398`) builds ordered `mo_upgrade` rows keyed (final version, final offset) at `:364-397`, step states advance via `versions.UpdateVersionUpgradeState` (`version_upgrade.go:137-146`) from `doUpgrade` (`:719`/`:732`), and `versions.GetUpgradeVersions` (`version_upgrade.go:26-61`) reads them in `UpgradeOrder`; `performUpgrade` (`:610-706`) iterates steps in order with early return and flips the route's marker at `:695` only when every step is Ready — so a READY-4.0.6 route to 4.0.18 persists ONLY the 4.0.18/1 marker plus steps [4.0.6 -> 4.0.7, 4.0.7 -> 4.0.18], exactly the finding's model. (c) Both misclassifications CONFIRMED: v35's startup rule skips the preflight for the 4.0.18-Created latest row even while the 4.0.7 step is pending (under-fire — a mid-crossing restart resumes without the re-check main guarantees), and v35's recheck read `GetVersionState("4.0.7", 41)` stays missing forever for any route this binary creates (over-fire — valid post-crossing tasks rejected even after the 4.0.7 step durably commits); the marker can exist only as a main-era leftover. (d) Recheck lifecycle CONFIRMED and description corrected: `checkUpgrade(true)` is invoked only from `newUpgradePass` when a retained NON-final target completes (`:530-534`, with the cross-CN race rationale in the design comment `:155-156`); it never runs after individual intermediate-step commits; the phased V407 interlude (`:518-526` -> `runV407ClusterPhase`) only delays the 4.0.7 step's Ready transition, keeping the in-flight crossing fail-closed. (e) Byte-identity and mock accounting CONFIRMED: under final 4.0.7 the creation gate's beyond-4.0.7 arm is unreachable (no binary on main's chain persists a version row past its own final), so the v36 predicate reduces to main's exact condition with ZERO added reads; at recheck, every 4.0.7-final route's LAST step is the 4.0.7 step and steps complete in order, so a not-READY final marker and a not-Ready 4.0.7 step are the same durable state — main's condition byte-identical with NO added read; preserved-fixture accounting — `TestDoCheckUpgradeRetainsInterruptedTarget` (synthetic final "4.0.12"): its generic `from mo_upgrade` mock case serves the added scan and its steps' `ToVersion` is 4.0.11, so the preflight stays skipped with no `mo_cdc_task` case needed; `TestDoCheckUpgradeRetainsInterruptedPublishedV406` (latest 4.0.6): the below-4.0.7 arm short-circuits, `routeReads == 1` preserved; the fake-DB recheck drivers (`TestUpgradeRecoveryRecheckAfterCompetingCN`, `TestUpgradeCompletionPublishedAfterCommit`, `TestUpgradeRecoveryAsyncLifecycle`) tolerate the added mo_upgrade SELECT — all pinned as merge-note obligations (g1)-(g3) for the integration receipt. Resolution: I7 re-expressed around ONE shared step-based predicate used consistently at startup (latest version row: below-4.0.7 CHECK / 4.0.7-not-READY CHECK / beyond-4.0.7 scan that row's own route for a not-Ready `ToVersion == "4.0.7"` step) and at recheck (final route's steps; under final 4.0.7 the marker itself, under later finals one `GetUpgradeVersions` scan); the CDC block gains the v36 persistence-model correction and the corrected recheck lifecycle, with the owner, query, 32-task cap, error texts, positions and ordering unchanged; the D7 bullet's CDC clause re-derived; the validation plan gains the required REAL-ROUTE interruption pair (READY 4.0.6 -> actual persisted route targeting 4.0.18, interrupted BEFORE and AFTER the 4.0.7 step completes, no synthesized `mo_version` marker; the default-reject mock doubles as the no-marker-read oracle) plus the consistent-eligibility recheck pair driven through `newUpgradePass`'s retained-target completion path, RETAINING the READY-4.0.7/41 valid-task survival control, the {READY 4.0.5, READY 4.0.6, interrupted 4.0.7} legacy matrix, the frozen-offset rows, and the three-oracle red/green ladder; the alternatives table gains the rejected marker-model row; merge note (g) expanded with the mock-accounting obligations. Every other v10-v35 decision is unchanged — including I8 and both frozen-offset guards, which the review explicitly supports. |

v36 scope note: the corrections above ship only after this revision passes design review; the completed v32 implementation, the v33 re-base work, and all archived proof stay untouched until then. The re-gates themselves (the two CDC sites on the shared step-based predicate; the two offset guards per v35), the integration merge (including dispositions (a)-(h) with the (g1)-(g3) mock-accounting checks), the new final-4.0.18 CDC/offset matrices with the real-route interruption pair, and the re-collection of the affected evidence are implementation-turn work after that PASS.
