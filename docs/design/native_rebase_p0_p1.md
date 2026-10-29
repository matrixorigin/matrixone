# Native Rebase P0/P1 - implementation record v0.1

## Frozen identities and scope

Inspected 2026-10-29. Worktree: `native-rebase-p0-p1/matrixone`, branch
`codex/native-rebase-p0-p1`. No production executor, SQL registration, migration,
Reset/Revert import or performance experiment is part of this closure.

| Input | Exact commit |
| --- | --- |
| Implementation base, official main | `b01e952ad1021ef8f188641f3a11a1360c001218` |
| Original local design base | `dd2b0b38f056ac56671a5d78c9de33b68b06ef07` |
| zuyu/matrixone branchbench-opt | `7611687f60de63d01aa4ac648a0aefad66f66176` |
| zuyu/db-fork mo-opt | `c2737beaf503dd1f3208efc457d369040f0aca40` |
| main/fork merge base | `b466f4ae9b74813a4ff93eaf43a6c81e6cbb8b0b` |

The old local base is NOT an ancestor of the selected current main. No old
worktree edits were discarded or silently rebased. The branch starts at the
verified official main, not the fork's historical parent. Adapter source SHA256:
`e3b71444682a18b7caf75fe06225e137ada938d190ae0d3d3889084d90001c52`.

## P0: minimal dependency map

| Boundary | Existing owner / reusable seam | Decision |
| --- | --- | --- |
| Four endpoint names, snapshots, table IDs | `resolveSnapshot`, `collectCloneDatabaseSource`, `resolveBranchCloneSource` | Resolve once; immutable U snapshot shared by every table. Never clone a moving current U per table. |
| Tenant and source/destination authorization | `authenticateDataBranchCreateDatabase`, source-table authorization | Reuse explicit caller checks before privileged catalog work; internal execution is not authorization. |
| Transaction and component admission | `getDataBranchComponentExecutor`, `beginLifecycleRCAdmission`, `finishDataBranchComponent` | Prefer one private pessimistic RC owner. Clone receives its non-nil BackgroundExec; never nest a fresh owner. |
| Clone and result IDs | `handleCloneDatabaseWithSource`, `cloneReceipt` | Return per-table receipts; use owner transaction to resolve uncommitted destination IDs. |
| New U base and retention | `updateBranchMetaTable`, `createBranchProtectSnapshot` | Existing child->parent/clone_ts edge plus kind=branch snapshot is the first candidate. Both write through the same BackgroundExec. No new catalog is justified yet. |
| Changes | `prepareDataBranchWorker`, hashdiff and batch output | Reuse bounded engine streams after four-endpoint validation; existing PICK is NOT a four-endpoint implementation and rejects fake-PK inputs. |
| Apply / constraints | ordinary typed batch DML and existing executor | Preserve PK/type/constraint enforcement and cancellation; no per-row client DELETE. Exact executor seam remains P1/P2 work. |
| Branch name and history | adapter `branches` registry and `_bb_commits` | Kernel must not own benchmark-specific history. R2 needs separate atomic publication/recovery proof. |

No fork commit is required by the P0/P1 probe. The fork has 26 commits after
its merge base, including database summaries, restore, planner and staging
optimizations. None is imported. Database-level summary SQL and Reset/Revert
are not prerequisites of a typed four-endpoint coordinator.

### Selected first milestone semantics

R1 is net-state replay, not commit-by-commit Git rewriting. Bf and Bu must be
proven corresponding logical base states; missing or mismatched ancestry is a
pre-write error, never the adapter's two-way fallback. F and U are immutable
endpoints; all source databases stay unchanged.

For a key's absent-or-whole-row state b/f/u: unchanged f selects u; unchanged u
selects f; equal f/u coalesce; otherwise FAIL, KEEP_UPSTREAM or KEEP_BRANCH.
Absence is not a tuple containing NULL. PK changes are delete+insert.
Adapter rebase `ours` maps to KEEP_UPSTREAM, `theirs` to KEEP_BRANCH (unlike
merge naming). Custom callbacks are rejected before native mutation.

Planned R1 summary fields are explicit: per-table `feature_changed_keys` and
`upstream_changed_keys` count distinct keys differing from their respective
bases, `conflict_keys` counts divergent overlapping keys, and
`applied_insert_rows`, `applied_update_rows`, `applied_delete_rows` describe
actual changes relative to U. Do not silently map these to an adapter mutation
count. Public wire shape and syntax remain unregistered until P2 design closure.

| Table/request category | First milestone contract |
| --- | --- |
| Same-tenant same-schema regular tables, explicit PK, ordinary stored scalar columns, literal defaults, no non-PK indexes/constraints | Selected implementation target; NOT yet runtime-qualified |
| Composite PK and nullable non-PK scalar columns | Same whole-row contract; require typed SQL tests before advertising support |
| No-PK table with zero proven visible delta | Preserve U without replay only after engine-level equality proof; otherwise reject |
| Changed no-PK table | Reject before writes; partial R1, not completed P3 |
| FK, non-PK unique or secondary indexes, generated columns, checks, triggers, sequences, views/routines, external/temporary/subscription tables | Reject before destination creation in initial R1; not silently omitted |
| Schema differences, table additions/deletions, custom conflict callbacks, cross-account endpoints | Reject before writes |
| Explicit client BEGIN | Do not advertise as public Rebase support; P1 uses existing SQL transactions solely to probe substrate |

### Adapter evidence and deliberate incompatibilities

Frozen `_rebase_impl` binds Bf from the branch base marker, Bu from its
`prev_db`/`prev_snap`, F from a new feature snapshot, and U from the fixed
upstream snapshot. It clones U, records a pre-replay base marker, reconciles
schema (including changing the old feature), replays rows, copies history,
unconditionally upserts the registry and drops the old generation.

The existing `_register` has no expected-generation predicate and connection
defaults are autocommit. Therefore this is NOT evidence for atomic or
recoverable R2 publication. Do not reuse its destructive pre-drop of the next
generation on an uncertain retry. An unknown commit acknowledgement requires
inspection, not legacy replay or unconditional destination removal.

The executable adapter-method checks show why the old bag model is not a
compatibility oracle: independent equal inserts are additive (two visible rows),
not count-coalesced; DELETE uses NULL-safe comparisons and LIMIT 1; an already
absent deleted row still increases the returned attempted-change count.
Delete-versus-update has no row-identity conflict resolution in that helper.
These are Python/helper facts under an independently supplied delta, not proof
that database DIFF produces that delta or that collation behaves like Python.
NULL/collation/hidden-key origin still need real database tests for P3.

## P1: transaction ownership and prototype

The existing `getBackExecutorInternal` either borrows an explicit session
transaction (its finish only closes the BackgroundExec) or opens and finishes
one private transaction. The component constructor requires pessimistic RC,
adds lifecycle admission, and frees its admitted process through finish.
`handleCloneDatabaseWithSource` does not create/finish another owner when bh is
provided. `updateBranchMetaTable` and `createBranchProtectSnapshot` both write
through that same bh. Public `doCreateSnapshot` is NOT a safe nested substitute:
it has its own begin/finish lifecycle.

The new `TestRebaseTransactionPrerequisites` uses the existing shared single-CN
SQL fixture, two two-row tables and a fixed upstream snapshot. It checks:

1. Clone creates destination edge and protection pin inside the transaction.
2. A first-table replay followed by a second-table duplicate-PK error and
   rollback removes destination, edge and pin, leaving source rows unchanged.
3. Successful replay commits rows, source parent IDs and matching protection
   timestamps together; all tables bind the same U.
4. After dropping the user's U snapshot and changing upstream, the retained
   clone_ts still reads original U rather than replayed destination data.

This is a prerequisite probe, NOT a native coordinator, a real Rebase conflict,
or proof that its transaction constructor supplies the eventual typed apply path.
It does not establish memory/spill bounds, admission order across all four
endpoints, cancellation, commit injection, restart, source-drop/GC races, or R2.
Do not infer those properties from an explicit client rollback.

### Evidence / exit status

See `native_rebase_validation.json` for actual command results. At introduction,
the local controlled CGo runner stopped before compilation: current worktree
has no libmo and the primary worktree lacks a valid native provenance stamp.
Local disk available was only 2.8 GiB, so a full local native rebuild was not
started. Ubuntu read-only admission found no cooperative owner or MO/build
actor, about 25.7 GiB available memory and 48.6 GiB free disk; its qualified
compiler is Go 1.27.1, matching this main. These facts alone are not Linux PASS.

P0 selected-PK semantic scope is recorded; production design stays blocked on
P1 runtime/retention/recovery closure. No new publishing framework is selected.
P1 is NOT COMPLETE until its required runtime, fault and restart evidence exists.
P2 must not start merely because offline/model tests pass.

### Actual Linux results and current blocker

Five separately frozen generations were executed. R1 was rejected by an archive
root-directory guard before build; R2 built native code but module transport
timed out. The archive guard was corrected with regression tests, and R3 used
the same pinned module through a private file proxy with go.sum still enforced.

R3 and R4 passed the original SQL prerequisite test. R4 also passed the new
isolated `TestRebaseTransactionRestart`: the existing CN commit-failure hook
rolled back the clone/replay; a real disconnected client did not publish its
uncommitted destination; after stop/start, successful rows/edges/pins survived
and failed/disconnected targets and pins were absent. This is graceful service
recovery, not SIGKILL/crash qualification. The isolated fixture is necessary
because the shared SQL fixture must never be restarted.

R5 passed `TestRebaseTransactionCancellation` and the prerequisite root. The
cancellation test uses the existing lifecycle lock hook after clone/edge but
before child-pin insertion. A second session sees no uncommitted destination,
commits a source write, and server-cancels the parked private owner before the
barrier is released. The request fails with no destination/child-edge/pin residue.
No timing sleep determines the result.

R5's later isolated test process aborted DURING `embed.StartTestCluster`, before
its fixture SQL and commit injection: C allocator `double free or corruption
(out)` / SIGABRT, stack in `SimpleCAllocator.deallocateMemory`. This is a real
failure preserved unchanged. R4's earlier PASS is valid scoped evidence but does
not make R5 PASS or prove stability. There is no clean-baseline reproduction of
this abort, so it is not attributed to main, proot, or the new tests. No allocator
change or unchanged retry was attempted.

Current status: P0 selected-PK contract recorded; P1 PARTIAL / BLOCKED, not
completed. The typed four-endpoint admission and coordinated source DROP/actual
snapshot-GC tests remain unproven. The verified single-owner clone/edge/pin path
is still the preferred candidate; there is no evidence justifying a new staging
or publication framework. Do not proceed to P2 or claim R1/R2 delivery yet.

All five worker/supervisor pairs were independently checked: 10 inactive/failed
units, MainPID=0, empty ControlGroup, no owned lease. Five selected evidence
archives were downloaded and re-read with SHA256/member checks. Sources, build
output and failure records remain remote; no foreign resources were cleaned.

The next discriminating action is a plain-main startup/restart control under the
same native/toolchain environment to separate the allocator abort from Rebase
SQL. A PASS there alone would not erase this failure or qualify arbitrary GC
races. Resolve that admission blocker, then finish the missing P1 boundaries.

### Reproduce the cheap checks

Fetch `dblib/matrixone.py` at the pinned c2737bea commit to an evidence directory,
then run from the implementation worktree:

```sh
NATIVE_REBASE_ADAPTER=/path/to/frozen/matrixone.py python3 -m unittest discover -s docs/design -p 'test_native_rebase*.py' -v
```

The test loader checks the complete adapter file SHA256 before compiling only
the two selected methods and their identifier quoting helper. Twenty-two
offline tests are selected: ten original scalar/bag models, six actual adapter
method checks and six wrapper checks. Bag cases describe a rejected proposal,
not the selected native no-PK contract. The expensive SQL probe is separately
named `TestRebaseTransactionPrerequisites`; it must not be reported as one of
these twenty-two passing database-independent checks.
