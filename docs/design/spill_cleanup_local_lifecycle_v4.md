# Spill cleanup: synchronous local lifetime, revision 4

Status: **proposed replacement; explicit approval of this exact revision required**.
This document selects the previously deferred decisions. It does not certify the
queue-based production code still present in #30099/#30102.

Series: [#30099](https://github.com/matrixorigin/matrixone/pull/30099),
[#30102](https://github.com/matrixorigin/matrixone/pull/30102).
Issues: #30097, #30098, #30100, #30103.
Inventory baseline: official main `6fb6531432aaec8c1fbd98a30364941559e0e1d6`.

## 1. Invariants and selected direction

Every spill file has one destruction owner, including partial creation,
cancellation, rejected publication, Reset/Free and pool release. A surviving
file must not be reported as deleted or have its disk charge refunded. Failed
deletion must not accumulate query objects, callbacks or new files indefinitely.

[Maintainer feedback](https://github.com/matrixorigin/matrixone/pull/30099#pullrequestreview-5567086544)
selects local open/unlink/close, synchronous operator deletion when FD rotation
requires names, and startup cleanup after the old process has stopped.
[Consolidation feedback](https://github.com/matrixorigin/matrixone/pull/30102#pullrequestreview-5567094738)
selects one implementation response rather than diverging parent/follow-up fixes.

Selected proposal:

- Use anonymous open -> unlink -> descriptor handoff -> close where the consumer
  does not need pathname reopening.
- Preserve named-file FD rotation where required. Delete synchronously from the
  terminal operator owner; do not substitute a background retry.
- On the first failed unlink, permanently fence new spill creation for that
  scratch-root incarnation. Already-admitted operations clean their own files.
- Close FDs and release memory at terminal cleanup. Preserve CN disk charges for
  surviving files without retaining a per-file/query callback collection.
- Recover a fenced root only at a controlled CN process restart: old owners
  stopped, exclusive root lease acquired, all old entries removed, fresh budget
  incarnation established, then spill admission published.

The availability tradeoff is deliberate: subsequent spill-required queries may
fail until restart. It is proposed for approval, not claimed as already accepted
by the maintainer. Do not add a hidden in-process recovery or retry protocol.

## 2. Concrete source inventory

| Boundary on the inspected main | Observed lifetime | Selected treatment |
| --- | --- | --- |
| GROUP ordered aggregate factories, `group/helper.go` | Already call `CreateAndRemoveFile` | Preserve anonymous lifetime; require successful unlink before publication. |
| GROUP/DISTINCT buckets and intermediate runs | Work-list membership and file lifetime differ | Explicit owner survives pop/rejection; anonymous if no reopening, otherwise terminal synchronous named cleanup. |
| HashBuild `spillFileBundle`, `hashbuild/types.go` | Creates/reopens/seeks/closes around writes and publishes reopenable files | Preserve rotation and FD budget; move destruction ownership only on successful publication. |
| JOIN `BucketWriter`, `spillutil/join_spill.go` | Named append/reopen, reopenable payload handoff | Preserve rotation; partial scatter/rejected handoff cleans every created child synchronously. |
| JoinMap, SpillEngine, hashjoin/dedupjoin/rightdedupjoin | Consumer teardown, prepared-cache Reset and pooled release | Exactly one final owner; cleanup does not wait for another Prepare/query. |
| `LocalFS` / `LocalETLFS.CreateAndRemoveFile` | Both ignore `os.Remove` error | Return no usable descriptor on unlink failure; close it and return the error. |
| `Process.GetSpillFileService`, `process/process.go` | Creates a subpath wrapper per call | All wrappers must share the CN-owned root health/lease, not independent per-query state. |
| `clearSpillFiles`, `cmd/mo-service/main.go` | List errors propagate; deletion errors are ignored | Propagate cleanup failure; publish no usable spill root before every entry is removed. |

Keeping all JOIN/HashBuild FDs open would change existing capacity. FD exhaustion
therefore returns its existing admission error; it must not choose a different
lifetime model. No spill FD/disk/memory cap is changed by this proposal.

## 3. Root owner, admission and boundedness

One CN-owned object represents one canonical scratch root and service
incarnation. All spill subpath wrappers use it. It stores a monotonic state and
one bounded first-error diagnostic, not filenames, tokens, query references or
cleanup callbacks. Generic data-file operations and unrelated roots are outside
this fence. States: `initializing -> healthy -> fenced -> stopped`.

Only initialization of a fresh incarnation can publish `healthy`. New creation
checks admission before acquiring a physical file. Failed unlink fences the
root before its error is published. Work already admitted before the fence may
finish its syscall and must clean up synchronously; post-fence work creates no
file. The bound is the finite already-admitted set, not a claim that a fence
cancels OS calls already in flight.

Do not hold a shared lock across filesystem I/O, operator teardown or callbacks.
Fenced admission and query cancellation must not wait behind a blocked unlink.
Root state is O(configured CN roots), not O(ended queries or failed files).
There are no workers, background timers, periodic scans, or global 16,384-file
admission policy. Existing memory/FD/disk acquisition still precedes resources.

## 4. Anonymous creation and named-file teardown

Anonymous states: `not created -> open/named -> open/unlinked -> closed`.
Unlink success is the descriptor-publication point. Create/context failure
releases only acquired resources. Unlink failure fences the root, closes the
unpublished descriptor exactly once and returns no usable file. Preserve both
unlink and close errors if both occur. The surviving name belongs to the fenced
scratch root's startup-recovery domain, not to a detached callback.

An anonymous descriptor's successful handoff moves its lifetime and accounting.
Failed handoff, cancellation, Reset/Free and terminal release close the last
owner. Multiple terminal calls must not close/refund twice.

Named states: `not created -> producer-owned -> optional consumer-owned -> removed`.
Closing/reopening an FD does not release name/disk ownership. A successful
publication moves the owner and clears the producer; rejection leaves or returns
ownership to the producer. All partial/scatter/constructor/teardown paths perform
synchronous deletion. Missing-file confirmation is success; every other removal
error fences the root and is not reported as a successful removal.

On terminal removal failure, close FDs, release optional memory and mark the
file's shared disk-token core `terminal-abandoned` under the CN ledger lock,
**without calling its successful-deletion Release**. This is a terminal state:
Release/Reconcile/Grow through a copied token cannot refund or change it. Drop
all terminal token/query wrappers; the root does not retain them. There is no
Go finalizer or deferred refund. The inspected CN ledger increments
`ExecutionResourceBudget.spillDiskUsed` at reservation and decrements it only in
explicit Release/Reconcile operations; generation Close does not refund it.
Dropping the terminal token/query wrapper therefore leaves a conservative CN
charge without a retained query reference. Test this against the actual ledger,
including exactly-once behavior of copied/repeated terminal wrappers.

Do not clear this charge in place: fenced-root reuse and in-process recovery are
unsupported by revision 4. A controlled process restart establishes a fresh
budget only after physical cleanup. This is an explicit conservative-accounting
decision, not evidence that the surviving file uses zero bytes.

## 5. Error visibility and blocking I/O

Where an error return exists, join cleanup errors without masking the original
execution/constructor failure. A void Reset/Free cannot retroactively turn a
client result already sent into an error: record the bounded root diagnostic,
fence new creation, and return its cause on subsequent spill admission. Do not
fabricate a successful cleanup result in metrics/logs.

Report root incarnation and failure phase with bounded text; no per-query or
filename metric labels and no repeated retry logs. Storage syscalls may block:
this proposal adds no goroutine-per-delete or timeout pretending to cancel an
OS unlink. No strict syscall completion deadline is claimed. Test blocking I/O
with a releasable barrier and prove independent admission/cancel paths progress.

## 6. Exclusive root, startup, replacement and rollback

Acquire a nonblocking, OS-backed exclusive lease for the canonical scratch root
before startup listing/deletion. Keep its lock outside the directory being
cleared. Retain it until all spill owners/FDs and the service have stopped.
Reject aliases or normalize them to the same lease; never follow cleanup paths
outside the designated scratch directory.

Startup list/delete/lease failure prevents successful initialization. Every
entry must be physically gone before publishing admission. Crash/kill releases
the OS lease; the next process acquires it before cleaning previous files.
Retaining a FileService pointer does not prove exclusivity.

Revision 4 rejects in-process replacement/root reuse for a fenced incarnation.
A new wrapper, execution generation or service ID cannot clear the fence.
Recovery requires a controlled process restart and fresh budget incarnation.
Normal shutdown stops admission, joins/closes all live owners, then releases
the lease. No callback can outlive the service because no detached callbacks
exist. A second live owner must fail before it can list/delete the old root.

Upgrade/rollback requires stopping the old CN before root reuse. A new lease
does not coordinate with old binaries that never acquired it; do not claim
mixed-version shared-root support. Spill data is disposable query data, not a
cross-version recovery format. Never adopt old spill files into a new query.

## 7. Alternatives and cost

| Alternative | Decision |
| --- | --- |
| Ignore removal errors | Reject: owner/charge loss and silent accumulation. |
| Detached queue/global admission/four workers | Reject: extra lifecycle/capacity policy and conflicts with maintainer direction. |
| Make every file anonymous, retaining every FD | Reject for rotating JOIN/HashBuild: changes FD capacity. |
| Synchronous cleanup plus sticky root fence/restart recovery | Selected proposal: bounded root state and no background work; trades spill availability for failure containment. |

Healthy paths add root admission/state checks, not row/batch queue work. Failed
files already admitted may remain on disk until restart; that residual exposure
and restart operational cost are explicit. No performance improvement is claimed.

## 8. Deterministic acceptance and delivery

Use small real files, injected operations and barriers, not sleeps or scale to
guess scheduling. Preserve distinct failure oracles from both PRs; queue-specific
eventual-retry expectations are replaced by the new selected contract.

1. Anonymous file: pathname absent before publication; read/write until final
   close; physical lifetime and exact FD/disk accounting.
2. Create/unlink/close failure and canceled first write: original errors,
   unpublished FD closed, root fenced, zero-byte file cannot enable unbounded
   subsequent creations.
3. Admission/fence race: admitted work cleans its own resource; post-fence work
   creates no file, including many repeated query attempts.
4. Named rotation: complete data survives reopen, existing maximum FDs unchanged,
   accounting and destruction move once on successful handoff.
5. HashBuild rejection, every JOIN constructor/scatter failure, GROUP popped
   buckets and DISTINCT intermediates: physical files and charges observed on
   success/error; preserve the original error and exact ownership.
6. Prepared Reset/Free/pool release: terminal cleanup without another Prepare;
   no duplicate close/refund or retained query; persistent delete failure fences
   admission and retains conservative CN charges after generation Close/GC.
7. Blocking unlink: admission and cancellation are independent of its lock;
   release barrier before teardown, assert no surviving test goroutine.
8. Two-process lease and in-process replacement rejection before deletion; old
   descriptors remain usable. After proven stop, startup removes every old file
   before admission. Include aliases, canceled startup and delete/list failure.
9. Owning suites and actual consumer tests, focused race for root/handoff/teardown,
   incremental vet/lint and matching CGo provenance. SQL/large-data and restart
   evidence are distinct from internal UT; none is claimed by this document.

Consolidate the eventual implementation on #30099 and preserve #30102's distinct
JOIN/HashBuild oracles. Resolve its HashBuild/main conflict during consolidation.
Do not automatically merge/close either GitHub PR. Remove the obsolete queue,
workers, global capacity and enqueue callers from the effective delivery diff.

## 9. Decision record required before implementation

The proposal now chooses unlink-failure ownership, FD rotation, terminal
accounting, sticky-root fencing, exclusive lease, process-restart recovery and
startup failure behavior. These choices require explicit approval referencing
this exact document revision, including the availability/rollback tradeoffs.
No prior queue UT/CI run or general repair request is an approval record.
Material deviations require design re-review. Do not describe production repair
or runtime acceptance as complete before code alignment and its validation.
