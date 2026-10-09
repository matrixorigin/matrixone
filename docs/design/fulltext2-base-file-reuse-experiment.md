# FULLTEXT2 immutable Base-file reuse experiment

Status: **default-off experiment; not a production rollout approval**.
Related work: [#27445](https://github.com/matrixorigin/matrixone/issues/27445)
and the independently merged CDC read-only cache contract, [#29227](https://github.com/matrixorigin/matrixone/pull/29227).

## Problem and scope

Normal stale/TTL replacement reconstructs a search Index. Its immutable Base
chunks may be streamed from SQL and materialized again even when only Tail/Delete
changed. This experiment retains the Base files within one CN service lifecycle,
not the decoded Index. Ordinary CDC must neither evict a warm Search nor access,
fill, invalidate or prewarm this pool. Normal freshness and DDL remain independent.

Only `fulltext2_base_file_reuse` builds install the service owner. Ordinary builds
keep the existing loader. There is no SQL variable, public capacity setting,
persistent cache format, background refresher, cross-restart reuse or shared
Segment. The experimental budget is 4 GiB / 64 files per service. This is not a
decision about a production default or unified disk budget.

## Ownership, identity and lifecycle

```
CN Start -> generation token -> service owner -> bounded immutable files
                                    |
                         Search -> lease -> private mmap/Segment/Index
```

Every load reads metadata with the current request/transaction before acquisition.
The key includes service, effective account, physical storage/metadata identity,
source/index/PK identity, build segment ID, checksum and size. Tail watermark is
not part of the key. Metadata/permission errors cannot be satisfied by local hits.
Missing identity uses the ordinary loader; checksum alone is not cross-index
identity. CREATE/REBUILD/MERGE use the existing per-build IDs; current and historical
loads may share a file only when their own metadata selects the same identity.

Files transition ABSENT -> FILLING -> READY. One request fills; waiters retain
their own cancellation and transactions. Filling reserves bytes and an FD before
creating the file. READY contents are not modified. Each lease pins before mmap;
every Search separately decodes and constructs Tail/Delete, liveness and scoring.
Only idle READY files are evicted, in acquisition-recency order.

Search.Destroy releases loaded resources but retains its original owner/closed
binding because the vector cache can retry that same algorithm object. CN Close
rejects/cancels owner operations, drains only its service's cache entries with
expected-entry identity, then closes the pool. The retryable owner cleanup is
outside CN closeOnce and preserves the CN fail-stop/local-completion contract.
Old generation tokens and delayed entry closers cannot drain a new instance.

Successful owner cleanup removes the registry entry. Pending entries remain owned
and charged until a later close succeeds. SQL factory never initializes an owner,
including when the registry is empty: only explicit CN Start can do so. The stable
process shutdown dispatcher is registered once and does not retain historical
owner closures. Global cache Destroy first closes registry/operation admission
and cancels every current owner's complete-operation context, before waiting for
the cache serve loop or entry locks. This prephase performs no pool cleanup or
drain; the post-eviction dispatcher drains/releases resources and retains pending
owners for retry. Housekeeping does not close admission. No cleanup worker or
permanent per-UUID tombstone is introduced.

## Failure and resource rules

| Condition | Outcome |
|---|---|
| Retention capacity/FD/pinned/validation-byte admission refusal | One ordinary load under original resource admission |
| Healthy waiter whose fill leader was canceled | One ordinary load with waiter's context and transaction |
| Unresolved deferred Segment or linked-file cleanup | Typed pending error before READY or ordinary mapping admission; no fallback |
| Waiter cancellation or closed owner/pool | Error; no resurrection through fallback |
| Published READY size/checksum/decode corruption | Retire, then one ordinary load, without refilling the pool |
| First fill source/permission/checksum/decode failure | Original error, no automatic retry |

Owner cancellation covers complete Preload/Load and nested ordinary fallback,
without replacing execution identity, snapshot or transaction. The pool reuses immutable
files, never shares decoded Segments. The first fill retains the existing validation
decode followed by the first caller's decode. Historical measurements belong
to the earlier revision described below; final-head net performance remains
unaccepted. This change retains that cost without introducing a handoff state machine.

Retired pinned files retain byte/FD charges. Failed FILLING cleanup keeps its FD
reservation until close. Failed munmap retains a reachable deferred Segment or
mapping. Reservation is retained while deferred bytes are registered, then
released: transient conservative double charging is allowed, undercharging is
not. Successful Search destruction releases its mappings/leases; idle files may
remain. Successful owner shutdown requires all files, mappings, deferred resources
and reservations to be zero; permanent OS cleanup failure remains explicit pending.

Experimental ordinary fallbacks pin the cleanup owner from initial mmap until
successful Free, independently of cache map residence. An eviction already removed
from the generic cache cannot make service cleanup report complete before that
consumer releases its mapping/path. Live pins do not authorize pool cleanup to
free a live Search; only failed releases enter the deferred retry collection.

Linked pool files whose unlink fails retain their exact path and disk-byte debt
after their descriptor is closed once. Failed fill passes the real handle to the
same cleanup owner. These debts quarantine new materialization, remain charged
to retention bytes, and are retried at later cleanup boundaries; successful
unlink or an already absent path clears debt once. No closed FD is charged or
reclosed. LOCAL absence or failure can reach this linked path in production.

Experimental ordinary fallbacks bind the same retry owner as pooled Segments
immediately after mmap, before checksum/decode validation or publication. Failed
Segment release preserves its mapping plus the cache's per-doc heap-model charge
under that owner after eviction. While any such cleanup remains pending, complete
owner operations and both READY and ordinary materialization reject new work with
a distinct pending error. Existing admitted work can finish and transfer its finite
resources under the original governor/tenant admission; capacity fallback cannot
bypass this quarantine. Successful retry removes each retained charge once and
reopens an otherwise open owner. No background retry worker or new resource budget
is introduced. Default ordinary loading keeps its previous semantics.

## Alternatives and decision record

* Ordinary per-load files: simplest and retained as the default/fallback; repeats
  Base SQL and file writes on reload.
* Share full Segment/Index or incrementally mutate cached Tail: rejected because
  decode/liveness/scoring ownership would cross search generations and the CDC
  read-only contract. It expands correctness scope for a narrower I/O problem.
* Retain only immutable files: selected as the narrow prototype. It still pays
  checksum, decode, Tail and liveness costs, retains disk/FDs, and does not help
  first load, restart or capacity misses. Each independent mmap remains charged by
  the existing search governor, even when file backing is shared.

The current repair scope accepts this default-off immutable-file architecture
and the bounded fallback/service lifecycle obligations. The task evidence
`trusted-design-scope-acceptance.json` records that current engineering decision;
it does not establish a historical human approval on 2026-10-08. Closed UUID
tombstones and query-side owner creation remain rejected. Production enablement
and final-head performance acceptance remain separate gates.

## Verification and rollout gates

Package tests cover single fill/private mappings, ordinary fallback, corruption,
context/transaction identity, canceled loads, quota/deferred ownership, cache drain,
old tokens, repeated cleanup and reclaimed/pending registry entries. Concurrent
tests use phase barriers and rescue/join before restoring mocks. The tagged embedded
SQL test covers fixed initial PKs {1,4}, cross-phase alpha score stability, warm
residence, old-versus-new Tail terms and unchanged Base identity across a new
Search reload, nonempty MERGE, REBUILD, same-name
recreation, snapshot/restore, accounts and actual CN close/restart. The canonical
snapshot BVT additionally checks current versus historical results across recreation
and REBUILD; neither SQL test nor BVT proves a file hit or capacity fallback.
A separate real-materializer component fixture compares fixed PKs and exact
score bits against an independently materialized ordinary loader, and counts
actual Base source streams for a new READY Segment and capacity fallback.
This is fixture equivalence, not complete ranking or performance acceptance.

Two historical Linux D->R pairs on the earlier `986ca0a7` source avoided 2,079,538,688
bytes of Base streaming and successful writes per reload, with 64 hits / 22 capacity
fallbacks. Reload wall time fell 17.936 -> 11.017 s and 19.082 -> 11.744 s. Initial
load increased 0.505 s and 0.451 s. These are historical same-order paired observations,
not final-main performance tests, not the planned three-pair gate, and remain
`PERF_INCONCLUSIVE_HOST_BUSY`. Both controls also had zero C10 timeouts, so no
claim that the historical timeout is fixed is supported.

Before Ready/default enablement: complete reversed/fixed repeatability, comparable
CPU/fallback/warm cost and a normal-lifecycle frequency/payback window, establish
production disk/FD admission policy, and finish exact-head CI/deployment QA. No
on-disk change needs migration; rollback is a normal build without the tag and
clean service shutdown. Unverified timing, permission-denial and deployment
scenarios remain explicit gaps rather than successful acceptance.
