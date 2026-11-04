# Statement authorization snapshot admission

Revision: 3 (2026-10-17). Status: design approved; performance acceptance pending. Owner: frontend / transaction client.
Issue: #29133. Implementation: PR #30400. Review feedback: 5555768268, 5557316266.

## Problem and gate

The current role-rule freshness fix adds `GetLatestSnapshot` before every
request. Privilege validation and transaction creation independently perform
the same class of snapshot admission. The retained authenticated TCP evidence
shows material throughput loss, particularly with freshness preserved or
transaction admission limited. Prepared lookup deduplication does not remove
this added admission.

This is a major refactor of frontend authorization and transaction-client
admission ownership. It crosses subsystem/lifecycle boundaries, affects a
security boundary and a hot path, and therefore requires the repository's
design-first gate. This document is the distinct design phase within the
existing implementation PR; no wire, catalog, persistent state or configuration
change is proposed.

The current CI pagination failure separately exhausts its shared three-minute
context. Its causal attribution requires a matched comparison. Increasing its
deadline is not part of this design.

## Contract and linearization

One semantic SQL statement selects a current authorization snapshot A through
the existing transaction client's limiter, readiness and logtail-wait owners.
This admission is the statement's freshness linearization point. Rules, active
role membership and grants must be evaluated consistently at A, independently
of any older user transaction. A commit ordered before admission must be
included according to the existing snapshot-selection policy; a concurrent
commit after A belongs to the next statement's authorization epoch.

The first owner is a statement-scoped frontend authorization epoch. It binds
the Session, principal, context account, statement generation, transaction
client, minimum commit frontier and cancellation/lifetime. It is created
before rewrite and prepared lookup, and ended at the semantic statement's
terminal boundary. Different protocol requests never share it. A principal or
minimum-frontier transition ends the old epoch; it must not upgrade only the
grant half of a decision whose role rewrite used A.

The existing user transaction's isolation and data snapshot remain unchanged.
Its old snapshot cannot provide a current authorization proof. A newly created
data transaction may instead consume the statement's already completed
admission and initialize with A. Subsequent RC snapshot advancement and
transparent execution retries retain their existing data-isolation semantics,
including a later data snapshot B; they do not move grant or rule reads to B.
The authorization decision remains at A for that logical statement.

## Interfaces and ownership

The transaction client creates an opaque `SnapshotAdmission` containing its
owner, admitted snapshot, origin context and bounded lifetime state. Only the
client can create a usable object; callers cannot provide an arbitrary TS to
bypass admission. An optional helper/capability may preserve the existing
`TxnClient` interface for mocks and older implementations. Unsupported clients
retain the existing independent proof/read behavior.

- `AcquireSnapshotAdmission(ctx, client, minimum)` performs the existing
  limiter/readiness/current-snapshot operation once.
- A client-validated read accessor exposes A only to the originating live
  epoch, with a satisfied minimum. The engine uses it to sample catalog proofs
  without another admission.
- `WithSnapshotAdmission(token)` permits one new data transaction to claim the
  already completed admission. It retains ordinary transaction registration,
  queue admission, close/cancellation gates, unknown-commit handling and all
  success/failure cleanup. An explicitly supplied historical snapshot cannot
  be combined with this option.
- `WithAuthorizationSnapshot(token)` fixes an independent authorization read
  transaction at A. This option still performs that actual background
  transaction's normal limiter admission, and does not claim the data
  transaction's reusable admission. It enforces SI and read-only behavior.
  The two options are mutually exclusive.

Authorization reader initialization must install **exactly A** after verifying
its readability, including the operator's catalog-read revision bookkeeping.
The ordinary `updateSnapshotWithClose(A)` can return and install a later waiter
timestamp B; its return value must not replace A for the authorization reader.

The original `GetPrivilegeCacheVersion` remains available. An additional engine
entry samples the same account-scoped catalog/object/version vector after
validating the supplied admission. Readiness, partition visibility, subscription
and physical identity checks remain intact. Sampling a proof is distinct from
acquiring an admitted snapshot.

## Token state and failure closure

The bounded state is issued -> claimed -> ended, with issued -> ended also
allowed. Read-only authorization consumers can inspect a live issued/claimed
token; only one data `New` can claim it. Claiming precedes transaction creation
and is never undone after a later creation failure. A second attempted claim
cannot skip limiter admission. Request cancellation, statement completion or
client closure prevents a later result from publishing success.

No token lock is held across limiter waits, catalog I/O or transaction admission.
No goroutine, timer, transaction, cache entry or background worker is retained
by the token. The statement owns its fixed-size object and terminal invalidation.
Existing `closeUnadmitted` / `abortCreatedTxn` paths own every failed transaction
generation, including close while queued and partial initialization.

An invalid token must not silently substitute newer authorization while
preserving an earlier rewrite decision. Principal changes, a real causal minimum
advance and new semantic statements require a new complete epoch or fail closed.
A live, already-claimed token is a different state: the normal transparent retry
or second-creation path uses ordinary admission and existing data-isolation
semantics, while authorization remains at A. It never reuses the credit or
re-runs side-effecting SQL/rewrite merely to refresh authorization. Explicitly
passing a claimed token to `WithSnapshotAdmission` again is an error; an invalid
token error is not a general fallback signal. Only unsupported capability permits
the complete legacy path.

## Consistent catalog reads and cache publication

`loadRuleCache`, the complete grant/role traversal in
`determineUserHasPrivilegeSet`, and the alternate active-role authorization path
use explicitly marked, independent snapshot-A SI background transactions.
Other background SQL receives no such capability. These transactions are
read-only and cannot inherit the user's writes or old snapshot.

In particular, a grant miss must not read a newer B after rules were evaluated
at A. This prevents a mixed decision when a concurrent commit both grants
access and adds a restrictive role rule. Sampling a proof whose partition
already contains changes ahead of A yields zero, as today: zero means a fresh
uncacheable snapshot-A read, not permission to stamp old rows with a new vector.
The epoch atomically publishes one proof with A and reuses that proof within
A; later B cannot change the contents authorized at A. Every reuse validates
principal, cancellation, lifetime and the current causal minimum. Failed or
ended acquisition publishes no usable proof. Zero stays uncacheable for the
entire epoch. A new epoch always acquires and samples again. Cache publication
retains the existing pre-read stamp, generation and identity checks. This
same-A sampling consolidation was independently confirmed against revision 2
by the design reviewer before validation.

Background read-only commits carry no user write frontier. Their internal
completion must not advance the upstream Session beyond A and invalidate the
statement halfway through authorization. Only explicitly marked read-only
authorization sessions suppress that propagation; normal background writes
and ordinary user commits retain current frontier propagation. Parent minimum
changes for a real causal commit still invalidate the epoch.

Principal/account, role-cache generation/revision and grant-cache invalidation
remain independently checked by their existing consumers. A context whose
authorization principal differs from its Session cannot borrow the parent's
capability. Nested same-statement authorization helpers can use A; independent
nested SQL, another Session/account or a definer-principal transition starts a
separate owner. Returning from a child never publishes its proof as its parent's.

## SQL/protocol boundaries and parsing compatibility

COM_STMT_PREPARE/EXECUTE/FETCH establish their first epoch before policy capture
or handle lookup. `doComQuery` adopts that first epoch, rather than replacing it
after a prepared lookup has already occurred. Each additional statement in
COM_QUERY starts a new epoch before capture/rewrite and wrapper construction.
The raw SQL continuation, including executable comments, is split by the MySQL
parser; semicolon text scanning is not a statement boundary.

A replacement BEGIN commits the preceding user transaction before the new
statement owner and policy are captured at the advanced commit frontier. It
uses the same complete refresh boundary as implicit-commit DDL, while keeping
BEGIN separate from the DDL statement-end commit flag: the newly opened user
transaction remains active and rollbackable. Commit failure preserves the
existing error conversion and does not execute the replacement statement.

Ordinary batches preserve the existing eager syntax-validation behavior before
executing the first statement, without performing authorization or creating a
transaction during validation. Execution then stages original raw statement
text, with current policy and principal for each statement. Existing SQL-mode
changing batches retain their established staged parsing behavior. A bad later
statement in an ordinary batch must not introduce a new partial-execution
behavior; mode-changing batches keep their existing behavior. The first
statement's materialized role rules must never be reused as raw input for later
statements.

Cleanup-only prepared lookup still performs no I/O. Every live prepared lookup
retains generation/invalidation checks. Error returns preserve the known handle
so EXECUTE/FETCH can release cursor accounting and pending parameters.

## Revision 3: remove redundant hot-path work

The epoch carries its two context values and prepared-request stamp directly.
All other values delegate to its original standard cancellation context,
including the private context keys required by `context.Cause` and child
cancellation. A notification channel is created only when another consumer
actually waits for acquisition. An exact-owner cancellation comparison does
not materialize a Done channel; independent callers retain the cancellation
bridge. Terminal cleanup completes under the existing epoch mutex once, so a
concurrent or outer terminal boundary cannot return before cancellation ends.

The admission token lazily creates its shared cancellation child only for
potentially waiting creation. End still seals the token under its mutex;
initialization after End immediately cancels the child. Each creation adapter
captures that stable lifetime and owns only its own cancellation. A direct
data creation may avoid an adapter only when the native monotonic frontier
already covers A.Prev() and the existing client mutex atomically proves open,
ready, no abort barrier and immediately available capacity. Ordinary opening
and this path share the same admission transition. Final close, cancellation,
token and failed-creation cleanup checks remain mandatory. Authorization
transactions and every potentially waiting path retain normal admission.

Prepared refresh uses the same complete proof, retry, publication and
invalidation owner as full policy capture. It discards value metadata instead
of cloning unused maps. Full capture still returns independent immutable maps.
Production proofs stay typed on the hot path; only cold cache publication
boxes the catalog vector. A lookup already holding the session mutex checks
the request stamp there, and releases that mutex before any actual refresh.
Principal, context lifetime, generation, revision and handle checks remain.

One proof can share the idle-subscription access-time sample across its warm
table reads. This timestamp only maintains idle reclamation bookkeeping; it
is neither the authorization snapshot nor a freshness cache. A cold/waiting
path discards it before later warm reads. All eight physical identities,
versions, pending/visibility checks and fault checks are still sampled.

Single COM_QUERY preflight uses the existing MySQL scanner to detect whether
there can be another statement, without building an unused AST for trailing
delimiters or semicolons in literals/comments. The parser remains authoritative
for actual splitting, SQL mode and eager syntax validation of ordinary batches.
Sibling diagnostic contexts are built only for an actual continuation, using
the original request parent captured before an implicit commit replaces the
first epoch.

These refinements do not waive the default-throughput requirement. The current
source and functional evidence are recorded in the
[hot-path validation report](../performance/statement-authorization-hot-path.md).
That report distinguishes unsuccessful pilots and diagnostic probes from
acceptance measurements; the historical revision-2 matrix is not a new-head
performance result. The subsequent [complete 32-cell delivery matrix](../performance/statement-authorization-delivery.md)
identifies its refreshed inputs and retains all samples and runner limitations;
completion of that protocol does not close the default performance gate.

## Alternatives and compatibility

1. Current independent proofs preserve correctness but demonstrably add hot-path
   admission and waits. Request-only memoization is too broad for multi-statement
   and nested authorization, and leaves the admin transaction path redundant.
2. Using an existing user transaction or a TTL/empty-rule shortcut can miss
   remote revocation/rules. Those alternatives violate the freshness invariant.
3. Creating a data transaction before parsing consumes isolation/admission state
   for statements that may not need a transaction. A small client-owned
   admission capability preserves the existing creation boundary and ownership.

This is an in-process contract; no persisted token or cross-CN token is sent.
Mixed versions continue using their local implementation and existing catalog
proof format. No MORPC capability, migration, restart worker, feature flag,
background cache, backup/restore change or rollout configuration is needed.
Rollback restores independent admissions and retains the existing fail-closed
rules. The design does not exempt admin or empty-rule sessions from freshness.

## Cost model, validation and delivery gate

Snapshot A is exclusive. The completed acquisition proves application through
A.Prev(); transaction creation rechecks that applied frontier, including close
and cancellation, and installs exactly A. Waiting for A itself would demand a
new logtail notification and defeat admission reuse. A real timestamp-waiter
regression seeds one frontier and sends no later notification; both data and
authorization creation must complete from that frontier.

All state is O(1) per active statement. Catalog proof sampling remains bounded
by the existing eight-table vector. There is at most one reusable admission per
epoch and no unbounded retry or waiter map. Real catalog read transactions still
pay their normal admission. Warm, provable autocommit paths are predicted to
use one admission (currently three for non-admin, two for admin); cold/miss and
existing-user-transaction paths have separate necessary work. These are design
predictions, not measured results.

Deterministic tests must prove token owner/minimum/lifetime checks, one claim,
second `New`, queued close/cancel and failed creation cleanup; role/grant/data
snapshot agreement for role/grant reads and preservation of data isolation;
zero-proof handling; principal/generation/frontier changes;
late success after completion; ordinary/mode-changing batch syntax compatibility;
same-request role changes; nested owner isolation; and unchanged RR data view
with current authorization. A waiter returning B>A must still initialize every
authorization reader exactly at A; a normal data RC update may advance to B.
A barrier case orders a remote combined role/grant
commit B between the current statement's A role and grant reads: A is consistent,
and the next statement observes B. Existing two-CN ADD/replacement/DROP,
prepared reprepare, fail-closed and six resource-cleanup cases remain required.

The performance matrix is matched official-main/candidate on the same runner
and harness: default, freshness-preserving and 100/s limited; text/binary;
role rules present/absent; admin/non-admin. First connection/PREPARE/execution
is separate from one warm-up and three measured rounds per cell. Keep request
errors, latency, throughput, snapshot and actual admission counts. Count-only
instrumentation, if needed for baseline admission observations, must be identical
in both builds and explicitly recorded. Cold real catalog work is reported
separately. No material regression, plus the functional/lifecycle closure above,
is the implementation delivery gate; an owner waiver is not substituted for it.

The shared-deadline pagination test is compared at the original PR head, clean
official main and the candidate with the same race/tags/CGo mode. Local platform
limits and CI attribution remain explicit. No deadline relaxation, retry/skip or
unrelated code change is a permitted substitute.

## Design review record

Independent gpt-6-astra medium design review: PASS of revision 2 at design-only
commit `23c27718e085cdc9aec20e54b745a98842f9a67f`, SHA-256
`e078fd951cd9d11182f4607b650f8a38b00ca6f6eab038506714a2f38282d223`.
After rebase, the same approved design blob is retained in commit
`02c72d4fc4c19301d1c2222607ed117a23c7019b`. Implementation and the matched acceptance results are recorded in
[the performance/validation report](../performance/statement-authorization-admission.md).
The review completed before implementation. RC data/retry
semantics, fixed authorization snapshot installation and parser compatibility
are explicit decisions above. Material deviations return to design review.

Revision 3 received incremental gpt-6-astra medium design and source reviews
before each implementation change. The completed source review found no
additional correctness or lifecycle blocker; the default performance gate
remains open and is not implied by that source approval.
