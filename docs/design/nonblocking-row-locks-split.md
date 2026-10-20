# Nonblocking row locks: complete series design

Owner: issue #25107; NOWAIT foundation #30183, dependent SHARE SKIP LOCKED #30272.
The design gate covers both parts together. The approval record below pins the
exact reviewed technical revision; implementation validation remains separate.

## Problem, goals and scope

Ordinary locking reads wait for incompatible row holders. Applications need an
explicit row-conflict error, or a result containing only admitted Shared rows.
An untyped FastFail error is insufficient: it can abort earlier transaction work,
and an old owner can ignore an unknown policy field and enter the waiting path.
The contract must cross parser, planner, coordinator, physical owner and rollback.

Support only `FOR UPDATE NOWAIT`, `FOR SHARE NOWAIT` and `FOR SHARE SKIP LOCKED`
in pessimistic transactions. Preserve ordinary locking reads, query-block policy
isolation, prior uncommitted work and transaction-owned grants. Optimistic mode,
remote nonblocking LockOps, coarse locks, mixed target policies and
`FOR UPDATE SKIP LOCKED` are excluded. No fairness or starvation guarantee is added.

The compatibility precedent is MySQL 8.0
[locking reads](https://dev.mysql.com/doc/refman/8.0/en/innodb-locking-reads.html):
NOWAIT reports row conflict as 3572/HY000; SKIP removes conflicted rows and can
return an inconsistent view, so it is unsuitable for general transactional reads.
Only row locks are covered. Binding, RPC, metadata and service work can still take
time; this is not a universal latency deadline. MySQL's
[error contract](https://dev.mysql.com/doc/refman/8.0/en/innodb-error-handling.html)
is the precedent for distinguishing statement errors from transaction failure.
MatrixOne retains its existing snapshot/definition retry and fatal-error rules.

## Invariants and first owners

| Boundary | Required invariant and owner |
|---|---|
| Syntax and plan | Grammar admits only the three forms; the owning query block carries per-target policy. |
| Execution placement | Compile gathers nonblocking LockOps at the transaction coordinator before execution; sender and receiver reject remote placement. |
| Physical admission | Lockservice attempts exact-row FastFail only after capability, method, options and binding checks; no silent wait/coarsening/cache bypass. |
| Conflict identity | Only a settled direct physical-owner conflict is recoverable. Transport, bind, cleanup, cancellation and unsupported-capability errors stay distinct. |
| Grants and cleanup | Existing active transaction/table bookkeeping owns all actual grants, including a partial prefix and an indeterminate result. |
| Returned rows | LockOp filters only settled Shared conflicts; every returned row retains its grants until transaction completion. |
| Retry and lifetime | Pending refresh/definition retry or a fatal later target wins before output publication; LockOp owns copied output until the next Call, Reset or Free. |

NoWait becomes MySQL 3572/HY000 without itself rolling back the explicit
transaction. Earlier writes and valid grants survive; ordinary fatal errors keep
the existing rollback policy. This is not a promise to recover transport failures.

## Physical protocol and state transitions

MORPC v123 introduces `TryLock` method 25, `ForwardTryLock` method 26,
`LockOptions.NoWait` field 15 and `LockResponse.NoWaitConflict` field 4.
Coordinator, forwarder and owner enforce admission before any grant. An owner
below v123 fails closed; the ordinary Lock method is never a compatibility retry.
Main's v112 WEEK/DAYOFYEAR, v113 DECIMAL, v114 Unicode UNIQUE, v115 calendar
interval, v116 SELECT division warnings, v117 collation ordering, v118 database/table defaults and v119 parameterized subquery and v120 KILL/session-control and v121 unsigned DECIMAL DIV and v122 temporal result contracts remain unchanged.

Plan NoWait field 18 and pipeline no_wait field 13 are additive; Part B adds
SkipLocked fields 19/14. Existing numbers, unknown fields and result ABI remain
intact. There is no on-disk/catalog format or new native ABI. Generated consumers
travel atomically with their schemas and use the existing canonical generators.

| Outcome | Publication and cleanup |
|---|---|
| Admission rejected | Return the capability/options/placement error; no successful grant is acknowledged. |
| All keys admitted | Return the result bound to the current physical table; transaction retains grants. |
| Settled direct conflict | NOWAIT reports 3572; Shared SKIP excludes the candidate. Earlier grants remain transaction-owned. |
| Binding/capability fence or ambiguous reply | Return a fatal error, never a skippable conflict; preserve a trustworthy cleanup-only binding for any possible partial grant. |
| Snapshot/definition retry | Return the existing retry identity before an accepted prefix reaches a LIMIT consumer. |
| Cancel/error/transaction end | Existing cancellation/unlock paths retire the operation and its table witnesses; late callbacks must not recreate completed transaction state. |

Register known table cleanup routes before admission. If the route was not yet
known, use the physical result binding, including on conflict and fatal fences.
Do not clear that cleanup witness when clearing successful-grant metadata. This
also closes a first read-only transaction's otherwise-empty client unlock ledger.
A cleanup binding does not acknowledge that every requested key was granted.
Unrelated holders remain untouched. Existing transaction generation/terminal
checks govern callback races, restart and duplicate cleanup; no per-key ledger
or new background recovery mechanism is introduced.

Async RPC callbacks retain only the method and a frozen options-validity value,
not pooled request pointers/slices. The snapshot is taken before request reuse.
Late response capability/binding validation remains independent of that snapshot.

## Coordinator filtering and resource ownership

Part A has no SkipLocked flag, row filter, LIMIT refill or borrowed output state;
it explicitly rejects SHARE SKIP LOCKED until Part B. Both reject UPDATE SKIP.
Part B uses the same v123 exact-row Shared API, with no new physical RPC. A v123
NOWAIT-only physical owner can serve it; an older SQL coordinator can reject the
new syntax. All targets must have consistent Shared/SkipLocked policy.

For each candidate, LockOp makes a one-row copy and acquires all target grants.
Only a direct settled conflict can remove it. A joined binding/transport/cleanup
error remains fatal. Grants acquired at an earlier target remain transaction-owned
even if a later target makes the candidate skippable; no row-level unlock is added.

An all-skipped nonterminal batch is not EOF: pull until accepted rows or actual
EOF. LIMIT/OFFSET stay above this row-removing operator; skipped candidates do not
consume the requested result count. Refresh/definition retry discovered at an
earlier target outranks a later conflict, EOF or successful prefix. A fatal later
error also prevents publication. Existing compiler/frontend retry owns reexecution.

The input batch remains child-owned. LockOp owns its accepted-index list, one
candidate copy and copied output. Downstream borrows output until the next Call;
Reset/Free release the last output when LIMIT stops early. Empty output and
failed candidate copies are released in their lexical owner on every terminal
path. There is no retained history, candidate queue, worker or new retry loop.

## Alternatives and cost decision

| Alternative | Decision |
|---|---|
| Plain FastFail on the ordinary RPC | Reject: unknown fields/default Wait and an untyped error do not establish owner support or a settled negative acknowledgement. |
| Reused process/session wait policy | Reject: statement and parallel/reused operator state can leak policy. Per-target serialized flags keep the request owner explicit. |
| Storage/remote-CN filtering or a new cleanup ledger | Reject: duplicates filtering/retry/ownership across boundaries. Coordinator filtering and existing transaction cleanup have one effective owner. |
| Vectorized acquisition of all candidates | Defer: cheaper RPC batching needs a precise per-row settled-result contract. One-row admission reuses the proven API without adding such a protocol. |
| One PR or separately delivered generated files | Use two functional PRs; generated files remain with source/schema. Both are buildable and the complete design gate is unchanged. |

Ordinary blocking requests allocate no filtering state. NOWAIT adds the admitted
physical request/response checks. Shared SKIP costs O(candidates * targets) lock
attempts, potentially one remote round trip per candidate/target, plus O(rows *
width) copy work. Memory is bounded by one child batch's selection/output and
one temporary row; existing row/transaction lock budgets still fail rather than
coarsen. Coordinator concentration and copy/RPC overhead are explicit opt-in
tradeoffs. No throughput improvement or benchmark result is claimed. Workload
performance must be evaluated before adopting this syntax for a production queue.

## Compatibility, rollout and diagnosis

Upgrade coordinators/forwarders/owners to v123 before enabling application use.
Do not route new syntax through an older SQL coordinator and assume it works.
Unsupported owners fail closed; homogeneous dual-CN tests do not constitute a
mixed-binary upgrade test. Existing capability/old-peer/two-service tests prove
no-grant rejection and cleanup boundaries at their stated source revisions.

Rollback is to stop issuing the new syntax and drain affected transactions before
downgrading; do not transparently rewrite NOWAIT/SKIP into ordinary waiting reads.
There is no persisted-data migration. Prepared plans and active v123 requests
must not be carried into an unsupported coordinator/owner generation.

Existing SQL privilege checks, tenant/table routing and transaction ownership
remain authoritative; the internal RPCs add no public authorization bypass.
Keep typed conflict and fatal errors distinguishable in diagnosis. Use existing
transaction/lock views and RPC/lockservice logs for holders, bindings and terminal
errors; no unbounded-label metric or new monitoring worker is added.

## Acceptance and reproducible validation

| Contract | Smallest independent proof |
|---|---|
| Grammar, query-block propagation and placement | Parser/plan/compile positive and negative tests, including quoted/unknown/mixed modifiers, unsupported mode and sender/receiver rejection. |
| Admission, ambiguity and rollback | Local/remote/forward owner tests, old-peer no-grant controls, actual read-only client rollback and focused RPC races. |
| Filtering, retry and borrowed lifetime | LockOp tests for intermediate empty batches, compatible holders, retry/fatal precedence, LIMIT consumer, Reset/Free and resource accounting. |
| Public frontend/transaction contract | `TestNoWaitAcrossCNs` and Part B `TestShareSkipLockedAcrossCNs` in `pkg/tests/issues/nonblocking_read_test.go`. |
| Generated/source and integration | Canonical idempotent parser/protobuf generation, owning package/vet/lint checks and final-head CI. |

The durable frontend tests reuse the existing authenticated two-CN fixture and
three rows. Fresh sessions reverse holder/reader CNs. Direct and binary-prepared
NOWAIT must return exactly 3572/HY000 while an earlier write survives, then rolls
back. Shared grants are independently probed, compatible reads succeed, and
rollback releases them. Part B additionally proves LIMIT refill, OFFSET after
filtering, all-conflict empty output and unchanged final data. The holder remains
open until these checks finish; a deadline guards hangs, not the error oracle.

After the repository native prerequisites, reproduce without external services:

```sh
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -vet=all -p=1 -timeout=20m \
  -run '^Test(NoWait|ShareSkipLocked)AcrossCNs$' ./pkg/tests/issues
```

Keep command/source/config identities, named results, exit statuses and cleanup
receipts. Reuse earlier evidence only where relevant semantic inputs are unchanged.
A green unrelated BVT or build is not execution evidence for the new SQL forms.
Real mixed-binary execution and workload throughput remain explicitly unclaimed.
No blocking design choice is deferred; any material deviation requires review.

## Generation and delivery boundary

NOWAIT and LOCKED remain ordinary identifiers. Immediately after FOR, the lexer
recognizes UPDATE/SHARE NOWAIT (and Part B SHARE SKIP LOCKED) as contextual
phrases, carrying the phrase in the existing mode token's semantic string.
The existing FOR UPDATE/FOR SHARE grammar actions select the corresponding
lock type. Lookahead compares exact case-insensitive unquoted tokens and restores
scanner state on a nonmatch. Unknown, quoted, repeated and conflicting options
remain syntax errors; FOR UPDATE SKIP LOCKED remains unsupported.
This follows the existing contextual phrase mechanism without adding global
keywords, token numbers or parser states. The unchanged Makefile generates the
output; no generated file is edited by hand.

Both PRs must have fewer than 10,000 added plus deleted lines against main,
including generated files. Upstream rules restrict dependency-branch creation,
so Part B still displays the foundation against main. It depends on Part A; after
Part A lands, update from main before reviewing/merging the remaining increment.
Both stay Ready with do-not-merge/wip under the task's explicit no-merge scope.

## Design approval

Decision: **APPROVE**; independent reviewer `/root/current_series_design_review` (`fork_turns=none`).
Reviewed technical revision: [02fde9006dd80036e6e1befa41113353378ed9cd](https://github.com/Lundomn/matrixone/blob/02fde9006dd80036e6e1befa41113353378ed9cd/docs/design/nonblocking-row-locks-split.md).
Design blob `ac1fe701f454e2c62a4e0397c5f3ea05fd6e81d3`; SHA256 `27da256fbcdef89a76c989151aedd4ba3cb5864975827697cc6c9628054f3e53`.
[Full independent assessment](https://github.com/matrixorigin/matrixone/pull/30183#independent-complete-series-design-review), SHA256 `f5dd94f74f4f5dc82654cabd993623be90c5c1659ac351f98981dc377b9649f8`.
Approval is design-only; implementation conformity, execution evidence and final-head CI remain separately validated.

## Main synchronization amendments (2026-10-16/17)

Main `47b9690d` already uses v114-v117. The nonblocking admission binding moves
to v118; methods, wire fields, ownership and SQL semantics are unchanged. Tests
reject v113-v117 before dispatch and admit v118. Pipeline generation preserves
main's statement-runtime SELECT field. Obsolete fulltext integration fixes and
helper tests are consolidated into main's ordering/snapshot coverage.
The AutoID generation-budget test fix is inherited from main.
The approval above records the original v114 design revision; it does not claim
independent approval of these maintenance edits. Integration and final-head CI
are validated separately. No FOR UPDATE SKIP LOCKED support is added.

The 2026-10-17 parser maintenance replaces appended modifier productions with
contextual mode-token phrases because main's ALTER CHECK grammar makes added
states repack over 15,000 generated lines. Speculation is bounded to one token
for NOWAIT and two for SHARE SKIP LOCKED. It preserves scanner cancellation and
budget accounting, quoted identifiers, comments, statement boundaries and normal
identifier use; parser tests and direct/prepared two-CN tests prove these
boundaries. This changes syntax recognition only, with no executor/RPC change.


## Current-main compatibility repair (2026-10-18)

Main 68e642fd owns MORPC v118 for database defaults and table charset alteration;
nonblocking admission now requires v119 and rejects v118 and earlier owners.
Wire method/field numbers and cleanup ownership remain unchanged.

Nonblocking targets must retain exact row identity even above the planner's
cardinality threshold; ordinary waiting Shared targets may still use main's
table-lock fallback. Compiler modes that disable physical statement locks must
reject nonblocking plans rather than silently bypass admission. Existing planner,
compiler and authenticated two-CN fixtures cover these integration boundaries.
The contextual lexer maintenance approved on 2026-10-17 remains authoritative;
its canonical generation avoids parser-state/table churn on current main.
Main already supplies the external-file policy fixture repair; no policy bypass
or unrelated fixture patch is carried in this series.

## Current main integration (2026-10-19)

Main `090b14fbb4c288819728f628e009311a11812e1f` retains KILL at v120,
unsigned DECIMAL DIV at v121, and temporal result contracts at v122.
Nonblocking admission uses v123: reject v122 and earlier before dispatch and
before acknowledging a grant, including forwarding. Method numbers, wire
fields, transaction cleanup and exact-row target validation are unchanged.

The dependent-query compiler constructs LockOp directly, so the shared
Compile-boundary validator enforces pessimistic transactions, enabled physical
admission and exact-row targets before either compiler constructs sources.
Retain main's temporal Compile/Run/transport/persisted-plan gates and CTAS
execution requirement, including CreateTable field 19. Rebuild plan bindings
from the merged schema with the official generator and postprocessors.

Independent reviewer `/root/contextual_lock_phrase_review` approved this narrow
integration before implementation. Validate v122 rejection, v123 admission,
late v123-to-v122 downgrade cleanup, temporal protocol controls, compiler and
planner integration, and existing two-CN/SQL BVT cases. Historical design and
CI evidence remains in the external task record at its original revisions.
Recheck the official allocation before delivery. Both PRs remain Ready; Part B
contains the shared foundation. FOR UPDATE SKIP LOCKED remains unsupported.
