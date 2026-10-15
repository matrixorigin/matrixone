# Nonblocking row locks: NOWAIT foundation and SHARE SKIP LOCKED follow-up

Owner: issue #25107. Part A is PR #30183; Part B depends on Part A.
This document records the delivery boundary for the existing nonblocking-lock
implementation. Splitting the series does not change its end-to-end contract.
`FOR UPDATE SKIP LOCKED` is excluded from both parts.

## Part A: NOWAIT and the common physical admission contract

`FOR UPDATE NOWAIT` and `FOR SHARE NOWAIT` attempt exact row admission without
waiting. A settled conflict becomes MySQL 3572/HY000. The transaction remains
active: earlier uncommitted work and valid grants survive until transaction end.
Transport, binding, cancellation and refresh errors retain their own meanings.

The planner carries `NoWait` to the physical LockOp. Compilation rejects
optimistic transactions before scheduling or constructing executable sources.
All candidate scopes are gathered at the transaction coordinator. Nonblocking
LockOps cannot be serialized to a remote executor; both sender and receiver
reject that placement. Ordinary blocking lock behavior remains unchanged.

The common physical protocol is MORPC v114: `TryLock` method 25,
`ForwardTryLock` method 26, `LockOptions.NoWait` field 15, and the settled
`LockResponse.NoWaitConflict` field 4. The coordinator, forwarder and physical
owner enforce capability admission before any grant. Unsupported v113 owners
fail closed; the old ordinary lock method is never a fallback. Main's v112
signed WEEK/DAYOFYEAR and v113 DECIMAL aggregate contracts remain unchanged.

Plan `NoWait` field 18 and pipeline `no_wait` field 13 are additive. Fields are
not renumbered or reused. Canonical generated consumers travel with schemas.
Part A has no SkipLocked flag, row filter, LIMIT refill or borrowed output state.

Each physical result records the current lock-table binding. The transaction
registers cleanup routes before admission when known and from the returned
binding otherwise, including conflicts, partial grants and fatal fences.
Read-only transactions retain effective cleanup routes through rollback.
Asynchronous RPC callbacks snapshot pooled request state before reuse.

## Part B: SHARE SKIP LOCKED on the same physical API

The coordinator admits the additional `FOR SHARE SKIP LOCKED` syntax and adds
plan field 19 / pipeline field 14. It uses Part A's exact nonblocking Shared
admission; this introduces no new physical RPC or weaker owner capability.
An older v114 NOWAIT owner can therefore serve the physical request. Older SQL
coordinators still reject the unsupported syntax, and remote LockOp placement
remains prohibited. No on-disk, catalog or existing numeric result ABI changes.

The first owner of filtering is LockOp. A candidate is skipped only for the
physical owner's settled direct conflict. Joined cleanup/binding/transport
errors are fatal. Each candidate uses a one-row copy and every successful row
retains Shared grants until transaction end. All targets must use the same
Shared/SkipLocked policy; invalid mixed targets and coarse/table locking fail.

An all-skipped child batch is not EOF: pull the next batch until an accepted row
or actual EOF. LIMIT/OFFSET remain above the row-removing lock filter so skipped
candidates do not consume the requested result count. A pending snapshot or
definition retry wins over a later skippable conflict before any accepted prefix
is published. A fatal later error also prevents prefix publication.

The returned copied batch belongs to LockOp and is borrowed downstream. It is
released on the next Call, Reset or Free, including early LIMIT termination.
Candidate copies and empty outputs are released in their lexical owner. Work is
linear in candidate rows and width; there is no retained candidate queue,
background worker or retry loop. Runtime exact-row budgets are unchanged.

## Alternatives and verification

A single PR preserves one integration boundary but mixes RPC/NOWAIT review with
filter/retry/output-lifetime review. Separating generated files from their
schema/grammar would leave a non-buildable boundary. The selected functional
split keeps each intermediate head buildable and moves every SKIP-specific
oracle with Part B. Generated parser tables and compressed protobuf descriptors
may still produce large mechanical diffs in each PR; do not edit them by hand.

Part A proves parser acceptance and query-block policy, optimistic rejection,
coordinator placement, typed SQL conflicts, protocol rejection before grants,
transaction continuity, partial-grant cleanup and ordinary-lock controls.
It explicitly rejects both UPDATE and SHARE SKIP LOCKED until Part B.
Part B retains filtering, compatible Shared holders, all-skipped continuation,
LIMIT refill, multi-target refresh/definition/fatal precedence and Reset/Free
ownership tests. UPDATE SKIP LOCKED remains rejected. Both parts require
candidate-specific canonical generation, owning-package checks, relevant race
controls, real frontend SQL in both CN directions and actual head CI results.

The recombined physical/protocol/filter implementation retains the original full
candidate; contextual parsing preserves its public syntax and rejection contract.
Source-to-generated coherence and all prior distinct oracles are preserved.
Real mixed-binary cluster execution is a separate evidence dimension, not
implied by protocol/two-service capability tests or a homogeneous SQL cluster.

## Contextual lock spelling and diff budget

NOWAIT and LOCKED stay ordinary identifiers. Only the lock-option productions
consume an unquoted ID and compare its spelling case-insensitively; unknown,
quoted, repeated and conflicting modifiers are rejected. Existing keyword and
literal-token numbers remain stable, avoiding global parser-table renumbering.
Part A explicitly rejects the SHARE SKIP identifier form in its grammar action;
Part B enables only the exact LOCKED spelling with its filter implementation.
The parser is generated by its unchanged Makefile; no output is edited by hand.
Both PRs must remain below 10,000 added plus deleted lines against main, including
all generated output. The dependent PR may still display the foundation until
it lands because upstream rules restrict dependency-branch creation.
