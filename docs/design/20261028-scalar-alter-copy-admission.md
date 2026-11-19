# Scalar ALTER COPY admission

Issue: #31401. Follow-up to merged #31435 and
[source refresh](20261027-alter-copy-source-refresh.md).

## Scope and contract

The source refresh correction lets the first ALTER succeed after an in-flight
LOAD commits, but rebuilding under the global SNAPSHOT exclusive gate delays
unrelated lifecycle operations. Ordinary scalar ADD COLUMN can refresh and
publish with shared admission. Physical preparation remains before admission;
source DML is not locked earlier, and there is no transaction restart.

Only permanent ordinary tables under pessimistic RC qualify. Existing primary
keys and auto-increment columns are allowed. Secondary indexes, partitions,
owned/hidden/special tables, generated columns, CHECK, incoming/outgoing/self
FK, protected Snapshot/PITR history and branch components retain broad handling.
The target and actual private relation must also be scalar. New columns cannot
introduce PK, unique/index/FK, allocator, generated or CHECK responsibilities.
Mixed ALTER operations retain broad handling.

This is a scoped availability improvement. A stale speculative generation still
costs a second full copy, and old objects may remain until transaction end/GC.
Shared admission still blocks broad exclusive owners. Complex and protected
tables, including late protection that requires promotion, retain the prior
global-exclusive refresh cost. No numeric latency, IO or capacity improvement
is implied.

## Affirmative planner certificate

The planner inspects the complete original typed option list before constraint
resolution or no-op filtering. At least one ADD COLUMN is required; only
validated algorithm and lock hints may accompany it. Nil, unknown, ignored
storage and all other semantic options disqualify certification.

After successful COPY planning, a pure statement with no other action
responsibilities gets one existing AddColumn action per new column, sorted by
final target position. Name and complete Type describe the bound final column;
Pos is its zero-based index in CopyTableDef.Cols, including hidden columns;
PreName is the preceding column in that same final layout, or empty at zero.
These are consistency markers, not engine ADD instructions. Deep copy isolates
action/payload/Type bytes; existing protobuf serialization preserves them.

The compiler requires a complete nonempty certificate matching the target,
all old column identities/semantics and their relative order. Missing or stale
markers cancel scoped eligibility. Physical coordinates, compression defaults,
origin hints and CREATE-versus-storage nullability representation are normalized;
user column/default/type semantics are retained. A certificate alone grants no
lifecycle authority.

## Ownership and ordering

1. Discover eligibility and protection before private preparation. Capture caller
   workspace history before CREATE; it controls the existing fixed-copy policy.
2. Prepare the private generation using the original lock-free statement tree.
   A lexical owner/actor/database/target receipt validates the actual nested
   typed CREATE before effects, including when DisableLock bypasses name pins.
   It rejects additional names, CTAS/replacement, hidden members and FK repair.
   Ordinary implicit relkind, storage-added rowid and PK helper regeneration
   are the only CREATE layout differences admitted.
3. Enter C S, G S, D S using the existing retained-lock policy. Only newly
   transaction-created preparation locks can be allowlisted. There is no
   transaction-local exemption. Pin the absent branch root K and require an
   empty component. Pin the exact source/private-name domain before source
   metadata/storage X locks and the destructive definition fence.
   Derive the retained-lock policy before adding C/G/D and carry it through K
   and the name pins: compatible G S alone cannot exclude a reverse edge from
   earlier caller DML through another owner's component or metadata locks.
   Fresh/private-only owners wait; owners retaining caller locks FastFail on
   those conflicts, retaining whole-transaction rollback semantics.
4. Advance through the workspace owner, reopen relations and revalidate source
   ID/version/database/logical identity, actual constraints, target ownership,
   actor/SYS history and both FK catalog directions. The private physical ID
   must be transaction-created and different from the public source.
5. Data-only changes retire the stale private generation and prepare once at the
   fresh frontier, preserving caller-local writes and relocated tombstones.
   Each private DROP has its own exact nonzero root receipt; allocator reset
   discard belongs to that generation. No third preparation is permitted.
6. Certified publication explicitly skips the lineage G write/preservation and
   FK repair/transfer, and borrows an exact source DROP receipt with branch
   reclaim disabled. Existing logical identity, privilege/publication, rename
   and final column/merge metadata behavior remains. Commit publishes atomically.

Late branch/history/FK protection promotes G using the existing retained-lock
FastFail policy; new replacement-name domains also use FastFail. Successful
promotion refreshes the frontier and uses broad publication. A conflict ends
the transaction instead of queuing an S-to-X upgrade or repeating COPY. Source
definition/identity changes and unavailable freshness proof remain terminal.
Cancellation and all preparation/publication failures retain the existing
transaction/workspace cleanup owner; synchronous receipts expire with defer.

## Verification boundaries

Typed-plan tests cover certificate production, mixed-operation rejection,
deep-copy isolation and protobuf round trips. Compiler tests cover malformed
certificates, side effects, exact CREATE/replica authority and existing receipt
contracts. Real frontend tests use two-row LOAD and preparation barriers to
verify successful first ALTER, defaults/indexes/allocator values, unrelated
same- and cross-account progress, protected broad controls, late promotion,
rollback/cancellation and failure cleanup. Prepared execution must retain the
certificate without performing an extra ADD. Existing independent-preparation,
retained-gate and relocated-tombstone controls remain applicable.
