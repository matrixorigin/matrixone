# Case preserving catalog name resolution

Revision 9. Owner: issue #29418; implementation: PR #29422.
This revision follows the rebase onto main `37ba071297` and replaces the
previous implementation-specific design narrative. Deployment uses a stopped
cluster upgrade: CNs and Proxy run the same version. Mixed-version operation
is outside the contract, as confirmed by the operator.

## Contract

`lower_case_table_names=0` preserves and compares physical spelling exactly;
mode 1 lowercases names; mode 2 preserves spelling and compares
`identifier.Fold(name)`. The fold is shared with parser `CStr.Compare()`:
Unicode lowercase for valid UTF-8, ASCII-only lowercase for malformed bytes.
The dynamic variable's existing scope and persistence are unchanged.

At a tenant, database and transaction snapshot, resolution returns zero, one,
or multiple visible physical objects. Zero preserves the typed absent error;
one returns physical name and immutable ID together; multiple returns ambiguity
for **every** spelling, including an exact spelling. Transaction-local inserts
and deletes override committed versions before uniqueness is decided.
`IF EXISTS` / `IF NOT EXISTS` suppress absence / duplicates respectively,
never ambiguity, cancellation, or storage errors. Physical generation IDs and
logical privilege IDs have different owners and must not be interchanged.

Database/table SQL, temporary tables, publications, SHOW, privilege checks,
Snapshot/PITR and recovery consume the same identity. Column, alias, index,
publication and routine comparison semantics are not changed. Public consumers
must not reconstruct an exact catalog predicate from the original alias after
resolution. Reserved system names are canonicalized before tenant routing.

## Ownership and implementation

| Responsibility | Owner and rule |
|---|---|
| Effective mode | The session system-variable snapshot; statement admission attaches the mode to context. No session mode means exact internal lookup. Do not maintain a second cached mode and initialization flag. |
| Committed visibility | disttae catalog cache, its existing version order, logtail watermark and GC. No separate visibility state machine. |
| Transaction visibility | Existing database/table operation chains. A lazily built folded membership index follows the same create/delete/rollback mutations. |
| Temporary aliases | Existing session alias map, mutation journal and mutex. One folded membership map; derive zero/one/ambiguity from that group instead of caching a second summary map. |
| Database locking | `openDDLDatabaseWithLock`: resolve physical name, acquire existing DB lock, refresh RC visibility, and verify the generation. Share it across DDL consumers. |
| Snapshot freshness | Reuse main's `advanceLifecycleAdmissionSnapshot` and engine TN-ordered logtail barrier. A local CN clock does not establish the ordering of a preceding owner's commit. |
| DROP / COPY / TRUNCATE lifecycle | Main's complete-domain or broad admission owns ordering and identity revalidation. Do not repeat its DB lock, snapshot refresh and generation check in a mode-2 branch. |
| Uniqueness | Existing transaction lock service with folded namespace keys; no separate lock manager or cleanup protocol. |
| Session migration | Existing migration RPC and lifecycle; carry effective mode before USE, temporary replay and PREPARE. A global-only setting is not an ordinary `SET SESSION` variable, so retain the small dedicated payload rather than changing variable replay semantics. |

### Catalog index decision

Keep the exact name trees and sparse folded trees. Canonical physical names
live only in the exact tree; other spellings additionally enter the folded
tree. Mode 2 merges the canonical candidate with noncanonical variants in
physical-name order. Within each name, the first snapshot-visible version is
authoritative; a tombstone hides older live versions. Existing insert/GC owners
maintain both indexes before their corresponding visibility boundaries.

A prototype with one tree ordered by `(tenant, database ID, fold, physical
name, version)` passed the catalog UTs, but failed the default-path cost gate.
Three serial, alternating benchmark runs on the same rebased source/toolchain,
GOMAXPROCS=2, 10,000 names, produced these medians:

| Operation | Sparse trees | Unified tree prototype |
|---|---:|---:|
| Exact mixed-case table lookup | 159.6 ns, 1 allocation | 262.8 ns, 2 allocations |
| Unique mode-2 table lookup | 302.9 ns, 0 allocations | 180.8 ns, 0 allocations |
| Lowercase table version update | 233.8 ns | 263.4 ns |
| Mixed-case table version update | 441.9 ns | 318.3 ns |
| Retained bytes per mixed-case name | 425.5 | 407.0 |

The extra fold on every exact query and comparator work outweigh the code and
heap reduction for default workloads. These are microbenchmarks on a shared
machine, not SQL throughput claims. Keep a zero-new-allocation exact path and
one tree update for canonical names. The final serial comparison against main `37ba071297` measured 226.5 ns/op
for main and 229.2 ns/op here (three-run medians, +1.2%); both allocated
352 B/op once. This meets the preselected 20% median-increase budget.
Do not introduce a custom Unicode comparator to rescue the prototype.

### Historical resolution

Cache completeness must hold both before and after visiting candidates because
GC publishes its new start watermark before removing versions. If completeness
is lost, discard the partial result and scan storage at the fixed transaction
snapshot. One streaming catalog query is scoped by tenant and database ID;
filter with the common fold in Go. Database and table scans share their
stream-consumption and snapshot-validation code. No whole-result buffering,
SQL collation approximation, repeated pagination scan, or extra persistent key.
The existing stream owner cancels, drains/closes result batches, and joins the
producer on all terminal paths. Early uniqueness failure must still finish
cleanup. Only the historical fallback scans the scoped catalog.

### DDL ordering and identity

Mode-2 creators serialize the absence-to-create transition on
`serial(account, reservedTag, foldedDatabase)` or
`serial(account, reservedTag, databaseID, foldedTable)` before rechecking.
Physical catalog row locks still use the resolved spelling, shared with
publication and ordinary DDL. A positive CREATE existence check needs no new
namespace wait. Internal callers that disable locking must still receive the
physical database name.

Known hidden-index names take the existing prefix gate in shared mode before
sorted per-name exclusive locks. COPY ALTER and TRUNCATE take the prefix gate
exclusively before table locks because nested CREATE generates fresh names.
The tuple arity separates the prefix gate from physical and namespace keys.
Retain this gate until a common lifecycle owner proves it redundant for every
creator, including user names with the internal prefix and SI/internal paths.
Ordinary unrelated names must not serialize on it.

Physical database generation must survive locking; table generation must match
the plan before mutation. Public RC destructive paths reuse main's admission;
SI keeps its fixed snapshot. Propagate lookup errors; only a demonstrated
generation change becomes a definition-change retry. A dropped/recreated name
must not inherit an old publication, privilege, temporary alias, or PITR target.
Publication database and table locks precede publication-row writes and are
ordered by tenant and physical name across multiple databases.

### Migration and recovery

The effective mode is captured from the source session, not the current global
value. Install it into a private target session-variable snapshot before any
name-dependent replay. Missing or invalid payload fails before replay; target
global defaults remain unchanged. Reuse ordinary migration cancellation and
target cleanup. No additional mixed-version negotiation or compatibility path.

Historical Snapshot/PITR resolution uses historical names and physical IDs,
not today's same-named object. Legacy folded PITR names may match only the same
folded identity and tenant; physical generation still decides lineage.
Subscription membership is checked after resolving the publisher's physical
table and cannot be shadowed by a session temporary table. SHOW and direct
catalog consumers use physical database/table names returned by resolution.

Account teardown consumes the physical database names returned by its catalog
scan as ASTs through the existing background executor. It must not stringify and
reparse them using the operator's naming mode, or silently skip mixed-case names
under `IF EXISTS`. The synchronous AST API borrows its input and uses the same
transaction checks, compiler and execution cleanup as ordinary background SQL.
Account deletion selects exact physical identities even when a SYS session uses
mode 2 and the target tenant contains legacy folded-name collisions.

## Validation and delivery

Implementation, tests and documentation are reviewed separately. A new helper
must replace repeated responsibility, not add a parallel execution path.
Remove replaced branches, summaries and assertions about index internals in
the same change. Preserve distinct behavioral oracles and reuse fixture owners.
No review report is committed with production changes.

- Cache UTs: exact controls; zero/one/ambiguity; Unicode and malformed bytes;
  physical ordering and visitor stop; tombstones, reincarnation and GC boundaries.
- Workspace / temporary UTs: create/drop/rollback/replacement and concurrent
  lookup/mutation under `-race`; migration with different defaults and bad payload.
- DDL UTs: physical lock key, generation change, lookup/cancellation/barrier error,
  disabled locks, SI, and reuse of already admitted RC paths.
- Public SQL BVT: modes 0/1 controls and all three mode-2 suites, including
  publications, temporary names, SHOW, recovery and transaction rollback.
  Compare real results and rerun on the same service to verify cleanup.
- Incremental gofmt/vet/lint cover all changed packages and affected consumers.
  Use bounded local builds/tests; no simultaneous performance contenders.
- Final review requires terminal UT/race/SCA/BVT evidence. A successful build or
  a prior revision's report is not a pass for a changed semantic boundary.
