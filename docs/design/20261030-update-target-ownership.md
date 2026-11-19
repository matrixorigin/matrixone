# UPDATE target ownership and admission

Refs #29249; implementation: #31798. Reviewed baseline: `c4b32eb33c4e`.

## Problem and ownership

READ ONLY admission collects UPDATE sources twice and repeatedly resolves source
columns for multiple unqualified SET assignments. It also performs this work for
READ WRITE statements, whose write verdict cannot reject the statement. After
modern UPDATE planning succeeds, self-referencing foreign-key detection calls
`getUpdateTableInfo`, resolving and binding the same targets a second time.
The legacy DML table-info structure also retains several write-only fields.

The frontend owns access-mode policy and pre-transaction admission; the modern
planner owns resolved UPDATE targets, assignment order and execution. A full
binder cannot run at the existing pre-Create boundary because it needs catalog
state, and moving all admission after binding changes transaction/error timing.
Keep those owners and the existing two-phase READ ONLY admission.

## Admission

Resolve and stamp the caller's access mode exactly where it is today. If it is
READ WRITE, or the background statement has no user origin, return without
building a write-ownership classification. READ ONLY still rejects DDL before
implicit commit and performs deferred UPDATE rechecking after Create but before
workspace StartStatement.

Use one local statement disposition: allowed, persistent write, or deferred.
Delete the separate `updateOwnershipDeferred` walk and bool-only UPDATE entry;
the existing `ExecCtx` flag remains the only cross-phase signal.

For an UPDATE, collect source identities once. When there is no catalog
transaction, scan all valid assignment shapes before deciding whether a
multi-source unqualified SET requires deferral. Deferral takes precedence even
when an earlier qualified assignment targets a persistent table. Otherwise,
classify qualified targets and unqualified column owners using the same existing
conservative rules. Reuse successful per-source column resolutions within this
one gate invocation; re-resolve failures for each visited assignment and reset
the unresolved-source verdict per assignment. Do not cross the pre/post-Create
boundary, retain session state or mutate the AST. Preserve EXPLAIN wrappers, temporary
bindings, prepared/default/remapped databases, ambiguous/unknown columns,
invalid assignments and origin-user procedure behavior.

## Modern UPDATE binding and legacy cleanup

Create one `DMLContext` in `bindAndOptimizeUpdateQuery` and pass it explicitly
into `bindUpdate`. After `createQuery` and irregular index maintenance, the
existing foreign-key-enabled and `HasForeignKeyAction` checks still decide
whether to generate self-reference detection SQL. At that exact point use the
already-bound writable target identities and column sets from the same
`DMLContext`, in `tableDefs` source-resolution order, skipping empty update
column sets. Do not reorder by SET, map iteration, or `updateTargetOrder`, or
deduplicate repeated aliases. Preserve schema/physical table name, self-FK
selection, generated-column DEFAULT exclusion, errors, and SQL generation.
Do not scan the physical query graph for targets: it also contains cascade and
index writes. Specialized UPDATE routes keep their existing independent owner.

Validate every original generated-column assignment before nonsequential UPDATE
collapses repeated SET targets. Any non-DEFAULT assignment must fail, even if a
later DEFAULT replaces it in the final map; DEFAULT assignments are removed
from the write set. This preserves the old self-FK validation and corrects the
same invalid-SET acceptance in modern UPDATEs without foreign-key actions.

Delete `getUpdateTableInfo` and its now-unused `updateKeys` field. Remove the
legacy `dmlTableInfo` fields and computations that have no readers:
`haveConstraint`, `nameToIdx`, `idToName`, `oldColPosMap`, `newColPosMap`.
The legacy `deleteToSelect` constraint switch is always passed true by its
only caller; remove the unreachable false branch and its unused parameter.
Keep `isMulti`, `needAggFilter`, aliases and all live insert/delete state. Update
existing internal tests to consume modern binding facts instead of the removed
legacy helper. No new persistent state, protocol, execution route or framework.

## Validation and cost

- Assert READ WRITE skips ownership lookup after stamping false; READ ONLY keeps
  pre-Create rejection/deferred ordering and live temporary-table behavior.
- Characterize multiple SET columns, aliases, CTE/derived sources, hidden and
  generated columns, no/ambiguous owners, cancellation and repeated catalog
  errors. A cached successful result must not survive the gate invocation.
- Verify self-FK detection SQL and actual rows for enabled/disabled checks,
  self-reference and non-self FK controls, multiple targets and aliases,
  generated DEFAULT and failed planning; ensure cascade/index targets are not
  included. Check repeated generated-column assignments in both orders, with and
  without self-FKs; invalid assignments fail before execution and DEFAULT stays
  allowed. Exercise modern and specialized routes independently.
- Preserve existing READ ONLY, foreign-key UPDATE and INSERT/DELETE SQL cases;
  compare actual results with cleanup. Repeat only where reuse or cleanup adds a
  distinct risk. Run affected owning-package normal and focused race tests plus
  configured incremental SCA.
- Compare a fixed admission microbenchmark before/after for READ WRITE, READ
  ONLY and single-statement controls; report catalog-call counts separately from
  elapsed timings and do not extrapolate stub results to end-to-end latency.
