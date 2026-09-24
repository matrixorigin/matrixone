# ODKU FULLTEXT value-change maintenance contract

- Status: Review required
- Tracking issue: [#27933](https://github.com/matrixorigin/matrixone/issues/27933)
- Implementation PR: [#28066](https://github.com/matrixorigin/matrixone/pull/28066)
- Design revision: 3
- Contract revision: R3
- Last updated: 2026-09-06

## Problem and intent

`INSERT ... ON DUPLICATE KEY UPDATE` (ODKU) currently treats every affected
modern irregular index as a delete-and-rebuild operation. For an existing row,
that work is unnecessary when the final row has exactly the same input to the
hidden index as the old row. This change adds an optional plan-layer proof hook
so the ODKU planner can retain the existing conservative path for unsupported
indexes and filter only rows whose hidden state is unchanged.

The optimization is a planner decision only. It does not change the hidden
table schema, fulltext tokenizer, posting format, transaction protocol, or
CDC/cron ownership of asynchronously maintained indexes.

## Scope and non-goals

In scope are existing modern inline-maintained irregular indexes, the ODKU
value-change marker, and the fulltext plan hook. The hook is optional and is
resolved through the index-plugin contract; the SQL planner has no algorithm
switch for this proof.

Out of scope are ordinary INSERT, UPDATE, REPLACE, INSERT ALL, asynchronous
fulltext/fulltext2 maintenance, primary-key mutation support, changes to
fulltext tokenization, and any persisted or wire-format migration.

## R3 contract and invariants

### Hidden-index identity invariant

For classic FULLTEXT, the generated hidden input is not just the indexed text.
`buildPreInsertFullTextIndex` constructs each hidden row from the base table
primary-key value used as `doc_id`, followed by every FULLTEXT part. The
primary-key value is therefore part of hidden-index identity even when the
tokenizer reads only the text parts.

The optional `DMLMaintenanceNoOpHook` returns a complete, conservative set of
stored base-table columns. SQL NULL-safe equality (`<=>`) for every returned
column must imply byte-for-byte-equivalent hidden-index input between the old
row image and the final ODKU row image. A hook must include row identity/doc
identity whenever that value is an input. It must return `supported=false` if
type comparison, external content, generated state, or any other dependency
can invalidate that implication.

The FULLTEXT implementation therefore requires a resolvable
`TableDef.Pkey.PkeyColName`, returns that primary-key column first, and then
returns each distinct resolvable FULLTEXT part. It supports only `VARCHAR` and
`TEXT` parts. CHAR, JSON, DATALINK, missing columns, missing primary-key
metadata, and empty definitions fail closed to the rebuild path.

### ODKU row and primary-key preconditions

ODKU rejects an assignment that can change any primary-key column before
value-change filters are selected. Under that enforced precondition, the old
and final row identity are equal for a conflicting row. The identity remains
in the generic proof contract nonetheless, so a future caller cannot reuse the
hook while silently omitting a hidden input.

For each conflicting row:

1. If every proof column is NULL-safely equal, the old hidden entries remain
   and no delete/rebuild is emitted for that logical index.
2. If any proof column differs, the existing delete-and-rebuild path runs.
3. A new row has no old hidden entries, so it remains on the insert-only path.

All physical definitions belonging to one logical index are grouped together.
The value-change proof is used only when every physical definition in that
group supports it. A missing plugin, missing hook, or `supported=false` result
keeps the complete logical group conservative. A hook error is returned as a
planning error and cannot silently select the optimization.

### State, ownership, and failure semantics

The final base-row image is the sole source for both the marker and subsequent
maintenance branches. The planner materializes one boolean value-change marker
per affected logical group and reuses the final-row source; it does not retain
old or final token streams in planner state.

The existing DML transaction owns deletion and insertion of hidden rows. The
optimization only changes which rows enter those existing branches. If marker
construction, plugin resolution, or source wiring fails, planning fails or
falls back according to the rules above; it never publishes a partial hidden
index update. Transaction commit and rollback semantics remain those of the
existing DML plan.

## Source fan-out and resource bounds

For a statement with `G` affected logical inline-maintained groups, the new
planner state contains at most `G` value-change marker expressions and at most
one value-filtered maintenance source per group, plus the existing shared
new-row source. Physical definitions inside a group are inspected for proof
closure but do not create additional value-change branches. Existing fulltext
tokenizer branches remain bounded by the physical index definitions already
being maintained.

The proof contains column references and NULL-safe comparisons only. It does
not copy token data, retain rows in a process-global cache, or allocate work
proportional to posting-list size. Long TEXT values may cost the normal SQL
column comparison for rows reaching the marker, while equal-value conflicts
avoid the old hidden-table scan and tokenizer work. New and changed rows keep
the existing maintenance cost and semantics.

## Alternatives considered

* Always rebuild: preserves behavior but spends hidden-table and tokenizer
  work for equal-value conflicts.
* Compare token streams or hidden postings: more exact at runtime, but couples
  planning to tokenizer/posting internals, adds state ownership, and does not
  provide a bounded generic plugin contract.
* Keep algorithm-specific switches in the SQL planner: duplicates plugin
  knowledge and would require a planner edit for every algorithm.
* Register test-only algorithms in the process-global registry: matches the
  production lookup but has no unregister operation and makes repeated or
  parallel tests order-dependent. Tests inject a scoped resolver instead.

## Rollout and rollback

The hook is additive and optional. An algorithm that has not opted in, whose
metadata is incomplete, or whose proof is uncertain automatically retains the
existing rebuild behavior. The rollout sequence is therefore code-only:

1. land the generic hook and fulltext implementation with the conservative
   fallback;
2. validate exact-head correctness and equal/changed/mixed/multi-index/long-
   TEXT performance on the PR;
3. observe CI and QA before merge.

Rollback is a source revert or disabling/removing the hook. No catalog, hidden
table, persisted metadata, protocol, or mixed-version migration is required.
The exact-head A/B measurements and changed-heavy regression bound are recorded
in the PR description so the implementation and evidence remain tied to the
same published commit.

## Validation contract

The unit and planner tests cover PK/doc identity in the returned proof,
equal-value marker construction, mixed and independent indexes, grouped
physical definitions, unsupported metadata, hook errors, and repeated test
execution without global registry mutation. The PR evidence must additionally
record exact-head A/B results for equal, changed, mixed, multi-index, and
long-TEXT workloads, including a changed-heavy regression bound and correctness
checks for row counts and FULLTEXT search results.
