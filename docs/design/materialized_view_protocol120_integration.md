# Main correlation integration and MV protocol 120

Parent 95dd4c0d passed ALL CI 39145960351 (including CI Required), both BVT
jobs, coverage, SCA, regular UT and both connector workflows. While that run
was executing, main 75d16a2b incorporated coordinator-owned correlated programs
and allocated protocol 119. The MV branch must preserve that allocation and
move its own admission to 120. Keeping the old number would falsely identify
a main119 node as MV-capable.

## Compatibility requirement

The MV planner, frontend operation admission, scan/registration compiler,
catalog 4.0.17 prerequisite and global cron admission all require120. Current
typed fixtures explicitly reject 119 at each relevant boundary before catalog
DDL, relation lookup or task publication. They accept 120 and retain ordinary
table/119 dependent-source controls. Main's dependent-program requirement
remains119. Catalog version4.0.17, offset 5, definition format 2 and executor 17
are unchanged.

| Minimum participating protocol | Ordinary dependent programs | MV admission |
| --- | --- | --- |
| 118 | Rejected by main's existing policy | Rejected |
| 119 | Supported | Rejected |
| 120 | Supported | Subject to normal owner, flag, catalog and population checks |

Earlier 119 MV candidate results describe the pre-integration development
branch; they do not identify upstream 119 as MV-capable. Historical upgrade
atomicity results remain scoped to their binaries. This integration changes
the required capability number, not migration DDL or persisted data layout.

## New execution consumers

Main's dependent source factory opens table readers without entering ordinary
compileTableScan. It now calls the same MV scan validator before freezing a
native source factory. That preserves protocol, population and source identity
checks in the active transaction. No reader is opened on rejection.

Dependent INSERT, DELETE and MULTI_UPDATE also construct operators directly.
The existing validateDMLTarget owner now invokes the transaction-bound MV
capability check for both ordinary scopes and dependent programs, after the
existing CCPR policy. Three later ordinary-only checks were removed. This also
places MV admission before the ordinary DELETE-to-TRUNCATE shortcut and before
child operator construction. Authorized refresh capabilities retain their
original account/table/transaction binding.

These changes are constant-time kind checks at existing compilation boundaries;
ordinary tables do not acquire a new catalog query. MV source validation uses
the existing borrowed SQL transaction and DisableIncrStatement owner. No new
state, lock, worker, cache or cleanup protocol is added. The dependent compiler
freezes sources before opening readers, so rejection has no reader to release.

## Validation and review

The existing capability fixture now reaches dependent-query write admission
before an engine/analysis/source can be touched. Existing protocol cases add
old/current dependent scans and an ordinary 119 control. Removing only the
dependent read check or only the shared MV write dispatch produces separate
failing counterexamples while the independent CCPR controls remain valid.

The native consumer fixture grows from 11 to 14 cases, reusing its two rows,
connections and cleanup. Deep correlated reads reject unpopulated MVs,
including LIMIT0 and WHERE FALSE; populated reads return the two stored rows
and the expected empty-result controls. Correlated INSERT/UPDATE/DELETE reject
without changing rows or complete publication metadata, even with creation
disabled. The report retains EXPLAIN, and an independent verbose EXPLAIN
confirms the dependent-program route. Both configured transaction modes run
the full 116-case entrypoint and independent cleanup/process checks. The
existing refresh-only fixture rechecks commit loss, both kill points, same-run
replay and cleanup on the integrated binary.

Full migration/MV/planner/compiler/frontend package tests and incremental
seven-package vet/lint cover the merge. The metadata service's synchronization
and lifecycle implementation is byte-identical to 95dd4c0d; its prior final
owning/focused race results remain valid for that owner. New compiler admission
adds no Go shared state or asynchronous execution. Existing borrowed-statement
ownership tests, typed failure ordering and native concurrent/crash cases cover
the added consumers. No new performance or repeated full-upgrade claim is made.

See the adjacent results file for exact source and binary identity, terminal
case counts and historical evidence limits. Final-head CI is reported in the
PR separately from the successful parent run.
