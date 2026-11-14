# Native Database Rebase

Native Rebase is net-state replay, not Git commit-by-commit rewriting. Its two
delivery layers have different owners and acceptance requirements:

| Layer | Result | Contract |
| --- | --- | --- |
| R1 | A new database with replayed data and the retained fixed upstream base | [Typed replay executor](native_rebase_r1_executor.md) |
| R2 | A benchmark branch publication with generation, history and durable recovery | [Transactional publication](native_rebase_r2_milestone_1.md) |

R1 can run independently. Production R2 remains disabled; its EXPECT variant
is admitted only by an exclusive integration test switch. The normal benchmark
backend does not silently select the candidate or fall back after an error.
Neither layer introduces Reset/Revert or a general branch-management platform.

## Four Immutable Endpoints

- Bf: feature branch's old base.
- F: feature head whose changes are replayed.
- Bu: upstream's corresponding old base.
- U: fixed upstream head onto which those changes are replayed.

Each endpoint resolves to tenant/database/table identity and timestamp plus a
protected reference. Bf and Bu must have equal typed visible state and valid
ancestry; unrelated names or a merely shared LCA are insufficient. The result
clones U and retains its actual pre-replay base. Later upstream writes remain
in upstream and are not silently mixed into this operation.

For a primary key, absence differs from a present NULL tuple. Unchanged feature
retains U; feature-only change takes F; equal concurrent changes coalesce;
otherwise the explicit policy fails or retains upstream/feature. A PK update is
a delete plus insert. Logical changed-key counts are not physical DML counts.

## Supported Boundary

| Category | Restricted candidate behavior |
| --- | --- |
| Tenant and schema | Same tenant, same logical schema; identities may differ |
| Replay table | Ordinary stored explicit PK, including composite PK |
| Values | Supported stored scalar values, NULLs and literal defaults |
| Floating-point PK | Rejected; floating-point non-key identity has separate tests |
| No-PK table | Retain only if full visible multisets are unchanged, including duplicates/NULLs |
| Changed no-PK, schema changes, callbacks | Reject before candidate/business/pointer writes |
| Replay indexes/constraints | Non-PK indexes, FK/check and generated/on-update combinations are rejected |
| Other object kinds | Routines, sequences, views, external/temporary/partitioned/subscription objects are outside replay scope |

Explicit table selection changes replay scope, not whole-database authorization,
inventory or retained-base validation. Omitted tables clone U only under the
validated omitted-table contract. Hidden row IDs are not a substitute for
no-PK event identity. No per-row Python DELETE loop is added.

## Atomicity And Concurrency

The existing lifecycle/snapshot admission protects R1's fixed endpoints. The
coordinator either owns its private transaction or borrows an admitted explicit
Pessimistic-RC owner. It never chains independently committing public handlers.
A borrowed result is provisional until its caller commits; statement failure
does not by itself prove the surrounding transaction rolled back.

R2 adds an ordinary durable per-database inventory conflict row and separate
table logical-write dependencies. Directory writers use their actual existing
transaction; they are not globally converted to another mode. Reservation,
candidate, base/history, pointer CAS, source retirement and COMMITTED result share
the publisher's transaction. See the [producer map](native_rebase_inventory_producers.md)
and [logical-write persistence](native_rebase_logical_dml_history.md).

Unknown snapshot or COMMIT outcomes retain their attempt and resources. Recovery
confirms the original durable result and never automatically repeats publication.
Physical old-generation reclamation requires independent reference safety;
logical source DROP and a disconnected client are not sufficient proof.

## Validation And Cost

Maintained tests exercise real SQL, late-table rollback, cancellation, commit
failure, protected references, actual process loss, repeated Rebase followed by
Merge, cross-CN same-TN conflicts and unrelated database/tenant isolation.
Catalog tests separately cover producer owners, logical restore identity and
normal shutdown upgrade from released data. Qualification artifacts bind exact
source/native/toolchain inputs and terminal cleanup; model or SQL rc alone is
not acceptance.

Relational lowering reuses engine equality, aggregation, join, spill and DML
ownership. It may scan full endpoints, even for a small delta. No O(delta)
complexity or speedup follows from one native statement. Compare the actual old
and native public adapter methods on one qualified build, reporting verb latency,
native-statement client time, SQL requests, preparation and cleanup separately.
One small/W1 pair is descriptive, not statistical significance or SF100 evidence.
