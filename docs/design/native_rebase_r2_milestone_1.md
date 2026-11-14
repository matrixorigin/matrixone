# Native Rebase R2: Transactional Publication

Status: restricted integration candidate. Production publication remains off.
R1 is independently reviewable at `5ebbf0f9de`; it does not require the adapter
publication protocol. Qualification artifacts identify the R1, kernel,
inventory, adapter and upstream revisions separately. Integrating later main
does not relabel an earlier build's evidence as current-head acceptance.

## Scope And Admission

Support same-tenant, same-schema ordinary explicit-primary-key tables. An
unchanged no-primary-key table is retained only after full visible multiset
equality, including duplicate multiplicity and NULLs. Reject unsupported schema,
constraints, changed no-PK data and custom conflict callbacks before candidate
or branch-pointer writes. Endpoint/permission checks remain the R1 checks.

Publication borrows an explicit Pessimistic-RC owner. Do not change ordinary
DDL/DML transaction modes. Retired optimistic modes are regression controls,
not a restored product contract. Upstream is a fixed snapshot: later upstream
writes remain in upstream and are not included in this result.

The parser accepts `FOR UPDATE EXPECT (database_id, epoch)` only with positive
uint64 literals. Bare `FOR UPDATE` is rejected. The handler refuses EXPECT
before opening an executor unless an exclusive integration fixture installs
the test-only switch. No SQL variable, environment setting or normal backend
registration enables production publication.

## Directory Conflict Point

`mo_catalog.mo_database_inventory` has a primary key on
`(account_id, database_id)` and a nonzero uint64 epoch. A replacement database
has a new ID. Name equality never makes an old token valid.

- New user databases initialize their row in the existing CREATE transaction.
- Directory producers increment it in their own existing transaction. The
  internal executor borrows that owner and does not commit or advance its
  outer statement. Each result is closed before returning.
- Publication checks live name/ID mapping and conditionally increments the
  fixed epoch, requiring exactly one affected row. The real write-set conflict
  lasts until commit or rollback. A rescan is not a replacement for this write.
- Missing, zero, duplicate or exhausted state fails closed. Ordinary users
  cannot directly update the internal table.
- Retirement reserves the row before deleting relations/routines/settings,
  then removes the row in the same transaction. Workspace coalescing must not
  erase the conflict dependency.
- System catalogs are explicitly excluded to avoid recursive writes. Routine
  and compatibility writers instead reserve their actual user database.

The [producer map](native_rebase_inventory_producers.md) identifies the common
mutation owners. No inventory SQL executes under the workspace mutex. Different
databases/tenants use different rows; this is not a global lock.

## Data Dependency Is Separate

After endpoint, schema, ancestry and quota checks, resolve every table in fixed
feature F against the live source in the borrowed owner. Verify account, DB ID,
table IDs and dependency capability before candidate creation. This includes
omitted adapter-ledger/no-PK tables. Reserve the epoch and register the fixed F
timestamp in stable name order; any failure rolls back through the same owner.

CN retains typed dependency entries across source DROP/workspace merging and
sends them before that DROP. Live source DML in the publication owner is
rejected. TN validates pending and committed logical writes after F before
preparing publication. Later competing writers cannot commit into a successfully
retired source. Physical flush/merge must not count as application writes.

[Logical-DML history](native_rebase_logical_dml_history.md) specifies the
default-off runtime, opt-in WAL V6/checkpoint 14 encoding, legacy conservative
floors, replay, backup rewriting and GC-checkpoint composition. Existing
allocator/flush watermarks and default format versions remain unchanged.

## Upgrade And Restore

Released v4.2.5/v4.2.7 catalogs end at 4.0.6. Inventory is part of the existing
pending 4.0.7 target, not an additional fictional released-version hop. Create
and idempotently backfill it before pending migration steps that invoke normal
user-relation DDL. Never overwrite an existing epoch.

Reuse the tenant upgrade owner: each acknowledged page may commit, but readiness
is published only after all work completes. An unknown commit or cancellation
stops progress; restart begins the idempotent traversal again. A supplied
transaction remains caller-owned and must not publish readiness in a cache.
This follows full-cluster shutdown upgrade; no mixed-CN negotiation is added.

Physical recovery retains rows. Logical Clone/Restore uses the target lifecycle:
a new DB ID gets a new row; same-ID table restore advances its current epoch.
Never copy a historical inventory over live identities. Old data and backups
must enter through normal migration before related writes are admitted.

## Adapter Publication And Recovery

The benchmark-owned `_bb_meta` stores immutable attempts and at most two
snapshot receipts. It is not a general branch service.

1. Resolve the original four endpoints and read source ID/epoch/generation.
2. Commit PREPARING with attempt-bound resource names before snapshot requests.
3. Commit REQUESTED before each independent CREATE SNAPSHOT; persist CONFIRMED
   UUID/TS/DB identity only after its acknowledged response.
4. Under branch-row then attempt-row locks, claim PREPARING as PUBLISHING.
   Only the acknowledged claimant may send publication, once.
5. In one publication transaction, revalidate registration/generation/endpoints,
   reserve the source, replay, retain the actual fixed U base, append history,
   CAS the pointer, retire the source and write COMMITTED.

Unknown claim, snapshot or COMMIT outcome never causes automatic replay or
resource deletion. Recovery uses a fresh connection with the same lock order,
confirms the original attempt and returns UNKNOWN when terminal proof is absent.
A durable success survives subsequent pointer advances. Only acknowledged
publication rollback plus confirmed snapshot requests permits ABORTED.

Source DROP is logical retirement. History snapshots and unknown-attempt
resources remain retained. Neither a disconnect nor absence from a catalog
proves reference-safe reclamation. No production sweeper is introduced.

## Executable Acceptance Boundaries

| Contract | Maintained tests |
| --- | --- |
| Real epoch competition, rollback, retirement, same-name/new-ID | `TestRebaseInventoryPrimitive`, `TestDatabaseInventoryCreateReservation` |
| CREATE after F cannot disappear on successful publication | `TestNativeRebaseInventoryPublication`, `TestNativeRebaseSnapshotDependencyPublication` |
| Directory producers and restore identity | `TestDatabaseInventoryRelationProducers`, routine/compatibility and Clone/Restore tests |
| WAL/checkpoint and actual process loss | `TestRebaseInventoryWALRecovery`, `TestRebaseInventoryCheckpointRecovery`, source-dependency recovery tests |
| Official old binary, old-volume restore and interrupted migration | `TestRebaseReleasedBinaryUpgrade`, `TestRebaseReleasedUpgradeCrashRecovery` |
| Cross-CN same-TN, unrelated DB/tenant isolation | `TestNativeRebaseCrossCNSourceProtection` |
| Real killed adapter, unknown snapshot/COMMIT and partial failure | `TestNativeRebaseAdapterPublication` with the external adapter fixture |
| Consecutive Rebase, history, Merge and public opt-in API | `TestNativeRebaseToolSuitePublication`, `TestNativeRebasePublicAPI` |

Run identity, exact source/binary/native inputs, raw results and cleanup receipts
belong in external qualification artifacts, not this maintained design. A model
or fixture-only PASS does not qualify the remaining boundaries. A test-only
candidate passing these cases does not enable production or certify all
distributed schedules.

After correctness, compare old/native on one qualified build and frozen adapter,
first small input then W1: one worker, 16 spine branches, seed 42, one warmup and
10 measured operations. Separate public adapter latency, native SQL client time,
SQL count, preparation and cleanup. A single pair cannot establish statistical
significance; unsupported or failed operations are not timing samples.
