# Subscription Table Routing for Table Snapshots and DATA BRANCH Reads

- Status: Proposed — awaiting independent design approval
- Owning issue: [#30949](https://github.com/matrixorigin/matrixone/issues/30949)
- Implementation PR: TBD
- Version: 7
- Last updated: 2026-10-23
- Review history: v1 rejected with three P2 findings (design review of
  SHA256 7cc79677cae28691f2e0ef38b9a89d02e3f7f98aa56ffc091f4fca0218190a34):
  (1) the relation-level routing did not carry the physical account through
  downstream reads (creation-commit-TS probes, historical relation
  reopening, LCA probes); (2) the design conflated publication ownership
  with physical data ownership; (3) the DATA BRANCH privilege probe's
  synthetic SELECT masks subscription visibility errors for ordinary users
  before the proposed routing is ever reached. v2 closed all three.
  v2 rejected with four P2 findings (design review of
  SHA256 25ee393bf1a0e791889139181fe28f203fb2627829c4f1c9bab77915f3e2b2a0):
  (4) physical-owner snapshot deletion was not bound to the target
  snapshot's publication scope (`doDropSnapshot` deletes by name with no
  coverage check); (5) the single-account admission helper
  (`admitLocalLifecycleRC`) cannot serve a two-identity world — passing
  `P_src` locks the wrong publication row, and the coverage lookup must
  query `mo_pubs` under `P_pub`; (6) ownership resolution covered only the
  LCA, not every lineage node reopen (`dataBranchPathTableDefs`,
  `buildSideCollectRange`), so supported mixed operands degrade to
  zero-history despite committed foreign history; (7) the explicit DROP
  statement's account namespace change had no mixed-version safety story.
  v3 closed all four. v3 rejected with three P2 findings (design review of
  SHA256 baacdbd4d3deb5c347a73ff49dd02f1c8bbfdd78f3bb5574c697e3c0d2de090a):
  (8) the v3 drop binding compared the persisted database id against the
  id of the *currently same-named* database instead of the snapshot's own
  recorded `objId`, so a drop-recreate-publish sequence could inherit
  deletability of the old database's snapshots;
  (9) the unconditional `P_src` derivation from the persisted
  `database_id` broke `DATABASE *` publications (stored `dbId = 0`,
  unresolvable), including their pre-existing table/database/account
  snapshot lifecycle;
  (10) the explicit CREATE/DROP protocol never applied the existing
  foreign-source admission rule
  (`publicationSourceOwnerAuthorized`/`publicationTargetsAdmitForeignSource`),
  so an unauthorized persisted `mo_pubs` row could authorize
  physical-owner mutations that `checkSubscriptionValidCommon` rejects.
  v4 closes all three (§3 I1/I7, §4.4, §4.5, §8, §9).
  v4 rejected with two P2 findings (design review of
  SHA256 5674f4e8374324cc8270ddfe32632f9b6fc7481eeef976cbe1ba6e3ddce068ac):
  (11) database-level DROP was bound by recorded identity alone, so a
  publication exposing only `d.t1` could delete an owner-created database
  snapshot covering unpublished `d.t2` — the creation boundary
  (`snapshot.go:4592-4597` rejects database-level targets unless
  `table_list == '*'`) had no deletion-side counterpart; (12) the
  alias-form admission locked only the physical D/T, quota, and `mo_pubs`
  keys and revalidated only `mo_pubs` after the frontier, so a concurrent
  drop of `N` followed by a recreate against a different publication (or
  as a local database) passed every post-frontier check while the
  in-flight snapshot still targeted the old physical database —
  `dropSubscription` (`pkg/sql/compile/pub_sub.go:110-129`) rewrites only
  `mo_subs` and never revokes the old publication's membership.
  v5 closes both (§3 I1/I7, §4.4, §4.5, §5, §8, §9, §10).
  v5 rejected with two P2 findings (design review of
  SHA256 c2111142647621914063bb7446ba0f9170f24092145fc090ead6cb1cb11e4804):
  (13) the table-level historical membership lookup used
  `rel_id == record.objId`, but snapshot creation records
  `rel_logical_id` (`authenticate.go:2165`) and COPY ALTER preserves
  that logical identity while replacing and dropping the original
  physical relation (`alter.go:1190-1191, 1743-1754`;
  `types.go:181-188`) — a snapshot created after COPY ALTER records
  logical ID L while the historical catalog row has physical
  `rel_id` R, so the v5 lookup finds no row and rejects an authorized
  publication-routed DROP; (14) the table-level binding checked only
  the recorded name against the current list plus the historical
  database id, and publication table lists carry names without object
  identities (`publication_subscription.go:2735-2756`) — an owner
  snapshot of private `d.t`, followed by dropping `t`, recreating
  `d.t` with a new logical identity, and publishing only the
  replacement, passes every v5 check for the old snapshot, so the
  subscriber could delete the original object's snapshot. v6 closes
  both (§1 C10, §3 I7, §4.5, §8, §9, §10). v6 rejected with one P2
  finding (design review of
  SHA256 f32cf6d6ef85d4354f5e5f3001c4d74551444abf7f971da5c59e0bea28a869d1):
  (15) the dropped-after-snapshot fallback authorized deletion from
  historical database membership alone. DROP TABLE scrubs dropped names
  only from publications owned by the dropping account
  (`updatePubTableList`, ddl.go:4940-4943 → pub_sub.go:138-178), so the
  supported sys-published foreign-source arrangement
  (`pub_view_scope.sql:534-568`) keeps a stale name list: owner
  snapshots private `d.t`, drops it, recreates `d.t`, sys publishes only
  the replacement, the replacement is dropped — the old snapshot passes
  every v6 check (name listed, current row absent, historical logical
  id in the same database) although the publication never authorized
  the original object. v7 closes it: the name-absent fallback is
  admissible only under whole-database coverage; a table-limited
  publication whose authorized name no longer resolves fails closed,
  retaining owner cleanup (§1 C10, §3 I7, §4.5, §8, §9, §10).

## 1. Problem and evidence

A subscription database `N` in subscriber account `S` is an alias over a
published database `D`. Ordinary reads route through
`getSubscriptionMeta`: the compiler attaches the physical owner account
identity to the context and rebinds the database name to `D` before
resolving physical tables (`pkg/frontend/compiler_context.go`,
`TxnCompilerContext.getRelation`).

Two owner paths bypass that routing, so readable subscription tables falsely
report as missing:

- `CREATE SNAPSHOT ... FOR TABLE N.t`
  (`pkg/frontend/snapshot.go` `doCreateSnapshot`, `SNAPSHOTLEVELTABLE`):
  `getSqlForCheckDatabaseTable` queries `mo_catalog.mo_tables` with
  `reldatabase = N` and `account_id = S`
  (`pkg/frontend/authenticate.go:2151-2168`). The physical rows live under
  `(D, physical owner)`, so the lookup returns nothing and the statement
  fails with `ERROR 20101: table N.t does not exist`.

- `DATA BRANCH DIFF/MERGE/PICK`
  (`pkg/frontend/data_branch.go` `getRelations`, lines 1914-2060):
  `eng.Database(ctx, N, txn)` under `S`'s identity returns the subscription
  marker database, and `.Relation(ctx, t, nil)` looks only in `S`'s local
  catalog, failing with `ERROR 1146: no such table N.t`.

Reproduced on `main@bc84bd142600b745503e225cb89cb1526df8d35e` with a local
publisher/subscriber pair (evidence: `repro-output.log` in the issue
artifact directory):

- `select count(*) from sub3.t1` → 3; `show tables from sub3` → `t1, t2`;
  `information_schema.tables` → `t1, t2`;
- `create snapshot sp_sub_t1 for table sub3 t1` →
  `ERROR 20101 (HY000): internal error: table sub3.t1 does not exist`;
- `data branch diff sub3.t1 against sub3.t1` →
  `ERROR 1146 (42S02): no such table sub3.t1`.

Existing contracts measured on the same build:

- C1 (reads through the alias): `SELECT`, `SHOW TABLES`, `SHOW TABLE
  STATUS`, `SHOW INDEX`, and `information_schema` all resolve `N.t` when the
  subscription is live and `t ∈ T`.
- C2 (explicit publisher-identified snapshot): the grammar form
  `CREATE SNAPSHOT s FOR TABLE D.t FROM P_account PUBLICATION pub`
  (`mysql_sql.y` `snapshot_object_opt`) succeeds from an authorized
  subscriber and records the snapshot row in the resolved owner account with
  `database_name = D`. Verified for an ordinary publication: `sp_exp`
  created by the subscriber was visible to `show snapshots` only from the
  publisher account, and the subscriber's plain `drop snapshot` could not
  remove it. `checkSnapshotPublicationCoverage` enforces `t ∈ T` and the
  publication's persisted database identity.
- C3 (database-level alias snapshot): `CREATE SNAPSHOT s FOR DATABASE N` is
  admitted and recorded in `S`'s own catalog as a relationship placeholder;
  account restore restores the subscription binding but no publisher data
  (`test/distributed/cases/snapshot/restore_sub.sql`; "skip restore
  subscription db" in `snapshot.go`).
- C4 (branch reads from subscription sources already work):
  `DATA BRANCH CREATE TABLE local.clone FROM N.t` succeeds today, because the
  clone path executes regular SQL where C1 routing applies.
- C5 (writes into subscription databases are already explicitly rejected):
  `CREATE TABLE N.x` fails with
  `internal error: cannot create table in subscription database`
  (`pkg/sql/plan/build_ddl.go:2927`).
- C6 (physical source ownership is distinct from publication ownership):
  `checkSubscriptionValidCommon`
  (`pkg/frontend/publication_subscription.go:3041-3089`) resolves the
  publication's persisted database identity
  (`mo_pubs.database_id` via `getDbAccountIdAndTypeById`) and sets
  `SubscriptionMeta.AccountName` to the **publication account** while
  setting `SubscriptionMeta.AccountId` to the **physical source account**.
  A publication can source a database owned by one of its target accounts;
  unauthorized foreign-source rows fail closed with
  `publicationForeignSourceUnauthorizedError`.
  `test/distributed/cases/publication_subscription/pub_view_scope.sql:534-568`
  exercises a sys publication over account `vw02`'s database with a
  same-named publisher decoy and requires reads to answer from the physical
  source, never the decoy.
- C7 (lineage edges already cross accounts): `create table clone` /
  `DATA BRANCH CREATE TABLE ... FROM N.t` records
  `(child table id, parent table id)` in the global `mo_branch_metadata`
  catalog under the sys account (`pkg/frontend/clone.go`
  `updateBranchMetaTable`, lines 1696-1777). Cloning from a subscription
  source therefore already persists edges whose parent id is the physical
  source table id owned by another account.
- C8 (publication-routed DROP is coverage-blind today):
  `doDropSnapshot` (`pkg/frontend/snapshot.go:700-771`) authorizes
  subscriber membership and session generation
  (`lockAccountFromPublication`), attaches the publication account, and
  deletes the snapshot **by name** (`getSqlForDropSnapshot`) with no check
  that the record is inside the publication's scope; only the branch-kind
  guard applies. A subscriber of any publication from account P can
  therefore delete any same-named snapshot in P, including snapshots of
  private databases. This is a pre-existing exposure for ordinary
  publications and becomes dangerous under v2's physical-owner routing for
  foreign sources; §4.5 closes it with a scope binding (I7).
- C9 (publication coverage shapes and identity facts): a database-scoped
  publication persists `mo_pubs.database_id != 0` and
  `database_name = D`, and `getDbAccountIdAndTypeById` resolves the exact
  `(dbId, dbName)` row to its owner — database ids are allocated globally
  and the row prevents a replaced database from inheriting the reference
  (`publication_subscription.go:2622-2645`). An account-covering
  publication (`CREATE PUBLICATION ... DATABASE *`) persists
  `database_name = '*'` and `database_id = 0` and cannot list tables
  (`publication_subscription.go:309-347`); the existing coverage rule
  admits `(TableAll && databaseID == 0)`
  (`snapshot.go:4593-4596`), the FROM-PUBLICATION forms create
  table/database snapshots through it today, and `SNAPSHOTLEVELACCOUNT`
  with FROM PUBLICATION performs account-level creates through the broad
  gate (`snapshot.go:393-403`). Foreign-source rows pass statement paths
  only through the rule in `checkSubscriptionValidCommon`
  (`publication_subscription.go:3050-3089`): publisher == owner, or
  publisher == sys, or every target is the owner
  (`publicationSourceOwnerAuthorized`, 2487-2494 /
  `publicationTargetsAdmitForeignSource`, 2516-2536); any other persisted
  row fails closed with
  `publicationForeignSourceUnauthorizedError` before resolution, as
  `publication_view_scope_test.go:2036-2123` proves for a
  (publisher=2, owner=3, subscriber=1) row whose membership check alone
  would pass. Snapshot records persist their object identity at creation:
  `snapshotRecord.objId` is the database id (db-level at
  `snapshot.go:541-560`), the table's **logical id** (`rel_logical_id`;
  table-level at 569-614 resolves through `getSqlForCheckDatabaseTable`
  whose SQL selects `rel_logical_id`, `authenticate.go:2150-2168`), or
  the account id (489-504), and existing scope validation compares that
  id directly (`pkg/sql/plan/types.go:140-179`, `ValidateSnapshotScope`).
- C10 (table identity is logical; publications carry names): the stable
  identity a table snapshot records is the logical table id —
  `SnapshotTableID` (`pkg/sql/plan/types.go:181-190`) returns
  `TableDef.LogicalId` when set, and `ValidateSnapshotScope` compares it
  everywhere (query_builder.go:13272, build_show.go:159,
  data_branch.go:2073). ALTER TABLE COPY preserves the logical id across
  a physical replacement: the replica inherits `oldLogicalId`
  (`WithKeepLogicalId`, pkg/sql/compile/alter.go:1190-1191) and the
  statement then drops the original relation internally
  (alter.go:1743-1754, with `WithIgnorePublish`); the RC admission layer
  documents the resulting split — "A replacement of a published table
  has a local physical ID but retains the published logical ID"
  (lifecycle_admission.go:399-402). Publication table lists persist
  **names only**: `resolvePubTableNames`
  (publication_subscription.go:2735-2756) stores sorted relname strings
  joined into `mo_pubs.table_list`, so a dropped-and-recreated table
  with the same name is indistinguishable in the publication row from
  the original object. Name lists also go **stale**: DROP TABLE scrubs
  the dropped name only from publications owned by the dropping account
  (`updatePubTableList`, ddl.go:4940-4943 → pub_sub.go:138-178, which
  selects `mo_pubs` rows with `account_id = <dropping account>`), so a
  foreign-owned publication over the same database — the supported
  sys-published arrangement of `pub_view_scope.sql:534-568` — keeps the
  dropped name in its `table_list`. A stale list is a live state: it
  proves only that some table once carried the name, never which
  logical object it authorized.

The user-visible inconsistency: the same table is readable and clonable, yet
the snapshot and diff machinery answers "table does not exist", a false
cause that also masks genuine missing-table errors.

## 2. Scope and design triggers

This design covers, for subscription database operands `N.t`:

1. `CREATE SNAPSHOT ... FOR TABLE N.t` (table level, no explicit
   `FROM ... PUBLICATION` clause) and the aligned explicit form,
   including its split-identity admission protocol;
2. `DROP SNAPSHOT ... FROM P_account PUBLICATION pub` scope binding —
   the lifecycle counterpart of item 1, which also closes the
   pre-existing coverage-blind delete (C8) on new CNs (I7);
3. DATA BRANCH read operands: both `DIFF` operands and the `MERGE`/`PICK`
   source;
4. DATA BRANCH write destinations: `MERGE ... INTO N.t`, `PICK ... INTO N.t`,
   and `DIFF ... OUTPUT AS N.o`;
5. the identity model (§3), the per-operand identity threading through
   every downstream catalog read (§6), and the shared
   classification/error boundary in front of the privilege probe (§7).

A design review is mandatory even though the user-visible symptom is a bug.
The change crosses snapshot lifecycle ownership, DATA BRANCH execution, and
publication/subscription metadata ownership boundaries, and it decides an
authorization and tenant-isolation boundary: which subscriber-initiated
operations may resolve relations owned by another account under that
account's identity, and which must stay rejected. Every prior fix of this
visibility class required a design review
(`CLAUDE_INFORMATION_SCHEMA_METADATA_VISIBILITY.md` for #27794,
`CLAUDE_INFORMATION_SCHEMA_SUBSCRIPTION_METADATA_ROUTING.md` for #27759).

Non-goals:

- changing publication/subscription creation, withdrawal, or scope
  semantics, or the foreign-source admission rules of
  `publicationSourceOwnerAuthorized`/`publicationTargetsAdmitForeignSource`;
- persisting publisher catalog rows or publisher data copies in `S`;
- restoring publisher data from subscriber-side snapshots (the C3
  relationship-only restore contract is unchanged);
- cluster/account-level snapshot semantics;
- PITR, CCPR, and `DATA BRANCH CREATE/DELETE TABLE|DATABASE` statements
  beyond the boundary decisions already in effect today (C4/C5);
- changing the graceful zero-history degradation for lineage ancestors
  whose owner is outside the two operand physical identities (§6.4);
- new SQL syntax, catalog schema changes, or protocol capability bits.

## 3. Identities and invariants

Three identities are modeled separately. Collapsing any two was v1's second
blocking gap:

- `S` — the statement account (the subscriber).
- `P_pub` — the publication account: owner of the `mo_pubs` row; the
  authorization anchor for subscriber membership.
- `P_src` — the physical source account: owner of the published database
  `D` and its tables, resolved from the publication's **persisted database
  identity** (`mo_pubs.database_id` → account), which is exactly what
  `SubscriptionMeta.AccountId` carries after `checkSubscriptionValidCommon`.
  `P_src == P_pub` for ordinary publications; `P_src != P_pub` for
  foreign-source publications (C6).

- I1 (authorization): an operation on `N.t` is admitted only while `S`
  holds a live subscription binding `N → (P_pub, pub)` at execution time;
  acting under a foreign identity additionally requires `S ∈ pub.accounts`
  under `P_pub`s row and — for `P_src != P_pub` — the persisted
  foreign-source admission must be valid
  (`publicationSourceOwnerAuthorized` /
  `publicationTargetsAdmitForeignSource` semantics, re-verified live); and
  `t ∈ pub.tables` is re-verified at execution time. Subscription metadata
  is never trusted from client input, plan caches, or earlier statements.
  For snapshot lifecycle statements the admission additionally re-reads
  every authorization fact from the **locked** publication row after the
  RC snapshot frontier advances: membership, the coverage shape
  (database-scoped vs account-covering), the persisted `database_id`,
  table scope, and — for a database-scoped row whose owner differs from
  the publisher — the foreign-source admission rule
  (`publicationSourceOwnerAuthorized` semantics per C9), not only on the
  alias path (which gets it through classification) but identically on
  the explicit CREATE and DROP paths, which never run classification.
  Any drift between the provisional read and the locked-row read fails
  closed with a retry-class, visibility-cause, or the stable
  foreign-source authorization error (§4.4). For the alias form the
  binding `N → (P_pub, pub)` itself is part of the re-verified state:
  `dropSubscription` (`pkg/sql/compile/pub_sub.go:110-129`) rewrites
  only `mo_subs` and never revokes the old publication's membership, so
  revalidating `mo_pubs` alone cannot detect a drop-and-recreate of `N`
  against a different publication or as a local database. The admission
  therefore also serializes the subscriber database key `(S, N)` inside
  the documented lock order and, after the frontier advances, revalidates
  both `N`'s persisted database identity under `S` and the live `mo_subs`
  mapping of `N` to the same publication, retaining the serialization
  through commit (§4.4 steps 1/3/5).
- I2 (ownership): no new cross-account catalog state class is introduced.
  Snapshot artifacts are recorded under `P_src` — the account whose GC
  protects the physical data and whose catalog resolves the object.
  Subscriber-side relationship placeholders (C3) and globally stored
  lineage edges (C7) are unchanged.
- I3 (tenant isolation): no subscriber operation may write into a foreign
  relation. Subscription tables are never valid DATA BRANCH write
  destinations, at any privilege level.
- I4 (read surface): subscription-aware routing exposes no read capability
  beyond what the same session already has via `SELECT` on `N.t`, including
  the existing `{MO_TS}` hint semantics. Named-snapshot hints
  (`{SNAPSHOT='name'}`) stay account-scoped to `S`: publisher-owned
  artifacts (this design's alias snapshots and the existing C2 artifacts)
  are not addressable by name from the subscriber, exactly as today.
- I5 (error honesty): a missing/unauthorized subscription operand fails
  with the visibility cause (not in publication scope, not subscribed,
  revoked, suspended, or write prohibition), never with a bare "table does
  not exist" indistinguishable from a genuinely missing local table, and
  never through an account mismatch that reports the physical cause only
  after a decoy could answer.
- I6 (identity discipline): every catalog read that targets an operand's
  physical owner — relation reopen by id, `mo_database` created_time CP-key
  probe, `mo_tables` commit-TS scans, LCA ancestor reopen, range/change
  collection — uses that operand's own physical identity (`S` for local
  operands, `P_src` for subscription operands). The `S` identity is
  retained only for: privilege evaluation on the user-named alias, `S`'s
  own account-scoped state (named snapshots, `OUTPUT AS`/MERGE/PICK
  destination tables), and globally stored lineage rows, whose storage
  account is already sys. Publication metadata (`mo_pubs` row lock,
  coverage lookup, membership) is consultet under `P_pub`; the physical
  identity supplies only the resolved database/table ids compared against
  that row (§4.4).
- I7 (deletion binding): a publication-routed `DROP SNAPSHOT` may delete
  only a record whose **recorded identity** (`snapshotRecord.objId`, set
  at creation and never remapped; `ValidateSnapshotScope`
  `pkg/sql/plan/types.go:140-179` is the precedent for comparing it
  directly) lies inside the scope of the live, locked publication row.
  "Scope" has two dimensions and deletion requires both: recorded
  identity **and** coverage. §4.5 binds database-level records by
  `objId` **and** by whole-database scope coverage — for database-scoped
  rows the locked row must carry `table_list == '*'`, the same boundary
  creation already enforces (`snapshot.go:4592-4597` rejects
  database-level targets when `table_list != '*'`), so a publication
  exposing only part of a database can never delete a snapshot that
  covers objects it does not publish; binds table-level records to the
  authorized table's **logical identity** (C10): `record.objId` is the
  recorded `rel_logical_id`, so the binding compares it against the
  *current* logical id behind the published name in the anchored
  database (`SnapshotTableID` precedent) — surviving COPY ALTER, which
  preserves the logical id across physical replacement, and rejecting
  drop/recreate substitutions, which mint a new one; historical catalog
  membership read at the record's own timestamp is keyed by
  `rel_logical_id` (never `rel_id`) with unambiguous
  database-membership validation, and serves as the binding only under
  **whole-database coverage** when the name no longer resolves
  (dropped-after-snapshot cleanup: under `'*'`, every logical identity
  ever anchored to the covered database is in scope by construction);
  a table-limited publication whose authorized name no longer resolves
  **fails closed** — its name-only list, possibly stale (C10), cannot
  identify which logical object it authorized, and historical database
  membership alone proves location, never authorization — with cleanup
  reverting to the record owner; and
  admits account-level records only when the publication covers the
  publisher's account. Name equality between a
  dropped-and-recreated database/table and the publication target never
  authorizes deletion of the old object's snapshots. Unrelated
  account/database/table snapshots anywhere in the resolved catalog are
  never deleted on a subscriber's authority; they fail with a scope
  error instead, in any account combination (ordinary, foreign-source,
  colliding names) and on any mix of old/new CNs where a new CN
  executes the statement.

## 4. Contract decisions

Normative behavior per statement when operand `db.t` resolves to a live
subscription database `N` in the session account.

### 4.1 Shared classification boundary (finding 3)

One shared helper owns subscription classification for every covered path:

```
classifySubscriptionOperand(ctx, ses, bh, txnPolicy, dbName, tblName)
    -> (local | visible | invisible)  +  (P_pub, P_src, D, meta)  +  error
```

- `local`: `dbName` is not a subscription database (live
  `getSubscriptionMeta` returns no meta). Behavior is identically today's,
  including the engine's genuine no-such-table errors.
- `visible`: live subscription and `tblName ∈ T`
  (`pubsub.InSubMetaTables`). Carries `(P_pub, P_src, D)` for routing.
- `invisible`: live subscription but `tblName ∉ T`, or live-ness
  revalidation fails (revoked publication, suspended publisher, dropped
  subscription): the helper returns the visibility-cause error
  (wording finalized at implementation, distinct per cause class: outside
  publication scope / subscription no longer valid).

Consumers and ordering:

1. `requireBranchReadOnTableName` (the DATA BRANCH privilege probe,
   `data_branch_privilege.go:493-530`): classify **before** building the
   synthetic `SELECT * FROM N.t`. On `invisible`, return the
   visibility-cause error instead of letting the plan builder's
   nil-resolution emit `SQL parser error: table "t" does not exist`
   (`pkg/sql/plan/query_builder.go:13266-13268`). On `visible`, run the
   existing synthetic SELECT probe unchanged: privilege is still evaluated
   against the user-named alias under `S`s RBAC, so denied reads keep
   failing with the privilege error (no RBAC bypass). On `local`,
   unchanged.
2. `getRelations` / branch execution: re-classify at execution time under
   the statement transaction (defense against metadata changed between
   probe and execution, same statement-txn read view), then route engine
   access per §4.2/§6.
3. MERGE/PICK/OUTPUT-AS destination validation: classify the destination;
   any non-`local` result is a write-prohibition error (§4.3).
4. `doCreateSnapshot` table level without an explicit publication clause:
   classify; on `visible`, proceed via §4.4; on `invisible`, return the
   visibility-cause error (the C2 publication-coverage error remains the
   authoritative message for out-of-scope tables); on `local`,
   unchanged.

### 4.2 DATA BRANCH read operands

Both `DIFF` operands and the `MERGE`/`PICK` source are routed when
classified `visible`: execution context attaches `P_src`; the engine
database name is rebound to `D`; the relation is opened under the
operand's physical identity. This is the canonical
`SubscriptionMeta.AccountId` routing (`TxnCompilerContext.getRelation`
lines 545-549), never a same-named publisher database: the decoy cannot
answer because the physical account is taken from the persisted database
identity, not from the publication account.

### 4.3 DATA BRANCH write destinations

- `MERGE ... INTO N.t`, `PICK ... INTO N.t`: reject before any write with
  `cannot perform DATA BRANCH <MERGE|PICK> into subscription database N.t`
  (mirroring C5).
- `DIFF ... OUTPUT AS N.o`: destination database must be local; the
  existing create-table rejection (C5) applies unchanged.

### 4.4 Table-level `CREATE SNAPSHOT ... FOR TABLE N.t` and the split-identity admission protocol

When the alias form is classified `visible`, the statement executes with
the identities resolved from the live subscription metadata. Two
accounts participate, and each admission fact is placed under exactly one
of them (finding 5: `admitLocalLifecycleRC` uses a single `accountID` for
the database, table, and quota keys **and** for the `mo_pubs` lock key
`(account_id, pub_name)`, and `checkSnapshotPublicationCoverage` queries
`mo_pubs` filtered by its account argument — so the create path needs a
two-identity variant, not an identity swap):

1. Provisional resolution and **shape-aware owner derivation**
   (finding 9): membership + `P_pub` identity via
   `resolveAccountFromPublication`-style read of the `mo_pubs` row
   (`account_name = pubAccountName, pub_name = pubName`), then the
   coverage shape of the locked row, per C9:
   - database-scoped (`database_id != 0`, `database_name = D`):
     `P_src := getDbAccountIdAndTypeById(D, database_id)` — the exact
     persisted row's owner, never a name lookup;
   - account-covering (`database_name == '*' && database_id == 0`):
     `P_src := P_pub`. These publications bind to no single physical
     database, and their scope is the publisher's own databases and
     account; a foreign source is impossible in this shape by
     construction (`dbId = 0` rows never point at another account).
   Both identities stay provisional until step 5. Subscription aliases
   only ever resolve to the database-scoped shape (a subscription
   database points at a concrete published database, C6). For the alias
   form, classification additionally captures, as provisional binding
   facts, the subscriber database's persisted database id (`N` resolved
   under `S`) and the live `mo_subs` binding row's
   `(pub_account_name, pub_name)` pair — the values step 5 compares
   against their post-frontier re-reads (finding 12).
2. Shared foreign-source gate (finding 10, I1): for a database-scoped
   row whose derived `P_src != P_pub`, run the same rule
   `checkSubscriptionValidCommon` applies (C9): publisher == owner, or
   publisher == sys, or `publicationTargetsAdmitForeignSource(row,
   P_src)` — else the stable
   `publicationForeignSourceUnauthorizedError`, returned **before** any
   physical resolution, matching the decoy-safety contract of
   `publication_view_scope_test.go:2036-2123`. This gate runs on the
   alias path **and** on the explicit CREATE/DROP paths, which never
   execute classification; membership checks alone are insufficient
   (a subscriber-only target passes membership while the row is
   unauthorized). Account-covering rows satisfy the gate trivially.
3. Admission locks, one background RC transaction, order preserved
   (`A` → `D` → `T` → `Q` → `P`: the interior `D` → `T` → `Q` → `P`
   order `admitLocalLifecycleRC` already documents — publication writers
   lock the database before updating the publication row, and the
   table-key before the publication key ordering is kept globally —
   with one new head key `A`, present only for the alias form):
   - `A` — the subscriber database key `(S, N)` (finding 12), the same
     `mo_database` lock the subscriber's own database DDL serializes on.
     Alias-form only: the explicit form names the publication directly
     and has no subscriber-side binding to protect. Every write path
     that changes the binding `N` resolves to — `createSubscription` /
     `dropSubscription` (`pkg/sql/compile/pub_sub.go:42-129`), running
     inside the `CREATE/DROP DATABASE` statement transaction — acquires
     `(S, N)` as part of the database row write, so holding `A` from
     this step through commit serializes the admission against any
     drop-and-recreate of `N`. Placing `A` at the head keeps the order
     acyclic: subscription DDL touches only `(S, N)` plus `mo_subs`
     rows under sys (which this protocol only reads, after the lock),
     and no existing transaction holds any of `D`/`T`/`Q`/`P` while
     requesting a subscriber database key. A rebind committed before
     `A` is granted is visible to the post-frontier re-reads in step 5;
     a rebind attempted after `A` is granted blocks until this
     transaction commits.
   - `D`/`T`: `mo_database` and `mo_tables` keys for the statement's
     **actual target** — `(P_src, D)` / `(P_src, D, t)`, where `D` is
     the alias's physical database or, for the explicit account-covering
     shape, the database the statement names — the rows protecting the
     physical target live under the physical owner;
   - `Q`: snapshot scope/quota query+lock under `P_src` — the record
     owner pays (I2);
   - `P`: `mo_pubs` key `(P_pub.id, pubName)` shared, under sys — this
     is the key the v2 draft got wrong when feeding `P_src` into the
     single-account helper.
   For the explicit form with ordinary publications (`P_src == P_pub`)
   every key is byte-identical to today's `admitLocalLifecycleRC` call;
   the alias form prepends exactly `A` to that sequence (its base-build
   outcome is the 20101 defect, so no existing lock pattern is
   perturbed).
4. `finish`: the RC frontier advances and the feature-registry identity
   is re-checked exactly as today
   (`lifecycle_admission_rc.go:210-234`).
5. Post-frontier revalidation (I1, the "locked-row" read): the
   publication row is re-read **while held** from `mo_pubs` under
   `(P_pub.id, pubName)`, and all authorization facts are re-derived from
   this read: `S ∈ account_list`; the coverage shape and, for
   database-scoped rows, the persisted `database_id` (→ `P_src`) plus
   the step-2 gate; `t ∈ table_list` for table-scoped rows. Any drift
   versus the provisional values — account removed, publication
   retargeted to another database or another shape, table dropped from
   scope, row missing — fails closed with a retry-class error
   (`NewTxnNeedRetryWithDefChanged` shape as today), the stable
   foreign-source authorization error, or the visibility-cause error;
   nothing is written on drift. The shared row lock held since step 3
   excludes committed `ALTER/DROP PUBLICATION` writes between the lock
   and this read under the pessimistic RC isolation the admission
   requires.
   For the alias form the binding itself is revalidated with the same
   discipline (finding 12): while `A = (S, N)` is held, re-read (a)
   `mo_database` under `S` — `N` still present with the persisted
   database id captured in step 1 — and (b) `mo_subs` under sys —
   exactly one `SubStatusNormal` row binding `(S, N)` to the same
   `(pub_account_name, pub_name)` pair captured in step 1. Any drift —
   `N` dropped, recreated as a local database, or rebound to another
   publication (each changes the database id and/or the `mo_subs`
   mapping, while leaving the old `mo_pubs` membership untouched,
   `pub_sub.go:110-129`) — fails closed with the retry-class error
   before any write, exactly as the `mo_pubs` drift cases do: no
   `mo_snapshots` row and no quota delta (step 8's single-transaction
   failure-atomicity). The lock and the re-reads are complementary:
   the lock serializes in-flight drop/recreate writers of `N` against
   this transaction's commit, and the re-reads expose a rebind that
   committed before `A` was granted (the provisional step-1 values may
   predate it). On failure the caller retries the statement, which
   re-classifies against the new binding — a recreated local `N` then
   takes today's ordinary local path, and a rebound `N` the new
   publication's.
6. Session generation (`checkPublicationSubscriberGeneration`) under `S`,
   as today.
7. Object resolution and coverage under split identities: `mo_tables`
   probe (`getSqlForCheckDatabaseTable`) runs with `ctx` attached to
   `P_src` and `reldatabase = D`; the database id is resolved under
   `P_src` via the engine (`StorageEngine.Database(ctx↦P_src, D)`).
   `checkSnapshotPublicationCoverage` is called with its publisher
   argument = `P_pub` (its `mo_pubs where account_id = ?` query is a
   publication-metadata lookup) while the compared `databaseID` is the
   physically resolved id from `P_src`. Both coverage shapes are
   handled by the existing rule (`snapshot.go:4592-4596`): a
   database-scoped row requires `pubDatabase == D &&
   pubDatabaseID == databaseID` (for a foreign-source publication this is
   precisely the decoy guard); an account-covering row matches on
   `pubDatabase == '*' && pubDatabaseID == 0`, so any resolvable
   publisher database qualifies — creating through `DATABASE *`
   publications keeps working exactly as today.
8. Recording: one `mo_snapshots` insert under `P_src` with
   `account_name`/`database_name` naming `P_src`/`D`;
   `checkSnapshotQuota` charged to `P_src`. All writes happen inside the
   same background transaction, so any error — including an injected
   error between quota admission and the insert or between the insert
   and commit — leaves no partial row and no quota delta
   (failure-atomic by transaction).
9. Visibility and lifecycle: the artifact is publisher-side, as with the
   explicit form (verified v1 evidence): the subscriber manages it
   through `DROP SNAPSHOT s FROM P_pub PUBLICATION pub`, which resolves
   the same persisted physical identity and is scope-bound (§4.5, I7).
   The visible subscription metadata already carried the foreign-source
   admission (C6), the classification helper re-verifies it live (I1),
   and the explicit paths apply the step-2 gate directly.

### 4.5 Aligned explicit forms and scope-bound deletion

**Create.** The explicit `CREATE SNAPSHOT ... FROM P_account PUBLICATION
pub` runs the §4.4 protocol verbatim (the alias form is pure sugar for
it). Consequence: for ordinary publications nothing changes
(`P_src == P_pub` — today's attach, lock keys, quota, and recording are
byte-identical). For foreign-source publications the explicit form
previously failed closed with a misleading outcome — a
`NewTxnNeedRetryWithDefChanged` at database level (resolved database id
never equals the persisted one) or the publication-coverage error at
table level — and now resolves the true physical owner instead. No
decoy-snapshot risk exists today (the persisted database-id comparison
already fails), and the alignment preserves that guard. No pre-existing
foreign-source snapshot rows can exist (the operation always failed), so
no legacy cross-owner artifacts need migration or dual-lookup.

**Drop, with scope binding (findings 4, 7, 8, 9, 10; I7).**
`DROP SNAPSHOT s FROM P_account PUBLICATION pub` keeps today's
authorization spine — membership and session generation through
`lockAccountFromPublication` (the publication row held FOR UPDATE under
`P_pub`, sys catalog) plus `lockDataBranchLineageOwnerLifecycle` and the
branch-kind guard — applies the §4.4 step-2 foreign-source gate to the
**locked** row exactly as creation does, and binds the delete to the
record's own identity and the publication's coverage:

1. Shape-aware owner from the locked row (finding 9): database-scoped
   row → `P_src := getDbAccountIdAndTypeById(D, database_id)`; then the
   shared gate (publisher == owner, sys, or owner-only targets, else
   the stable unauthorized error — finding 10). Account-covering row
   (`'*'`, id 0) → `P_src := P_pub` (no foreign source exists for
   `dbId = 0`). Ordinary publications resolve exactly as today.
2. Attach `P_src`, load the named record from that catalog
   (`getSnapshotByName` → `snapshotRecord.level/accountName/
   databaseName/tableName/objId/ts`), and bind by recorded identity
   (finding 8, I7) — name equality with a dropped-and-recreated object
   never authorizes deletion of the old object's snapshots:
   - `record.accountName == P_src`'s resolved name in every case;
   - `DATABASE` level: `record.objId` equals the database identity the
     publication anchors **and** the locked row covers the whole of
     that database (finding 11). Coverage comes first: database-scoped
     rows must carry `table_list == '*'` — the deletion-side counterpart
     of the existing creation boundary (`snapshot.go:4592-4597` rejects
     database-level targets whenever `table_list != '*'`), so a
     publication exposing only part of a database (`d.t1` of `d = {t1,
     t2}`) can never delete an owner-created database snapshot covering
     objects it does not publish; table-limited publications delete
     `TABLE`-level records only (next bullet). Identity then binds as
     in v4. Database-scoped row: `record.objId == pub.database_id`
     (direct comparison, the `ValidateSnapshotScope` precedent, C9).
     Account-covering row: no new coverage rule is needed — `'*'`
     publications disallow `table_list` at creation
     (`publication_subscription.go:343-347`), so whole-database
     coverage holds by construction — and
     `record.objId == resolve(record.databaseName under P_pub).id` —
     only a snapshot of a database that *currently exists with that
     identity* in the publisher's catalog is deletable; a snapshot of a
     dropped-and-replaced database keeps a stale `objId` and is
     rejected;
   - `TABLE` level: coverage first — `record.tableName ∈ pub.table_list`
     (trivially true for `'*'` lists) **and** `record.databaseName`
     equals the anchored database name (`D` for database-scoped rows;
     the record's own qualifier for account-covering rows). Identity
     then binds by logical table id (findings 13, 14; C10): resolve the
     current catalog row for (anchored database, `record.tableName`)
     under `P_src` in the post-frontier view and read its
     `rel_logical_id` `L_cur` — the exact SQL shape
     `getSqlForCheckDatabaseTable` already emits
     (`authenticate.go:2165`)
     - current row present: require `record.objId == L_cur`. COPY
       ALTER preserves the logical id across the physical replacement
       (`alter.go:1190-1191`), so snapshots of the altered table —
       taken before or after the ALTER — stay deletable through the
       publication; a drop/recreate mints a new logical id, so the old
       object's record fails even though its name and its historical
       database id would still match (finding 14's replaced-table
       inheritance);
     - current row absent (table dropped after snapshotting and not
       recreated): admissible only under whole-database coverage. When
       the locked row carries `table_list == '*'`, every logical
       identity ever anchored to the covered database is in scope by
       construction, so the binding may fall back to historical
       membership — read `mo_tables {MO_TS = record.ts}` under `P_src`
       filtered by `rel_logical_id == record.objId` (**never** `rel_id`:
       creation records the logical id, `snapshot.go:569-601` →
       `authenticate.go:2165`, and a COPY ALTER between snapshot and
       drop leaves only physical-id history — finding 13), and require
       exactly one visible row whose `reldatabase_id` equals the
       anchored database identity from the DATABASE-level rule above
       (`pub.database_id` for database-scoped rows; the current
       publisher-resolution id for account-covering rows). Zero rows,
       more than one visible row, a row anchored to another database,
       or history no longer readable → fail closed with the scope
       error. This keeps the dropped-after-snapshot cleanup contract
       (B11) keyed by the logical identity, so a same-named replacement
       created later in the same database can never reach the old
       record. A table-limited list gives **no such fallback**
       (finding 15): the persisted list authorizes names, and once the
       authorized name no longer resolves there is no record of which
       logical identity it authorized — foreign-owned publications keep
       stale lists (`updatePubTableList` scrubs only the dropping
       account's own publications, C10), so historical database
       membership alone would admit a never-published original or a
       replaced-then-dropped substitute. Every table-limited record
       whose authorized name no longer resolves fails closed with the
       scope error — whether it covers the never-published original,
       the replaced object, or the authorized table dropped later —
       and remains for the owner's own `DROP SNAPSHOT` (§10);
   - `ACCOUNT` level: admissible only for account-covering publications,
     and only when `record.objId == P_pub`'s account id — the
     publication covers the publisher's account; account snapshots of
     any other account are rejected. `CLUSTER` level: always rejected.
3. Rejection: any record failing a rule fails with
   `snapshot <name> is outside the publication's scope` and is left
   intact; absence follows today's semantics (name missing from the
   `P_src` catalog: `IF EXISTS` → silent no-op; otherwise
   `snapshot <name> does not exist`). Presence only in some other
   account's catalog is not "existence" for this statement. Only a
   fully bound record is deleted.

The binding narrows what subscriber membership may delete (closing C8's
exposure on ordinary publications too): the coverage rule mirrors the
creation boundary, and the identity rule pins the exact recorded object
— the record owner's own `DROP SNAPSHOT s` (account-scoped,
owner-authorized) is untouched by both.

**Namespace decision (finding 7).** The only statement whose catalog
account changes is the publication-routed DROP for database-scoped
foreign-source publications (`P_pub` → `P_src`); ordinary and
account-covering publications keep the publisher namespace. Because
new-CN deletes are scope-bound by identity, an unrelated same-named row
in `P_pub`'s catalog is never a candidate on a new CN; on an old CN the
statement keeps today's coverage-blind `P_pub`-scoped behavior (C8,
neither introduced nor worsened by this change — it cannot see `P_src`
rows at all). §5 carries the full mixed-version matrix and the
collision/absent-name validation plan.

### 4.6 Unchanged surfaces

- `CREATE SNAPSHOT ... FOR DATABASE N`: the C3 relationship placeholder is
  unchanged. The asymmetry with table level is deliberate: database-level
  snapshots exist for account-restore of the subscription binding;
  table-level snapshots are data artifacts and must live with the data
  owner (I2).
- `SHOW SNAPSHOTS` / plain `DROP SNAPSHOT`: account-scoped, unchanged
  (I4).
- creation of DATABASE/ACCOUNT/CLUSTER-level snapshots and all restore
  paths: unchanged. The §4.5 binding only narrows what a
  publication-routed DROP may delete; which levels can be created,
  through which statement, is untouched (account-covering explicit
  creates keep working, §4.4 step 7).
- `DATA BRANCH CREATE/DELETE TABLE|DATABASE`: unchanged (C4/C5 already
  hold through ordinary SQL routing).

### 4.7 Decision rationale

- Recording table snapshots under `S` was rejected in v1 and stays
  rejected: a dangling cross-account object id with no GC protection and
  no resolvable consumer, contradicting C3's documented restore contract.
- Recording under `P_pub` for foreign-source publications (v1's
  assumption) was rejected by review: `P_pub` may own only a decoy
  database; the artifact would protect nothing and could bind the wrong
  object id. The only coherent owner is the physical source account
  `P_src`, whose catalog resolves the object and whose snapshot list
  protects its data.
- Rejecting table snapshots and branch reads with a documented limitation
  stays rejected: branch reads from subscription sources already succeed
  (C4), and DIFF/MERGE-src/PICK-src are pure reads.

## 5. Standards, precedent, and alternatives

- Canonical routing: `TxnCompilerContext.getRelation`,
  `handleShowTableStatus`, and `getSubscriptionMeta` — the identity attach
  (`uint32(sub.AccountId)`) plus database rebind — are reused verbatim.
  They already honor the C6 physical/publisher split, so branch reads
  inherit the decoy protection by construction.
- `#27794` (information_schema) and `#27759` (SHOW INDEX/STATISTICS) fixed
  the same visibility gap class by routing into physical owner catalogs
  with publication-scope filtering; same direction here.
- `pub_view_scope.sql` established the persisted-physical-source contract
  for reads including decoy protection; this design extends the identical
  rule to snapshot recording and branch execution instead of inventing a
  second rule.
- C5's explicit write prohibition is the clause MERGE/PICK destinations
  mirror.
- Alternative: reject subscription operands everywhere with a documented
  error. Rejected (see 4.7); it would widen the inconsistency the issue
  reports, since equivalent reads already succeed elsewhere.
- Compatibility and mixed-version safety (finding 7): no catalog, wire,
  protocol, or persisted-layout change. The only behavioral changes with
  version-sensitive meaning are (a) read-side routing of subscription
  operands and admission of alias table snapshots — new capabilities on
  new CNs, old CNs keep failing closed with today's (misleading) message
  and grant no access either side forbids; (b) the scope binding on
  publication-routed `DROP SNAPSHOT` (§4.5, I7) — a narrowing of what a
  subscriber may delete, applied wherever a new CN executes the
  statement; old CNs keep today's coverage-blind delete under the
  publication account (C8), which this change neither introduces nor
  worsens; and (c) the catalog namespace of that DROP for database-scoped
  foreign-source publications (`P_pub` → `P_src`; account-covering
  publications and ordinary publications keep the publisher namespace on
  both versions, so their pre-existing `DATABASE *` lifecycle —
  table/database/account snapshot create and drop — is untouched by (c)).
  Since (c) applies only where `P_src != P_pub`, and that combination
  never created a snapshot before this change, no existing statement
  outcome that users can depend on is altered: the consequences are
  confined to mixed-version halves:
  - Rolling upgrade (new CN creates `P_src.s`; old CN receives the
    documented cleanup statement): the old CN looks only in `P_pub`,
    reports absent/no-op under `IF EXISTS`, and cannot touch `P_src.s`
    (fail-safe direction: residue, never wrong deletion by the new
    contract). If `P_pub` independently owns a same-named `s`, the old
    CN's delete of it is today's C8 behavior, unchanged; the new CN's
    scope-bound delete never matches it. The subscriber re-issues the
    cleanup on a new CN, or `P_src` drops its own row with plain
    `DROP SNAPSHOT s`. Upgrade ordering note: publication-routed
    lifecycle for foreign-source snapshots is driven from upgraded CNs
    until the roll completes.
  - Downgrade (cluster rolls back while `P_src` rows exist): old CNs
    cannot see or delete them (scope fail-safe again); new CN
    functionality disappears, matching pre-change foreign-source
    behavior; ordinary publications are entirely unaffected on both
    versions because `P_src == P_pub` (no namespace change, and the
    scope binding only rejects rows that a subscriber was never meant
    to delete).
  - Colliding names and absent publisher-side names are validated
    explicitly (§9 U6/B9): new-CN delete removes only the bound row in
    the physical catalog, assertions run against both catalogs, and an
    absent name yields today's absent-drop semantics on either side.
  - Every admission re-verifies live publication metadata under its row
    lock plus post-frontier revalidation (§4.4): mixed-version execution
    grants no access either side forbids, including during the upgrade
    window.

## 6. Per-operand identity threading (finding 1)

### 6.1 Operand descriptor

Routing produces, per DIFF operand and per MERGE/PICK source, an operand
descriptor:

```
branchOperand {
    userDb, userTable          // statement spelling (alias or local)
    physAccount                // S for local, P_src for subscription
    physDb                     // physical database name (D when routed)
    tableName
    ctx                        // statement ctx with physAccount attached
    rel                        // relation opened under ctx
    snapshotTS                 // operand snapshot (from hint or current)
    isSubscription             // classification
}
```

All downstream consumers of an operand's data or of `physAccount`'s
catalog rows take `descriptor.ctx`/`descriptor.physAccount` — never a
global caller context and never `ses.GetAccountId()` beside a physical
name. `S`-identity surfaces are enumerated in I6 and are unchanged.

### 6.2 Catalog probes

- `getTablesCreationCommitTS` (data_branch.go:3031): signatures take the
  operand descriptors for tar and base. Per operand:
  - `getDatabaseCreatedTimeLowerBound`: the CP key is built from the
    operand's `physAccount` + `physDb` (+ database id); the scan context
    is `descriptor.ctx`. Today's `accountID := ses.GetAccountId()`
    (line 3188) beside the physical `DbName` is the bug class this closes:
    for a subscription operand it probes `(S, D)`, finds no row, and
    raises `cannot find database ... created_time at snapshot ...`
    (line 3268-3271) even for the degenerate self-DIFF, because
    `decideCollectRange` calls this probe unconditionally.
  - `getTableCreationCommitTSByCollectChanges` /
    `getTableCreationCommitTSByID`: the `mo_tables` CollectChanges / range
    scan executes under `descriptor.ctx` so it reads the owner account's
    catalog at the operand snapshot.
  - the existing caches are keyed additionally by
    `physAccount` (the `dbCreatedTimeLowerBoundKey.accountID` field now
    receives the operand identity, not the session identity).
- `scanSnapshotRelationByID(WithFallback)` (data_branch_helpers.go:352-520):
  `storage.GetRelationById(ctx, rangeTxnOp, tableID)` and `Ranges(ctx, ...)`
  run under the operand/owner context passed by the caller.
- `getRelationById` (data_branch.go:1864): caller passes the reopen
  context per §6.4.

### 6.3 Data scans

`constructChangeHandle` and the diff/merge row scans consume
`tables.tarRel`/`tables.baseRel` through the operand descriptors: range
resolution, `Ranges`, `NewReader`, and the hash-diff collection run under
each operand's own context. Mixed operand pairs (one local, one
subscription) therefore read each side under its own owner — comparison
itself is over row content by primary key and needs no shared identity.
Destination-side writes (`OUTPUT AS`, MERGE/PICK `INTO`) always execute
under `S` (I3).

### 6.4 Lineage node ownership (finding 6)

- `buildBranchMetaInfo`/`loadBranchDAGWithBH` read the global
  `mo_branch_metadata` under sys exactly as today (C7); subscription-source
  clone edges continue to exist and need no migration.
- **Every** non-endpoint lineage node reopen is owner-resolved, not just
  the LCA: `dataBranchPathTableDefs` intermediate path nodes
  (data_branch.go:2322-2328, which today pins
  `Tenant: ses.GetAccountId()`), `buildSideCollectRange` intermediate
  segments (data_branch.go:2945-2950, same pin), the LCA reopen
  (data_branch.go:1560-1650), and the per-node creation-commit-TS lookup
  that follows a successful reopen (data_branch.go:2968-2982 and the
  CTS calls inside `resolveSameTransactionDataBranchRelation`). A single
  helper reopens by id under a deterministic candidate order — the
  walking side's endpoint `physAccount` first, then the opposite
  endpoint's — and returns the winning identity; the node's CTS probe
  runs under that winning identity, and any tenant-pinned snapshot
  (`lcaSnapshot.Tenant`, data_branch.go:1583) is set from it, keeping
  tenant-pinned reads consistent with the relation's owner.
- Why the union of the two endpoint owners suffices: an ancestor lies on
  at least one endpoint's parent chain, and the only cross-account edges
  recorded today are subscriber-clone → physical source (C7), whose
  ancestor lives under the subscription endpoint's `P_src` — always one
  of the two candidates. A chain cannot hop through a third account
  without first materializing a local clone in an endpoint's account.
- Degradation is preserved, not widened: a node missing under **all**
  candidates falls into `resolveSameTransactionDataBranchRelation`
  (data_branch.go:2361-2401) exactly as today (both lookups
  missing-by-id and `nodeSnapshot < endpointCTS` → zero-history). The
  change is only that an **owner mismatch** on a supported mixed operand
  (e.g. local clone `S.c` of subscription `P_src.t`, whose local-side
  path is `[P_src.t, S.c]`) now resolves under `P_src` instead of being
  misclassified as absent history — today that path produces
  whole-history/zero-history results despite committed foreign history,
  which finding 6 flags as wrong for operands this design newly
  supports. Local-local operands with a third-party subscription ancestor
  (neither endpoint is the subscription) keep today's zero-history
  outcome byte-for-byte: B6 regression, explicit non-goal (§2).

### 6.5 Snapshot hint handling in operands

`resolveSnapshot` for `{SNAPSHOT='name'}` stays `S`-scoped (I4);
`{MO_TS}` hints apply to the operand's physical relation directly, as
`SELECT ... {MO_TS}` does through C1 today. `validateDataBranchNamedSnapshotScope`
keeps operating on the user-visible names.

## 7. RBAC preservation

The privilege layer never consults the physical mapping, so the routing
cannot widen it:

- read authorization continues to be evaluated by
  `authenticateDataBranchDiff`/`authenticateDataBranchReadWriteTables`
  against the user-named alias (`N.t`) under `S`s role closure — granted
  subscriptions keep working, denied reads keep failing, and the
  classification step (§4.1) only replaces the masking *cause*, never the
  decision;
- destination write checks (`branchWriteTableRequirement`) run unchanged
  and the destination classification rejects subscription databases before
  them;
- `doCheckCreateSnapshotPriv`/`doCheckRole` for snapshot creation is
  unchanged: the alias form inherits the same role gate as the explicit
  form, and the publisher-side write happens only after the C2 subscriber
  authorization;
- publication-routed DROP keeps membership + session generation
  authorization exactly as today; the added scope binding (I7) only
  **narrows** the set of rows that authorization may delete — it cannot
  widen access.

## 8. Logical and ownership closure

Owner paths and intended edits (implementation map for review orientation;
no code is written before approval):

1. New shared helper (§4.1/§6.1), placed beside the DATA BRANCH privilege
   code so classification is reachable before `buildPlan`.
2. `data_branch_privilege.go` `requireBranchReadOnTableName`: consult the
   classifier first (§4.1.1).
3. `data_branch.go` `getRelations`: classify and build descriptors for both
   operands (§4.2, §6.1); reject write-resolvable mis-route impossible —
   destinations are classified separately in the MERGE/PICK handlers
   (§4.3).
4. `data_branch.go` `decideCollectRange` → `getTablesCreationCommitTS`
   family, `scanSnapshotRelationByID*`, `getRelationById`, LCA reopen and
   `resolveSameTransactionDataBranchRelation`: thread operand contexts and
   identities (§6.2-§6.4).
5. `snapshot.go` `doCreateSnapshot` table level: classification hook for
   the alias form (§4.4) and the split-identity admission for both alias
   and explicit forms (§4.4/§4.5): a two-identity variant of
   `admitLocalLifecycleRC` taking `(physAccount=P_src,
   publisherAccount=P_pub)` beside today's single-account function
   (byte-identical when equal), with D/T keys for the statement's actual
   target under `P_src`, quota under `P_src`, the `mo_pubs` lock key
   under `(P_pub.id, pubName)`, a shape-aware owner resolver
   (database-scoped: persisted `database_id` →
   `getDbAccountIdAndTypeById`; account-covering: `P_src = P_pub`), the
   shared foreign-source gate as a small reusable predicate beside
   `publicationSourceOwnerAuthorized` /
   `publicationTargetsAdmitForeignSource` (applied to the locked row on
   every publication-routed CREATE and DROP), the locked-row
   post-frontier revalidation (§4.4 step 5) — extended for the alias
   form by the subscriber database key `(S, N)` at the head of the
   lock order and the binding re-reads (`N`'s persisted database id
   under `S` plus the `mo_subs` mapping to the captured
   `(pub_account_name, pub_name)` pair; finding 12) — and
   `checkSnapshotPublicationCoverage` called with publisher argument
   `P_pub` and the `P_src`-resolved `databaseID` (its `mo_pubs` query is
   keyed on the publication account — the v2 instruction to reuse it
   "with the physical identity" was wrong for that argument);
6. `snapshot.go` `doDropSnapshot` publication branch: keep
   `lockAccountFromPublication` + generation + lineage + branch-kind
   guards, apply the shared gate and shape-aware owner to the locked
   row, load the named `snapshotRecord` under `P_src`, and delete only
   when §4.5's coverage-and-identity binding matches (I7): database
   level requires whole-database scope (`table_list == '*'` on
   database-scoped rows, finding 11) plus direct `objId` comparison,
   table-level by `table_list` membership plus logical-identity binding
   (current `rel_logical_id` behind the published name, findings 13/14;
   `rel_logical_id`-keyed historical membership at `record.ts` under
   `P_src` admissible only under whole-database coverage when the name
   no longer resolves — table-limited lists fail closed in that state,
   finding 15), account-level only
   for account-covering publications with `objId == P_pub` account id.

State/lifecycle:

- no new persistent state, background worker, cache, or queue. Routed
  resolution performs the same `mo_pubs`/`mo_subs` metadata reads
  `getSubscriptionMeta` performs per subscribed SELECT — same cost class,
  bounded by existing publication metadata; the split admission adds one
  row lock (`mo_pubs` under `P_pub`, already the case today) and one
  locked-row re-read per snapshot DDL;
- snapshot artifacts: `P_src` catalog (`account_name`/`database_name`
  naming the physical owner); quota charged to `P_src`; locks held for
  the duration of the single background transaction;
- failure semantics: revoked publication/suspended publisher/dropped
  subscription between classification and execution resolve under the
  statement transaction's consistent view of `mo_subs`/`mo_pubs`; the
  classifier re-runs at execution (I1). That consistent view alone does
  **not** cover a binding change committed mid-statement — a drop of
  `N` plus recreate against another publication or as a local database
  can land between classification and admission while leaving every
  `mo_pubs` fact unchanged (`pub_sub.go:110-129`); the alias form
  closes that window with the `(S, N)` subscriber database lock plus
  the post-frontier binding re-reads (§4.4 steps 3/5, finding 12), not
  with statement-level read consistency. Post-frontier drift fails
  closed before any write (§4.4 step 5). Catalog writes in
  `doCreateSnapshot` happen after classification/authorization inside
  one transaction — failure-atomic on error, including an error
  injected between the quota admission and the row insert.
  Publication-routed DROP is a single transaction: the scope binding is
  evaluated before the delete, so a rejected drop writes nothing. DIFF
  leaves no partial state. MERGE/PICK rejection happens before any
  destination write.

## 9. Verification plan

Cheapest deterministic evidence per invariant; focused proofs first.
Existing v1 base reproduction (20101/1146 at base `bc84bd1426`) is the red
side; the same statements run on the changed tree for green.

Unit tests (existing `pkg/frontend` mock `BackgroundExec` patterns):

- U1 classifier (§4.1): ordinary db → local with unchanged errors;
  live subscription + in-scope table → visible with `P_pub/P_src/D`
  (including a persisted foreign-source meta where `P_src != P_pub`);
  out-of-scope table → visibility-cause error; meta error propagation
  (revoked/suspended).
- U2 privilege probe (§4.1.1): out-of-scope subscription operand errors
  with the visibility cause instead of the synthetic SELECT's
  `table "t" does not exist`; a role denied SELECT on a visible alias
  still fails with the privilege error; genuinely missing local table
  keeps the engine no-such-table error.
- U3 identity threading (§6): with routed descriptors, the
  `mo_database` CP key and `mo_tables` scans receive the operand
  `physAccount`, not `ses.GetAccountId()`; local operands still receive
  `S`; the CP-key cache distinguishes two owners for the same database
  name; LCA and intermediate-node reopen order/identity union asserted
  with injected lookup failures (missing under one candidate → resolved
  under the other with the winner's CTS context; missing under all →
  preserved zero-history degradation).
- U4 snapshot form equivalence (§4.4/§4.5): alias statement and explicit
  statement emit identical `mo_snapshots` inserts for an ordinary
  publication (same account binding, `database_name = D`); foreign-source
  meta records under `P_src`; decoy (persisted database id mismatch)
  keeps failing coverage; account-covering (`DATABASE *`) forms record
  table/database targets under the publisher exactly as today.
- U5 split admission (§4.4): lock-key construction — D/T keys under the
  physical account for the statement's actual target, quota keys under
  the physical account, `mo_pubs` key `(P_pub.id, pubName)`; key order
  A→D→T→Q→P, with the subscriber database key `A = (S, N)` present in
  exactly the alias form and absent from the explicit form; for the
  explicit form with `P_pub == P_src` the key sequence is byte-identical
  to today's single-account helper; shape-aware resolver: database-scoped
  row resolves via persisted id, account-covering row resolves to the
  publisher (no `getDbAccountIdAndTypeById` call with id 0). Locked-row
  revalidation drift cases — membership removed, `database_id` retargeted,
  shape retargeted (`'*'` ⇄ concrete), table dropped from `table_list` —
  each fails closed before any write; an injected error between quota
  admission and the insert leaves no `mo_snapshots` row and no quota
  delta (failure-atomicity). Alias-binding cases (finding 12):
  classification captures `N`'s persisted database id and the
  `(pub_account_name, pub_name)` pair; post-frontier binding drift — `N`
  rebound to another publication, `N` recreated as a local database, `N`
  dropped, each simulated against the old `mo_pubs` row left fully
  intact (the `dropSubscription` precondition) — fails closed with the
  retry-class error; a committed-before-`A` rebind is caught by the
  re-reads alone (lock acquisition trivially uncontended); a blocked
  writer (rebind attempted while `A` is held) resumes after commit and
  the retried statement then classifies against the new binding —
  ordinary local path for a local recreate, the new publication's
  protocol for a rebound `N`. Rejected attempts leave no `mo_snapshots`
  row and no quota delta in either account.
- U6 drop binding (§4.5, I7): database-level records delete only on
  whole-database coverage **and** identity — database-scoped row with
  `table_list != '*'` (publication exposes `d.t1` of `d = {t1, t2}`)
  rejecting a database-level record of `d` even on a matching
  `record.objId`, the record asserted byte-identical afterwards
  (finding 11), with the whole-database control (`table_list == '*'`,
  same `objId`) deleting, and the permitted table-level control (a
  `TABLE` record of the published `d.t1`) deleting through the same
  table-limited publication; direct `objId` match (database-scoped) /
  current-identity match (account-covering) otherwise unchanged; a
  same-named dropped-and-replaced database's records keep their stale
  `objId` and are rejected and intact; table-level records bind by
  **logical identity** (findings 13, 14): with the current row present,
  deletion requires `record.objId ==` the row's `rel_logical_id` — COPY
  ALTER continuity cases (snapshot taken before the ALTER, and alias
  plus explicit snapshot creation after the ALTER, then
  publication-routed DROP) all delete, because
  `WithKeepLogicalId` preserves the recorded identity across the
  physical replacement (a `rel_id`-keyed lookup would reject the
  after-ALTER case — the finding-13 regression is asserted against);
  the same-database drop/recreate/publish counterexample (owner
  snapshot of private `d.t`; drop `t`; recreate `d.t`; publish only the
  replacement by name) rejects the old record with the scope error and
  the record asserted byte-identical afterwards, while a snapshot of
  the replacement deletes through the same publication and a
  whole-database (`table_list == '*'`) control deletes its own records;
  with the current row absent (dropped-after-snapshot), the
  `rel_logical_id`-keyed historical membership fallback at `record.ts`
  (exactly-one-visible-row and `reldatabase_id` validation; zero,
  multiple, or foreign-database rows fail closed) runs **only** under
  whole-database coverage; under a table-limited list the same state
  fails closed — including the finding-15 sequence: sys-published
  foreign-source publication listing only `t` (stale after the owner
  drops it, `updatePubTableList` per C10) over a database whose owner
  snapshotted private `d.t`, dropped `t`, recreated it, and finally
  dropped the replacement — the subscriber's DROP of the original
  record is rejected with the scope error and the record asserted
  byte-identical, and the same applies to the authorized replacement's
  own snapshot after its drop (owner cleanup retained through plain
  `DROP SNAPSHOT`), while a whole-database control over the same
  database still deletes its dropped-table records via the historical
  fallback and COPY ALTER keeps the name resolvable so the primary
  binding governs; records of replaced
  databases are rejected; account-level records delete only for
  account-covering rows with `objId == P_pub`'s
  id (other-account account snapshots rejected); foreign-source setup
  with an independent same-named `P_pub` row → only the `P_src` row is
  deleted, the `P_pub` row asserted intact; absence with/without
  `IF EXISTS` keeps today's semantics.
- U7 shared foreign-source gate (§4.4 step 2, finding 10): persisted
  `mo_pubs` rows mirroring `publication_view_scope_test.go:2036-2123` —
  (publisher=2, owner=3, targets={1})/ACCOUNT ALL foreign row → both
  CREATE and DROP reject with the stable unauthorized error before any
  physical resolution, and snapshot + quota state is byte-identical
  before/after; permitted controls — sys publisher (0,3,{1}), owner-only
  targets ({3}), ordinary (2,2,{1}) — admit under their normal checks.

Multi-tenant BVT (new file beside the git4data/publication trees):

- B1 happy paths at issue granularity: `CREATE SNAPSHOT s FOR TABLE N t`
  succeeds; from `P_src`/`P_pub` (ordinary publication: same account)
  `SHOW SNAPSHOTS` lists it with `database_name = D`; subscriber drops it
  via the explicit `DROP SNAPSHOT s FROM P PUBLICATION pub` while an
  unrelated publisher snapshot (different database, chosen to collide in
  no scope) is asserted intact before and after (positive lifecycle +
  out-of-scope non-deletion, finding 4).
  `DATA BRANCH DIFF N.t AGAINST N.t` → empty diff; after the publisher
  inserts a row, `DIFF N.t{MO_TS=before} AGAINST N.t` reports exactly that
  insert (nonempty historical difference — the issue's workflow).
- B2 mixed operands: `DIFF N.t AGAINST local.t` and reverse; `MERGE N.t
  INTO local.t` succeeds (read-side routing, `S`-owned destination);
  `MERGE local.t INTO N.t` and `PICK ... INTO N.t` fail with the
  write-prohibition error and the publisher table is byte-identical.
- B3 scope and revocation: table-scoped publication (`t1` only) —
  `N.t2` fails for DIFF and table snapshot with the visibility cause;
  after `ALTER PUBLICATION ... ACCOUNT DROP S` (or `DROP PUBLICATION`),
  DIFF/snapshot fail with the subscription-invalid cause.
- B4 foreign source + decoy (mirroring `pub_view_scope.sql:534-568`):
  sys publication over `vw02`'s database with a same-named publisher
  decoy; subscription reads and DIFF answer from `vw02` (physical
  source), never the decoy; the alias table snapshot records under
  `vw02` with `database_name = D` and is visible only from `vw02`'s
  `SHOW SNAPSHOTS`; the subscriber's scope-bound DROP removes exactly
  that row while both the decoy rows and any independent sys/vw02
  snapshot are asserted intact; a decoy-only table cannot be snapshotted
  or diffed; an unauthorized persisted foreign-source row (publisher
  general account, owner third account, sole target the subscriber)
  yields the stable authorization error on explicit CREATE and DROP
  with snapshot/quota state unchanged (finding 10).
- B5 non-admin subscriber (finding 3): a non-admin user of `S` with
  SELECT on the subscription runs B1's DIFF; a user without SELECT gets
  the privilege error; out-of-scope and genuinely missing local tables
  are distinguishably different errors.
- B6 lineage regression: two local tables cloned from the same
  subscription source diff with today's identical zero-history semantics
  (behavior byte-for-byte equal to the base: succeed with the same diff
  shape); `DATA BRANCH CREATE TABLE ... FROM N.t` and ordinary
  local-local DIFF/MERGE/PICK suites unchanged (existing git4data and
  snapshot BVT runs).
- B7 lifecycle revocation/retargeting and atomicity (findings 5, 12):
  deterministic interleavings in which an `ALTER PUBLICATION` — account
  revoke, `table_list` removal, or database retarget — races an in-flight
  alias table snapshot create: each run ends either in full success with
  one correctly-owned row or in failure before any insert; concurrent
  publication-routed drops follow the same all-or-nothing rule; aborted
  attempts consume no quota. Alias-rebind races (finding 12): a second
  session drops the subscription database `N` and recreates it (a) from
  a different publication and (b) as a plain local database, timed both
  before the in-flight create acquires `(S, N)` and while it is held;
  every run ends in full success against the serialized-consistent
  binding or in the retry-class/visibility-cause error, and rejected
  attempts leave no `mo_snapshots` row in either catalog and no quota
  delta; a reissued statement after the committed rebind targets the
  new binding (new publication's physical database, or the ordinary
  local path for the local recreate) — never silently the old physical
  database.
- B8 mixed lineage conflict (finding 6): `DATA BRANCH CREATE TABLE S.c
  FROM N.t`; publisher then inserts, updates, and deletes rows; subscriber
  makes independent updates on `S.c`. `DIFF S.c AGAINST N.t` and the
  reverse order both report the exact post-clone change sets (the foreign
  ancestor resolves — no whole-history/zero-history misclassification);
  `MERGE N.t INTO S.c` with independent post-clone updates on both sides
  produces the same conflict/dedup results as the equivalent local-local
  merge; `MERGE S.c INTO N.t` stays rejected (§4.3).
- B9 mixed-version compatibility (finding 7): collision setup — `P_pub`
  (or `P_src` for foreign sources) independently owns a same-named
  snapshot outside the publication scope; the new path's scope-bound DROP
  removes only the bound row, with assertions against **both** catalogs
  (`P_pub` and `P_src`) proving the unrelated row bytes-identical;
  absent-name DROP with and without `IF EXISTS` behaves as today; the
  same suite run against the base build documents the base behavior for
  the version-diff record.
- B10 `DATABASE *` lifecycle (finding 9): an account-covering
  publication (`sys`/authorized account, explicit target account):
  subscriber creates table-snapshot, database-snapshot, and
  account-snapshot records through the explicit forms (coverage as
  today); scope-bound DROP removes each; a DROP of an account-level
  snapshot of a *different* account through that publication is rejected
  and the record intact; absence with/without `IF EXISTS` asserted;
  identical statements pass against the base build (no regression of
  the pre-existing supported operations the v3 protocol break would have
  caused).
- B11 replaced-database retention (finding 8) and table-limited
  coverage (finding 11): private database `d` snapshot at database
  level and at table level (`t`); drop `d`; recreate `d` with the same
  name and table; publish the replacement to the subscriber; subscriber
  DROP of the old (`d`, `d.t`) snapshots via the publication is
  rejected with the scope error for both levels and both records remain
  byte-identical; the owner then drops them through plain
  `DROP SNAPSHOT` (unchanged path); the same sequence for a table
  dropped *after* snapshotting inside a still-covered database deletes
  successfully at table level through a whole-database publication
  (historical membership provable — the v7 fallback is `'*'`-only);
  under a table-limited list the identical final state fails closed
  with the record intact (§4.5).
  Table-limited publication set: publish `d` with `table_list = t1`
  only while `d` also holds unpublished `t2`; the owner creates a
  database-level snapshot of `d` (covering private `t2`) and a
  table-level snapshot of `d.t1`; the subscriber's publication-routed
  DROP of the database-level record is rejected with the scope error
  and the record is asserted byte-identical via the owner's
  `SHOW SNAPSHOTS` (rejected records remain unchanged); the
  table-level `d.t1` record deletes through the same publication
  (permitted table-snapshot control); a whole-database publication
  (`table_list == '*'`) over the same `d` deletes the database-level
  record (whole-database control). Logical-identity sets (findings 13,
  14): same-database replacement counterexample — owner snapshots
  private `d.t`; owner drops `d.t`, recreates `d.t` with fresh data,
  and publishes only the replacement `t`; subscriber publication-routed
  DROP of the *old* record is rejected with the scope error and
  asserted byte-identical via the owner's `SHOW SNAPSHOTS`, while a
  snapshot of the replacement deletes through the same publication and
  a whole-database publication control deletes its own table record.
  COPY ALTER continuity — owner snapshots `d.t`; owner runs
  `ALTER TABLE d.t ... ALGORITHM=COPY` (or the plain `ALTER` that maps
  to copy internally); subscriber then creates a table snapshot via the
  alias form on the subscription over `d` and via the explicit form;
  publication-routed DROP deletes both pre-ALTER and post-ALTER records
  (the recorded logical id survived the physical replacement), and the
  post-ALTER `rel_logical_id` catalog state (`WithKeepLogicalId`,
  original relation dropped) is asserted directly so a regression to
  `rel_id`-keyed matching fails the test. Name-absent authorization
  (finding 15): sys-published foreign-source set mirroring
  `pub_view_scope.sql:534-568` — owner snapshots private `d.t`; owner
  drops `d.t`, recreates `d.t`; sys publishes `d` with `table_list = t`
  to the subscriber; owner drops the replacement (the sys-owned list
  keeps `t`, C10); subscriber publication-routed DROP of the original
  record is rejected with the scope error and the record asserted
  byte-identical via the owner's `SHOW SNAPSHOTS`; the authorized
  replacement's own snapshot, taken before its drop, is likewise
  rejected after the drop and deleted by the owner through plain
  `DROP SNAPSHOT` (cleanup retained); whole-database control — an
  account-covering or `table_list == '*'` publication over the same
  database — deletes its dropped-table records via the
  `rel_logical_id`-keyed historical fallback; COPY ALTER control — the
  altered name stays resolvable and the primary
  current-`rel_logical_id` binding governs deletion before any drop.

Counterexample orientation: the fix must not pass while the defect
survives — BVT asserts the new success cases AND the continued rejection
of (a) subscription destinations, (b) out-of-scope tables, (c) revoked
subscriptions, (d) decoy resolution, plus (e) the absence of any
subscriber-catalog snapshot row for alias snapshots (I2), (f) the
survival of unrelated snapshot rows in every catalog a publication-routed
DROP can reach (I7), (g) nonempty conflict results wherever committed
foreign lineage exists (no whole-history/zero-history shortcut on
supported mixed operands), (h) retention of replaced-database snapshots
after drop/recreate/publish (identity-pinned deletion, §4.5), (i)
rejection of unauthorized persisted foreign-source rows on the explicit
paths with unchanged catalog and quota state (§4.4 step 2), (j)
rejection of database-level deletions through table-limited
publications with the record unchanged (coverage-pinned deletion,
§4.5), (k) no snapshot row and no quota residue from an alias
create raced by a drop-and-recreate of the subscription database
(binding-pinned admission, §4.4 steps 3/5), and (l) survival of a
same-database replaced table's old snapshot against the name-only
publication list while COPY-ALTERED tables keep their snapshot
lifecycle (logical-identity-pinned deletion, §4.5), and (m) survival of
every table-limited snapshot — never-published original, replaced
object, or authorized table — once the authorized name no longer
resolves, including under a foreign-owned publication whose stale list
still names it (name-absent fail-closed, §4.5), with the
whole-database fallback retained for `'*'` cleanup.

## 10. Drawbacks, risks, open questions

- `SHOW SNAPSHOTS` never lists publisher-owned artifacts from the
  subscriber (existing C2 contract, unchanged). Documented limitation;
  subscriber lifecycle runs through the explicit
  `... FROM P PUBLICATION pub` forms.
- §4.5 changes the explicit form's foreign-source outcome from a fail
  (retry/coverage mismatch) to correct resolution. Scoped deliberately:
  the failure was never specified, the decoy guard is preserved, and no
  legacy cross-owner artifacts can exist.
- The §4.5 scope binding deliberately narrows a C8 exposure: a subscriber
  can no longer delete an out-of-scope same-named snapshot through any
  publication. This changes behavior users could technically observe on
  main, in the direction of the documented authorization boundary; the
  record owner's own DROP SNAPSHOT path is untouched and always
  sufficient. Finding 11 adds a second narrowing in the same direction:
  database-level records are no longer deletable through table-limited
  publications (`table_list != '*'`) even when the record's identity
  matches — mirroring the creation boundary that base code already
  enforces; table-level deletions through such publications keep
  working, and the whole-database publication keeps the database-level
  lifecycle. Findings 13/14 add the identity-dimension correction:
  table-level deletion binds by logical table id (C10), so snapshots
  of a table since dropped and recreated lose their subscriber-side
  deletability by name — again in the C8 direction and leaving the
  owner's own DROP untouched — while COPY-ALTERED tables keep their
  full snapshot lifecycle (creation before or after the ALTER, and
  publication-routed deletion), because the binding uses the identity
  the snapshot format itself records. Finding 15 adds the boundary for
  dropped tables: a table-limited publication deletes table-level
  records only while the authorized table still resolves; after its
  drop the name-only list cannot testify which logical identity it
  authorized (foreign-owned lists stay stale, C10), so cleanup reverts
  to the record owner — a further narrowing in the same C8 direction.
  Whole-database (`'*'`) publications keep the historical
  `rel_logical_id` fallback, which is sound there because every logical
  identity ever anchored to the covered database is in scope by
  construction.
- The alias form's admission now holds the subscriber database lock
  `(S, N)` for the duration of one background transaction (§4.4 step 3,
  finding 12): subscription DDL on the same `N` (drop/recreate) briefly
  serializes behind an in-flight alias table snapshot create. Bounded —
  one extra lock in a statement class that fails on base, no new waits
  for any existing statement — and acyclic by the head placement: no
  transaction holds `D`/`T`/`Q`/`P` keys while requesting a subscriber
  database key.
- On a rolling upgrade where an old CN processes the documented cleanup
  statement for a foreign-source snapshot, the physical row survives
  (fail-safe residue, §5); the statement takes effect when re-issued on a
  new CN. The residue cannot be misused (snapshot reads stay
  account-scoped).
- Table-level drop binding reads the recorded table's historical
  `mo_tables` membership at the record's timestamp (catalog snapshot
  read, as in restore); beyond catalog retention the read fails closed
  and the record is retained for the owner. Bounded: one point lookup
  per drop.
- Subscription-source clones whose lineage carries a third-party
  subscription ancestor neither endpoint owns keep today's zero-history
  degradation (B6): correct but lossy diff range, unchanged from base;
  revisiting it needs a lineage-owner recording change, out of scope.
- Routing rechecks `mo_pubs`/`mo_subs` per statement (same class as a
  subscribed SELECT).
- Open, non-blocking: exact error message wording for the visibility
  causes, the scope-binding error, and the write prohibition
  (implementation review decides within I5/I7).

## 11. Review record

```text
Change scope: subscription-table routing for table-level CREATE SNAPSHOT
(alias + aligned explicit form, split-identity admission with a shared
foreign-source gate, identity-bound publication-routed DROP,
DATABASE * lifecycle preservation) and DATA BRANCH read operands, with
explicit write-destination rejection, per-operand and per-lineage-node
identity threading, and a shared classification boundary in front of
the privilege probe
Trigger: authorization/tenant-isolation boundary change across snapshot
lifecycle, DATA BRANCH execution, and publication metadata ownership
Design: docs/design/20261023-subscription-table-branch-snapshot-routing.md
version 7 (revision for the v6 P2 finding), pending independent
approval
Blocking findings: pending review
Decision log: v1 → keep. B chosen over A/C additionally because C2
publisher recording was disproved for foreign sources (owner = P_src).
v2 adds: three-identity model (S/P_pub/P_src); operand descriptor identity
threading incl. LCA reopen union rule and preserved zero-history
degradation; classification boundary before the RBAC probe; explicit-form
alignment for foreign sources with persisted decoy guard.
v3 adds: I7 scope-bound deletion (closes the pre-existing coverage-blind
publication-routed DROP, C8); split-identity RC admission with locked-row
post-frontier revalidation (two-account admission variant; P_pub keeps
the mo_pubs key + coverage query, P_src owns D/T/Q keys, artifact and
quota); lineage-node owner union for every non-endpoint reopen and node
CTS (owner mismatch on supported mixed operands no longer degrades to
zero-history; third-party ancestors unchanged); mixed-version namespace
matrix with collision/absent-name validation.
v4 adds: I7 rebound to recorded identity — database-level drops compare
record.objId directly (ValidateSnapshotScope precedent), table-level
drops prove historical mo_tables membership read at record.ts, and
account-level drops admit only account-covering publications on the
publisher's own account id; shape-aware owner resolution preserving the
DATABASE * (dbId=0) lifecycle end to end; the existing foreign-source
admission rule applied as a shared gate to the locked row on alias and
explicit CREATE/DROP alike, with decoy-safety on all paths.
v5 adds: I7 gains the coverage dimension — database-level deletion now
requires whole-database scope (table_list == '*' on database-scoped
rows), the deletion-side counterpart of the existing creation boundary
(snapshot.go:4592-4597), so a table-limited publication can never delete
a database snapshot covering objects it does not publish (finding 11);
I1/§4.4 gain binding-pinned alias admission — the subscriber database
key (S, N) is taken at the head of the admission lock order
(A→D→T→Q→P), and after the frontier advances the persisted database id
of N under S and the live mo_subs mapping to the captured publication
pair are re-read and compared, because dropSubscription leaves the old
mo_pubs membership intact (pub_sub.go:110-129) and a mo_pubs-only
revalidation cannot see a drop-and-recreate of N against another
publication or as a local database (finding 12); §5/§8 scope the old
consistent-view claim accordingly; U5/U6/B7/B11 gain the alias-rebind
race cases, the table-limited coverage negative with both controls, and
rejected-records-unchanged/residue-free assertions.
v6 adds: C10 records the logical-identity contract — table snapshots
persist rel_logical_id (snapshot.go:569-601 via authenticate.go:2165),
SnapshotTableID/ValidateSnapshotScope compare it everywhere, COPY ALTER
preserves it across a physical replacement that internally drops the
original relation (alter.go:1190-1191/1743-1754,
lifecycle_admission.go:399-402), and publication table lists persist
names only (publication_subscription.go:2735-2756). I7/§4.5 re-key the
table-level binding by that logical identity: with the published name
currently resolvable, record.objId must equal the current row's
rel_logical_id — COPY ALTER continuity preserved, drop/recreate
substitution rejected (finding 14); with the name gone, historical
membership is read at record.ts filtered by rel_logical_id (never
rel_id) with exactly-one-visible-row reldatabase_id validation
(finding 13), so the dropped-after-snapshot cleanup contract survives
without reachability by a later same-named replacement. U6 gains the
COPY ALTER continuity cases (snapshot before/after ALTER; alias and
explicit post-ALTER creation; publication-routed deletion; explicit
regression assertion against rel_id-keyed matching) and the
same-database drop/recreate/publish counterexample with controls;
B11 gains both sets end to end; counterexample list gains (l);
§10 discloses the identity-dimension narrowing.
v7 adds: C10 records that publication name lists go stale —
updatePubTableList (ddl.go:4940-4943 → pub_sub.go:138-178) scrubs a
dropped name only from the dropping account's own publications, so the
supported sys-published foreign-source arrangement
(pub_view_scope.sql:534-568) keeps names whose objects are gone.
I7/§4.5 narrow the name-absent fallback to whole-database coverage:
under table_list == '*' every logical identity ever anchored to the
covered database is in scope by construction and the
rel_logical_id-keyed historical membership read remains sound, while a
table-limited list whose authorized name no longer resolves fails
closed — historical database membership proves location, never
authorization (finding 15) — with every table-limited record
(never-published original, replaced object, or authorized table dropped
later) reverting to owner cleanup. U6/B11 gain the sys-published
foreign-source replacement-then-drop sequence with the original record
asserted byte-identical, the authorized replacement's post-drop
rejection with owner cleanup retained, whole-database fallback
controls, and COPY ALTER resolvable-name controls; counterexample list
gains (m); §10 discloses the name-absent boundary.
Decision: PENDING
Implementation deviations: none yet
```
