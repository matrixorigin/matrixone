# #28935 design revision D2-DTS-1

Disposition: replace the v1 generic dependency-certificate recommendation with a bounded direct-table SELECT candidate. This document is source-qualified design, **not implementation approval or test PASS**. Its supported domain includes the two reported empty-policy fixtures. Broader policy/SQL migration remains explicitly unsupported. Root must decide whether this bounded contract is sufficient, and independently review it before production edits.

Source: `2d616a3847a59cb09560fa119ff6b35b745d3caf`; base: `main@e0314478159c80b062483c307180b02dc009dbc0`. Fresh GitHub read verified own author/fork `VioletQwQ-0`, OPEN/Ready, branch `codex/issue-28910-crc32-json-20260915`, exact source/base. Owning clean worktree: `/Users/violet/.codex/automations/matrixone-open-issue-pr/evidence/parallel-ready-sol61-20261014.htJ8FGSh/pr28935`, branch `codex/ready-sol61-28935-htj8`. All source anchors below refer to that worktree at this commit. Current main allocation has not been refreshed for publication; **124 is provisional**, not blocked by another unmerged claim.

Read in full: `../28935-design-review.md`. v1 is preserved at `heartbeat-20261019-2031.PiKvSO/worker-28935/migration-contract-proposal.md`. The original finding ledger remains untouched/open. Reported CI failure is still FAIL; nothing here changes that result.

## Four review blockers: source-checked answers

| Review blocker | Concrete v2 answer | Boundary |
|---|---|---|
| 1. Unnamed pre-optimizer capture/hash | Existing `Query.CatalogDependencies` is populated at table resolution before column pruning (`pkg/sql/plan/query_builder.go:13070-13101`), including views; `resetPreparePlan` merges it into `Prepare.Schemas` (`pkg/sql/plan/utils.go:3831-3840`), and `prepareSchemaRefWithSnapshot` already records DB ID, table ID and schema version (`build_dcl.go:587-611`). Restrict SQL to one direct table/column-only SELECT and reject views/generated/unknown metadata. Use explicit small typed witness fields; no graph, no hash, no new pre-optimizer observer. | The witness certifies only the enumerated shape and one unchanged ordinary catalog object. It does not certify arbitrary expressions/views. |
| 2. Unnamed target dry-bind under future state | The admitted grammar has no params, variables, unqualified table, literals, function/type syntax or SQL-mode-sensitive tokens. Parse transferred `sql_mode` from a local typed decode; look up the one catalog object at an admitted cache snapshot. No `doComQuery`, `USE`, `TxnHandler.Create`, live compiler mutation or policy capture in this preflight. Current target authorization/policy remains owned by actual `prepareStmtMigration.Migrate`, `beginAuthorizationEpoch`, `captureRewritePolicy`, `buildPlanWithAuthorization` and `self_handle.go` checks. | v2 admits only an empty captured source policy and an empty **fresh target** policy. Nonempty target policy causes refusal before target PREPARE, followed by target disposal if replay already began. This is an explicit unsupported positive case, not a claim to support all policy migrations. |
| 3. Two hashes do not pin a generation; direct RPC cannot inherit proxy cleanup | Preflight snapshot is advisory admission, not a snapshot lent across USE/SET. Actual target PREPARE uses its normal statement transaction, an exact-object `CompilerContext.Resolve` guard, and `CatalogReadStamp`/workspace-visibility checks through `SetPrepareStmt`. Retain the small contract on the migrated handle: schema/mode/protocol rebuild requires explicit reprepare. Add explicit target failure disposition to RPC. Pre-replay rejection leaves direct target intact; after replay starts, only an opted-in target may be closed. Suppress certified-path CN-global variable writes, which closing a connection cannot roll back. | No promise that a failed post-replay target retains its old session state. No claim that a fixed statement snapshot locks out concurrent committed DDL. |
| 4. Actual test omits capability | At `issue_29148_test.go:190-192`, add existing `PreparedStatementProtocolSupported: true` to the successful control-RPC client; also explicitly opt its target into post-replay close-on-failure. Add independent absent-capability and absent-target-disposition negatives. All original success/result/policy/stale-handle assertions remain. | The current unmodified helper would still fail export under v1/v2. This setup change is required and must not be hidden. |

## 1. Enumerated supported domain and proof production

New helper names in this document are proposed APIs, not existing functions. Existing APIs are identified separately.

`parseDirectTableSelectV1(ctx, sql, sqlMode) (directSelectShape, bool)` is a pure bounded parser/classifier. Admit exactly:

```sql
SELECT c1 [, c2 ...] FROM database.table [ORDER BY c1 [ASC|DESC] [, c2 ...]]
```

Require one `*tree.Select` containing `*tree.SelectClause`; projections/order operands are unqualified non-star `*tree.UnresolvedName` with no aliases. FROM has one `*tree.AliasedTableExpr` / parser's single-sided CROSS wrapper, no alias/index hint/right relation/condition/option/tolerance; leaf is `*tree.TableName` with explicit schema, no catalog, time travel or Iceberg ref. No WITH/WHERE/HAVING/GROUP/window/DISTINCT/options/limit/lock/INTO/export/union/subquery/parenthesis wrapper/parameter/UDF/function/cast/arithmetic/hint. Every remaining AST field must be its zero value. Check both raw retained SQL and saved AST, not just the optimized plan. Require raw identifiers `[a-z_][a-z0-9_]{0,63}`; keywords may vary case; allow only ASCII whitespace, identifiers, comma and dot tokens. Reject comments, quotes, escapes, semicolons and executable comments. This lexical domain removes all six parser-flag-sensitive constructs; parsing in the decoded source mode must still produce the exact same structural shape. Unknown AST fields/forms refuse admission.

Bounds: SQL <=4096 bytes, <=16 projected names and <=16 order names; order names must be projected; <=16 prepared items per certified migration, <=64 visible table columns, <=8 KiB encoded witness per item and <=64 KiB witnesses per request. Duplicate projected names may be rejected in v1; duplicate metadata names/seqnums always reject. Overflow/unknown/cancel returns `OkExpectedNotSafeToStartTransfer` (or caller cancellation), not a partial witness. Keep existing 16 MiB limits for each typed variable snapshot.

`sourceDirectTableSelectV1(ctx, ses, st, catalogReader)` runs only while the existing `migrateController` exclusively owns the source session (`migrate.go:96-122,177-201`; export takes the operation at `routine.go:764+`). The stored statement is first authoritative owner, not proxy SQL history. It must satisfy all of:

- Existing `getPrepareStmtsForMigration` policy provenance/monotonic invalidation checks (`session.go:3162-3188`), captured empty policy (`rewritePolicyCaptured && !rewritePolicyEnabled`), empty remap, no `needsRebuild`, current local DDL/temp/protocol generation, and no active source transaction/private catalog state. Original retained SQL and saved AST match the grammar descriptor; both `st.ParamTypes` and `Prepare.ParamTypes` are empty. Captured parser mode must agree with the exported typed `sql_mode`.
- `st.PreparePlan.GetDcl().GetPrepare().Schemas` contains exactly one ordinary nonhistorical/nonpublisher/nonsubscription object with positive DB/table IDs. Its `Server` is the uint32 schema version, **not** a server address. `build_dcl.go:601-604` assigns precisely these fields. `Query.ViewReferences` must be empty; all `CatalogDependencies` must name that same object. Walk stored bound expressions using the existing expression visitor and reject any CRC32/unknown retained owner. This is an additional cross-check, not the completeness argument.
- Read the **complete**, unpruned source schema from the existing catalog cache at a current admitted snapshot, and require its account/DB/name/table ID/version to match the captured `Schemas` tuple. Source metadata changed or missing => refuse; do not rebind or modify the source handle. Cache `TableItem.TableDef` is built from all catalog columns (`cache/catalog.go:809-842`), whereas the optimized scan's column slice can be pruned (`query_builder.go:13098+`). Never infer a complete table schema from the pruned scan.
- Admit only `catalog.SystemOrdinaryRel` (`"r"`), with no view SQL/table-function/dynamic/temp/subscription/partition/feature/index/FK/check/cluster-by dependency. No generated column anywhere, including unprojected columns. All visible columns are ordinary non-auto-increment INT32 (`types.T_int32 == 22`), with no string/enum/collation flags; encode their name/seqnum/width/nullability explicitly. Hidden columns are limited to `catalog.Row_ID`/`T_Rowid == 101` and the normal fake primary key `catalog.FakePrimaryKeyColName`/`T_uint64 == 28`, with no generated expression. Accept only the normal fake-PK metadata, not arbitrary hidden expressions. The actual fixture creates `t(id int, tenant int)` (`issue_29148_test.go:72`), so this domain includes its catalog shape without pretending every table qualifies.

The source provenance is closed without capturing every expression before optimization: admitted raw SQL has no expression-producing syntax; the one resolved object is proven an ordinary, nongenerated base table; existing pre-pruning dependency records prevent a view/removed scan from being mistaken for that table. A view `v` exposing a pruned CRC expression, a CASE with a dead CRC branch, a generated-column table, a plugin index and an empty-result rewrite all fail explicit gates.

Only emit witnesses after successful complete typed user/system-variable export (`routine.go:886-927`), with no temp tables or user-level locks. Overflow/raw fallback keeps the source pinned. A failed export does not stamp the source handle or change its execute behavior.

## 2. Explicit wire fields; no canonical graph digest

Proposed additive schema, field numbers checked only against source2d (not a publication allocation claim):

```proto
message DirectSelectColumnV1 {
  string Name = 1;       // lowercase ASCII identifier
  uint32 Seqnum = 2;
  int32 TypeID = 3;      // exactly 22 in v1
  int32 Width = 4;       // nonnegative; exact source metadata value
  bool NotNullable = 5; // exactly ColDef.Typ.NotNullable from the catalog reader
}
message DirectSelectOrderV1 {
  string Column = 1;
  bool Desc = 2;         // omitted/default ASC normalize to false
}
message PreparedDirectTableSelectV1 {
  uint32 Version = 1;    // exactly 1
  uint32 AccountID = 2;
  uint64 DatabaseID = 3;
  uint64 TableID = 4;
  uint32 TableVersion = 5;
  string DatabaseName = 6;
  string TableName = 7;
  repeated DirectSelectColumnV1 Columns = 8;
  repeated string Projection = 9;
  repeated DirectSelectOrderV1 OrderBy = 10;
}
// PrepareStmt: PreparedDirectTableSelectV1 DirectTableSelect = 5;
// MigrateConnToRequest: bool CloseTargetOnReplayFailure = 17;
```

Columns are sorted by `(Seqnum, Name)`; Projection/OrderBy retain SQL order. Compare semantic fields explicitly, including array lengths and duplicates; protobuf byte ordering, map ordering, pointer equality, `String()` output and a SHA hash are not proofs. Names and IDs are both checked: drop/recreate with the same name and rename/version changes refuse. Width is copied exactly; scale=0, enumvalues empty, auto-increment=false and string/collation flags absent are admission requirements, not omitted unknown semantics. Normalize default ORDER direction to ASC; require default NULL placement.

Retain `Name/SQL/ParamTypes/RequiredProtocolVersion` fields 1-4. For any witness set the existing required floor to at least provisional124, and require existing source-request `PreparedStatementProtocolSupported=true`. This reuses its literal promise to enforce a floor; it does not redefine that bool as proof. Current proxy advertises it (`conn_migration.go:41-46`) and checks floors **before** `SET transferred=1` (`:125-190`). The verified exact base e031 already contains the floor field/receiver check; source diff only adds CRC preflight and v123. This is not a new check of the current remote main tip. No new source-request capability is necessary for safety.

Compatibility: old source/no witness → retain current catalog-free receiver rule; current-source/old proxy with absent capability → source refuses this new path; an older proxy already advertising generic floor support may forward the witness but omit field17, so the new receiver safely refuses before user replay. New proxy sets field17 only for its unpublished backend. Target <provisional124 fails existing floor checks. v123 remains the CRC32 execution-identity floor; 124 is a distinct provisional receiver/lifecycle contract and needs an actual-main allocation check before publication.

## 3. Target preflight: pure local decode + existing admitted metadata reads

`preflightDirectTableMigrationV1(ctx, ses, req) (*directMigrationAdmission, error)` is a NEW helper called after target routine exclusive lifecycle admission but **before `migrateOnce.Do`, deferred counter restoration and `Migrate` mutations**. For a request containing any witness:

1. Require field17, complete typed user/system snapshot flags, empty `SetVarStmts`, empty temp/user-lock lists, valid LastInsertID marker and per-item protocol floor, bounded item/witness counts, target account equal to witness account, and `req.DB == witness.DatabaseName`. Require target no active txn, prepared handles, temp tables or foreign resources. Existing user variables/counters may exist, but the caller explicitly relinquishes them if replay starts; no pretending this is a fresh session merely because it has a ConnID.
2. Run existing `decodeUserDefinedVars` / `decodeSessionSystemVars` into local values; they allocate/decode but do not install state (`user_var_migration.go:215+,278-366`). Validate dynamic session definitions/type conversions without invoking `UpdateSessVar` hooks. Obtain a unique typed `sql_mode`, parse the bounded SQL and compare its exact shape to the witness. No parameter/value-dependent binding is admitted. Other transferred variables cannot alter this grammar's CRC identity. Actual restoration can still fail later; that is the explicit disposable-target phase.
3. Obtain `*disttae.Engine` by the existing EntireEngine-unwrapping pattern (`rewrite_rule.go:138+`). Call `AcquireCatalogSnapshot(ctx, ses.getLastCommitTS())`, require supported/non-nil admission, then `ValidateCatalogSnapshot` and always `admission.End()` (`disttae/privilege_cache.go:61-68`, `txn/client/snapshot_admission.go:40-88,115-146`). These APIs admit a fixed applied timestamp; they neither start a session transaction nor install session values. Query `cache.GetDatabase(&DatabaseItem{AccountId, Name, Ts:S})` then `cache.GetTable(&TableItem{AccountId, DatabaseId, Name, Ts:S})`; require matching DB/table ID/version, complete TableDef and the same plain-table column witness. `GetDatabase`/`GetTable` are existing timestamp-keyed readers (`cache/catalog.go:499-518,606-622`); missing history/schema/cache refuses. Do not use the latest-only `GetTableByName`, or resolve a foreign publisher.
4. Prove the certified path will not write CN-global state. `snapshotSessionSystemVars` exports **all** dynamic session definitions, not merely changed values (`user_var_migration.go:131-159`). Three names are CN-global effects (`hasMigrationRuntimeSideEffect`, `:58-65`; `session.go:4780-4783`). For each, require source `RuntimeValue` presence/value to match target runtime presence/value exactly, after typed decoding; absent/absent is accepted. In the certified replay path, restore its local session value but **skip the CN-global setter entirely**. Do not restore an old global on failure; that could overwrite a concurrent actor. Other variable setter effects inspected here are session-local (the only registered UpdateSessVar callback is timezone). A mismatch refuses before user replay. Recheck presence/value before successful reply; independent concurrent global changes are not rolled back.

This is deliberately not a dry SQL bind. `buildPlanWithStats` calls `ses.SetAccountId` and `ctx.SetContext` (`mysql_cmd_executor.go:4333-4388`); the existing view-schema helper requires an already active statement/txn and freezes the **live** environment (`view_schema_provider.go:56-58,83-126`), not req's future state. Neither is a valid initial migration scratchpad. The narrowed grammar removes the need to invent one.

Preflight's catalog read is an internal compatibility read, not a grant decision. Do not cache or import source authorization. Auth revocation/current role rules must still be checked by target PREPARE, under the target authenticated principal.

## 4. Actual binding generation, current policy and publication

Do not carry preflight S into `USE`/SET and hope it remains active. Preflight may see S1; actual PREPARE owns S2. The exact source object/version tuple is the contract between them, not a promise that S1==S2.

Proposed `directTableMigrationCompiler` embeds the actual `plan.CompilerContext` and guards `Resolve` before returning its metadata to the planner. Only the named one object, same account/DB/table ID/version and nongenerated INT32 schema may resolve. Reject historical/subscription/foreign/temp contexts and alternate ResolveById/index paths in this v1 domain. Install this wrapper only for the migration's PREPARE by a private `ExecCtx.directTableMigration` field (`types.go:1155+`); pass it at the existing `createPrepareStmtInSession` call to `buildPlanWithAuthorization` (`mysql_cmd_executor.go:3163-3165`). The planner uses that passed context (`:4270-4318`) and resolves the table before pruning/view expansion (`query_builder.go:13070+`). A table replaced with a view between S1 and S2 is refused **before** its stored SQL can be bound, not merely noticed after PREPARE.

The live target statement already owns a transaction/workspace before frontend dispatch (`mysql_cmd_executor.go:5744-5766,5784-5803,5862-5897`). After its statement boundary, sample `TxnOperator.CatalogReadStamp` and workspace `CatalogVisibility`; check the same txn pointer/ID/snapshot/revision/workspace visibility, execution generation, target identity, local DDL/temp versions and protocol before publishing the candidate at **both** `SetPrepareStmt` call sites (`mysql_cmd_executor.go:2924,3051`). There is an existing full check pattern in `view_schema_provider.go:156-185`; reuse its proven fields in a small private guard, not the whole ViewSchemaRequest. Stamp is a statement-domain check, not a concurrent workspace lock (`txn/client/catalog_read_stamp.go:19-35`). Lifecycle admission already excludes concurrent session SQL.

After actual build, verify `Prepare.Schemas` still contains exactly the witnessed object, no ViewReferences/foreign dependency and no CRC32 in the actual bound plan, then attach the small contract to the new target `PrepareStmt`. Check cancellation and guard before `SetPrepareStmt`. `buildPlanWithAuthorization` keeps its existing plan-based permission check; `self_handle.go:82-120` keeps its later prepare/execute authorization check and removal on failure.

Current-policy boundary is explicit: target `prepareStmtMigration.Migrate` still starts `beginAuthorizationEpoch`, calls fresh `captureRewritePolicy` (`session.go:4432-4442`), and **v1 refuses if the captured policy is enabled/nonempty**. No source role/policy proof is installed. For empty-policy handles, normal `SetPrepareStmt` generation ownership and later `GetPrepareStmt` policy refresh/invalidation remain (`session.go:3035-3099,3114-3145`). Thus an already active target rule is never bypassed; the reported post-migration ALTER ROLE → filtering/need-reprepare oracle is retained.

To close a DDL change after S2 binding but before RPC/proxy publication, do not claim two comparisons freeze global time. Publish an immutable bound generation identified by the witnessed IDs/version. Retain `PrepareStmt.directTableMigration` on the target and, at existing schema/mode/protocol rebuild decision (`computation_wrapper.go:1343-1403`), return existing need-reprepare for that handle instead of transparently rebuilding it from newly resolved catalog SQL. Statistics-only changes may recompile only with the same guard. A newer table/view generation therefore cannot silently enter this migrated handle; explicit client PREPARE creates a fresh ordinary handle. This conservative invalidation behavior is part of the v2 contract and requires a regression oracle. It avoids a generic catalog lock/snapshot surviving across statements or proxy publication.

## 5. Failure ownership and retry states

Existing `migrateController.beginOperationWithContext` waits for SQL requests and excludes new requests, not merely cleanup (`migrate.go:96-122,177-224`). Existing target `migrateOnce` caches failures inside `Routine.migrateConnectionTo` (`routine.go:706-728`). Proposed states:

| State | Failure result/owner |
|---|---|
| BEFORE_ADMISSION / PURE_PREFLIGHT | No user state replay, no counters deferred, no `migrateOnce` consumed. Return expected refusal/cancel; direct target remains usable. Source still executes. |
| REPLAY_STARTED | Requires field17. Target state may have changed; any error/cancel/panic seals target admission while still owning operation (`mc.startClose`, nonwaiting), ends the operation, then closes target protocol via existing routine kill/close path. Never call synchronous cleanup while its own operation is active: cleanup waits in `waitAndClose`. |
| TARGET_SUCCESS / reply not confirmed | Proxy closes its unpublished `ServerConn` on error/ambiguous RPC outcome (`client_conn.go:1283-1300`). An explicit direct-RPC caller must close its opted-in target if SendMessage success is not confirmed. The target may have completed despite transport error; no retry on that same ConnID. |
| PUBLISHED | Proxy replaces/closes source only after successful migration (`tunnel.go:979-1036`). Failed attempts retain/resume source. Later schema/policy drift returns need-reprepare, never reuses a stale contract. |

`CloseTargetOnReplayFailure=false` is default and cannot silently opt a pre-existing direct target into disposal. New proxy sets true for its unpublished backend; the test helper explicitly sets true and closes target on unconfirmed RPC. Reject certified requests without it before replay. Preflight retry on untouched target is allowed; once replay begins, failure/uncertain success requires a fresh target. Existing proxy retry ownership does not authorize server-side changes to arbitrary direct targets.

For certified requests the proxy must gate full typed state, both snapshot-too-large flags false, no raw replay and no temps **before** its internal marker at `conn_migration.go:182`. Then the branch at `:196-206` and system fallback at `:251-253` cannot run. The internal `SET transferred=1` still occurs on the disposable proxy target; do not call the entire proxy path zero SQL. The zero-replay claim applies to transferred user state before CN preflight; proxy setup is discarded with its unpublished target. On the target, field17 admission/preflight must be outside the existing once body; errors after the first USE/user-state mutation require disposal, not fake rollback.

CN-global side effects are suppressed as specified above; temporary clones and user locks are excluded. Closing the target then owns the remaining partial session/txn/handle resources via existing `Routine.cleanup` (`routine.go:615-703`). Source state is never compensated or deallocated on a rejected transfer.

## 6. Exact fixture correction and deterministic oracles (all NOT_RUN)

Successful `migrateIssue29148PreparedConnection` setup changes:

```go
// MigrateConnFromRequest, current source lines190-192:
PreparedStatementProtocolSupported: true,
// MigrateConnToRequest, current source lines203-210:
CloseTargetOnReplayFailure: true,
```

Its target is the dedicated authenticated `openUser(targetCN)` connection; the helper must close it on any unconfirmed target-RPC result. Assert a version1 witness, source object/version/INT32 schema, protocol floor, typed snapshots and no raw fallback. Retain the exact original assertion at line194 and all rows/current policy/stale handle/reprepare assertions at lines124-155. The procedure path does not replay CALL: both producers converge at `createPrepareStmtInSession`, and migration transports the resulting concrete SELECT SQL.

Focused proposed fixtures:

1. Both existing real two-CN sql/procedure migration subtests succeed. New separate source request with capability omitted returns expected refusal, then source `execute owner_handle` still returns `[1,2]`; no target request is made.
2. Valid witness + omitted field17 rejects before USE/vars/counters/handles; seed a direct target with DB/@keep/LAST_INSERT_ID to observe it. Correct the cause and retry the untouched target to prove preflight did not consume once. Replay-started failures instead assert target connection closed, source usable and retry uses a fresh target.
3. Dead CASE CRC, constant/pruned CRC, any view including one hiding a pruned CRC, generated column on unprojected output, UDF, params (including JSON/ANY), SQL-mode-dependent quoting, unqualified DB and unknown AST nodes all reject. Existing catalog-free literal CRC migration cases retain their current oracles.
4. Deterministic barrier after preflight: replace table by same-name view with CRC or increment schema version before actual Resolve. Guard refuses before view bind/PREPARE publication. Another barrier changes CatalogReadStamp/workspace revision between Resolve and SetPrepareStmt: candidate closes, target disposed, source unchanged. DDL after SetPrepareStmt but before response/first execute produces need-reprepare; it must not silently bind the new view.
5. Revoke target SELECT or add an actual current target role rule before PREPARE: no stale source auth/policy accepted. Revocation yields auth error; nonempty target policy yields expected refusal/disposal. After an admitted empty-policy migration, retain original ALTER ROLE filtering and stale-handle NeedReprepare assertions.
6. Source RuntimeValue present/absent or value differs from target → preflight refusal and no user/CN-global writes. Equal states → injected later auth/cancel failure closes target while service runtime map values/presence stay unchanged. Do not use equal fallback defaults as proof that absent and present are interchangeable.
7. Protocol matrix: absent witness/table request stays refused; absent source capability refuses; old proxy missing field17 refuses on new target; target below required floor sees **no** marker/raw SET/RPC replay. Current-binary synthetic capability tests are not a mixed-binary rollout PASS.

Use existing frontend `crc32_migration_test.go`, routine/migrate lifecycle tests and proxy `conn_migration_test.go` for bounded seams; real migration remains `pkg/tests/issues/issue_29148_test.go` plus relevant embed coverage. Existing access-control/CRC32 distributed SQL BVTs have no discovered rebalancer/control-RPC hook; do not weaken their results or invent a BVT migration oracle.

## 7. Review decisions, unsupported boundaries and smallest next step

This candidate is narrower than v1 and materially restores the **reported empty-policy direct INT-table/procedure cases**, subject to implementation and real test evidence. Its proof construction does not need a generic graph, catalog snapshot transport, source plan transport or hash canonicalizer. Existing metadata capture and current authorization remain first owners.

The following are precise unsupported boundaries, not deferred implementation guesses: nonempty source or target policy at the instant of migration; arbitrary SELECT expressions/WHERE/joins/views/generated/text/JSON columns; active transaction/temp/catalog-private state; raw SET fallback; cross-account/subscription metadata; retaining a direct target's old state after replay starts; CN-global runtime mismatch. Preserve the existing refusal for them. If root requires a positive migration oracle in any of these domains, this v2 is **not** a complete solution for that expanded acceptance criterion; smallest next task is one specific domain/ownership extension with an oracle, not resurrecting the generic certificate.

Root's next technical review should accept/refute three explicit tradeoffs: the narrow SQL/empty-policy domain; explicit disposable target semantics; migrated-generation drift returning need-reprepare. The implementation seams, field encodings and limits are specified above; no new human approval is requested. The protocol allocation check belongs before source publication, and provisional124 is not itself a blocker to this design. All new helper/wrapper/guard behavior still requires implementation review and tests; no external review approval is fabricated.

## Handoff

- `version`: D2-DTS-1; `source/base`: 2d616a3847a59cb09560fa119ff6b35b745d3caf / e0314478159c80b062483c307180b02dc009dbc0; own OPEN/Ready guard rechecked; source clean.
- `proposed_files`: `proto/query.proto:612-618,691-716` (witness field5 and explicit target disposition field17); `pkg/defines/const.go:158+` (provisional protocol floor); `pkg/frontend/crc32_migration.go` (bounded classifier/source/preflight); `routine.go:706-728,840-853,886-940` (admission, export and failure owner); `session.go:4408-4443,4545-4777` (actual target policy/prepare contract and local-only runtime effects); `mysql_cmd_executor.go:2924,3051,3134-3165` (guarded build and publication); `types.go:297+,1155+` (small target contract/ExecCtx carrier); `computation_wrapper.go:1343-1417` (need-reprepare on contract drift); proxy `conn_migration.go:118-206,228-253`; focused tests described above. These are proposals only.
- `files_READ`: full root review; reused already-read execution contract and mo-pr-workflow/mo-dev/mo-self-review/mo-pr-preflight-review instructions/references, including feature-design-review; current v1 proposal; exact-source proto/query and plan schemas; frontend CRC migration, prepared producers/types/session/routine/manager/migrate controller/txn/compiler/computation wrapper/authorization snapshot/rewrite policy/view-schema provider/variables/user-var migration/self_handle paths; plan build_dcl/resetPreparePlan/GetParamRule/query_builder/view_dependency; txn catalog-read-stamp/snapshot-admission; disttae privilege-cache/catalog-visibility/cache metadata readers/builders; proxy migration/client/tunnel; issue29148, current CRC migration tests and narrow lifecycle/proxy test anchors; type/catalog constants.
- `evidence_paths`: this revision; `/Users/violet/.codex/automations/matrixone-open-issue-pr/evidence/heartbeat-20261019-2129.yoyQB2/28935-design-review.md`; preserved v1; original `/Users/violet/.codex/automations/matrixone-open-issue-pr/evidence/heartbeat-20261019-1924.rbqeOy/28935-current-ci-diagnosis.json`. Raw logs not copied.
- `tests_NOT_RUN`: ALL tests/builds/race/BVT/services NOT_RUN. `external_actions=NONE`: no GitHub mutation/commit/push/rerun, no shared/public/developer-pipeline state writes, no locks/watchdog/subagents/chats/messages. Evidence-only filesystem writes were confined to this v2 worker directory.
- `remaining_blockers`: root independent technical review of the stated bounded acceptance domain/lifecycle semantics; then implementation and actual validation. If successful migration under an already nonempty current policy or target-state rollback is mandatory, those positive domains remain UNSOLVED with the exact boundaries above. Actual-main protocol allocation must be refreshed before code publication.
- `writer_stopped=true`: handback complete; no further writes or waiting for root/CI/resources.
