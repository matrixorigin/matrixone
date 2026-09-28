// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"cmp"
	"context"
	"github.com/gogo/protobuf/proto"
	"reflect"
	"slices"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func lockDataBranchLineageOwnerLifecycle(ctx context.Context, bh BackgroundExec) error {
	return databranchutils.LockLineageOwnerLifecycle(func(sql string) error {
		return execDataBranchLineageOwnerLifecycleSQL(ctx, bh, sql)
	})
}

func execDataBranchLineageOwnerLifecycleSQL(
	ctx context.Context,
	bh BackgroundExec,
	sql string,
) error {
	lockCtx := defines.AttachAccountId(ctx, catalog.System_Account)
	bh.ClearExecResultSet()
	err := bh.Exec(lockCtx, sql)
	bh.ClearExecResultSet()
	return err
}

func lockDataBranchLineageOwnerLifecycleForFeatureAdmission(
	ctx context.Context,
	bh BackgroundExec,
) error {
	txnOp := backgroundExecTxnOperator(bh)
	gateSQL := databranchutils.LineageOwnerLifecycleLockSQLForTxn(txnOp)
	if err := execDataBranchLineageOwnerLifecycleSQL(ctx, bh, gateSQL); err != nil {
		return err
	}
	if gateSQL == databranchutils.LineageOwnerLifecyclePessimisticLockSQL() {
		if backExec, ok := bh.(*backExec); ok && backExec != nil && backExec.backSes != nil {
			backExec.backSes.lineageOwnerLifecycleWritePending = true
		}
	}
	return nil
}

func writePendingDataBranchLineageOwnerLifecycle(
	ctx context.Context,
	bh BackgroundExec,
) error {
	backExec, ok := bh.(*backExec)
	if !ok || backExec == nil || backExec.backSes == nil ||
		!backExec.backSes.lineageOwnerLifecycleWritePending {
		return nil
	}
	backExec.backSes.lineageOwnerLifecycleWritePending = false
	return execDataBranchLineageOwnerLifecycleSQL(
		ctx, bh, databranchutils.LineageOwnerLifecycleLockSQL(),
	)
}

func backgroundExecTxnOperator(bh BackgroundExec) client.TxnOperator {
	backExec, ok := bh.(*backExec)
	if !ok || backExec == nil || backExec.backSes == nil || backExec.backSes.GetTxnHandler() == nil {
		return nil
	}
	return backExec.backSes.GetTxnHandler().GetTxn()
}

func validateDataBranchLineageOwnerLifecycleAtCommit(
	ctx context.Context,
	ses FeSession,
	txnOp client.TxnOperator,
) error {
	rt := moruntime.ServiceRuntime(ses.GetService())
	if rt == nil {
		return moerr.NewInternalErrorNoCtx("missing runtime for lifecycle commit validation")
	}
	value, ok := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		return moerr.NewInternalErrorNoCtx("missing executor for lifecycle commit validation")
	}
	sqlExecutor, ok := value.(executor.SQLExecutor)
	if !ok {
		return moerr.NewInternalErrorNoCtx("invalid executor for lifecycle commit validation")
	}
	return validateDataBranchLineageOwnerLifecycleWithExecutor(
		ctx, sqlExecutor, txnOp, ses.GetTimeZone(),
	)
}

func validateDataBranchLineageOwnerLifecycleWithExecutor(
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	txnOp client.TxnOperator,
	timeZone *time.Location,
) error {
	// Commit validation intentionally retains the write barrier for pessimistic
	// transactions. The write dependency detects an owner writer that completed
	// after this transaction mutated branch catalogs; a row-locking read would
	// only serialize the statement and would let the stale transaction commit.
	opts := executor.Options{}.
		WithDisableIncrStatement().
		WithTxn(txnOp).
		WithKeepTxnAlive().
		WithTimeZone(timeZone).
		WithAccountID(catalog.System_Account).
		WithStatementOption(executor.StatementOption{}.
			WithWaitPolicy(lockpb.WaitPolicy_FastFail).
			WithAccountID(catalog.System_Account))
	result, err := sqlExecutor.Exec(
		ctx,
		databranchutils.LineageOwnerLifecycleLockSQL(),
		opts,
	)
	if err != nil {
		return err
	}
	result.Close()
	return nil
}

// admitFeatureLimitedLineageOwnerMutation installs the TN-ordered catalog
// frontier before crossing the lifecycle write barrier. An explicit-SI data
// branch transaction keeps its fixed snapshot; its quota check uses a separate
// RC transaction for freshness. Advancing an RC snapshot after the gate write
// can expose both workspace versions of the feature-registry row to later
// quota reads.
func admitFeatureLimitedLineageOwnerMutation(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
) error {
	if !featureLimitTxnUsesFixedSnapshot(bh) {
		if err := advanceFeatureLimitSnapshot(ctx, ses, bh); err != nil {
			return err
		}
	}
	return lockDataBranchLineageOwnerLifecycleForFeatureAdmission(ctx, bh)
}

// getDataBranchComponentExecutor owns C/G through the same transaction as the
// nested clone/drop. Explicit fixed-SI and optimistic owners are rejected before
// any catalog lock or mutation; no terminal S-to-X registry upgrade is needed.
func getDataBranchComponentExecutor(ctx context.Context, ses *Session, useTxnHandler bool) (BackgroundExec, *lifecycleRCAdmission, func(error) error, error) {
	factory := getBackExecutor
	if useTxnHandler {
		factory = getBackExecutorWithTxnHandler
	}
	bh, finish, err := factory(ctx, ses, &BackgroundExecOption{
		forcePessimisticRC: true, cloneSnapshotUsesBackgroundTxn: true,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	handedOff := false
	defer func() {
		if p := recover(); p != nil {
			if !handedOff {
				_ = finish(moerr.ConvertPanicError(ctx, p))
			}
			panic(p)
		}
	}()
	op := backgroundExecTxnOperator(bh)
	if op == nil || !op.Txn().IsPessimistic() || !op.Txn().IsRCIsolation() {
		err = moerr.NewNotSupported(ctx, "DATA BRANCH CREATE/DELETE requires pessimistic RC")
		handedOff = true
		return nil, nil, nil, finish(err)
	}
	admission, err := beginLifecycleRCAdmission(ctx, ses, bh)
	if err != nil {
		handedOff = true
		return nil, nil, nil, finish(err)
	}
	handedOff = true
	return bh, admission, func(err error) error {
		defer admission.proc.Free()
		return finish(err)
	}, nil
}

// A panic must never turn a partially executed private mutation into COMMIT.
// Preserve the original panic after releasing the owned transaction/process.
func finishDataBranchComponent(ctx context.Context, finish func(error) error, err *error) {
	if p := recover(); p != nil {
		_ = finish(moerr.ConvertPanicError(ctx, p))
		panic(p)
	}
	*err = finish(*err)
}

type branchCloneSource struct {
	account         uint32
	database, table string
	snapshot        *plan.Snapshot
}

type branchCloneDatabase struct {
	account uint32
	name    string
}

type branchResolvedSource struct {
	source branchCloneSource
	ref    *plan.ObjectRef
	def    *plan.TableDef
}

func cloneDatabaseAdmissionRequests(source cloneDatabaseSource, stmt *tree.CloneDatabase) []branchCloneSource {
	fromAccount := source.opAccountId
	if source.snapshot != nil && source.snapshot.Tenant != nil {
		fromAccount = source.snapshot.Tenant.TenantID
	}
	requests := make([]branchCloneSource, 0, 2+len(source.srcTblInfos)+len(source.fkTableMap))
	requests = append(requests, branchCloneSource{fromAccount, source.srcResolveDBName, "", source.snapshot},
		branchCloneSource{source.opAccountId, stmt.SrcDatabase.String(), "", source.snapshot})
	for _, table := range source.sourceTableInfosForLifecycle() {
		requests = append(requests, branchCloneSource{fromAccount, table.dbName, table.tblName, source.snapshot})
	}
	for _, table := range source.fkTableMap {
		if table != nil {
			requests = append(requests, branchCloneSource{fromAccount, table.dbName, table.tblName, source.snapshot})
		}
	}
	return requests
}

// Resolve through the background owner, including its own catalog writes and
// historical/subscription routing. Never borrow the outer session's operator.
func resolveBranchCloneSource(ctx context.Context, ses *Session, bh BackgroundExec, source branchCloneSource) (branchResolvedSource, error) {
	back := bh.(*backExec)
	proc, err := newCloneDatabaseTargetLockProcess(ctx, ses, bh)
	if err != nil {
		return branchResolvedSource{}, err
	}
	defer proc.Free()
	tcc := InitTxnCompilerContext(ses.GetTxnCompileCtx().DefaultDatabase())
	tcc.SetExecCtx(&ExecCtx{reqCtx: defines.AttachAccountId(ctx, source.account), ses: back.backSes, proc: proc})
	defer tcc.Close()
	ref, def, err := tcc.Resolve(source.database, source.table, source.snapshot)
	if err != nil {
		return branchResolvedSource{}, err
	}
	if ref == nil || def == nil || def.TblId == 0 {
		// Match the CLONE planner's missing-source diagnostic. Admission may
		// discover the absence first, but must not change the public error.
		return branchResolvedSource{}, moerr.NewParseErrorf(ctx,
			"table %v-%v does not exist", source.database, source.table)
	}
	if source.snapshot != nil && source.snapshot.Tenant != nil {
		source.account = source.snapshot.Tenant.TenantID
	}
	if ref.PubInfo != nil {
		source.account = uint32(ref.PubInfo.TenantId)
	}
	source.database, source.table = ref.SchemaName, ref.ObjName
	return branchResolvedSource{source: source, ref: ref, def: plan2.DeepCopyTableDef(def, true)}, nil
}

// admitBranchCloneRC takes the entire name domain before K and source T. The
// caller rechecks any database inventory after this returns, before quota or
// target writes. This is a single synchronous mutation, not a session cache.
func (a *lifecycleRCAdmission) admitBranchCloneRC(ctx context.Context, ses *Session, bh BackgroundExec,
	requests []branchCloneSource, target branchCloneDatabase, targetTable string, createDatabase bool,
	afterDomains func(error) (bool, error),
) (databranchutils.BranchReclaimDag, error) {
	var empty databranchutils.BranchReclaimDag
	databases := map[branchCloneDatabase]lockpb.LockMode{target: lockpb.LockMode_Shared}
	if createDatabase {
		databases[target] = lockpb.LockMode_Exclusive
	}
	requests = slices.Clone(requests)
	slices.SortFunc(requests, compareBranchCloneSource)
	requests = slices.CompactFunc(requests, func(a, b branchCloneSource) bool {
		return compareBranchCloneSource(a, b) == 0 && a.snapshot == b.snapshot
	})
	before := make([]branchResolvedSource, len(requests))
	ids := make([]uint64, 0, len(requests))
	var discoveryErr error
	for i, request := range requests {
		databases[branchCloneDatabase{request.account, request.database}] = lockpb.LockMode_Shared
		if request.table == "" {
			continue
		}
		resolved, err := resolveBranchCloneSource(ctx, ses, bh, request)
		if err != nil {
			discoveryErr = err
			break
		}
		before[i] = resolved
		ids = append(ids, resolved.def.TblId)
		databases[branchCloneDatabase{resolved.source.account, resolved.source.database}] = lockpb.LockMode_Shared
		// A branch source can store DATA BRANCH CREATE as its original SQL.
		// SHOW CREATE reconstructs the schema consumed by CREATE LIKE.
		if len(resolved.def.Fkeys) != 0 {
			createSQL, err := getCreateTableSql(defines.AttachAccountId(ctx, resolved.source.account), bh, resolved.source.snapshot, resolved.source.database, resolved.source.table)
			if err != nil {
				discoveryErr = err
				break
			}
			deps, err := getFkDepsFromTableInfos(ctx, []*tableInfo{{dbName: resolved.source.database, tblName: resolved.source.table, createSql: createSQL}})
			if err != nil {
				discoveryErr = err
				break
			}
			for _, parents := range deps {
				for _, key := range parents {
					dbName, _ := splitKey(key)
					databases[branchCloneDatabase{resolved.source.account, dbName}] = lockpb.LockMode_Shared
					if dbName != resolved.source.database {
						databases[branchCloneDatabase{target.account, dbName}] = lockpb.LockMode_Shared
					}
				}
			}
		}
	}
	if discoveryErr != nil {
		if afterDomains == nil {
			return empty, discoveryErr
		}
		// A pre-lock source read is provisional for IF NOT EXISTS. Pin only
		// the target name and let the fresh frontier decide the no-op.
		databases = map[branchCloneDatabase]lockpb.LockMode{target: lockpb.LockMode_Exclusive}
		requests = nil
	}
	// Empty-table requests pin the whole source database inventory, including
	// an empty database, while ordinary table sources keep D shared.
	for _, request := range requests {
		if request.table == "" {
			databases[branchCloneDatabase{request.account, request.database}] = lockpb.LockMode_Exclusive
		}
	}
	// Preserve the target X if it was also encountered as a source/foreign key.
	if createDatabase {
		databases[target] = lockpb.LockMode_Exclusive
	}
	names := make([]branchCloneDatabase, 0, len(databases))
	for name := range databases {
		names = append(names, name)
	}
	slices.SortFunc(names, func(a, b branchCloneDatabase) int {
		if n := cmp.Compare(a.account, b.account); n != 0 {
			return n
		}
		return cmp.Compare(a.name, b.name)
	})
	for _, name := range names {
		keys, err := cloneCatalogLockBatch(a.proc, name.account, name.name)
		if err != nil {
			return empty, err
		}
		if err = a.lockKey(catalog.MO_DATABASE, defines.AttachAccountId(ctx, name.account), keys, databases[name], name.account); err != nil {
			return empty, err
		}
	}
	if afterDomains != nil {
		if err := a.finish(ctx, ses, bh); err != nil {
			return empty, err
		}
		stop, err := afterDomains(discoveryErr)
		if stop || err != nil {
			return empty, err
		}
		if discoveryErr != nil {
			return empty, discoveryErr
		}
	}
	rt := moruntime.ServiceRuntime(ses.GetService())
	if rt == nil {
		return empty, moerr.NewInternalError(ctx, "missing branch component executor")
	}
	value, ok := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
	sqlExec, valid := value.(executor.SQLExecutor)
	if !ok || !valid {
		return empty, moerr.NewInternalError(ctx, "missing branch component executor")
	}
	options := executor.Options{}.WithTxn(a.proc.GetTxnOperator()).WithDisableIncrStatement().WithKeepTxnAlive().WithAccountID(catalog.System_Account).WithTimeZone(ses.GetTimeZone())
	systemCtx := defines.AttachAccountId(ctx, catalog.System_Account)
	relationID := func() (uint64, error) {
		db, err := a.proc.GetSessionInfo().StorageEngine.Database(systemCtx, catalog.MO_CATALOG, a.proc.GetTxnOperator())
		if err != nil {
			return 0, err
		}
		rel, err := db.Relation(systemCtx, catalog.MO_BRANCH_METADATA, nil)
		if err != nil {
			return 0, err
		}
		return rel.GetTableID(systemCtx), nil
	}
	type tableName struct {
		account         uint32
		database, table string
	}
	tableModes := make(map[tableName]lockpb.LockMode, len(before)+1)
	for _, source := range before {
		if source.def != nil && shouldLockDataBranchCloneSource(source.source.snapshot) {
			tableModes[tableName{source.source.account, source.source.database, source.source.table}] = lockpb.LockMode_Shared
		}
	}
	if targetTable != "" {
		tableModes[tableName{target.account, target.name, targetTable}] = lockpb.LockMode_Exclusive
	}
	tables := make([]tableName, 0, len(tableModes))
	for name := range tableModes {
		tables = append(tables, name)
	}
	slices.SortFunc(tables, func(a, b tableName) int {
		return compareBranchCloneSource(branchCloneSource{account: a.account, database: a.database, table: a.table},
			branchCloneSource{account: b.account, database: b.database, table: b.table})
	})
	dag, err := databranchutils.LoadLockedBranchComponents(ctx, ids,
		func(ctx context.Context, sql string) (executor.Result, error) {
			return sqlExec.Exec(defines.AttachAccountId(ctx, catalog.System_Account), sql, options)
		},
		func(roots []uint64) error {
			lockedID, err := relationID()
			if err != nil {
				return err
			}
			keys := batch.NewWithSize(1)
			keys.Vecs[0] = vector.NewVec(types.T_uint64.ToType())
			for _, root := range roots {
				if err = vector.AppendFixed(keys.Vecs[0], root, false, a.proc.Mp()); err != nil {
					keys.Vecs[0].Free(a.proc.Mp())
					return err
				}
			}
			if err = a.lockKey(catalog.MO_BRANCH_METADATA, systemCtx, keys, lockpb.LockMode_Exclusive, catalog.System_Account); err != nil {
				return err
			}
			// COPY ALTER is excluded by G; inplace ALTER is excluded only by T.
			for _, source := range tables {
				keys, err := cloneCatalogLockBatch(a.proc, source.account, source.database, source.table)
				if err != nil {
					return err
				}
				if err = a.lockKey(catalog.MO_TABLES, defines.AttachAccountId(ctx, source.account), keys, tableModes[source], source.account); err != nil {
					return err
				}
			}
			if err = a.finish(ctx, ses, bh); err != nil {
				return err
			}
			currentID, err := relationID()
			if err != nil {
				return err
			}
			if currentID != lockedID {
				return moerr.NewTxnNeedRetryWithDefChanged(ctx)
			}
			return nil
		})
	if err != nil {
		return empty, err
	}
	// Empty source databases still need a post-D frontier.
	if len(ids) == 0 {
		if err = a.finish(ctx, ses, bh); err != nil {
			return empty, err
		}
	}
	for i, request := range requests {
		if request.table == "" {
			continue
		}
		after, err := resolveBranchCloneSource(ctx, ses, bh, request)
		if err != nil {
			return empty, err
		}
		if after.source.account != before[i].source.account || !proto.Equal(after.ref, before[i].ref) || !proto.Equal(after.def, before[i].def) {
			return empty, moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
	}
	return dag, nil
}

func (a *lifecycleRCAdmission) lockBranchSnapshotName(ctx context.Context, tableID uint64) error {
	return a.lockSnapshotName(ctx, databranchutils.BranchSnapshotName(tableID), catalog.System_Account)
}

func (a *lifecycleRCAdmission) lockSnapshotName(ctx context.Context, name string, ownerAccountID uint32) error {
	keys := batch.NewWithSize(1)
	keys.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	if err := vector.AppendBytes(keys.Vecs[0], []byte(name), false, a.proc.Mp()); err != nil {
		keys.Vecs[0].Free(a.proc.Mp())
		return err
	}
	return a.lockKey(catalog.MO_SNAPSHOTS, defines.AttachAccountId(ctx, ownerAccountID), keys, lockpb.LockMode_Exclusive, ownerAccountID)
}

func compareBranchCloneSource(a, b branchCloneSource) int {
	if n := cmp.Compare(a.account, b.account); n != 0 {
		return n
	}
	if n := cmp.Compare(a.database, b.database); n != 0 {
		return n
	}
	return cmp.Compare(a.table, b.table)
}

func (a *lifecycleRCAdmission) lockBranchQuotaKey(ctx context.Context, account uint32) error {
	// mo_feature_limit.account_id is BIGINT UNSIGNED, unlike the uint32
	// account component in the mo_database/mo_tables primary keys.
	columns := []*vector.Vector{vector.NewVec(types.T_uint64.ToType()), vector.NewVec(types.T_varchar.ToType()), vector.NewVec(types.T_varchar.ToType())}
	defer func() {
		for _, col := range columns {
			col.Free(a.proc.Mp())
		}
	}()
	if err := vector.AppendFixed(columns[0], uint64(account), false, a.proc.Mp()); err != nil {
		return err
	}
	if err := vector.AppendBytes(columns[1], []byte(featureCodeBranch), false, a.proc.Mp()); err != nil {
		return err
	}
	if err := vector.AppendBytes(columns[2], nil, false, a.proc.Mp()); err != nil {
		return err
	}
	encoded, err := function.RunFunctionDirectly(a.proc, function.SerialFunctionEncodeID, columns, 1)
	if err != nil {
		return err
	}
	keys := batch.NewWithSize(1)
	keys.Vecs[0] = encoded
	return a.lockKey(catalog.MO_FEATURE_LIMIT, defines.AttachAccountId(ctx, catalog.System_Account), keys, lockpb.LockMode_Exclusive, catalog.System_Account)
}

func lockBranchQuotaAdmission(ctx context.Context, ses *Session, bh BackgroundExec, account uint32) error {
	proc, err := newCloneDatabaseTargetLockProcess(ctx, ses, bh)
	if err != nil {
		return err
	}
	defer proc.Free()
	return (&lifecycleRCAdmission{proc: proc}).lockBranchQuotaKey(ctx, account)
}

// Catalog scans and independent topological sorts need not return the same
// order. Normalize copies, retaining all source definitions consumed by clone.
func sameBranchCloneDatabaseSource(a, b cloneDatabaseSource) bool {
	normalize := func(s cloneDatabaseSource) cloneDatabaseSource {
		s.srcTblInfos = slices.Clone(s.srcTblInfos)
		slices.SortFunc(s.srcTblInfos, func(a, b *tableInfo) int {
			if a == nil || b == nil {
				if a == b {
					return 0
				}
				if a == nil {
					return -1
				}
				return 1
			}
			if n := cmp.Compare(a.dbName, b.dbName); n != 0 {
				return n
			}
			return cmp.Compare(a.tblName, b.tblName)
		})
		s.sortedFkTbls = slices.Sorted(slices.Values(s.sortedFkTbls))
		s.userDefinedFuncs = slices.Clone(s.userDefinedFuncs)
		slices.SortFunc(s.userDefinedFuncs, func(a, b userDefinedFunctionDefinition) int {
			return cmp.Or(cmp.Compare(a.name, b.name), cmp.Compare(a.argTypes, b.argTypes))
		})
		s.storedProcedures = slices.Clone(s.storedProcedures)
		slices.SortFunc(s.storedProcedures, func(a, b storedProcedureDefinition) int { return cmp.Compare(a.name, b.name) })
		return s
	}
	return reflect.DeepEqual(normalize(a), normalize(b))
}
