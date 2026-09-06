// Copyright 2025 Matrix Origin
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
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	injectedError = "injected table clone error"
)

const (
	dataBranchLevel_Table    = "table"
	dataBranchLevel_Database = "database"
	dataBranchLevel_Account  = "account"
)

const (
	insertIntoBranchMetadataSql = `insert into %s.%s values(%d, %d, %d, %d, '%s', false)`
	scanBranchMetadataSql       = `select * from %s.%s`
)

type cloneReceipt struct {
	dstDb  string
	dstTbl string
	srcDb  string
	srcTbl string
	// this valid only when the snapshot is nil
	snapshotTS int64
	snapshot   *plan.Snapshot

	opAccount  uint32
	toAccount  uint32
	srcAccount uint32

	// Resolved identifiers used by branch bookkeeping. They are populated by
	// updateBranchMetaTable so the matching branch-protect snapshot insert can
	// reuse them without a second catalog round-trip.
	srcTableID     uint64
	dstTableID     uint64
	srcAccountName string
}

type dataBranchCloneLockCtxKey struct{}

func shouldLockDataBranchCloneSource(snapshot *plan.Snapshot) bool {
	// A named snapshot already publishes a durable historical owner before the
	// clone runs. Timestamp hints carry only TS/Tenant, so they still need the
	// live source-row lock to serialize metadata publication with COPY ALTER.
	return snapshot == nil || snapshot.ExtraInfo == nil
}

func isTimestampDataBranchCloneSource(snapshot *plan.Snapshot) bool {
	return snapshot != nil && snapshot.TS != nil && snapshot.ExtraInfo == nil
}

func shouldRevalidateTimestampDataBranchCloneSource(
	ctx context.Context,
	snapshot *plan.Snapshot,
) bool {
	dataBranchClone, _ := ctx.Value(dataBranchCloneLockCtxKey{}).(bool)
	return dataBranchClone && isTimestampDataBranchCloneSource(snapshot)
}

func shouldLockNamedDataBranchCloneSnapshot(
	ctx context.Context,
	snapshot *plan.Snapshot,
) bool {
	dataBranchClone, _ := ctx.Value(dataBranchCloneLockCtxKey{}).(bool)
	return dataBranchClone && snapshot != nil && snapshot.TS != nil &&
		snapshot.ExtraInfo != nil && snapshot.ExtraInfo.Name != ""
}

func namedDataBranchCloneSnapshotLockSQL(
	ctx context.Context,
	snapshotName string,
) (string, error) {
	if err := inputNameIsInvalid(ctx, snapshotName); err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"%s where sname = '%s' for update",
		getSnapshotFormat, snapshotName,
	), nil
}

func validateNamedDataBranchCloneSnapshotRecord(
	snapshot *plan.Snapshot,
	record *snapshotRecord,
) error {
	if record == nil || snapshot == nil || snapshot.TS == nil || snapshot.ExtraInfo == nil ||
		record.snapshotName != snapshot.ExtraInfo.Name ||
		record.ts != snapshot.TS.PhysicalTime ||
		record.level != snapshot.ExtraInfo.Level ||
		record.objId != snapshot.ExtraInfo.ObjId ||
		record.kind == branchSnapshotKind {
		return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
	}
	return nil
}

func lockNamedDataBranchCloneSnapshot(
	ctx context.Context,
	bh BackgroundExec,
	snapshot *plan.Snapshot,
) error {
	if !shouldLockNamedDataBranchCloneSnapshot(ctx, snapshot) {
		return nil
	}
	sql, err := namedDataBranchCloneSnapshotLockSQL(ctx, snapshot.ExtraInfo.Name)
	if err != nil {
		return err
	}
	records, err := getSnapshotRecords(ctx, bh, sql)
	if err != nil {
		return err
	}
	if len(records) != 1 {
		return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
	}
	return validateNamedDataBranchCloneSnapshotRecord(snapshot, records[0])
}

func validateTimestampDataBranchSourceIDs(
	selectedTableID, currentTableID uint64,
	dag *databranchutils.DataBranchDAG,
) error {
	if selectedTableID == currentTableID {
		return nil
	}
	if dag != nil {
		if _, _, _, ok := dag.FindLCA(selectedTableID, currentTableID); ok {
			return nil
		}
	}
	return moerr.NewInvalidInputNoCtx(
		"data branch: timestamp source generation is not connected to the current table",
	)
}

func validateTimestampDataBranchSourceAfterLock(
	snapshot *plan.Snapshot,
	resolveTableID func(*plan.Snapshot) (uint64, error),
	loadDAG func() (*databranchutils.DataBranchDAG, error),
) error {
	if !isTimestampDataBranchCloneSource(snapshot) {
		return nil
	}
	selectedTableID, err := resolveTableID(snapshot)
	if err != nil {
		return err
	}
	currentTableID, err := resolveTableID(nil)
	if err != nil {
		return err
	}
	if selectedTableID == currentTableID {
		return nil
	}
	dag, err := loadDAG()
	if err != nil {
		return err
	}
	return validateTimestampDataBranchSourceIDs(selectedTableID, currentTableID, dag)
}

func revalidateTimestampDataBranchCloneSource(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	snapshot *plan.Snapshot,
	fromAccountID uint32,
	databaseName, tableName string,
) error {
	if !isTimestampDataBranchCloneSource(snapshot) {
		return nil
	}

	sourceCtx := defines.AttachAccountId(ctx, fromAccountID)
	tcc := ses.GetTxnCompileCtx()
	originalCtx := tcc.GetContext()
	tcc.SetContext(sourceCtx)
	defer tcc.SetContext(originalCtx)

	return validateTimestampDataBranchSourceAfterLock(
		snapshot,
		func(at *plan.Snapshot) (uint64, error) {
			_, tableDef, err := tcc.Resolve(databaseName, tableName, at)
			if err != nil {
				return 0, err
			}
			if tableDef == nil {
				return 0, moerr.NewNoSuchTable(sourceCtx, databaseName, tableName)
			}
			return tableDef.TblId, nil
		},
		func() (*databranchutils.DataBranchDAG, error) {
			return constructBranchDAGForUpdate(ctx, ses, bh)
		},
	)
}

func withDataBranchCloneSourceLock(
	snapshot *plan.Snapshot,
	lockSource func() error,
) error {
	if !shouldLockDataBranchCloneSource(snapshot) {
		return nil
	}
	return lockSource()
}

func withCloneLockContext(
	proc *process.Process,
	ctx context.Context,
	lockRows func() error,
) error {
	oldCtx := proc.Ctx
	proc.Ctx = ctx
	defer func() {
		proc.Ctx = oldCtx
	}()
	return lockRows()
}

func lockDataBranchCloneSource(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	fromAccountID uint32,
	databaseName, tableName string,
) error {
	if locked, _ := ctx.Value(dataBranchCloneLockCtxKey{}).(bool); !locked {
		return nil
	}
	lockProc := newDataBranchCloneLockProcess(ctx, ses, bh)
	defer lockProc.Free()
	txnOp := lockProc.GetTxnOperator()
	if err := validateDataBranchCreateTxn(txnOp.Txn().IsPessimistic()); err != nil {
		return err
	}
	sourceCtx := defines.AttachAccountId(ctx, fromAccountID)
	eng := lockProc.GetSessionInfo().StorageEngine
	db, err := eng.Database(sourceCtx, catalog.MO_CATALOG, txnOp)
	if err != nil {
		return err
	}
	rel, err := db.Relation(sourceCtx, catalog.MO_TABLES, nil)
	if err != nil {
		return err
	}
	lockBat, err := cloneCatalogLockBatch(
		lockProc, fromAccountID, databaseName, tableName,
	)
	if err != nil {
		return err
	}
	defer lockBat.Vecs[0].Free(lockProc.Mp())
	// ALTER locks this exact mo_tables composite key exclusively. A shared
	// catalog-row lock serializes source-ID/snapshot selection with ALTER while
	// allowing source-table DML and sibling branch clones to continue.
	return withCloneLockContext(lockProc, sourceCtx, func() error {
		return lockop.LockRows(
			eng,
			lockProc,
			rel,
			rel.GetTableID(sourceCtx),
			lockBat,
			0,
			*lockBat.Vecs[0].GetType(),
			lock.LockMode_Shared,
			lock.Sharding_None,
			fromAccountID,
		)
	})
}

var lockCloneDatabaseTarget = func(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	accountID uint32,
	databaseName string,
) error {
	lockProc, err := newCloneDatabaseTargetLockProcess(ctx, ses, bh)
	if err != nil {
		return err
	}
	defer lockProc.Free()

	targetCtx := defines.AttachAccountId(ctx, accountID)
	eng := lockProc.GetSessionInfo().StorageEngine
	db, err := eng.Database(targetCtx, catalog.MO_CATALOG, lockProc.GetTxnOperator())
	if err != nil {
		return err
	}
	rel, err := db.Relation(targetCtx, catalog.MO_DATABASE, nil)
	if err != nil {
		return err
	}
	lockBat, err := cloneCatalogLockBatch(lockProc, accountID, databaseName)
	if err != nil {
		return err
	}
	defer lockBat.Vecs[0].Free(lockProc.Mp())

	// The background transaction owns this exact mo_database key through the
	// destination CREATE. Its deferred commit or rollback releases the lock.
	return withCloneLockContext(lockProc, targetCtx, func() error {
		return lockop.LockRows(
			eng,
			lockProc,
			rel,
			rel.GetTableID(targetCtx),
			lockBat,
			0,
			*lockBat.Vecs[0].GetType(),
			lock.LockMode_Exclusive,
			lock.Sharding_None,
			accountID,
		)
	})
}

func checkCloneDatabaseTarget(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	accountID uint32,
	databaseName string,
) (bool, error) {
	targetCtx := defines.AttachAccountId(ctx, accountID)
	for attempts := 0; ; attempts++ {
		if err := lockCloneDatabaseTarget(targetCtx, ses, bh, accountID, databaseName); err != nil {
			if attempts == 0 && isCloneDatabaseTargetLockRetry(err) {
				retried, retryErr := restartCloneDatabaseTargetLockTxn(targetCtx, bh)
				if retryErr != nil {
					return false, retryErr
				}
				if retried {
					continue
				}
			}
			return false, err
		}
		return checkDatabaseExists(targetCtx, bh, databaseName)
	}
}

func isCloneDatabaseTargetLockRetry(err error) bool {
	return moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) ||
		moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged)
}

func restartCloneDatabaseTargetLockTxn(ctx context.Context, bh BackgroundExec) (bool, error) {
	back, ok := bh.(*backExec)
	if !ok || back.backSes.GetTxnHandler().IsShareTxn() {
		return false, nil
	}
	if locked, _ := ctx.Value(dataBranchCloneLockCtxKey{}).(bool); locked {
		return false, nil
	}
	if err := bh.Exec(ctx, "rollback;"); err != nil {
		return false, err
	}
	if err := bh.Exec(ctx, "begin;"); err != nil {
		return false, err
	}
	return true, nil
}

func newDataBranchCloneLockProcess(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
) *process.Process {
	return newCloneLockProcess(ctx, ses, cloneSnapshotTxnOperator(ses, bh))
}

func newCloneDatabaseTargetLockProcess(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
) (*process.Process, error) {
	back, ok := bh.(*backExec)
	if !ok {
		return nil, moerr.NewInternalError(ctx, "database clone target lock requires a background executor")
	}
	return newCloneLockProcess(ctx, ses, back.backSes.GetTxnHandler().GetTxn()), nil
}

func newCloneLockProcess(
	ctx context.Context,
	ses *Session,
	txnOp TxnOperator,
) *process.Process {
	outer := ses.proc
	lockProc := process.NewTopProcess(
		ctx,
		outer.Mp(),
		outer.Base.TxnClient,
		txnOp,
		outer.Base.FileService,
		outer.Base.LockService,
		outer.Base.QueryClient,
		outer.Base.Hakeeper,
		outer.Base.UdfService,
		outer.Base.Aicm,
		outer.Base.TaskService,
	)
	lockProc.Base.SessionInfo = outer.Base.SessionInfo
	return lockProc
}

func cloneCatalogLockBatch(
	proc *process.Process,
	accountID uint32,
	names ...string,
) (*batch.Batch, error) {
	inputs := make([]*vector.Vector, len(names)+1)
	defer func() {
		for _, input := range inputs {
			if input != nil {
				input.Free(proc.GetMPool())
			}
		}
	}()
	inputs[0] = vector.NewVec(types.T_uint32.ToType())
	if err := vector.AppendFixed(inputs[0], accountID, false, proc.GetMPool()); err != nil {
		return nil, err
	}
	for i, name := range names {
		inputs[i+1] = vector.NewVec(types.T_varchar.ToType())
		if err := vector.AppendBytes(inputs[i+1], []byte(name), false, proc.GetMPool()); err != nil {
			return nil, err
		}
	}
	encoded, err := function.RunFunctionDirectly(
		proc, function.SerialFunctionEncodeID, inputs, 1,
	)
	if err != nil {
		return nil, err
	}
	bat := batch.NewWithSize(1)
	bat.SetVector(0, encoded)
	return bat, nil
}

func lockDataBranchCloneDatabaseSources(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	source cloneDatabaseSource,
) error {
	if locked, _ := ctx.Value(dataBranchCloneLockCtxKey{}).(bool); !locked {
		return nil
	}
	if !shouldLockDataBranchCloneSource(source.snapshot) {
		return nil
	}
	fromAccountID := source.opAccountId
	if source.snapshot != nil && source.snapshot.Tenant != nil {
		fromAccountID = source.snapshot.Tenant.TenantID
	}
	tables := append([]*tableInfo(nil), source.sourceTableInfosForLifecycle()...)
	sort.Slice(tables, func(i, j int) bool {
		if tables[i].dbName != tables[j].dbName {
			return tables[i].dbName < tables[j].dbName
		}
		return tables[i].tblName < tables[j].tblName
	})
	for _, table := range tables {
		if err := lockDataBranchCloneSource(
			ctx, ses, bh, fromAccountID, table.dbName, table.tblName,
		); err != nil {
			return err
		}
	}
	return nil
}

func revalidateTimestampDataBranchCloneDatabaseSource(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	source cloneDatabaseSource,
) error {
	if !shouldRevalidateTimestampDataBranchCloneSource(ctx, source.snapshot) {
		return nil
	}
	if _, err := tryToIncreaseTxnPhysicalTS(ctx, cloneSnapshotTxnOperator(ses, bh)); err != nil {
		return err
	}
	fromAccountID := source.opAccountId
	if source.snapshot.Tenant != nil {
		fromAccountID = source.snapshot.Tenant.TenantID
	}
	return forEachCloneDatabaseSourceTable(source, func(table *tableInfo) error {
		return revalidateTimestampDataBranchCloneSource(
			ctx, ses, bh, source.snapshot, fromAccountID,
			table.dbName, table.tblName,
		)
	})
}

func forEachCloneDatabaseSourceTable(
	source cloneDatabaseSource,
	fn func(*tableInfo) error,
) error {
	for _, table := range source.sourceTableInfosForLifecycle() {
		if err := fn(table); err != nil {
			return err
		}
	}
	return nil
}

func cloneSnapshotTxnOperator(ses *Session, bh BackgroundExec) TxnOperator {
	back := bh.(*backExec)
	if back.backSes.cloneSnapshotUsesBackgroundTxn {
		return back.backSes.GetTxnHandler().GetTxn()
	}
	return ses.proc.GetTxnOperator()
}

func getBackExecutor(
	ctx context.Context,
	ses *Session,
	opts ...*BackgroundExecOption,
) (BackgroundExec, func(error) error, error) {
	return getBackExecutorInternal(ctx, ses, false, opts...)
}

// getBackExecutorWithTxnHandler is used by database clone, which can run
// immediately after BEGIN before the process transaction operator is refreshed.
func getBackExecutorWithTxnHandler(
	ctx context.Context,
	ses *Session,
	opts ...*BackgroundExecOption,
) (BackgroundExec, func(error) error, error) {
	return getBackExecutorInternal(ctx, ses, true, opts...)
}

func getBackExecutorInternal(
	ctx context.Context,
	ses *Session,
	useTxnHandler bool,
	opts ...*BackgroundExecOption,
) (BackgroundExec, func(error) error, error) {

	var (
		err      error
		bh       BackgroundExec
		deferred func(error) error
	)

	var explicitTxn bool
	if useTxnHandler {
		// TxnHandler is authoritative for an explicit transaction. The process
		// operator is not refreshed by the BEGIN statement itself.
		explicitTxn = ses.GetTxnHandler().OptionBitsIsSet(OPTION_BEGIN)
	} else {
		explicitTxn = ses.proc.GetTxnOperator().TxnOptions().ByBegin
	}
	if explicitTxn {
		bh = ses.GetShareTxnBackgroundExec(ctx, false)
		bh.ClearExecResultSet()
		return bh, func(err error) error {
			bh.Close()
			return err
		}, nil
	}

	bh = ses.GetBackgroundExec(ctx, opts...)
	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, "begin"); err != nil {
		bh.Close()
		return nil, nil, err
	}

	deferred = func(err2 error) error {
		err2 = finishTxn(ctx, bh, err2)
		bh.Close()
		return err2
	}

	return bh, deferred, nil
}

func resolveSnapshot(
	ses *Session, atTsExpr *tree.AtTimeStamp,
) (*plan.Snapshot, error) {

	var (
		err      error
		snapshot *plan.Snapshot
	)

	if atTsExpr != nil {
		builder := plan.NewQueryBuilder(plan2.Query_INSERT, ses.txnCompileCtx, false, true)
		if snapshot, err = builder.ResolveTsHint(atTsExpr); err != nil {
			return nil, err
		}
	}

	return snapshot, nil
}

var resolveSnapshotForClone = resolveSnapshot

func newMoTimestampHint(snapshotTS int64) *tree.AtTimeStamp {
	origin := strconv.FormatInt(snapshotTS, 10)
	return &tree.AtTimeStamp{
		Type: tree.ATMOTIMESTAMP,
		Expr: tree.NewNumVal[int64](snapshotTS, origin, false, tree.P_int64),
	}
}

func cloneTableRestoreSQL(stmt *tree.CloneTable, snapshotTS int64) string {
	restoreStmt := *stmt
	restoreStmt.ToAccountOpt = nil
	restoreStmt.CopyGrants = false
	if snapshotTS != 0 {
		restoreStmt.SrcTable.AtTsExpr = newMoTimestampHint(snapshotTS)
	}
	return tree.StringWithOpts(
		&restoreStmt,
		dialect.MYSQL,
		tree.WithQuoteIdentifier(),
		tree.WithSingleQuoteString(),
	)
}

// generatedCloneRestoreSnapshotTS returns the timestamp to put in nested clone
// restore SQL. An explicit transaction must retain the shared transaction view
// so it can read its own uncommitted source data and metadata. The caller still
// keeps the generated timestamp for data-branch bookkeeping.
func generatedCloneRestoreSnapshotTS(ses *Session, snapshotTS int64) int64 {
	if ses.GetTxnHandler().OptionBitsIsSet(OPTION_BEGIN) {
		return 0
	}
	return snapshotTS
}

func cloneTargetTableExists(ctx context.Context, bh BackgroundExec, dbName, tableName string, accountID uint32) (bool, error) {
	sql, err := getSqlForCheckDatabaseTableWithSnapshot(ctx, dbName, tableName, accountID, 0)
	if err != nil {
		return false, err
	}
	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return false, err
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return false, err
	}
	return execResultArrayHasData(erArray), nil
}

func newQualifiedCloneTableName(dbName, tblName string, atTsExpr *tree.AtTimeStamp) tree.TableName {
	return *tree.NewTableName(
		tree.Identifier(tblName),
		tree.ObjectNamePrefix{
			SchemaName:     tree.Identifier(dbName),
			ExplicitSchema: true,
		},
		atTsExpr,
	)
}

func getOpAndToAccountId(
	reqCtx context.Context,
	ses *Session,
	bh BackgroundExec,
	toAccountOpt *tree.ToAccountOpt,
	atTsExpr *tree.AtTimeStamp,
) (opAccountId, toAccountId uint32, snapshot *plan2.Snapshot, err error) {

	if snapshot, err = resolveSnapshotForClone(ses, atTsExpr); err != nil {
		if atTsExpr != nil && plan.IsSnapshotNotFound(err) {
			return 0, 0, nil, plan.NewSnapshotNotFoundError(
				reqCtx,
				atTsExpr.SnapshotName,
			)
		}
		return 0, 0, nil, err
	}

	if opAccountId, toAccountId, err = getCloneTargetAccountIds(reqCtx, bh, toAccountOpt); err != nil {
		return 0, 0, nil, err
	}

	return opAccountId, toAccountId, snapshot, nil
}

func getCloneTargetAccountIds(
	ctx context.Context,
	bh BackgroundExec,
	toAccountOpt *tree.ToAccountOpt,
) (opAccountId, toAccountId uint32, err error) {
	if opAccountId, err = defines.GetAccountId(ctx); err != nil {
		return 0, 0, err
	}
	if toAccountOpt == nil {
		return opAccountId, opAccountId, nil
	}
	if toAccountId, err = getAccountId(ctx, bh, toAccountOpt.AccountName.String()); err != nil {
		return 0, 0, err
	}
	return opAccountId, toAccountId, nil
}

type cloneAccountResolution struct {
	opAccountId uint32
	toAccountId uint32
	snapshot    *plan2.Snapshot
}

// create table x.y clone r.s {MO_TS, SNAPSHOT}
// create table x.y clone r.s {MO_TS, SNAPSHOT} to account t
func handleCloneTable(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.CloneTable,
	bh BackgroundExec,
	resolvedAccounts *cloneAccountResolution,
) (receipt cloneReceipt, err error) {
	if stmt.CreateTable.Temporary {
		switch {
		case stmt.ToAccountOpt != nil:
			return receipt, moerr.NewInvalidInputNoCtx(
				"CREATE TEMPORARY TABLE ... CLONE cannot be used with TO ACCOUNT",
			)
		case stmt.CopyGrants:
			return receipt, moerr.NewInvalidInputNoCtx(
				"CREATE TEMPORARY TABLE ... CLONE cannot be used with COPY GRANTS",
			)
		}
	}

	var (
		ctx    context.Context
		reqCtx = execCtx.reqCtx

		deferred      func(error) error
		faultInjected bool

		snapshot   *plan2.Snapshot
		snapshotTS int64

		toAccountId   uint32
		opAccountId   uint32
		fromAccountId uint32

		tempTargetDB               string
		tempTargetAlias            string
		tempTargetExistedBeforeRun bool
	)
	// This defer is intentionally registered before the background transaction's
	// finish defer. It therefore observes commit failures as well as execution
	// failures and removes only aliases introduced by this statement.
	defer func() {
		removeFailedTemporaryCloneAlias(
			ses, tempTargetDB, tempTargetAlias, tempTargetExistedBeforeRun, err,
		)
	}()

	if reqCtx.Value(tree.CloneLevelCtxKey{}) == nil {
		reqCtx = context.WithValue(reqCtx, tree.CloneLevelCtxKey{}, tree.NormalCloneLevelTable)
	}

	if bh == nil {
		// do not open another transaction,
		// if the clone already executed within a transaction.
		if bh, deferred, err = getBackExecutor(
			reqCtx, ses, &BackgroundExecOption{forcePessimisticRC: true},
		); err != nil {
			return
		}

		defer func() {
			if deferred != nil {
				//if r := recover(); r != nil {
				//	err = moerr.ConvertPanicError(reqCtx, r)
				//}
				err = deferred(err)
			}
		}()
	}

	if resolvedAccounts != nil {
		opAccountId = resolvedAccounts.opAccountId
		toAccountId = resolvedAccounts.toAccountId
		snapshot = resolvedAccounts.snapshot
	} else {
		if opAccountId, toAccountId, snapshot, err = getOpAndToAccountId(
			reqCtx, ses, bh, stmt.ToAccountOpt, stmt.SrcTable.AtTsExpr,
		); err != nil {
			return
		}
	}

	if stmt.CopyGrants && stmt.ToAccountOpt != nil {
		err = moerr.NewInvalidInputNoCtx("COPY GRANTS cannot be used with TO ACCOUNT")
		return
	}

	if snapshot == nil && opAccountId != toAccountId {
		err = moerr.NewInternalErrorNoCtxf("clone table between different accounts need a snapshot")
		return
	}

	fromAccountId = opAccountId
	if snapshot != nil && snapshot.Tenant != nil {
		fromAccountId = snapshot.Tenant.TenantID
	}
	if stmt.CopyGrants && fromAccountId != toAccountId {
		err = moerr.NewInvalidInputNoCtx("COPY GRANTS cannot be used when cloning across accounts")
		return
	}

	if stmt.SrcTable.SchemaName == "" {
		// src acc = op acc
		// src acc = to acc
		// src != op acc and src != to acc
		if fromAccountId == opAccountId {
			stmt.SrcTable.SchemaName = tree.Identifier(ses.GetTxnCompileCtx().DefaultDatabase())
		}
	}

	if stmt.SrcTable.SchemaName == "" {
		err = moerr.NewInternalErrorNoCtxf(
			"no db selected for the src table %s", stmt.SrcTable.ObjectName)
		return
	}

	if stmt.CreateTable.Table.SchemaName == "" {
		if toAccountId == opAccountId {
			stmt.CreateTable.Table.SchemaName = tree.Identifier(ses.GetTxnCompileCtx().DefaultDatabase())
		}
	}

	if stmt.CreateTable.Table.SchemaName == "" {
		err = moerr.NewInternalErrorNoCtxf(
			"no db selected for the dst table %s", stmt.CreateTable.Table.ObjectName)
		return
	}
	if stmt.CreateTable.Temporary {
		tempTargetDB = stmt.CreateTable.Table.SchemaName.String()
		tempTargetAlias = stmt.CreateTable.Table.ObjectName.String()
		_, tempTargetExistedBeforeRun = ses.GetTempTable(tempTargetDB, tempTargetAlias)
	}

	oldDefault := bh.(*backExec).backSes.GetDatabaseName()
	bh.(*backExec).backSes.SetDatabaseName(ses.GetTxnCompileCtx().DefaultDatabase())
	defer func() {
		bh.(*backExec).backSes.SetDatabaseName(oldDefault)
	}()

	if stmt.CreateTable.Table.SchemaName == moCatalog {
		err = moerr.NewInternalErrorNoCtxf("cannot clone data into system database")
		return
	}

	if opAccountId != sysAccountID && opAccountId != toAccountId {
		err = moerr.NewInternalErrorNoCtxf("only sys can clone table to another account")
		return
	}
	if err = lockNamedDataBranchCloneSnapshot(
		defines.AttachAccountId(reqCtx, fromAccountId), bh, snapshot,
	); err != nil {
		return
	}
	if err = withDataBranchCloneSourceLock(snapshot, func() error {
		return lockDataBranchCloneSource(
			reqCtx,
			ses,
			bh,
			fromAccountId,
			stmt.SrcTable.SchemaName.String(),
			stmt.SrcTable.ObjectName.String(),
		)
	}); err != nil {
		return
	}
	if shouldRevalidateTimestampDataBranchCloneSource(reqCtx, snapshot) {
		// The timestamp was resolved before waiting for the source-row lock.
		// Advance the RC snapshot while the lock is held, then ensure an ALTER
		// that won the lock either preserved a path to the selected generation
		// or causes this branch creation to fail before publishing metadata.
		if _, err = tryToIncreaseTxnPhysicalTS(reqCtx, cloneSnapshotTxnOperator(ses, bh)); err != nil {
			return
		}
		if err = revalidateTimestampDataBranchCloneSource(
			reqCtx,
			ses,
			bh,
			snapshot,
			fromAccountId,
			stmt.SrcTable.SchemaName.String(),
			stmt.SrcTable.ObjectName.String(),
		); err != nil {
			return
		}
	}

	ctx = defines.AttachAccountId(reqCtx, toAccountId)

	var sql string
	var dstTableExistedBeforeRestore bool

	if snapshot == nil {
		if snapshotTS, err = tryToIncreaseTxnPhysicalTS(
			reqCtx, cloneSnapshotTxnOperator(ses, bh),
		); err != nil {
			return
		}
	}
	restoreSnapshotTS := generatedCloneRestoreSnapshotTS(ses, snapshotTS)
	sql = cloneTableRestoreSQL(stmt, restoreSnapshotTS)

	if stmt.CopyGrants && stmt.CreateTable.IfNotExists {
		if dstTableExistedBeforeRestore, err = cloneTargetTableExists(
			ctx,
			bh,
			stmt.CreateTable.Table.SchemaName.String(),
			stmt.CreateTable.Table.ObjectName.String(),
			toAccountId,
		); err != nil {
			return
		}
	}

	if err = bh.ExecRestore(ctx, sql, opAccountId, toAccountId); err != nil {
		return
	}

	if stmt.CopyGrants && !dstTableExistedBeforeRestore {
		copyGrantsSnapshotTS := restoreSnapshotTS
		if snapshot != nil && snapshot.TS != nil {
			copyGrantsSnapshotTS = snapshot.TS.PhysicalTime
		}
		if err = copyTablePrivileges(ctx, ses, bh,
			stmt.SrcTable.SchemaName.String(), stmt.SrcTable.ObjectName.String(),
			stmt.CreateTable.Table.SchemaName.String(), stmt.CreateTable.Table.ObjectName.String(),
			fromAccountId, toAccountId, copyGrantsSnapshotTS,
		); err != nil {
			return
		}
	}

	receipt.srcDb = stmt.SrcTable.SchemaName.String()
	receipt.srcTbl = stmt.SrcTable.ObjectName.String()
	receipt.dstDb = stmt.CreateTable.Table.SchemaName.String()
	receipt.dstTbl = stmt.CreateTable.Table.ObjectName.String()
	receipt.snapshot = snapshot
	receipt.snapshotTS = snapshotTS
	receipt.toAccount = toAccountId
	receipt.opAccount = opAccountId
	receipt.srcAccount = fromAccountId

	if faultInjected, _ = objectio.LogCNCloneFailedInjected(
		stmt.CreateTable.Table.SchemaName.String(), stmt.CreateTable.Table.ObjectName.String(),
	); faultInjected {
		err = moerr.NewInternalErrorNoCtx(injectedError)
	}

	return
}

func removeFailedTemporaryCloneAlias(
	ses *Session,
	dbName, alias string,
	existedBeforeRun bool,
	err error,
) {
	if err != nil && alias != "" && !existedBeforeRun {
		ses.RemoveTempTable(dbName, alias)
	}
}

// create database x clone y {MO_TS, SNAPSHOT}
// create database x clone y {MO_TS, SNAPSHOT} to account t
func handleCloneDatabase(
	execCtx *ExecCtx,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.CloneDatabase,
) (receipts []cloneReceipt, err error) {
	return handleCloneDatabaseWithSource(execCtx, ses, bh, stmt, nil)
}

func handleCloneDatabaseWithSource(
	execCtx *ExecCtx,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.CloneDatabase,
	resolvedSource *cloneDatabaseSource,
) (receipts []cloneReceipt, err error) {

	var (
		reqCtx = execCtx.reqCtx

		deferred func(error) error

		ctx1 context.Context

		sortedViews      []string
		rewrittenViewMap map[string]*tableInfo
		rewrittenViews   []string

		snapshotTS int64
		source     cloneDatabaseSource
		accounts   cloneDatabaseAccountResolution
	)

	oldDefault := ses.GetTxnCompileCtx().DefaultDatabase()
	defer func() {
		ses.GetTxnCompileCtx().SetDatabase(oldDefault)
	}()

	if reqCtx.Value(tree.CloneLevelCtxKey{}) == nil {
		reqCtx = context.WithValue(reqCtx, tree.CloneLevelCtxKey{}, tree.NormalCloneLevelDatabase)
	}

	if bh == nil {
		options := []*BackgroundExecOption{{forcePessimisticRC: true}}
		if stmt.IfNotExists {
			// The target lock must observe the holder's committed CREATE before
			// deciding whether this statement is a no-op. A private pessimistic
			// RC transaction refreshes after a lock wait; the retry below then
			// re-checks the target with that fresh snapshot.
			options[0].cloneSnapshotUsesBackgroundTxn = true
		}
		if bh, deferred, err = getBackExecutorWithTxnHandler(reqCtx, ses, options...); err != nil {
			return
		}

		defer func() {
			if deferred != nil {
				err = deferred(err)
			}
		}()
	}
	if resolvedSource != nil {
		accounts = cloneDatabaseAccountResolution{
			opAccountId: resolvedSource.opAccountId,
			toAccountId: resolvedSource.toAccountId,
			snapshot:    resolvedSource.snapshot,
		}
	} else if accounts, err = resolveCloneDatabaseAccounts(reqCtx, ses, bh, stmt); err != nil {
		return
	}
	if err = validateCloneDatabaseAccounts(reqCtx, accounts); err != nil {
		return
	}

	if stmt.IfNotExists {
		var exists bool
		if exists, err = checkCloneDatabaseTarget(
			reqCtx, ses, bh, accounts.toAccountId, stmt.DstDatabase.String(),
		); err != nil {
			return
		}
		if exists {
			return
		}
	}

	if resolvedSource != nil {
		source = *resolvedSource
	} else {
		if source, err = collectCloneDatabaseSource(reqCtx, ses, bh, stmt, &accounts); err != nil {
			return
		}
	}
	// Source collection validates public clone requests. Keep the persistence
	// boundary defensive too: resolved sources can come from data-branch flow,
	// and no path may create a target database for an imported package UDF whose
	// external lifecycle is unsupported by database clone.
	if err = validateCloneUserDefinedFunctions(source.userDefinedFuncs); err != nil {
		return
	}
	fromAccountID := source.opAccountId
	if source.snapshot != nil && source.snapshot.Tenant != nil {
		fromAccountID = source.snapshot.Tenant.TenantID
	}
	if err = lockNamedDataBranchCloneSnapshot(
		defines.AttachAccountId(reqCtx, fromAccountID), bh, source.snapshot,
	); err != nil {
		return
	}
	if err = lockDataBranchCloneDatabaseSources(reqCtx, ses, bh, source); err != nil {
		return
	}
	if err = revalidateTimestampDataBranchCloneDatabaseSource(reqCtx, ses, bh, source); err != nil {
		return
	}
	if source.hasFkCycle {
		oldForeignKeyChecksReplayable, hadForeignKeyChecksReplayability :=
			ses.getMigrationSystemVarReplayability("foreign_key_checks")
		oldForeignKeyChecks, getErr := ses.GetSessionSysVar("foreign_key_checks")
		if getErr != nil {
			return nil, getErr
		}
		if err = ses.SetSessionSysVar(reqCtx, "foreign_key_checks", int64(0)); err != nil {
			return nil, err
		}
		defer func() {
			restoreErr := ses.SetSessionSysVar(reqCtx, "foreign_key_checks", oldForeignKeyChecks)
			ses.restoreMigrationSystemVarReplayability(
				"foreign_key_checks",
				oldForeignKeyChecksReplayable,
				hadForeignKeyChecksReplayability,
			)
			if err == nil {
				err = restoreErr
			}
		}()
	}

	ctx1 = defines.AttachAccountId(reqCtx, source.toAccountId)
	if err = bh.Exec(ctx1,
		fmt.Sprintf("create database %s", quoteIdentifierForSQL(stmt.DstDatabase.String())),
	); err != nil {
		return
	}

	if stmt.AtTsExpr == nil {
		// consider the following example:
		// (within a session)
		//   ...
		// insert into t1 values (1) ---> commit ts (P2-L3)
		// insert into t1 values (2) ---> commit ts (P2-L3)
		// create table t2 clone t1 ---> the read snapshot ts is P2.
		//
		// limited by the format for the snapshot read TS, the logic TS is truncated,
		// so in this example, the clone cannot read the newly inserted data.
		//
		// so we try to increase the txn physical ts here to make sure the snapshot TS
		// the clone will get is greater than P2.
		if snapshotTS, err = tryToIncreaseTxnPhysicalTS(
			reqCtx, cloneSnapshotTxnOperator(ses, bh),
		); err != nil {
			return
		}
	}
	restoreSnapshotTS := generatedCloneRestoreSnapshotTS(ses, snapshotTS)
	sequenceSnapshotTS := restoreSnapshotTS
	if source.snapshot != nil && source.snapshot.TS != nil {
		sequenceSnapshotTS = source.snapshot.TS.PhysicalTime
	}

	cloneSequence := func(srcTbl *tableInfo) error {
		createSQL, rewriteErr := rewriteCloneSequenceCreateSQL(
			srcTbl.createSql,
			stmt.DstDatabase.String(),
			srcTbl.tblName,
			parserLowerCaseTableNames(ses),
		)
		if rewriteErr != nil {
			return rewriteErr
		}
		return restoreSequence(
			reqCtx,
			bh,
			createSQL,
			source.srcResolveDBName,
			srcTbl.tblName,
			stmt.DstDatabase.String(),
			srcTbl.tblName,
			sequenceSnapshotTS,
			fromAccountID,
			source.toAccountId,
		)
	}

	cloneTable := func(dstDb, dstTbl, srcDb, srcTbl string) error {
		srcTable := newQualifiedCloneTableName(srcDb, srcTbl, stmt.AtTsExpr)
		if stmt.AtTsExpr == nil && restoreSnapshotTS != 0 {
			srcTable.AtTsExpr = newMoTimestampHint(restoreSnapshotTS)
		}
		dstTable := newQualifiedCloneTableName(dstDb, dstTbl, nil)
		cloneStmt := &tree.CloneTable{
			SrcTable: srcTable,
			CreateTable: tree.CreateTable{
				Table:         dstTable,
				LikeTableName: srcTable,
				IsAsLike:      true,
			},
			ToAccountOpt: stmt.ToAccountOpt,
		}

		var (
			receipt     cloneReceipt
			tempExecCtx = &ExecCtx{
				// Database clone already holds every source-row lock in sorted
				// order, so nested table clones must not acquire them again.
				reqCtx: context.WithValue(reqCtx, dataBranchCloneLockCtxKey{}, false),
			}
		)
		tableSnapshot, resolveErr := resolveSnapshot(ses, cloneStmt.SrcTable.AtTsExpr)
		if resolveErr != nil {
			return resolveErr
		}

		if receipt, err = handleCloneTable(
			tempExecCtx, ses, cloneStmt, bh, &cloneAccountResolution{
				opAccountId: source.opAccountId,
				toAccountId: source.toAccountId,
				snapshot:    tableSnapshot,
			},
		); err != nil {
			return err
		}

		receipts = append(receipts, receipt)
		return nil
	}

	for _, srcTbl := range source.cloneableTableInfos() {
		if isSequence(srcTbl) {
			if err = cloneSequence(srcTbl); err != nil {
				return
			}
		}
	}

	for _, srcTbl := range source.cloneableTableInfos() {

		key := genKey(srcTbl.dbName, srcTbl.tblName)
		if _, ok := source.fkTableMap[key]; ok {
			continue
		}

		if srcTbl.typ == view || isSequence(srcTbl) {
			continue
		}

		if err = cloneTable(
			stmt.DstDatabase.String(), srcTbl.tblName,
			stmt.SrcDatabase.String(), srcTbl.tblName,
		); err != nil {
			return
		}
	}

	// clone foreign key related table
	for _, key := range source.sortedFkTbls {
		if tblInfo := source.fkTableMap[key]; tblInfo != nil {
			if !isCloneableCloneDatabaseTable(tblInfo) {
				continue
			}
			if err = cloneTable(
				stmt.DstDatabase.String(), tblInfo.tblName,
				stmt.SrcDatabase.String(), tblInfo.tblName,
			); err != nil {
				return
			}
		}
	}

	lowerCaseTableNames := parserLowerCaseTableNames(ses)
	// Build one omission closure before sorting views. Views and routines can
	// depend on each other, so filtering routines after view planning would
	// allow a view to bind an omitted UDF and fail during restoration.
	omissions, err := collectCloneDatabaseOmissionSet(
		reqCtx, source, lowerCaseTableNames,
	)
	if err != nil {
		return
	}
	applyCloneDatabaseOmissionSet(&source, omissions, lowerCaseTableNames)

	if len(source.viewMap) != 0 {
		viewSnapshot := prepareCloneViewSnapshot(source.snapshot, restoreSnapshotTS)
		fromAccount := source.opAccountId
		if viewSnapshot != nil && viewSnapshot.Tenant != nil {
			fromAccount = viewSnapshot.Tenant.TenantID
		}

		if sortedViews, err = sortedViewInfos(
			reqCtx, ses, bh, "", viewSnapshot, source.viewMap, fromAccount, source.toAccountId,
		); err != nil {
			return
		}
	}

	if err = validateCloneUserDefinedFunctions(source.userDefinedFuncs); err != nil {
		return
	}
	if source.userDefinedFuncs, err = rewriteCloneUserDefinedFunctionBodies(
		reqCtx,
		source.userDefinedFuncs,
		source.srcResolveDBName,
		stmt.DstDatabase.String(),
		lowerCaseTableNames,
	); err != nil {
		return
	}
	if source.storedProcedures, err = rewriteCloneStoredProcedureBodies(
		reqCtx,
		source.storedProcedures,
		source.srcResolveDBName,
		stmt.DstDatabase.String(),
		lowerCaseTableNames,
	); err != nil {
		return
	}

	// Routines are catalog metadata rather than mo_tables. Restore functions
	// before views so view binding can resolve function dependencies.
	routineTenant := ses.GetTenantInfo()
	if len(source.userDefinedFuncs) != 0 || len(source.storedProcedures) != 0 {
		routineTenant, err = resolveCloneDatabaseRoutineTenant(
			reqCtx, bh, ses.GetTenantInfo(), source.toAccountId,
		)
		if err != nil {
			return nil, err
		}
	}
	if err = restoreCloneDatabaseUserDefinedFunctions(
		ctx1, bh, routineTenant, source.userDefinedFuncs, stmt.DstDatabase.String(),
	); err != nil {
		return
	}
	if err = restoreCloneDatabaseStoredProcedures(
		ctx1, bh, routineTenant, source.storedProcedures, stmt.DstDatabase.String(),
	); err != nil {
		return
	}

	// clone view table
	if len(source.viewMap) != 0 {
		rewrittenViewMap, rewrittenViews, err = rewriteCloneViewInfos(
			source.viewMap,
			sortedViews,
			source.srcResolveDBName,
			stmt.DstDatabase.String(),
			parserLowerCaseTableNames(ses),
		)
		if err != nil {
			return
		}

		// The function metadata above is intentionally still uncommitted: the
		// clone must remain atomic. Mark view restoration so ResolveUdf uses the
		// same clone transaction and can bind newly restored functions.
		if err = restoreViews(withResolveUdfInCallerTxn(reqCtx), ses, bh, "", rewrittenViewMap, source.toAccountId, rewrittenViews, true); err != nil {
			return
		}
	}

	return
}

func prepareCloneViewSnapshot(snapshot *plan.Snapshot, snapshotTS int64) *plan.Snapshot {
	if plan.IsSnapshotValid(snapshot) || snapshotTS == 0 {
		return snapshot
	}
	if snapshot == nil {
		return &plan.Snapshot{
			TS: &timestamp.Timestamp{PhysicalTime: snapshotTS},
		}
	}

	cloned := *snapshot
	cloned.TS = &timestamp.Timestamp{PhysicalTime: snapshotTS}
	return &cloned
}

func rewriteCloneViewInfos(
	viewMap map[string]*tableInfo,
	sortedViews []string,
	srcDBName string,
	dstDBName string,
	lowerCaseTableNames int64,
) (map[string]*tableInfo, []string, error) {
	rewrittenViews := make([]string, 0, len(sortedViews))
	for _, key := range sortedViews {
		dbName, tblName := splitKey(key)
		if tblName == "" {
			rewrittenViews = append(rewrittenViews, strings.ReplaceAll(key, srcDBName, dstDBName))
			continue
		}
		if dbName == srcDBName {
			key = genKey(dstDBName, tblName)
		}
		rewrittenViews = append(rewrittenViews, key)
	}

	rewrittenViewMap := make(map[string]*tableInfo, len(viewMap))
	for key, info := range viewMap {
		dbName, tblName := splitKey(key)
		if tblName == "" {
			key = strings.ReplaceAll(key, srcDBName, dstDBName)
		} else if dbName == srcDBName {
			key = genKey(dstDBName, tblName)
		}
		createSQL, err := rewriteCloneCreateSQL(
			info.createSql,
			srcDBName,
			dstDBName,
			lowerCaseTableNames,
		)
		if err != nil {
			return nil, nil, err
		}

		clonedInfo := *info
		clonedInfo.dbName = dstDBName
		clonedInfo.createSql = createSQL
		rewrittenViewMap[key] = &clonedInfo
	}

	return rewrittenViewMap, rewrittenViews, nil
}

func rewriteCloneCreateSQL(sql, srcDBName, dstDBName string, lowerCaseTableNames int64) (string, error) {
	if srcDBName == "" || srcDBName == dstDBName {
		return sql, nil
	}

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, lowerCaseTableNames)
	if err != nil {
		return "", err
	}
	createView, ok := stmt.(*tree.CreateView)
	if !ok {
		return "", moerr.NewInternalErrorNoCtxf("clone view SQL is %T, expected *tree.CreateView", stmt)
	}

	opts := []tree.FmtCtxOption{tree.WithSingleQuoteString(), tree.WithQuoteIdentifier()}
	original := tree.StringWithOpts(createView, dialect.MYSQL, opts...)

	// Subscription metadata functions are private to the canonical
	// information_schema views. A cloned information_schema remains useful as
	// a local catalog snapshot, but must not turn a user-owned view into a new
	// cross-account execution boundary. Restore TABLES and COLUMNS from their
	// local-only definitions before remapping the clone target.
	if strings.EqualFold(srcDBName, sysview.InformationDBConst) {
		var localDDL string
		switch {
		case strings.EqualFold(string(createView.Name.ObjectName), "TABLES"):
			localDDL = sysview.InformationSchemaTablesV41DDL
		case strings.EqualFold(string(createView.Name.ObjectName), "COLUMNS"):
			localDDL = sysview.InformationSchemaColumnsV41DDL
		}
		if localDDL != "" {
			localStmt, parseErr := parsers.ParseOne(context.Background(), dialect.MYSQL, localDDL, lowerCaseTableNames)
			if parseErr != nil {
				return "", parseErr
			}
			localCreateView, localOK := localStmt.(*tree.CreateView)
			if !localOK {
				return "", moerr.NewInternalErrorNoCtxf(
					"local information_schema view SQL is %T, expected *tree.CreateView", localStmt)
			}
			createView = localCreateView
		}
	}
	cloneTargetDatabase := dstDBName
	if lowerCaseTableNames == 1 {
		cloneTargetDatabase = tree.NewCStr(dstDBName, lowerCaseTableNames).Compare()
	}
	remapDbInStmt(createView, remapDbContext{
		databases: map[string]string{
			tree.NewCStr(srcDBName, lowerCaseTableNames).Compare(): cloneTargetDatabase,
		},
		lowerCaseTableNames: lowerCaseTableNames,
	})
	rewritten := tree.StringWithOpts(createView, dialect.MYSQL, opts...)
	if rewritten == original {
		return sql, nil
	}
	if !strings.HasSuffix(strings.TrimSpace(rewritten), ";") {
		rewritten += ";"
	}
	return rewritten, nil
}

func rewriteCloneSequenceCreateSQL(
	sql string,
	dstDBName string,
	dstTblName string,
	lowerCaseTableNames int64,
) (string, error) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, lowerCaseTableNames)
	if err != nil {
		return "", err
	}
	createSequence, ok := stmt.(*tree.CreateSequence)
	if !ok {
		return "", moerr.NewInternalErrorNoCtxf(
			"clone sequence SQL is %T, expected *tree.CreateSequence", stmt)
	}
	targetName := newQualifiedCloneTableName(dstDBName, dstTblName, nil)
	createSequence.Name = &targetName
	return tree.StringWithOpts(
		createSequence,
		dialect.MYSQL,
		tree.WithSingleQuoteString(),
		tree.WithQuoteIdentifier(),
	), nil
}

func tryToIncreaseTxnPhysicalTS(
	ctx context.Context, txnOp client.TxnOperator,
) (updatedPhysical int64, err error) {

	curTxnPhysicalTS := txnOp.SnapshotTS().PhysicalTime

	if ctx.Value(defines.TenantIDKey{}) == nil {
		return curTxnPhysicalTS, nil
	}

	// a slight increase added to the physical to make sure
	// the updated ts is greater than the old txn timestamp (physical + logic)
	curTxnPhysicalTS += int64(time.Microsecond)
	if err = txnOp.UpdateSnapshot(ctx, timestamp.Timestamp{
		PhysicalTime: curTxnPhysicalTS,
	}); err != nil {
		return
	}

	updatedPhysical = txnOp.SnapshotTS().PhysicalTime
	if updatedPhysical <= curTxnPhysicalTS {
		return 0, moerr.NewInternalErrorNoCtxf("try to update the snapshot ts failed in clone database")
	}

	// return a nanosecond precision
	updatedPhysical -= int64(time.Nanosecond)

	return updatedPhysical, nil
}

func updateBranchMetaTable(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	receipt *cloneReceipt,
) (err error) {

	var (
		srcTblDef  *plan.TableDef
		dstTblDef  *plan.TableDef
		dstDB      engine.Database
		dstRel     engine.Relation
		cloneTxnOp TxnOperator
		level      string
	)

	switch ctx.Value(tree.CloneLevelCtxKey{}).(tree.CloneLevelType) {
	case tree.NormalCloneLevelTable:
		level = dataBranchLevel_Table
	case tree.NormalCloneLevelDatabase:
		level = dataBranchLevel_Database
	case tree.NormalCloneLevelAccount:
		level = dataBranchLevel_Account
	default:
		// we do not record the branch metadata for restore
		return nil
	}

	srcCtx := defines.AttachAccountId(ctx, receipt.srcAccount)
	tcc := ses.GetTxnCompileCtx()
	origCtx := tcc.GetContext()
	tcc.SetContext(srcCtx)
	defer tcc.SetContext(origCtx)

	// The metadata parent must be the physical generation that supplied the
	// clone data. For snapshot clones that can differ from the table currently
	// reachable by name after one or more copy-and-swap ALTERs.
	if _, srcTblDef, err = tcc.Resolve(receipt.srcDb, receipt.srcTbl, receipt.snapshot); err != nil {
		return err
	}
	if srcTblDef == nil {
		return moerr.NewNoSuchTable(srcCtx, receipt.srcDb, receipt.srcTbl)
	}

	dstCtx := defines.AttachAccountId(ctx, receipt.toAccount)

	// the back session did the clone operation,
	// we need it's txnOp to read the uncommit table info.
	cloneTxnOp = bh.(*backExec).backSes.GetTxnHandler().GetTxn()
	if dstDB, err = ses.proc.GetSessionInfo().StorageEngine.Database(
		dstCtx, receipt.dstDb, cloneTxnOp,
	); err != nil {
		return err
	}

	if dstRel, err = dstDB.Relation(dstCtx, receipt.dstTbl, nil); err != nil {
		return err
	}
	dstTblDef = dstRel.GetTableDef(dstCtx)

	if receipt.snapshot != nil {
		receipt.snapshotTS = receipt.snapshot.TS.PhysicalTime
	}

	// Persist the resolved ids so the branch-protect snapshot insert (and any
	// other downstream bookkeeping) can avoid re-resolving them.
	receipt.srcTableID = srcTblDef.TblId
	receipt.dstTableID = dstTblDef.TblId

	// write branch info into branch_metadata table
	updateMetadataSql := fmt.Sprintf(
		insertIntoBranchMetadataSql,
		catalog.MO_CATALOG,
		catalog.MO_BRANCH_METADATA,
		dstTblDef.TblId,
		receipt.snapshotTS,
		srcTblDef.TblId,
		receipt.toAccount,
		level,
	)

	tempCtx := ctx
	if receipt.opAccount != sysAccountID {
		tempCtx = defines.AttachAccountId(tempCtx, sysAccountID)
	}
	if err = bh.Exec(tempCtx, updateMetadataSql); err != nil {
		return err
	}

	return nil
}
