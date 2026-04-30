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
	"bytes"
	"context"
	"fmt"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

const (
	dataBranchPitrNamePrefix = "__mo_data_branch_pitr"
	dataBranchPitrLength     = 1
	dataBranchPitrUnit       = "y"
	dataBranchPitrKind       = "internal"
)

type dataBranchPitrRecord struct {
	pitrName     string
	level        string
	accountID    uint64
	accountName  string
	databaseName string
	tableName    string
	objectID     uint64
}

type dataBranchExistingPitrRecord struct {
	exists     bool
	kind       string
	active     bool
	pitrLength uint64
	pitrUnit   string
}

func newBranchHashmapAllocator(limitRate float64) *branchHashmapAllocator {
	throttler := rscthrottler.NewMemThrottler(
		"DataBranchHashmap",
		limitRate,
		rscthrottler.WithAcquirePolicy(rscthrottler.AcquirePolicyForDataBranch),
	)
	return &branchHashmapAllocator{
		upstream:  malloc.GetDefault(nil),
		throttler: throttler,
	}
}

func (a *branchHashmapAllocator) Available() int64 {
	if a == nil || a.throttler == nil {
		return 0
	}
	return a.throttler.Available()
}

func (a *branchHashmapAllocator) Allocate(
	size uint64,
	hints malloc.Hints,
) ([]byte, malloc.Deallocator, error) {

	if size == 0 {
		return nil, nil, nil
	}
	if a.throttler != nil {
		if _, ok := a.throttler.Acquire(int64(size)); !ok {
			return nil, nil, nil
		}
	}
	buf, dec, err := a.upstream.Allocate(size, hints)
	if err != nil {
		if a.throttler != nil {
			a.throttler.Release(int64(size))
		}
		return nil, nil, err
	}
	if buf == nil {
		if a.throttler != nil {
			a.throttler.Release(int64(size))
		}
		return nil, nil, nil
	}
	return buf, &branchHashmapDeallocator{
		upstream:  dec,
		throttler: a.throttler,
		size:      size,
	}, nil
}

func (d *branchHashmapDeallocator) Deallocate() {
	if d.upstream != nil {
		d.upstream.Deallocate()
	}
	if d.throttler != nil && d.size > 0 {
		d.throttler.Release(int64(d.size))
	}
}

func (d *branchHashmapDeallocator) As(target malloc.Trait) bool {
	if d.upstream != nil {
		return d.upstream.As(target)
	}
	return false
}

func typeMatched(vec *vector.Vector, typ types.Type) bool {
	if vec == nil {
		return false
	}

	t := vec.GetType()
	return t.Oid == typ.Oid && t.Width == typ.Width && t.Scale == typ.Scale
}

func (retBatchPool *retBatchList) acquireRetBatch(tblStuff tableStuff, forTombstone bool) *batch.Batch {
	retBatchPool.mu.Lock()
	defer retBatchPool.mu.Unlock()

	var (
		bat *batch.Batch
	)

	if retBatchPool.pinned == nil {
		retBatchPool.pinned = make(map[*batch.Batch]struct{})
	}

	if retBatchPool.dataVecCnt == 0 {
		retBatchPool.dataVecCnt = len(tblStuff.def.colNames)
		retBatchPool.tombVecCnt = 3
		retBatchPool.dataTypes = tblStuff.def.colTypes
		retBatchPool.tombstoneType = tblStuff.def.colTypes[tblStuff.def.pkColIdx]
		retBatchPool.tombRowIDType = types.T_Rowid.ToType()
		retBatchPool.tombKeyType = types.T_varbinary.ToType()
	}

	if forTombstone {
		if len(retBatchPool.tList) == 0 {
			bat = batch.NewWithSize(retBatchPool.tombVecCnt)
			bat.Vecs[0] = vector.NewVec(retBatchPool.tombstoneType)
			bat.Vecs[1] = vector.NewVec(retBatchPool.tombRowIDType)
			bat.Vecs[2] = vector.NewVec(retBatchPool.tombKeyType)
			goto done
		}

		bat = retBatchPool.tList[0]
		retBatchPool.tList = retBatchPool.tList[1:]

		if bat.VectorCount() != retBatchPool.tombVecCnt {
			panic(moerr.NewInternalErrorNoCtxf("retBatchPool: unexpected tombstone vec count %d, expect %d", bat.VectorCount(), retBatchPool.tombVecCnt))
		}
		if !typeMatched(bat.Vecs[0], retBatchPool.tombstoneType) {
			panic(moerr.NewInternalErrorNoCtxf("retBatchPool: tombstone vec type mismatch, got %v expect %v", bat.Vecs[0].GetType(), retBatchPool.tombstoneType))
		}
		if !typeMatched(bat.Vecs[1], retBatchPool.tombRowIDType) {
			panic(moerr.NewInternalErrorNoCtxf("retBatchPool: tombstone rowid vec type mismatch, got %v expect %v", bat.Vecs[1].GetType(), retBatchPool.tombRowIDType))
		}
		if !typeMatched(bat.Vecs[2], retBatchPool.tombKeyType) {
			panic(moerr.NewInternalErrorNoCtxf("retBatchPool: tombstone key vec type mismatch, got %v expect %v", bat.Vecs[2].GetType(), retBatchPool.tombKeyType))
		}

		bat.CleanOnlyData()
		goto done

	} else {
		if len(retBatchPool.dList) == 0 {
			bat = batch.NewWithSize(retBatchPool.dataVecCnt)
			for i := range tblStuff.def.colNames {
				bat.Vecs[i] = vector.NewVec(retBatchPool.dataTypes[i])
			}
			goto done
		}

		bat = retBatchPool.dList[0]
		retBatchPool.dList = retBatchPool.dList[1:]

		if bat.VectorCount() != retBatchPool.dataVecCnt {
			panic(moerr.NewInternalErrorNoCtxf("retBatchPool: unexpected data vec count %d, expect %d", bat.VectorCount(), retBatchPool.dataVecCnt))
		}

		for i := 0; i < retBatchPool.dataVecCnt; i++ {
			if !typeMatched(bat.Vecs[i], retBatchPool.dataTypes[i]) {
				panic(moerr.NewInternalErrorNoCtxf("retBatchPool: data vec[%d] type mismatch, got %v expect %v", i, bat.Vecs[i].GetType(), retBatchPool.dataTypes[i]))
			}
			bat.Vecs[i].CleanOnlyData()
		}

		goto done
	}

done:
	retBatchPool.pinned[bat] = struct{}{}
	return bat
}

func (retBatchPool *retBatchList) releaseRetBatch(bat *batch.Batch, forTombstone bool) {
	if retBatchPool == nil {
		panic(moerr.NewInternalErrorNoCtx("retBatchPool is nil"))
	}

	retBatchPool.mu.Lock()
	defer retBatchPool.mu.Unlock()

	if _, ok := retBatchPool.pinned[bat]; !ok {
		msg := "retBatchPool: release unknown or already released batch"
		panic(moerr.NewInternalErrorNoCtx(msg))
	}

	expectCnt := retBatchPool.dataVecCnt
	if forTombstone {
		expectCnt = retBatchPool.tombVecCnt
	}
	if bat.VectorCount() != expectCnt {
		panic(moerr.NewInternalErrorNoCtxf("retBatchPool: release vec count %d, expect %d", bat.VectorCount(), expectCnt))
	}

	bat.CleanOnlyData()

	if forTombstone {
		retBatchPool.tList = append(retBatchPool.tList, bat)
	} else {
		retBatchPool.dList = append(retBatchPool.dList, bat)
	}

	delete(retBatchPool.pinned, bat)
}

func (retBatchPool *retBatchList) freeAllRetBatches(mp *mpool.MPool) {
	if retBatchPool == nil {
		return
	}

	retBatchPool.mu.Lock()
	defer func() {
		retBatchPool.mu.Unlock()
	}()

	for _, bat := range retBatchPool.dList {
		bat.Clean(mp)
	}
	for _, bat := range retBatchPool.tList {
		bat.Clean(mp)
	}
	for bat := range retBatchPool.pinned {
		bat.Clean(mp)
	}

	retBatchPool.dList = nil
	retBatchPool.tList = nil
	retBatchPool.pinned = nil

}

func handleDataBranch(
	execCtx *ExecCtx,
	ses *Session,
	stmt tree.Statement,
) error {

	switch st := stmt.(type) {
	case *tree.DataBranchCreateTable:
		return dataBranchCreateTable(execCtx, ses, st)
	case *tree.DataBranchCreateDatabase:
		return dataBranchCreateDatabase(execCtx, ses, st)
	case *tree.DataBranchDeleteTable:
		return dataBranchDeleteTable(execCtx, ses, st)
	case *tree.DataBranchDeleteDatabase:
		return dataBranchDeleteDatabase(execCtx, ses, st)
	case *tree.DataBranchDiff:
		return handleBranchDiff(execCtx, ses, st)
	case *tree.DataBranchMerge:
		return handleBranchMerge(execCtx, ses, st)
	case *tree.DataBranchPick:
		return handleBranchPick(execCtx, ses, st)
	default:
		return moerr.NewNotSupportedNoCtxf("data branch not supported: %v", st)
	}
}

func dataBranchCreateTable(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.DataBranchCreateTable,
) (err error) {
	var (
		bh          BackgroundExec
		deferred    func(error) error
		receipt     cloneReceipt
		cloneStmt   *tree.CloneTable
		tempExecCtx *ExecCtx
	)

	if bh, deferred, err = getBackExecutor(execCtx.reqCtx, ses); err != nil {
		return
	}

	defer func() {
		if deferred != nil {
			err = deferred(err)
		}
	}()

	if err = checkBranchQuota(execCtx.reqCtx, ses, bh, 1); err != nil {
		return err
	}

	cloneStmt = &tree.CloneTable{
		SrcTable:     stmt.SrcTable,
		CreateTable:  stmt.CreateTable,
		ToAccountOpt: stmt.ToAccountOpt,
	}

	oldDefault := ses.GetTxnCompileCtx().DefaultDatabase()
	defer func() {
		ses.GetTxnCompileCtx().SetDatabase(oldDefault)
	}()

	//data branch create table xxx from yyy snap_opt to_account_op;
	re := regexp.MustCompile(`(?i)^DATA\s+BRANCH\s+CREATE\s+TABLE\s+(\S+)\s+FROM\s+(.+?);?$`)
	srcAndDst := re.FindStringSubmatch(execCtx.input.sql)
	if srcAndDst == nil {
		return moerr.NewInternalErrorNoCtxf("cannot find src and dst table: %s", execCtx.input.sql)
	}

	sql := fmt.Sprintf("CREATE TABLE %s CLONE %s", srcAndDst[1], srcAndDst[2])

	execCtx.reqCtx = context.WithValue(execCtx.reqCtx, tree.CloneLevelCtxKey{}, tree.NormalCloneLevelTable)

	tempExecCtx = &ExecCtx{
		reqCtx: execCtx.reqCtx,
		input:  &UserInput{sql: sql},
	}

	if receipt, err = handleCloneTable(tempExecCtx, ses, cloneStmt, bh); err != nil {
		return
	}

	if err = updateBranchMetaTable(execCtx.reqCtx, ses, bh, receipt); err != nil {
		return
	}

	if err = createDataBranchTablePitr(execCtx.reqCtx, ses, bh, receipt); err != nil {
		return
	}

	return nil
}

func dataBranchCreateDatabase(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.DataBranchCreateDatabase,
) (err error) {
	var (
		bh       BackgroundExec
		deferred func(error) error
		receipts []cloneReceipt
	)

	if bh, deferred, err = getBackExecutor(execCtx.reqCtx, ses); err != nil {
		return
	}

	defer func() {
		if deferred != nil {
			err = deferred(err)
		}
	}()

	execCtx.reqCtx = context.WithValue(
		execCtx.reqCtx, tree.CloneLevelCtxKey{}, tree.NormalCloneLevelDatabase,
	)

	if receipts, err = handleCloneDatabase(execCtx, ses, bh, &stmt.CloneDatabase); err != nil {
		return
	}

	if err = checkBranchQuota(execCtx.reqCtx, ses, bh, int64(len(receipts))); err != nil {
		return err
	}

	for _, rcpt := range receipts {
		if err = updateBranchMetaTable(execCtx.reqCtx, ses, bh, rcpt); err != nil {
			return
		}
	}

	if err = createDataBranchDatabasePitr(execCtx.reqCtx, ses, bh, &stmt.CloneDatabase, receipts); err != nil {
		return
	}

	return nil
}

func dataBranchPitrName(level string, objectID string) string {
	return fmt.Sprintf("%s_%s_%s", dataBranchPitrNamePrefix, level, objectID)
}

func dataBranchCloneTxn(ctx context.Context, bh BackgroundExec) (TxnOperator, error) {
	be, ok := bh.(*backExec)
	if !ok || be.backSes == nil || be.backSes.GetTxnHandler() == nil {
		return nil, moerr.NewInternalError(ctx, "data branch pitr requires background transaction")
	}
	return be.backSes.GetTxnHandler().GetTxn(), nil
}

func createDataBranchTablePitr(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	receipt cloneReceipt,
) error {
	if receipt.dstDb == "" || receipt.dstTbl == "" {
		return moerr.NewInternalError(ctx, "data branch table pitr requires target table")
	}

	cloneTxnOp, err := dataBranchCloneTxn(ctx, bh)
	if err != nil {
		return err
	}
	if err = createDataBranchTablePitrForTarget(
		ctx, ses, bh, cloneTxnOp, receipt.toAccount, receipt.dstDb, receipt.dstTbl, "target",
	); err != nil {
		return err
	}
	if receipt.srcDb == "" || receipt.srcTbl == "" {
		return nil
	}
	return createDataBranchTablePitrForTarget(
		ctx, ses, bh, cloneTxnOp, receipt.srcAccount, receipt.srcDb, receipt.srcTbl, "source",
	)
}

func createDataBranchDatabasePitr(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.CloneDatabase,
	receipts []cloneReceipt,
) error {
	if stmt == nil {
		return moerr.NewInternalError(ctx, "data branch database pitr requires clone database statement")
	}

	var (
		toAccountID  uint32
		srcAccountID uint32
		err          error
	)
	if len(receipts) > 0 {
		toAccountID = receipts[0].toAccount
		srcAccountID = receipts[0].srcAccount
	} else {
		var snapshot *plan2.Snapshot
		srcAccountID, toAccountID, snapshot, err = getOpAndToAccountId(
			ctx, ses, bh, stmt.ToAccountOpt, stmt.AtTsExpr,
		)
		if err != nil {
			return err
		}
		if snapshot != nil && snapshot.Tenant != nil {
			srcAccountID = snapshot.Tenant.TenantID
		}
	}

	dstDb := stmt.DstDatabase.String()
	if dstDb == "" {
		return moerr.NewInternalError(ctx, "data branch database pitr requires target database")
	}
	cloneTxnOp, err := dataBranchCloneTxn(ctx, bh)
	if err != nil {
		return err
	}
	if err = createDataBranchDatabasePitrForTarget(
		ctx, ses, bh, cloneTxnOp, toAccountID, dstDb, "target",
	); err != nil {
		return err
	}

	srcDb := stmt.SrcDatabase.String()
	if len(receipts) > 0 && receipts[0].srcDb != "" {
		srcDb = receipts[0].srcDb
	}
	if srcDb == "" {
		return nil
	}
	return createDataBranchDatabasePitrForTarget(
		ctx, ses, bh, cloneTxnOp, srcAccountID, srcDb, "source",
	)
}

func createDataBranchTablePitrForTarget(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	cloneTxnOp TxnOperator,
	accountID uint32,
	dbName string,
	tableName string,
	role string,
) error {
	tableID, err := dataBranchTableID(ctx, ses, cloneTxnOp, accountID, dbName, tableName)
	if err != nil {
		return err
	}
	pitrName := dataBranchPitrName("table", strconv.FormatUint(tableID, 10))
	accountName, err := dataBranchPitrAccountName(ctx, ses, bh, accountID)
	if err != nil {
		return err
	}
	logutil.Info(
		"DataBranch-CreatePITR-Table",
		zap.String("role", role),
		zap.String("pitr-name", pitrName),
		zap.Uint64("table-id", tableID),
		zap.Uint32("account-id", accountID),
		zap.String("database", dbName),
		zap.String("table", tableName),
	)
	return createDataBranchPitrRecord(ctx, ses, bh, cloneTxnOp, dataBranchPitrRecord{
		pitrName:     pitrName,
		level:        tree.PITRLEVELTABLE.String(),
		accountID:    uint64(accountID),
		accountName:  accountName,
		databaseName: dbName,
		tableName:    tableName,
		objectID:     tableID,
	})
}

func createDataBranchDatabasePitrForTarget(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	cloneTxnOp TxnOperator,
	accountID uint32,
	dbName string,
	role string,
) error {
	databaseID, err := dataBranchDatabaseID(ctx, ses, cloneTxnOp, accountID, dbName)
	if err != nil {
		return err
	}
	pitrName := dataBranchPitrName("database", strconv.FormatUint(databaseID, 10))
	accountName, err := dataBranchPitrAccountName(ctx, ses, bh, accountID)
	if err != nil {
		return err
	}
	logutil.Info(
		"DataBranch-CreatePITR-Database",
		zap.String("role", role),
		zap.String("pitr-name", pitrName),
		zap.Uint64("database-id", databaseID),
		zap.Uint32("account-id", accountID),
		zap.String("database", dbName),
	)
	return createDataBranchPitrRecord(ctx, ses, bh, cloneTxnOp, dataBranchPitrRecord{
		pitrName:     pitrName,
		level:        tree.PITRLEVELDATABASE.String(),
		accountID:    uint64(accountID),
		accountName:  accountName,
		databaseName: dbName,
		objectID:     databaseID,
	})
}

func dataBranchDatabaseID(
	ctx context.Context,
	ses *Session,
	cloneTxnOp TxnOperator,
	accountID uint32,
	dbName string,
) (uint64, error) {
	dbCtx := defines.AttachAccountId(ctx, accountID)
	db, err := ses.proc.GetSessionInfo().StorageEngine.Database(dbCtx, dbName, cloneTxnOp)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(db.GetDatabaseId(dbCtx), 10, 64)
}

func dataBranchTableID(
	ctx context.Context,
	ses *Session,
	cloneTxnOp TxnOperator,
	accountID uint32,
	dbName string,
	tableName string,
) (uint64, error) {
	dbCtx := defines.AttachAccountId(ctx, accountID)
	db, err := ses.proc.GetSessionInfo().StorageEngine.Database(dbCtx, dbName, cloneTxnOp)
	if err != nil {
		return 0, err
	}
	rel, err := db.Relation(dbCtx, tableName, nil)
	if err != nil {
		return 0, err
	}
	return rel.GetTableDef(dbCtx).TblId, nil
}

func dataBranchPitrAccountName(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	accountID uint32,
) (string, error) {
	if accountID == sysAccountID {
		return sysAccountName, nil
	}
	if tenantInfo := ses.GetTenantInfo(); tenantInfo != nil && accountID == tenantInfo.GetTenantID() {
		return tenantInfo.GetTenant(), nil
	}

	sysCtx := defines.AttachAccountId(ctx, sysAccountID)
	sql := fmt.Sprintf(
		"select account_name from mo_catalog.mo_account where account_id = %d",
		accountID,
	)
	bh.ClearExecResultSet()
	if err := bh.Exec(sysCtx, sql); err != nil {
		return "", err
	}
	erArray, err := getResultSet(sysCtx, bh)
	if err != nil {
		return "", err
	}
	if !execResultArrayHasData(erArray) {
		return "", moerr.NewInternalErrorf(ctx, "account %d does not exist", accountID)
	}
	return erArray[0].GetString(sysCtx, 0, 0)
}

func createDataBranchPitrRecord(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	cloneTxnOp TxnOperator,
	record dataBranchPitrRecord,
) error {
	now := cloneTxnOp.SnapshotTS().ToStdTime().UTC().UnixNano()
	existing, err := getDataBranchPitrRecord(ctx, bh, record.pitrName, record.accountID)
	if err != nil {
		return err
	}
	if existing.exists {
		if err = ensureDataBranchPitrRecord(ctx, bh, now, record, existing); err != nil {
			return err
		}
		return ensureDataBranchMoCatalogPitr(
			ctx, ses, bh, cloneTxnOp, now,
		)
	}

	pitrID, err := uuid.NewV7()
	if err != nil {
		return err
	}
	sql, err := getSqlForCreateDataBranchPitrRecord(ctx, pitrID.String(), now, record)
	if err != nil {
		return err
	}

	sysCtx := defines.AttachAccountId(ctx, sysAccountID)
	if err = bh.Exec(sysCtx, sql); err != nil {
		// Concurrent branch creation can win the primary key race. Re-read the
		// record and normalize it if it is the expected internal PITR.
		existing, readErr := getDataBranchPitrRecord(ctx, bh, record.pitrName, record.accountID)
		if readErr == nil && existing.exists && existing.kind == dataBranchPitrKind {
			if ensureErr := ensureDataBranchPitrRecord(ctx, bh, now, record, existing); ensureErr != nil {
				return ensureErr
			}
			return ensureDataBranchMoCatalogPitr(ctx, ses, bh, cloneTxnOp, now)
		}
		return err
	}
	return ensureDataBranchMoCatalogPitr(ctx, ses, bh, cloneTxnOp, now)
}

func getDataBranchPitrRecord(
	ctx context.Context,
	bh BackgroundExec,
	pitrName string,
	accountID uint64,
) (dataBranchExistingPitrRecord, error) {
	sysCtx := defines.AttachAccountId(ctx, sysAccountID)
	sql := fmt.Sprintf(
		"select kind, pitr_status, pitr_length, pitr_unit from mo_catalog.mo_pitr where pitr_name = %s and create_account = %d",
		quoteSQLStringLiteral(pitrName),
		accountID,
	)
	bh.ClearExecResultSet()
	if err := bh.Exec(sysCtx, sql); err != nil {
		return dataBranchExistingPitrRecord{}, err
	}
	erArray, err := getResultSet(sysCtx, bh)
	if err != nil {
		return dataBranchExistingPitrRecord{}, err
	}
	if !execResultArrayHasData(erArray) {
		return dataBranchExistingPitrRecord{}, nil
	}
	kind, err := erArray[0].GetString(sysCtx, 0, 0)
	if err != nil {
		return dataBranchExistingPitrRecord{}, err
	}
	status, err := erArray[0].GetUint64(sysCtx, 0, 1)
	if err != nil {
		return dataBranchExistingPitrRecord{}, err
	}
	pitrLength, err := erArray[0].GetUint64(sysCtx, 0, 2)
	if err != nil {
		return dataBranchExistingPitrRecord{}, err
	}
	pitrUnit, err := erArray[0].GetString(sysCtx, 0, 3)
	if err != nil {
		return dataBranchExistingPitrRecord{}, err
	}
	return dataBranchExistingPitrRecord{
		exists:     true,
		kind:       kind,
		active:     status == 1,
		pitrLength: pitrLength,
		pitrUnit:   pitrUnit,
	}, nil
}

func ensureDataBranchPitrRecord(
	ctx context.Context,
	bh BackgroundExec,
	now int64,
	record dataBranchPitrRecord,
	existing dataBranchExistingPitrRecord,
) error {
	setParts := []string{
		fmt.Sprintf("modified_time = %d", now),
		fmt.Sprintf("level = %s", quoteSQLStringLiteral(record.level)),
		fmt.Sprintf("account_id = %d", record.accountID),
		fmt.Sprintf("account_name = %s", quoteSQLStringLiteral(record.accountName)),
		fmt.Sprintf("database_name = %s", quoteSQLStringLiteral(record.databaseName)),
		fmt.Sprintf("table_name = %s", quoteSQLStringLiteral(record.tableName)),
		fmt.Sprintf("obj_id = %d", record.objectID),
		"pitr_status = 1",
		fmt.Sprintf("kind = %s", quoteSQLStringLiteral(dataBranchPitrKind)),
	}
	if !existing.active {
		setParts = append(setParts, fmt.Sprintf("pitr_status_changed_time = %d", now))
	}
	needDurationUpdate, err := dataBranchPitrDurationNeedsUpdate(existing.pitrLength, existing.pitrUnit)
	if err != nil {
		return err
	}
	if needDurationUpdate {
		setParts = append(setParts,
			fmt.Sprintf("pitr_length = %d", dataBranchPitrLength),
			fmt.Sprintf("pitr_unit = %s", quoteSQLStringLiteral(dataBranchPitrUnit)),
		)
	}

	sysCtx := defines.AttachAccountId(ctx, sysAccountID)
	sql := fmt.Sprintf(
		"update mo_catalog.mo_pitr set %s where pitr_name = %s and create_account = %d",
		strings.Join(setParts, ", "),
		quoteSQLStringLiteral(record.pitrName),
		record.accountID,
	)
	return bh.Exec(sysCtx, sql)
}

func dataBranchPitrDurationNeedsUpdate(oldLength uint64, oldUnit string) (bool, error) {
	oldMinTs, err := addTimeSpan(time.Time{}, int(oldLength), oldUnit)
	if err != nil {
		return false, err
	}
	newMinTs, err := addTimeSpan(time.Time{}, dataBranchPitrLength, dataBranchPitrUnit)
	if err != nil {
		return false, err
	}
	return newMinTs.Before(oldMinTs), nil
}

func getSqlForCreateDataBranchPitrRecord(
	ctx context.Context,
	pitrID string,
	now int64,
	record dataBranchPitrRecord,
) (string, error) {
	if err := inputNameIsInvalid(ctx, record.pitrName); err != nil {
		return "", err
	}
	return fmt.Sprintf(`insert into mo_catalog.mo_pitr(
		pitr_id,
		pitr_name,
		create_account,
		create_time,
		modified_time,
		level,
		account_id,
		account_name,
		database_name,
		table_name,
		obj_id,
		pitr_length,
		pitr_unit,
		pitr_status_changed_time,
		kind) values (%s, %s, %d, %d, %d, %s, %d, %s, %s, %s, %d, %d, %s, %d, %s);`,
		quoteSQLStringLiteral(pitrID),
		quoteSQLStringLiteral(record.pitrName),
		record.accountID,
		now,
		now,
		quoteSQLStringLiteral(record.level),
		record.accountID,
		quoteSQLStringLiteral(record.accountName),
		quoteSQLStringLiteral(record.databaseName),
		quoteSQLStringLiteral(record.tableName),
		record.objectID,
		dataBranchPitrLength,
		quoteSQLStringLiteral(dataBranchPitrUnit),
		now,
		quoteSQLStringLiteral(dataBranchPitrKind),
	), nil
}

func ensureDataBranchMoCatalogPitr(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	cloneTxnOp TxnOperator,
	now int64,
) error {
	sysCtx := defines.AttachAccountId(ctx, sysAccountID)
	sql := fmt.Sprintf(
		"select pitr_length, pitr_unit from mo_catalog.mo_pitr where pitr_name = %s",
		quoteSQLStringLiteral(SYSMOCATALOGPITR),
	)
	bh.ClearExecResultSet()
	if err := bh.Exec(sysCtx, sql); err != nil {
		return err
	}
	erArray, err := getResultSet(sysCtx, bh)
	if err != nil {
		return err
	}

	if execResultArrayHasData(erArray) {
		sysPitrLength, err := erArray[0].GetUint64(sysCtx, 0, 0)
		if err != nil {
			return err
		}
		sysPitrUnit, err := erArray[0].GetString(sysCtx, 0, 1)
		if err != nil {
			return err
		}
		oldMinTs, err := addTimeSpan(time.Time{}, int(sysPitrLength), sysPitrUnit)
		if err != nil {
			return err
		}
		newMinTs, err := addTimeSpan(time.Time{}, dataBranchPitrLength, dataBranchPitrUnit)
		if err != nil {
			return err
		}
		setParts := []string{
			fmt.Sprintf("modified_time = %d", now),
			"pitr_status = 1",
			fmt.Sprintf("kind = %s", quoteSQLStringLiteral(dataBranchPitrKind)),
		}
		if !newMinTs.Before(oldMinTs) {
			sql = fmt.Sprintf(
				"update mo_catalog.mo_pitr set %s where pitr_name = %s and create_account = %d",
				strings.Join(setParts, ", "),
				quoteSQLStringLiteral(SYSMOCATALOGPITR),
				sysAccountID,
			)
			return bh.Exec(sysCtx, sql)
		}
		setParts = append(setParts,
			fmt.Sprintf("pitr_length = %d", dataBranchPitrLength),
			fmt.Sprintf("pitr_unit = %s", quoteSQLStringLiteral(dataBranchPitrUnit)),
		)
		sql = fmt.Sprintf(
			"update mo_catalog.mo_pitr set %s where pitr_name = %s and create_account = %d",
			strings.Join(setParts, ", "),
			quoteSQLStringLiteral(SYSMOCATALOGPITR),
			sysAccountID,
		)
		return bh.Exec(sysCtx, sql)
	}

	moCatalogDB, err := ses.proc.GetSessionInfo().StorageEngine.Database(
		sysCtx, catalog.MO_CATALOG, cloneTxnOp,
	)
	if err != nil {
		return err
	}
	moCatalogID, err := strconv.ParseUint(moCatalogDB.GetDatabaseId(sysCtx), 10, 64)
	if err != nil {
		return err
	}
	pitrID, err := uuid.NewV7()
	if err != nil {
		return err
	}
	sql, err = getSqlForCreateDataBranchPitrRecord(ctx, pitrID.String(), now, dataBranchPitrRecord{
		pitrName:     SYSMOCATALOGPITR,
		level:        tree.PITRLEVELDATABASE.String(),
		accountID:    sysAccountID,
		accountName:  sysAccountName,
		databaseName: catalog.MO_CATALOG,
		objectID:     moCatalogID,
	})
	if err != nil {
		return err
	}
	return bh.Exec(sysCtx, sql)
}

func deleteDataBranchInternalPitrRecord(
	ctx context.Context,
	bh BackgroundExec,
	accountID uint64,
	pitrName string,
) error {
	sysCtx := defines.AttachAccountId(ctx, sysAccountID)
	sql := fmt.Sprintf(
		"delete from mo_catalog.mo_pitr where pitr_name = %s and create_account = %d and (kind = %s or pitr_name like %s)",
		quoteSQLStringLiteral(pitrName),
		accountID,
		quoteSQLStringLiteral(dataBranchPitrKind),
		quoteSQLStringLiteral(dataBranchPitrName("", "")+"%"),
	)
	if err := bh.Exec(sysCtx, sql); err != nil {
		return err
	}
	return cleanupDataBranchMoCatalogPitrIfUnused(ctx, bh)
}

func cleanupDataBranchMoCatalogPitrIfUnused(ctx context.Context, bh BackgroundExec) error {
	sysCtx := defines.AttachAccountId(ctx, sysAccountID)
	sql := fmt.Sprintf(
		"select pitr_id from mo_catalog.mo_pitr where pitr_name != %s limit 1",
		quoteSQLStringLiteral(SYSMOCATALOGPITR),
	)
	bh.ClearExecResultSet()
	if err := bh.Exec(sysCtx, sql); err != nil {
		return err
	}
	erArray, err := getResultSet(sysCtx, bh)
	if err != nil {
		return err
	}
	if execResultArrayHasData(erArray) {
		return nil
	}
	sql = fmt.Sprintf(
		"delete from mo_catalog.mo_pitr where pitr_name = %s and create_account = %d and kind = %s",
		quoteSQLStringLiteral(SYSMOCATALOGPITR),
		sysAccountID,
		quoteSQLStringLiteral(dataBranchPitrKind),
	)
	return bh.Exec(sysCtx, sql)
}

func cleanupDataBranchTablePitrIfUnused(
	ctx context.Context,
	bh BackgroundExec,
	accountID uint64,
	tableID uint64,
) error {
	hasRef, err := dataBranchTablesHaveActiveReference(ctx, bh, []uint64{tableID})
	if err != nil {
		return err
	}
	if hasRef {
		return nil
	}
	return deleteDataBranchInternalPitrRecord(
		ctx,
		bh,
		accountID,
		dataBranchPitrName("table", strconv.FormatUint(tableID, 10)),
	)
}

func cleanupAllDataBranchTablePitrsIfUnused(ctx context.Context, bh BackgroundExec) error {
	sysCtx := defines.AttachAccountId(ctx, sysAccountID)
	sql := fmt.Sprintf(
		"select create_account, obj_id from mo_catalog.mo_pitr where (kind = %s or pitr_name like %s) and level = %s and pitr_name like %s",
		quoteSQLStringLiteral(dataBranchPitrKind),
		quoteSQLStringLiteral(dataBranchPitrName("", "")+"%"),
		quoteSQLStringLiteral(tree.PITRLEVELTABLE.String()),
		quoteSQLStringLiteral(dataBranchPitrName("table", "")+"%"),
	)
	bh.ClearExecResultSet()
	if err := bh.Exec(sysCtx, sql); err != nil {
		return err
	}
	erArray, err := getResultSet(sysCtx, bh)
	if err != nil {
		return err
	}
	if !execResultArrayHasData(erArray) {
		return nil
	}

	type tablePitr struct {
		accountID uint64
		tableID   uint64
	}
	pitrs := make([]tablePitr, 0, erArray[0].GetRowCount())
	for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
		accountID, err := erArray[0].GetUint64(sysCtx, i, 0)
		if err != nil {
			return err
		}
		tableID, err := erArray[0].GetUint64(sysCtx, i, 1)
		if err != nil {
			return err
		}
		pitrs = append(pitrs, tablePitr{accountID: accountID, tableID: tableID})
	}

	for _, pitr := range pitrs {
		if err = cleanupDataBranchTablePitrIfUnused(ctx, bh, pitr.accountID, pitr.tableID); err != nil {
			return err
		}
	}
	return nil
}

func cleanupAllDataBranchDatabasePitrsIfUnused(ctx context.Context, bh BackgroundExec) error {
	sysCtx := defines.AttachAccountId(ctx, sysAccountID)
	sql := fmt.Sprintf(
		"select create_account, obj_id, database_name, create_time from mo_catalog.mo_pitr where (kind = %s or pitr_name like %s) and level = %s and pitr_name like %s",
		quoteSQLStringLiteral(dataBranchPitrKind),
		quoteSQLStringLiteral(dataBranchPitrName("", "")+"%"),
		quoteSQLStringLiteral(tree.PITRLEVELDATABASE.String()),
		quoteSQLStringLiteral(dataBranchPitrName("database", "")+"%"),
	)
	bh.ClearExecResultSet()
	if err := bh.Exec(sysCtx, sql); err != nil {
		return err
	}
	erArray, err := getResultSet(sysCtx, bh)
	if err != nil {
		return err
	}
	if !execResultArrayHasData(erArray) {
		return nil
	}

	type dbPitr struct {
		accountID  uint64
		databaseID uint64
		database   string
		createTime int64
	}
	pitrs := make([]dbPitr, 0, erArray[0].GetRowCount())
	for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
		accountID, err := erArray[0].GetUint64(sysCtx, i, 0)
		if err != nil {
			return err
		}
		databaseID, err := erArray[0].GetUint64(sysCtx, i, 1)
		if err != nil {
			return err
		}
		database, err := erArray[0].GetString(sysCtx, i, 2)
		if err != nil {
			return err
		}
		createTime, err := erArray[0].GetInt64(sysCtx, i, 3)
		if err != nil {
			return err
		}
		pitrs = append(pitrs, dbPitr{
			accountID:  accountID,
			databaseID: databaseID,
			database:   database,
			createTime: createTime,
		})
	}

	for _, pitr := range pitrs {
		tableIDs, err := dataBranchDatabaseTableIDsAt(ctx, bh, pitr.accountID, pitr.database, pitr.createTime)
		if err != nil {
			return err
		}
		if err = cleanupDataBranchDatabasePitrIfUnused(
			ctx, bh, pitr.accountID, pitr.databaseID, tableIDs,
		); err != nil {
			return err
		}
	}
	return nil
}

func cleanupDataBranchDatabasePitrIfUnused(
	ctx context.Context,
	bh BackgroundExec,
	accountID uint64,
	databaseID uint64,
	tableIDs []uint64,
) error {
	if len(tableIDs) > 0 {
		hasRef, err := dataBranchTablesHaveActiveReference(ctx, bh, tableIDs)
		if err != nil {
			return err
		}
		if hasRef {
			return nil
		}
	}
	return deleteDataBranchInternalPitrRecord(
		ctx,
		bh,
		accountID,
		dataBranchPitrName("database", strconv.FormatUint(databaseID, 10)),
	)
}

func dataBranchDatabaseTableIDsAt(
	ctx context.Context,
	bh BackgroundExec,
	accountID uint64,
	databaseName string,
	ts int64,
) ([]uint64, error) {
	sysCtx := defines.AttachAccountId(ctx, sysAccountID)
	sql := fmt.Sprintf(
		"select rel_id from mo_catalog.mo_tables {MO_TS = %d} where account_id = %d and reldatabase = %s",
		ts,
		accountID,
		quoteSQLStringLiteral(databaseName),
	)
	bh.ClearExecResultSet()
	if err := bh.Exec(sysCtx, sql); err != nil {
		return nil, err
	}
	erArray, err := getResultSet(sysCtx, bh)
	if err != nil {
		return nil, err
	}
	if !execResultArrayHasData(erArray) {
		return nil, nil
	}
	tableIDs := make([]uint64, 0, erArray[0].GetRowCount())
	for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
		tableID, err := erArray[0].GetUint64(sysCtx, i, 0)
		if err != nil {
			return nil, err
		}
		tableIDs = append(tableIDs, tableID)
	}
	return tableIDs, nil
}

func dataBranchTablesHaveActiveReference(
	ctx context.Context,
	bh BackgroundExec,
	tableIDs []uint64,
) (bool, error) {
	if len(tableIDs) == 0 {
		return false, nil
	}
	sysCtx := defines.AttachAccountId(ctx, sysAccountID)
	const batchSize = 512
	for start := 0; start < len(tableIDs); start += batchSize {
		end := start + batchSize
		if end > len(tableIDs) {
			end = len(tableIDs)
		}
		idList := dataBranchUintListSQL(tableIDs[start:end])
		sql := fmt.Sprintf(
			"select table_id from mo_catalog.mo_branch_metadata where table_deleted = false and (table_id in (%s) or p_table_id in (%s)) limit 1",
			idList,
			idList,
		)
		bh.ClearExecResultSet()
		if err := bh.Exec(sysCtx, sql); err != nil {
			return false, err
		}
		erArray, err := getResultSet(sysCtx, bh)
		if err != nil {
			return false, err
		}
		if execResultArrayHasData(erArray) {
			return true, nil
		}
	}
	return false, nil
}

func dataBranchUintListSQL(ids []uint64) string {
	var sqlBuilder strings.Builder
	for i, id := range ids {
		if i > 0 {
			sqlBuilder.WriteByte(',')
		}
		sqlBuilder.WriteString(strconv.FormatUint(id, 10))
	}
	return sqlBuilder.String()
}

func markBranchTablesDeleted(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	accId uint32,
	tableIDs []uint64,
) error {
	updateCtx := ctx
	if accId != sysAccountID {
		updateCtx = defines.AttachAccountId(updateCtx, sysAccountID)
	}

	const batchSize = 512
	for start := 0; start < len(tableIDs); start += batchSize {
		end := start + batchSize
		if end > len(tableIDs) {
			end = len(tableIDs)
		}

		var sqlBuilder strings.Builder
		sqlBuilder.Grow(128 + (end-start)*20)
		sqlBuilder.WriteString("update ")
		sqlBuilder.WriteString(catalog.MO_CATALOG)
		sqlBuilder.WriteByte('.')
		sqlBuilder.WriteString(catalog.MO_BRANCH_METADATA)
		sqlBuilder.WriteString(" set table_deleted = true where table_id in (")

		for i, id := range tableIDs[start:end] {
			if i > 0 {
				sqlBuilder.WriteByte(',')
			}
			sqlBuilder.WriteString(strconv.FormatUint(id, 10))
		}
		sqlBuilder.WriteString(")")

		updateRet, err := runSql(updateCtx, ses, bh, sqlBuilder.String(), nil, nil)
		if err != nil {
			return err
		}
		updateRet.Close()
	}

	return nil
}

func dataBranchDeleteTable(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.DataBranchDeleteTable,
) (err error) {
	var (
		bh       BackgroundExec
		deferred func(error) error
	)

	if bh, deferred, err = getBackExecutor(execCtx.reqCtx, ses); err != nil {
		return
	}

	defer func() {
		if deferred != nil {
			err = deferred(err)
		}
	}()

	var (
		dbName  = stmt.TableName.SchemaName
		tblName = stmt.TableName.ObjectName
		accId   uint32
		sqlRet  executor.Result
		tblID   uint64
		found   bool
	)

	if len(dbName) == 0 {
		dbName = tree.Identifier(ses.GetTxnCompileCtx().DefaultDatabase())
	}

	if accId, err = defines.GetAccountId(execCtx.reqCtx); err != nil {
		return
	}

	if sqlRet, err = runSql(
		execCtx.reqCtx, ses, bh, fmt.Sprintf(
			"select rel_id from %s.%s where account_id = %d and reldatabase = '%s' and relname = '%s'",
			catalog.MO_CATALOG, catalog.MO_TABLES, accId, dbName, tblName,
		), nil, nil,
	); err != nil {
		return
	}

	sqlRet.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows == 0 {
			return false
		}
		tblID = vector.GetFixedAtWithTypeCheck[uint64](cols[0], 0)
		found = true
		return false
	})
	sqlRet.Close()

	{
		var dropRet executor.Result
		defer func() {
			dropRet.Close()
		}()

		dropSQL := fmt.Sprintf("drop table if exists `%s`.`%s`", dbName, tblName)
		if dropRet, err = runSql(execCtx.reqCtx, ses, bh, dropSQL, nil, nil); err != nil {
			return
		}
	}

	if !found {
		return nil
	}

	if err = markBranchTablesDeleted(execCtx.reqCtx, ses, bh, accId, []uint64{tblID}); err != nil {
		return
	}

	if err = cleanupDataBranchTablePitrIfUnused(execCtx.reqCtx, bh, uint64(accId), tblID); err != nil {
		return
	}
	if err = cleanupAllDataBranchTablePitrsIfUnused(execCtx.reqCtx, bh); err != nil {
		return
	}
	if err = cleanupAllDataBranchDatabasePitrsIfUnused(execCtx.reqCtx, bh); err != nil {
		return
	}

	return nil
}

func dataBranchDeleteDatabase(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.DataBranchDeleteDatabase,
) (err error) {
	var (
		bh       BackgroundExec
		deferred func(error) error
	)

	if bh, deferred, err = getBackExecutor(execCtx.reqCtx, ses); err != nil {
		return
	}

	defer func() {
		if deferred != nil {
			err = deferred(err)
		}
	}()

	var (
		dbName   = stmt.DatabaseName
		accId    uint32
		sqlRet   executor.Result
		dbID     uint64
		dbFound  bool
		tableIDs []uint64
	)

	if accId, err = defines.GetAccountId(execCtx.reqCtx); err != nil {
		return
	}

	if sqlRet, err = runSql(
		execCtx.reqCtx, ses, bh, fmt.Sprintf(
			"select dat_id from %s.%s where account_id = %d and datname = '%s'",
			catalog.MO_CATALOG, catalog.MO_DATABASE, accId, dbName,
		), nil, nil,
	); err != nil {
		return
	}

	sqlRet.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows == 0 {
			return false
		}
		dbID = vector.GetFixedAtWithTypeCheck[uint64](cols[0], 0)
		dbFound = true
		return false
	})
	sqlRet.Close()

	if sqlRet, err = runSql(
		execCtx.reqCtx, ses, bh, fmt.Sprintf(
			"select rel_id from %s.%s where account_id = %d and reldatabase = '%s'",
			catalog.MO_CATALOG, catalog.MO_TABLES, accId, dbName,
		), nil, nil,
	); err != nil {
		return
	}

	sqlRet.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows == 0 {
			return true
		}
		tableIDs = append(tableIDs, executor.GetFixedRows[uint64](cols[0])...)
		return true
	})
	sqlRet.Close()

	{
		var dropRet executor.Result
		defer func() {
			dropRet.Close()
		}()

		dropSQL := fmt.Sprintf("drop database if exists `%s`", dbName)
		if dropRet, err = runSql(execCtx.reqCtx, ses, bh, dropSQL, nil, nil); err != nil {
			return
		}
	}

	if err = markBranchTablesDeleted(execCtx.reqCtx, ses, bh, accId, tableIDs); err != nil {
		return
	}

	for _, tableID := range tableIDs {
		if err = cleanupDataBranchTablePitrIfUnused(execCtx.reqCtx, bh, uint64(accId), tableID); err != nil {
			return
		}
	}
	if err = cleanupAllDataBranchTablePitrsIfUnused(execCtx.reqCtx, bh); err != nil {
		return
	}
	if dbFound {
		if err = cleanupDataBranchDatabasePitrIfUnused(execCtx.reqCtx, bh, uint64(accId), dbID, tableIDs); err != nil {
			return
		}
	}
	if err = cleanupAllDataBranchDatabasePitrsIfUnused(execCtx.reqCtx, bh); err != nil {
		return
	}

	return nil
}

func diffMergeAgency(
	ses *Session,
	execCtx *ExecCtx,
	stmt tree.Statement,
) (err error) {

	var (
		bh       BackgroundExec
		deferred func(error) error
	)

	if err = validate(execCtx.reqCtx, ses, stmt); err != nil {
		return err
	}

	// do not open another transaction,
	// if this already executed within a transaction.
	if bh, deferred, err = getBackExecutor(execCtx.reqCtx, ses); err != nil {
		return
	}

	defer func() {
		if deferred != nil {
			err = deferred(err)
		}
	}()

	var (
		ctx    context.Context
		cancel context.CancelFunc
	)

	ctx, cancel = context.WithCancel(execCtx.reqCtx)

	var (
		dagInfo   branchMetaInfo
		tblStuff  tableStuff
		copt      compositeOption
		ok        bool
		diffStmt  *tree.DataBranchDiff
		mergeStmt *tree.DataBranchMerge
		pickStmt  *tree.DataBranchPick
	)

	defer func() {
		cancel()
	}()

	if diffStmt, ok = stmt.(*tree.DataBranchDiff); !ok {
		if mergeStmt, ok = stmt.(*tree.DataBranchMerge); !ok {
			if pickStmt, ok = stmt.(*tree.DataBranchPick); !ok {
				return moerr.NewNotSupportedNoCtxf("data branch not supported: %v", stmt)
			}
		}
	}

	if diffStmt != nil {
		if diffStmt.OutputOpt != nil && len(diffStmt.OutputOpt.DirPath) != 0 {
			copt.conflictOpt = &tree.ConflictOpt{
				Opt: tree.CONFLICT_ACCEPT,
			}
			copt.outputSQL = true
			copt.expandUpdate = true
		}
		if tblStuff, err = getTableStuff(
			ctx, ses, bh, diffStmt.TargetTable, diffStmt.BaseTable,
		); err != nil {
			return
		}
		if err = validateProjectedColumns(diffStmt, tblStuff); err != nil {
			return
		}
	} else if mergeStmt != nil {
		copt.conflictOpt = mergeStmt.ConflictOpt
		copt.expandUpdate = true
		if tblStuff, err = getTableStuff(
			ctx, ses, bh, mergeStmt.SrcTable, mergeStmt.DstTable,
		); err != nil {
			return
		}
	} else {
		// Always use ACCEPT at hashDiff level for PICK so that conflicts on
		// non-picked keys do not abort the operation. The user's actual
		// conflict choice (FAIL/SKIP/ACCEPT) is enforced in the PICK pipeline
		// via synthetic base-delete conflict markers plus the consumer logic.
		copt.conflictOpt = &tree.ConflictOpt{Opt: tree.CONFLICT_ACCEPT}
		copt.expandUpdate = true
		copt.preservePickConflicts = true
		if tblStuff, err = getTableStuff(
			ctx, ses, bh, pickStmt.SrcTable, pickStmt.DstTable,
		); err != nil {
			return
		}
		if tblStuff.def.pkKind == fakeKind {
			err = moerr.NewNotSupportedNoCtxf(
				"DATA BRANCH PICK requires a table with a primary key; table %s has no primary key",
				pickStmt.SrcTable.ObjectName)
			return
		}
	}

	if dagInfo, err = decideLCABranchTSFromBranchDAG(
		ctx, ses, bh, tblStuff,
	); err != nil {
		return
	}
	var (
		done      bool
		wg        = new(sync.WaitGroup)
		outputErr atomic.Value
		retBatCh  = make(chan batchWithKind, 10)
		stopCh    = make(chan struct{})
		stopOnce  sync.Once
		emit      emitFunc
		stop      func()
		waited    bool
	)

	defer func() {
		if retBatCh != nil {
			close(retBatCh)
		}
		if !waited {
			wg.Wait()
		}
		if tblStuff.retPool != nil {
			tblStuff.retPool.freeAllRetBatches(ses.proc.Mp())
		}
	}()

	emit = newEmitter(ctx, stopCh, retBatCh)
	stop = func() {
		stopOnce.Do(func() {
			close(stopCh)
		})
	}

	if diffStmt != nil {
		if err = buildOutputSchema(ctx, ses, diffStmt, tblStuff); err != nil {
			return
		}

		if done, err = tryDiffAsCSV(ctx, ses, bh, diffStmt, tblStuff); err != nil {
			return
		}

		if done {
			return
		}
	}

	// Materialize PICK keys and build PK filter in one unified pass.
	// Both pickKeyHashmap (for producer-side precise batch filtering in
	// buildHashmapForTable) and pkFilter (for object/block ZoneMap pruning
	// inside CollectChanges) are produced here, BEFORE the goroutines are
	// launched.
	var pkFilter *engine.PKFilter
	var pickKeyHashmap databranchutils.BranchHashmap
	if pickStmt != nil && pickStmt.Keys != nil {
		var err2 error
		pickKeyHashmap, pkFilter, err2 = materializePickKeysAndFilter(ctx, ses, bh, pickStmt, tblStuff)
		if err2 != nil {
			err = err2
			return
		}
	}
	defer freePKFilter(pkFilter, ses.proc.Mp())
	defer func() {
		if pickKeyHashmap != nil {
			pickKeyHashmap.Close()
		}
	}()

	// Resolve BETWEEN SNAPSHOT timestamps for PICK.
	// These are passed to constructChangeHandle which intersects them with the
	// target collect range computed by decideCollectRange.
	var betweenFrom, betweenTo *types.TS
	if pickStmt != nil && pickStmt.BetweenFrom != "" && pickStmt.BetweenTo != "" {
		betweenFrom, betweenTo, err = resolveBetweenSnapshots(ses, pickStmt.BetweenFrom, pickStmt.BetweenTo)
		if err != nil {
			return
		}
	}

	wg.Add(2)

	go func() {
		defer wg.Done()
		if diffStmt != nil {
			// 1. all rows
			// 2. limit rows
			// 3. count
			// 4. as table
			// 5. as file

			if err2 := satisfyDiffOutputOpt(
				ctx, cancel, stop, ses, bh, diffStmt, dagInfo, tblStuff, retBatCh,
			); err2 != nil {
				outputErr.Store(err2)
			}
		} else if pickStmt != nil {
			if err2 := pickMergeDiffs(
				ctx, cancel, ses, bh, pickStmt, dagInfo, tblStuff, retBatCh,
			); err2 != nil {
				outputErr.Store(err2)
			}
		} else {
			if err2 := mergeDiffs(
				ctx, cancel, ses, bh, mergeStmt, dagInfo, tblStuff, retBatCh,
			); err2 != nil {
				outputErr.Store(err2)
			}
		}
	}()

	if err = diffOnBase(
		ctx, ses, bh, wg, dagInfo, tblStuff, copt, emit, pkFilter, pickKeyHashmap,
		betweenFrom, betweenTo,
	); err != nil {
		// If the consumer cancelled the context (e.g., PICK conflict FAIL),
		// wait for it to finish and prefer its real error over context.Canceled.
		if retBatCh != nil {
			close(retBatCh)
			retBatCh = nil
		}
		waited = true
		wg.Wait()
		if outputErr.Load() != nil {
			err = outputErr.Load().(error)
		}
		return
	}

	close(retBatCh)
	retBatCh = nil
	waited = true

	wg.Wait()

	if outputErr.Load() != nil {
		err = outputErr.Load().(error)
	}

	return err
}

func handleBranchDiff(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.DataBranchDiff,
) (err error) {
	return diffMergeAgency(ses, execCtx, stmt)
}

func handleBranchMerge(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.DataBranchMerge,
) (err error) {

	if stmt.ConflictOpt == nil {
		stmt.ConflictOpt = &tree.ConflictOpt{
			Opt: tree.CONFLICT_FAIL,
		}
	}

	return diffMergeAgency(ses, execCtx, stmt)
}

func validate(
	ctx context.Context,
	ses *Session,
	stmt tree.Statement,
) error {
	if stmt == nil {
		return nil
	}

	var (
		ok       bool
		diffStmt *tree.DataBranchDiff
	)

	if diffStmt, ok = stmt.(*tree.DataBranchDiff); !ok {
		return nil
	}

	if diffStmt.OutputOpt != nil && len(diffStmt.OutputOpt.DirPath) > 0 {
		if err := validateOutputDirPath(ctx, ses, diffStmt.OutputOpt.DirPath); err != nil {
			return err
		}
	}

	return nil
}

func getTableStuff(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	srcTable tree.TableName,
	dstTable tree.TableName,
) (tblStuff tableStuff, err error) {

	var (
		tarTblDef  *plan.TableDef
		baseTblDef *plan.TableDef
	)

	defer func() {
		if err == nil {
			tblStuff.worker, err = ants.NewPool(runtime.NumCPU())
		}
	}()

	if tblStuff.tarRel, tblStuff.baseRel, tblStuff.tarSnap, tblStuff.baseSnap, err = getRelations(
		ctx, ses, bh, srcTable, dstTable,
	); err != nil {
		return
	}

	tarTblDef = tblStuff.tarRel.GetTableDef(ctx)
	baseTblDef = tblStuff.baseRel.GetTableDef(ctx)

	if !isSchemaEquivalent(tarTblDef, baseTblDef) {
		err = moerr.NewInternalErrorNoCtx("the target table schema is not equivalent to the base table.")
		return
	}

	if baseTblDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
		tblStuff.def.pkKind = fakeKind
		for i, col := range baseTblDef.Cols {
			if col.Name != catalog.FakePrimaryKeyColName && col.Name != catalog.Row_ID {
				tblStuff.def.pkColIdxes = append(tblStuff.def.pkColIdxes, i)
			}
		}
	} else if baseTblDef.Pkey.CompPkeyCol != nil {
		// case 2: composite pk, combined all pks columns as the PK
		tblStuff.def.pkKind = compositeKind
		pkNames := baseTblDef.Pkey.Names
		for _, name := range pkNames {
			idx := int(baseTblDef.Name2ColIndex[name])
			tblStuff.def.pkColIdxes = append(tblStuff.def.pkColIdxes, idx)
		}
	} else {
		// normal pk
		tblStuff.def.pkKind = normalKind
		pkName := baseTblDef.Pkey.PkeyColName
		idx := int(baseTblDef.Name2ColIndex[pkName])
		tblStuff.def.pkColIdxes = append(tblStuff.def.pkColIdxes, idx)
	}

	tblStuff.def.pkColIdx = int(baseTblDef.Name2ColIndex[baseTblDef.Pkey.PkeyColName])
	tblStuff.def.pkSeqnum = int(baseTblDef.Cols[tblStuff.def.pkColIdx].Seqnum)

	for i, col := range tarTblDef.Cols {
		if col.Name == catalog.Row_ID {
			continue
		}

		t := types.New(types.T(col.Typ.Id), col.Typ.Width, col.Typ.Scale)

		tblStuff.def.colNames = append(tblStuff.def.colNames, col.Name)
		tblStuff.def.colTypes = append(tblStuff.def.colTypes, t)

		if col.Name == catalog.FakePrimaryKeyColName ||
			col.Name == catalog.CPrimaryKeyColName {
			continue
		}

		tblStuff.def.visibleIdxes = append(tblStuff.def.visibleIdxes, i)
	}

	tblStuff.retPool = &retBatchList{}
	tblStuff.bufPool = &sync.Pool{
		New: func() any {
			return &bytes.Buffer{}
		},
	}
	tblStuff.hashmapAllocator = newBranchHashmapAllocator(dataBranchHashmapLimitRate)
	tblStuff.lcaReaderProbeMode = &atomic.Bool{}

	return

}

func diffOnBase(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	wg *sync.WaitGroup,
	dagInfo branchMetaInfo,
	tblStuff tableStuff,
	copt compositeOption,
	emit emitFunc,
	pkFilter *engine.PKFilter,
	pickKeyHashmap databranchutils.BranchHashmap,
	betweenFrom, betweenTo *types.TS,
) (err error) {

	defer func() {
		wg.Done()
	}()

	var (
		tarHandle  []engine.ChangesHandle
		baseHandle []engine.ChangesHandle
	)

	closeHandle := func() {
		for _, h := range tarHandle {
			if h != nil {
				_ = h.Close()
			}
		}
		for _, h := range baseHandle {
			if h != nil {
				_ = h.Close()
			}
		}
		tarHandle = nil
		baseHandle = nil
	}

	defer func() {
		closeHandle()
	}()

	if dagInfo.lcaType != lcaEmpty {
		if dagInfo.lcaTableId == tblStuff.tarRel.GetTableID(ctx) {
			tblStuff.lcaRel = tblStuff.tarRel
		} else if dagInfo.lcaTableId == tblStuff.baseRel.GetTableID(ctx) {
			tblStuff.lcaRel = tblStuff.baseRel
		} else {
			lcaSnapshot := &plan2.Snapshot{
				Tenant: &plan.SnapshotTenant{
					TenantID: ses.GetAccountId(),
				},
			}
			lcaSnapshot.TS = &timestamp.Timestamp{PhysicalTime: dagInfo.tarBranchTS.Physical()}
			if dagInfo.baseBranchTS.LT(&dagInfo.tarBranchTS) {
				lcaSnapshot.TS.PhysicalTime = dagInfo.baseBranchTS.Physical()
			}

			if tblStuff.lcaRel, err = getRelationById(
				ctx, ses, bh, dagInfo.lcaTableId, lcaSnapshot); err != nil {
				return
			}
		}
	}
	// has no lca
	if tarHandle, baseHandle, err = constructChangeHandle(
		ctx, ses, bh, tblStuff, &dagInfo, pkFilter, betweenFrom, betweenTo,
	); err != nil {
		return
	}

	if err = hashDiff(
		ctx, ses, bh, tblStuff, dagInfo,
		copt, emit, tarHandle, baseHandle, pickKeyHashmap,
	); err != nil {
		return
	}

	closeHandle()
	return
}

func isSchemaEquivalent(leftDef, rightDef *plan.TableDef) bool {
	if len(leftDef.Cols) != len(rightDef.Cols) {
		return false
	}

	for i := range leftDef.Cols {
		if leftDef.Cols[i].ColId != rightDef.Cols[i].ColId {
			return false
		}

		if leftDef.Cols[i].Typ.Id != rightDef.Cols[i].Typ.Id {
			return false
		}

		if leftDef.Cols[i].ClusterBy != rightDef.Cols[i].ClusterBy {
			return false
		}

		if leftDef.Cols[i].Primary != rightDef.Cols[i].Primary {
			return false
		}

		if leftDef.Cols[i].Seqnum != rightDef.Cols[i].Seqnum {
			return false
		}

		if leftDef.Cols[i].NotNull != rightDef.Cols[i].NotNull {
			return false
		}
	}

	return true
}

func getRelationById(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tableId uint64,
	snapshot *plan.Snapshot,
) (rel engine.Relation, err error) {

	txnOp := bh.(*backExec).backSes.GetTxnHandler().txnOp
	snapshotStr := "current"

	if snapshot != nil && snapshot.TS != nil {
		txnOp = txnOp.CloneSnapshotOp(*snapshot.TS)
		snapshotStr = types.TimestampToTS(*snapshot.TS).ToString()
	}

	logutil.Info(
		"DataBranch-GetRelationByID-Start",
		zap.Uint64("table-id", tableId),
		zap.String("snapshot-ts", snapshotStr),
	)
	_, _, rel, err = ses.GetTxnHandler().GetStorage().GetRelationById(ctx, txnOp, tableId)
	if err != nil {
		logutil.Error(
			"DataBranch-GetRelationByID-Error",
			zap.Uint64("table-id", tableId),
			zap.String("snapshot-ts", snapshotStr),
			zap.String("txn", txnOp.Txn().DebugString()),
			zap.Error(err),
		)
		return nil, err
	}
	if rel != nil {
		logutil.Info(
			"DataBranch-GetRelationByID-Done",
			zap.Uint64("table-id", tableId),
			zap.String("table-name", rel.GetTableName()),
			zap.String("db-name", rel.GetTableDef(ctx).DbName),
			zap.String("snapshot-ts", snapshotStr),
		)
	}
	return rel, err
}

func getRelations(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tarName tree.TableName,
	baseName tree.TableName,
) (
	tarRel engine.Relation,
	baseRel engine.Relation,
	tarSnap *plan.Snapshot,
	baseSnap *plan.Snapshot,
	err error,
) {

	var (
		tarDB  engine.Database
		baseDB engine.Database

		tarDBName   string
		baseDBName  string
		tarTblName  string
		baseTblName string
	)

	if tarSnap, err = resolveSnapshot(ses, tarName.AtTsExpr); err != nil {
		return
	}

	if baseSnap, err = resolveSnapshot(ses, baseName.AtTsExpr); err != nil {
		return
	}

	txnOpA := bh.(*backExec).backSes.GetTxnHandler().txnOp
	txnOpB := bh.(*backExec).backSes.GetTxnHandler().txnOp

	if tarSnap != nil && tarSnap.TS != nil {
		txnOpA = txnOpA.CloneSnapshotOp(*tarSnap.TS)
	}

	if baseSnap != nil && baseSnap.TS != nil {
		txnOpB = txnOpB.CloneSnapshotOp(*baseSnap.TS)
	}

	tarDBName = tarName.SchemaName.String()
	tarTblName = tarName.ObjectName.String()
	if len(tarDBName) == 0 {
		tarDBName = ses.GetTxnCompileCtx().DefaultDatabase()
	}

	baseDBName = baseName.SchemaName.String()
	baseTblName = baseName.ObjectName.String()
	if len(baseDBName) == 0 {
		baseDBName = ses.GetTxnCompileCtx().DefaultDatabase()
	}

	if len(tarDBName) == 0 || len(baseDBName) == 0 {
		err = moerr.NewInternalErrorNoCtxf("the base or target database cannot be empty.")
		return
	}

	tarSnapStr := "current"
	if tarSnap != nil && tarSnap.TS != nil {
		tarSnapStr = types.TimestampToTS(*tarSnap.TS).ToString()
	}
	baseSnapStr := "current"
	if baseSnap != nil && baseSnap.TS != nil {
		baseSnapStr = types.TimestampToTS(*baseSnap.TS).ToString()
	}
	logutil.Info(
		"DataBranch-GetRelations-Start",
		zap.String("target-db", tarDBName),
		zap.String("target-table", tarTblName),
		zap.String("target-snapshot-ts", tarSnapStr),
		zap.String("base-db", baseDBName),
		zap.String("base-table", baseTblName),
		zap.String("base-snapshot-ts", baseSnapStr),
	)

	eng := ses.proc.GetSessionInfo().StorageEngine
	if tarDB, err = eng.Database(ctx, tarDBName, txnOpA); err != nil {
		logutil.Error(
			"DataBranch-GetRelations-TargetDB-Error",
			zap.String("target-db", tarDBName),
			zap.String("target-table", tarTblName),
			zap.String("target-snapshot-ts", tarSnapStr),
			zap.String("target-txn", txnOpA.Txn().DebugString()),
			zap.Error(err),
		)
		return
	}

	if tarRel, err = tarDB.Relation(ctx, tarTblName, nil); err != nil {
		logutil.Error(
			"DataBranch-GetRelations-TargetTable-Error",
			zap.String("target-db", tarDBName),
			zap.String("target-table", tarTblName),
			zap.String("target-snapshot-ts", tarSnapStr),
			zap.Error(err),
		)
		return
	}

	if baseDB, err = eng.Database(ctx, baseDBName, txnOpB); err != nil {
		logutil.Error(
			"DataBranch-GetRelations-BaseDB-Error",
			zap.String("base-db", baseDBName),
			zap.String("base-table", baseTblName),
			zap.String("base-snapshot-ts", baseSnapStr),
			zap.String("base-txn", txnOpB.Txn().DebugString()),
			zap.Error(err),
		)
		return
	}

	if baseRel, err = baseDB.Relation(ctx, baseTblName, nil); err != nil {
		logutil.Error(
			"DataBranch-GetRelations-BaseTable-Error",
			zap.String("base-db", baseDBName),
			zap.String("base-table", baseTblName),
			zap.String("base-snapshot-ts", baseSnapStr),
			zap.Error(err),
		)
		return
	}

	logutil.Info(
		"DataBranch-GetRelations-Done",
		zap.Uint64("target-table-id", tarRel.GetTableID(ctx)),
		zap.Uint64("base-table-id", baseRel.GetTableID(ctx)),
		zap.String("target-db", tarRel.GetTableDef(ctx).DbName),
		zap.String("base-db", baseRel.GetTableDef(ctx).DbName),
	)

	return
}

func constructChangeHandle(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tables tableStuff,
	branchInfo *branchMetaInfo,
	pkFilter *engine.PKFilter,
	betweenFrom, betweenTo *types.TS,
) (
	tarHandle []engine.ChangesHandle,
	baseHandle []engine.ChangesHandle,
	err error,
) {
	var (
		handle    engine.ChangesHandle
		tarRange  collectRange
		baseRange collectRange
		start     = time.Now()
	)

	defer func() {
		fields := []zap.Field{
			zap.Int("target-handle-cnt", len(tarHandle)),
			zap.Int("base-handle-cnt", len(baseHandle)),
			zap.Duration("duration", time.Since(start)),
		}
		if err != nil {
			fields = append(fields, zap.Error(err))
			logutil.Warn("DataBranch-ConstructChangeHandle-Error", fields...)
			return
		}
		logutil.Info("DataBranch-ConstructChangeHandle-Done", fields...)
	}()

	if tarRange, baseRange, err = decideCollectRange(
		ctx, ses, bh, tables, branchInfo,
	); err != nil {
		return
	}

	// BETWEEN SNAPSHOT: intersect the target collect range with [betweenFrom.Next(), betweenTo].
	// This narrows the time window on the source side so only changes within the
	// snapshot range are collected.  The base (destination) side is not narrowed.
	if betweenFrom != nil && betweenTo != nil {
		bFrom := betweenFrom.Next()
		bTo := *betweenTo
		j := 0
		for i := range tarRange.rel {
			// Intersect: from = max(original, bFrom), end = min(original, bTo)
			f := tarRange.from[i]
			e := tarRange.end[i]
			if bFrom.GT(&f) {
				f = bFrom
			}
			if bTo.LT(&e) {
				e = bTo
			}
			if f.LE(&e) {
				tarRange.from[j] = f
				tarRange.end[j] = e
				tarRange.rel[j] = tarRange.rel[i]
				j++
			}
		}
		tarRange.from = tarRange.from[:j]
		tarRange.end = tarRange.end[:j]
		tarRange.rel = tarRange.rel[:j]
	}

	// collectFn dispatches to the PK-filtered or plain CollectChanges variant.
	collectFn := func(
		ctx context.Context,
		rel engine.Relation,
		from, end types.TS,
		mp *mpool.MPool,
	) (engine.ChangesHandle, error) {
		if pkFilter != nil && len(pkFilter.Segments) > 0 {
			return databranchutils.CollectChangesWithPKFilter(
				ctx, rel, from, end, mp, pkFilter,
			)
		}
		return databranchutils.CollectChanges(ctx, rel, from, end, mp)
	}

	for i := range tarRange.rel {
		collectStart := time.Now()
		if handle, err = collectFn(
			ctx,
			tarRange.rel[i],
			tarRange.from[i],
			tarRange.end[i],
			ses.proc.Mp(),
		); err != nil {
			return
		}
		logutil.Info("DataBranch-CollectChanges-Open",
			zap.String("side", "target"),
			zap.Uint64("table-id", tarRange.rel[i].GetTableID(ctx)),
			zap.String("from", tarRange.from[i].ToString()),
			zap.String("to", tarRange.end[i].ToString()),
			zap.Bool("has-handle", handle != nil),
			zap.Duration("duration", time.Since(collectStart)),
		)

		if handle != nil {
			tarHandle = append(tarHandle, handle)
		}
	}

	for i := range baseRange.rel {
		collectStart := time.Now()
		if handle, err = collectFn(
			ctx,
			baseRange.rel[i],
			baseRange.from[i],
			baseRange.end[i],
			ses.proc.Mp(),
		); err != nil {
			return
		}
		logutil.Info("DataBranch-CollectChanges-Open",
			zap.String("side", "base"),
			zap.Uint64("table-id", baseRange.rel[i].GetTableID(ctx)),
			zap.String("from", baseRange.from[i].ToString()),
			zap.String("to", baseRange.end[i].ToString()),
			zap.Bool("has-handle", handle != nil),
			zap.Duration("duration", time.Since(collectStart)),
		)

		if handle != nil {
			baseHandle = append(baseHandle, handle)
		}
	}

	return
}

func decideCollectRange(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tables tableStuff,
	dagInfo *branchMetaInfo,
) (
	tarCollectRange collectRange,
	baseCollectRange collectRange,
	err error,
) {

	var (
		lcaRel engine.Relation

		tarSp  types.TS
		baseSp types.TS

		tarCTS  types.TS
		baseCTS types.TS

		tblCommitTS []types.TS

		tarTableID  = tables.tarRel.GetTableID(ctx)
		baseTableID = tables.baseRel.GetTableID(ctx)

		txnSnapshot = types.TimestampToTS(ses.GetTxnHandler().GetTxn().SnapshotTS())
	)

	tarSp = txnSnapshot
	if tables.tarSnap != nil && tables.tarSnap.TS != nil {
		tarSp = types.TimestampToTS(*tables.tarSnap.TS)
	}

	baseSp = txnSnapshot
	if tables.baseSnap != nil && tables.baseSnap.TS != nil {
		baseSp = types.TimestampToTS(*tables.baseSnap.TS)
	}

	if tblCommitTS, err = getTablesCreationCommitTS(
		ctx, ses, tables.tarRel, tables.baseRel,
		[]types.TS{tarSp, baseSp},
	); err != nil {
		return
	}

	tarCTS = tblCommitTS[0]
	baseCTS = tblCommitTS[1]

	// Boundary semantics:
	// 1. childCreateCommitTS marks when the child table itself becomes visible.
	//    The cloned rows are materialized by that DDL, so child-owned change collection
	//    must start from childCreateCommitTS.Next() to avoid re-reading inherited rows.
	// 2. branchTS marks the source snapshot on the parent/LCA side used by the clone.
	//    Parent-side incremental comparison must start from branchTS.Next() because the
	//    snapshot at branchTS is already included in the cloned contents.

	// now we got the t1.snapshot, t1.branchTS, t2.snapshot, t2.branchTS and txnSnapshot,
	// and then we need to decide the range that t1 and t2 should collect.
	//
	// case 0: special cases:
	//	i. tar = base
	//   -|------------|-------------|----
	//   cts          sp1           sp2
	//	diff t(sp1) against t(sp2)
	// 		diff empty against (sp1, sp2]
	//	diff t(sp2) against t(sp2)
	//		diff (sp1, sp2] against empty
	if tarTableID == baseTableID {
		if tarSp.LE(&baseSp) {
			// tar collect nothing
			baseCollectRange = collectRange{
				from: []types.TS{tarSp.Next()},
				end:  []types.TS{baseSp},
				rel:  []engine.Relation{tables.baseRel},
			}
			dagInfo.tarBranchTS = tarSp
			dagInfo.baseBranchTS = tarSp
		} else {
			tarCollectRange = collectRange{
				from: []types.TS{baseSp.Next()},
				end:  []types.TS{tarSp},
				rel:  []engine.Relation{tables.tarRel},
			}

			dagInfo.tarBranchTS = baseSp
			dagInfo.baseBranchTS = baseSp
			// base collect nothing
		}

		dagInfo.lcaTableId = baseTableID

		return
	}

	//
	// case 1: t1 and t2 have no LCA
	//	==> t1 collect [0, sp], t2 collect [0, sp]
	if dagInfo.lcaTableId == 0 {
		tarCollectRange = collectRange{
			from: []types.TS{types.MinTs()},
			end:  []types.TS{tarSp},
			rel:  []engine.Relation{tables.tarRel},
		}
		baseCollectRange = collectRange{
			from: []types.TS{types.MinTs()},
			end:  []types.TS{baseSp},
			rel:  []engine.Relation{tables.baseRel},
		}
		return
	}

	// case 2: t1 and t2 have the LCA t0 (not t1 nor t2)
	// 	i. t1 and t2 branched from to at the same ts
	//		==> t1 collect [branchTS+1, sp], t2 collect [branchTS+1, sp]
	//	ii. t1 and t2 have different branchTS
	//                             seg2  sp2
	//		  common    seg1  t2 --------|--->
	//   t0 |-------|---------|-------------->
	//             t1 ----------------|----->
	//					seg3		 sp1
	// the diff between	(t0.seg1 ∩ t2.seg2)	 and t1.seg3
	if dagInfo.lcaTableId != tarTableID && dagInfo.lcaTableId != baseTableID {
		tarCollectRange = collectRange{
			from: []types.TS{tarCTS.Next()},
			end:  []types.TS{tarSp},
			rel:  []engine.Relation{tables.tarRel},
		}
		baseCollectRange = collectRange{
			from: []types.TS{baseCTS.Next()},
			end:  []types.TS{baseSp},
			rel:  []engine.Relation{tables.baseRel},
		}
		if dagInfo.tarBranchTS.EQ(&dagInfo.baseBranchTS) {
			// do nothing
		} else {
			if lcaRel, err = getRelationById(
				ctx, ses, bh, dagInfo.lcaTableId, &plan2.Snapshot{
					Tenant: &plan.SnapshotTenant{TenantID: ses.GetAccountId()},
					TS:     &timestamp.Timestamp{PhysicalTime: tarSp.Physical()},
				}); err != nil {
				return
			}

			if dagInfo.tarBranchTS.GT(&dagInfo.baseBranchTS) {
				tarCollectRange.rel = append(tarCollectRange.rel, lcaRel)
				tarCollectRange.from = append(tarCollectRange.from, dagInfo.baseBranchTS.Next())
				tarCollectRange.end = append(tarCollectRange.end, dagInfo.tarBranchTS)
				dagInfo.tarBranchTS = dagInfo.baseBranchTS
			} else {
				baseCollectRange.rel = append(baseCollectRange.rel, lcaRel)
				baseCollectRange.from = append(baseCollectRange.from, dagInfo.tarBranchTS.Next())
				baseCollectRange.end = append(baseCollectRange.end, dagInfo.baseBranchTS)
				dagInfo.baseBranchTS = dagInfo.tarBranchTS
			}
		}
		return
	}

	// case 3: t1 is the LCA of t1 and t2
	//	i. t1.sp < t2.branchTS
	//		t1 -----sp1--------|-------------->
	//				    seg1  t2-------sp2---->
	//							  seg2
	//		==> the diff between null and (t1.seg1 ∩ t2.seg2)
	//  ii. t1.sp == t2.branchTS
	//		==> t1 collect nothing, t2 collect [branchTS+1, sp]
	// iii. t1.sp > t2.branchTS
	//		==> t1 collect [branchTS+1, sp], t2 collect [branchTS+1, sp]
	if dagInfo.lcaTableId == baseTableID {
		// base is the lca
		if baseSp.LT(&dagInfo.tarBranchTS) {
			tarCollectRange = collectRange{
				from: []types.TS{tarCTS.Next(), baseSp.Next()},
				end:  []types.TS{tarSp, dagInfo.tarBranchTS},
				rel:  []engine.Relation{tables.tarRel, tables.baseRel},
			}
			// base collect nothing
		} else if baseSp.EQ(&dagInfo.tarBranchTS) {
			tarCollectRange = collectRange{
				from: []types.TS{tarCTS.Next()},
				end:  []types.TS{tarSp},
				rel:  []engine.Relation{tables.tarRel},
			}
			// base collect nothing
		} else {
			tarCollectRange = collectRange{
				from: []types.TS{tarCTS.Next()},
				end:  []types.TS{tarSp},
				rel:  []engine.Relation{tables.tarRel},
			}
			baseCollectRange = collectRange{
				from: []types.TS{dagInfo.tarBranchTS.Next()},
				end:  []types.TS{baseSp},
				rel:  []engine.Relation{tables.baseRel},
			}
		}
		return
	}

	// case 4: t2 is the LCA of t1 and t2
	// tar is the lca
	if tarSp.LT(&dagInfo.baseBranchTS) {
		baseCollectRange = collectRange{
			from: []types.TS{baseCTS.Next(), tarSp.Next()},
			end:  []types.TS{baseSp, dagInfo.baseBranchTS},
			rel:  []engine.Relation{tables.baseRel, tables.tarRel},
		}
		// tar collect nothing
	} else if tarSp.EQ(&dagInfo.baseBranchTS) {
		baseCollectRange = collectRange{
			from: []types.TS{baseCTS.Next()},
			end:  []types.TS{baseSp},
			rel:  []engine.Relation{tables.baseRel},
		}
		// tar collect nothing
	} else {
		baseCollectRange = collectRange{
			from: []types.TS{baseCTS.Next()},
			end:  []types.TS{baseSp},
			rel:  []engine.Relation{tables.baseRel},
		}
		tarCollectRange = collectRange{
			from: []types.TS{dagInfo.baseBranchTS.Next()},
			end:  []types.TS{tarSp},
			rel:  []engine.Relation{tables.tarRel},
		}
	}

	return
}

// getTablesCreationCommitTS resolves the creation commit timestamp for
// both tar and base tables.  It tries CollectChanges first (fast,
// preserves real commit_ts from partition state), falling back to the
// Reader-based snapshot scan when partition state is unavailable.
func getTablesCreationCommitTS(
	ctx context.Context,
	ses *Session,
	tar engine.Relation,
	base engine.Relation,
	snapshot []types.TS,
) ([]types.TS, error) {
	txnSnap := types.TimestampToTS(ses.GetTxnHandler().GetTxn().SnapshotTS())

	snapFor := func(idx int) types.TS {
		if idx < len(snapshot) && !snapshot[idx].IsEmpty() {
			return snapshot[idx]
		}
		return txnSnap
	}

	resolve := func(tableID uint64, snap types.TS) (types.TS, error) {
		totalStart := time.Now()
		collectStart := time.Now()
		ts, err := getTableCreationCommitTSByCollectChanges(ctx, ses, tableID, snap)
		if err == nil {
			logutil.Info("DataBranch-TableCTS-Resolve-Done",
				zap.Uint64("table-id", tableID),
				zap.String("snapshot", snap.ToString()),
				zap.String("path", "CollectChanges"),
				zap.String("commit-ts", ts.ToString()),
				zap.Duration("collectchanges-cost", time.Since(collectStart)),
				zap.Duration("total-cost", time.Since(totalStart)),
			)
			return ts, nil
		}
		collectCost := time.Since(collectStart)
		logutil.Warn("getTablesCreationCommitTS: CollectChanges failed, trying Reader fallback",
			zap.Uint64("table-id", tableID),
			zap.String("snapshot", snap.ToString()),
			zap.Duration("collectchanges-cost", collectCost),
			zap.Error(err),
		)
		readerStart := time.Now()
		ts, readerErr := getTableCreationCommitTSByID(ctx, ses, tableID, snap)
		if readerErr != nil {
			logutil.Warn("DataBranch-TableCTS-Resolve-Error",
				zap.Uint64("table-id", tableID),
				zap.String("snapshot", snap.ToString()),
				zap.String("path", "ReaderFallback"),
				zap.Duration("collectchanges-cost", collectCost),
				zap.Duration("reader-cost", time.Since(readerStart)),
				zap.Duration("total-cost", time.Since(totalStart)),
				zap.Error(readerErr),
			)
			return types.TS{}, readerErr
		}
		logutil.Info("DataBranch-TableCTS-Resolve-Done",
			zap.Uint64("table-id", tableID),
			zap.String("snapshot", snap.ToString()),
			zap.String("path", "ReaderFallback"),
			zap.String("commit-ts", ts.ToString()),
			zap.Duration("collectchanges-cost", collectCost),
			zap.Duration("reader-cost", time.Since(readerStart)),
			zap.Duration("total-cost", time.Since(totalStart)),
		)
		return ts, nil
	}

	tarCTS, err := resolve(tar.GetTableID(ctx), snapFor(0))
	if err != nil {
		return nil, err
	}
	baseCTS, err := resolve(base.GetTableID(ctx), snapFor(1))
	if err != nil {
		return nil, err
	}
	return []types.TS{tarCTS, baseCTS}, nil
}

// getTableCreationCommitTSByID scans the mo_tables snapshot at the given
// timestamp and returns the earliest commit_ts for the specified table ID.
func getTableCreationCommitTSByID(
	ctx context.Context,
	ses *Session,
	tableID uint64,
	snapshotTS types.TS,
) (types.TS, error) {
	mp := ses.proc.Mp()

	filterVec := vector.NewVec(types.T_uint64.ToType())
	defer filterVec.Free(mp)
	if err := vector.AppendFixed(filterVec, tableID, false, mp); err != nil {
		return types.TS{}, err
	}

	attrs := []string{catalog.SystemRelAttr_ID, objectio.DefaultCommitTS_Attr}
	colTypes := []types.Type{types.T_uint64.ToType(), types.T_TS.ToType()}
	filterExpr := readutil.ConstructInExpr(ctx, catalog.SystemRelAttr_ID, filterVec)

	found := false
	result := types.TS{}

	err := scanSnapshotRelationByID(
		ctx, "table-creation-commit-ts", ses,
		catalog.MO_TABLES_ID, snapshotTS,
		attrs, colTypes, filterExpr, 1,
		func(bat *batch.Batch) error {
			ids := vector.MustFixedColWithTypeCheck[uint64](bat.Vecs[0])
			cts := vector.MustFixedColWithTypeCheck[types.TS](bat.Vecs[1])
			for i, id := range ids {
				if id != tableID || bat.Vecs[1].IsNull(uint64(i)) {
					continue
				}
				if !found || cts[i].LT(&result) {
					result = cts[i]
				}
				found = true
			}
			return nil
		},
	)
	if err != nil {
		return types.TS{}, err
	}
	if found {
		return result, nil
	}
	return types.TS{}, moerr.NewInternalErrorNoCtxf(
		"cannot find table %d commit ts at snapshot %s",
		tableID, snapshotTS.ToString(),
	)
}

// getTableCreationCommitTSByCollectChanges uses CollectChanges on mo_tables
// to find the creation commit_ts.  BuildTS(0,1) forces the partition-state
// path which preserves real per-row commit timestamps.
func getTableCreationCommitTSByCollectChanges(
	ctx context.Context,
	ses *Session,
	tableID uint64,
	snapshotTS types.TS,
) (types.TS, error) {
	storage := ses.GetTxnHandler().GetStorage()
	txnOp := ses.GetTxnHandler().GetTxn()
	mp := ses.proc.Mp()
	start := time.Now()
	var (
		found             bool
		result            types.TS
		dataBatchCnt      int
		dataRowCnt        int
		tombstoneBatchCnt int
		tombstoneRowCnt   int
	)

	defer func() {
		fields := []zap.Field{
			zap.Uint64("table-id", tableID),
			zap.String("snapshot", snapshotTS.ToString()),
			zap.Bool("found", found),
			zap.Int("data-batch-cnt", dataBatchCnt),
			zap.Int("data-row-cnt", dataRowCnt),
			zap.Int("tombstone-batch-cnt", tombstoneBatchCnt),
			zap.Int("tombstone-row-cnt", tombstoneRowCnt),
			zap.Duration("duration", time.Since(start)),
		}
		if found {
			fields = append(fields, zap.String("commit-ts", result.ToString()))
		}
		logutil.Info("DataBranch-TableCTS-CollectChanges-Done", fields...)
	}()

	_, _, rel, err := storage.GetRelationById(ctx, txnOp, catalog.MO_TABLES_ID)
	if err != nil {
		return types.TS{}, err
	}

	handle, err := rel.CollectChanges(ctx, types.BuildTS(0, 1), snapshotTS, true, mp)
	if err != nil {
		return types.TS{}, err
	}
	defer handle.Close()

	for {
		data, tombstone, _, err := handle.Next(ctx, mp)
		if err != nil {
			return types.TS{}, err
		}
		if data == nil && tombstone == nil {
			break
		}
		if tombstone != nil {
			tombstoneBatchCnt++
			tombstoneRowCnt += tombstone.RowCount()
			tombstone.Clean(mp)
		}
		if data == nil {
			continue
		}
		dataBatchCnt++
		dataRowCnt += data.RowCount()

		relIDIdx, commitTSIdx := locateColumnsInChangeBatch(data)
		if relIDIdx >= len(data.Vecs) || commitTSIdx >= len(data.Vecs) {
			data.Clean(mp)
			continue
		}

		ids := vector.MustFixedColWithTypeCheck[uint64](data.Vecs[relIDIdx])
		cts := vector.MustFixedColWithTypeCheck[types.TS](data.Vecs[commitTSIdx])
		for i, id := range ids {
			if id != tableID || data.Vecs[commitTSIdx].IsNull(uint64(i)) {
				continue
			}
			if !found || cts[i].LT(&result) {
				result = cts[i]
			}
			found = true
		}
		data.Clean(mp)
	}

	if found {
		return result, nil
	}
	return types.TS{}, moerr.NewInternalErrorNoCtxf(
		"cannot find table %d commit ts at snapshot %s (via CollectChanges)",
		tableID, snapshotTS.ToString(),
	)
}

// locateColumnsInChangeBatch finds rel_id and commit_ts column indices
// in a CollectChanges data batch.  Named attrs are tried first; when
// absent (aobj path), positional access is used.
func locateColumnsInChangeBatch(data *batch.Batch) (relIDIdx, commitTSIdx int) {
	relIDIdx, commitTSIdx = -1, -1
	for i, attr := range data.Attrs {
		switch attr {
		case catalog.SystemRelAttr_ID:
			relIDIdx = i
		case objectio.DefaultCommitTS_Attr:
			commitTSIdx = i
		}
	}
	if relIDIdx < 0 || commitTSIdx < 0 {
		// aobj batches lack Attrs; use positional access:
		// rel_id at MO_TABLES_REL_ID_IDX, commit_ts at last vector.
		relIDIdx = catalog.MO_TABLES_REL_ID_IDX
		commitTSIdx = len(data.Vecs) - 1
	}
	return
}

func decideLCABranchTSFromBranchDAG(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tblStuff tableStuff,
) (
	branchInfo branchMetaInfo,
	err error,
) {

	var (
		dag *databranchutils.DataBranchDAG

		tarBranchTableID  uint64
		baseBranchTableID uint64
		hasLca            bool
		lcaType           int

		lcaTableID   uint64
		tarBranchTS  types.TS
		baseBranchTS types.TS
	)

	defer func() {
		branchInfo = branchMetaInfo{
			lcaType:      lcaType,
			lcaTableId:   lcaTableID,
			tarBranchTS:  tarBranchTS,
			baseBranchTS: baseBranchTS,
		}
	}()

	if dag, err = constructBranchDAG(ctx, ses, bh); err != nil {
		return
	}

	// 1. has no lca
	//		[0, now] join [0, now]
	// 2. t1 and t2 has lca
	//		1. t0 is the lca
	//			t1's [branch_t1_ts + 1, now] join t2's [branch_t2_ts + 1, now]
	// 		2. t1 is the lca
	//			t1's [branch_t2_ts + 1, now] join t2's [branch_t2_ts + 1, now]
	//      3. t2 is the lca
	//			t1's [branch_t1_ts + 1, now] join t2's [branch_t1_ts + 1, now]
	//
	// The DAG only answers lineage questions. The branch timestamp attached to an
	// outgoing edge is the source snapshot used by the clone operation, not the
	// creation commit timestamp of the child table.
	if lcaTableID, tarBranchTableID, baseBranchTableID, hasLca = dag.FindLCA(
		tblStuff.tarRel.GetTableID(ctx), tblStuff.baseRel.GetTableID(ctx),
	); hasLca {
		if lcaTableID == tblStuff.baseRel.GetTableID(ctx) {
			lcaType = lcaRight
			var ts int64
			if ts, hasLca = dag.GetCloneTS(tarBranchTableID); !hasLca {
				err = moerr.NewInternalErrorNoCtxf("cannot find clone ts for table %d", tarBranchTableID)
				return
			}
			tarBranchTS = types.BuildTS(ts, 0)
			baseBranchTS = tarBranchTS
		} else if lcaTableID == tblStuff.tarRel.GetTableID(ctx) {
			lcaType = lcaLeft
			var ts int64
			if ts, hasLca = dag.GetCloneTS(baseBranchTableID); !hasLca {
				err = moerr.NewInternalErrorNoCtxf("cannot find clone ts for table %d", baseBranchTableID)
				return
			}
			baseBranchTS = types.BuildTS(ts, 0)
			tarBranchTS = baseBranchTS
		} else {
			lcaType = lcaOther
			var ts int64
			if ts, hasLca = dag.GetCloneTS(tarBranchTableID); !hasLca {
				err = moerr.NewInternalErrorNoCtxf("cannot find clone ts for table %d", tarBranchTableID)
				return
			}
			tarBranchTS = types.BuildTS(ts, 0)
			if ts, hasLca = dag.GetCloneTS(baseBranchTableID); !hasLca {
				err = moerr.NewInternalErrorNoCtxf("cannot find clone ts for table %d", baseBranchTableID)
				return
			}
			baseBranchTS = types.BuildTS(ts, 0)
		}
	} else if tblStuff.tarRel.GetTableID(ctx) == tblStuff.baseRel.GetTableID(ctx) {
		lcaTableID = tblStuff.tarRel.GetTableID(ctx)
		if tblStuff.tarSnap == nil && tblStuff.baseSnap == nil {
			lcaType = lcaRight
		} else if tblStuff.tarSnap == nil {
			// diff tar{now} against base{sp}
			lcaType = lcaRight
		} else if tblStuff.baseSnap == nil {
			// diff tar{sp} against base{now}
			lcaType = lcaLeft
		} else if tblStuff.tarSnap.TS.LessEq(*tblStuff.baseSnap.TS) {
			lcaType = lcaLeft
		} else {
			lcaType = lcaRight
		}
	}
	return
}

func constructBranchDAG(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
) (dag *databranchutils.DataBranchDAG, err error) {

	var (
		rowData []databranchutils.DataBranchMetadata

		sqlRet executor.Result
		oldCtx context.Context
		sysCtx context.Context
	)

	oldCtx = ses.proc.Ctx
	sysCtx = defines.AttachAccountId(ctx, catalog.System_Account)
	ses.proc.Ctx = sysCtx
	defer func() {
		ses.proc.Ctx = oldCtx
		sqlRet.Close()
	}()

	if sqlRet, err = runSql(
		sysCtx, ses, bh,
		fmt.Sprintf(
			"select table_id, clone_ts, p_table_id, table_deleted from %s.%s",
			catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA,
		),
		nil, nil,
	); err != nil {
		return
	}

	rowData = make([]databranchutils.DataBranchMetadata, 0, sqlRet.AffectedRows)
	sqlRet.ReadRows(func(rows int, cols []*vector.Vector) bool {
		tblIds := vector.MustFixedColNoTypeCheck[uint64](cols[0])
		cloneTS := vector.MustFixedColNoTypeCheck[int64](cols[1])
		pTblIds := vector.MustFixedColNoTypeCheck[uint64](cols[2])
		tableDeleted := vector.MustFixedColNoTypeCheck[bool](cols[3])
		for i := range tblIds {
			rowData = append(rowData, databranchutils.DataBranchMetadata{
				TableID:      tblIds[i],
				CloneTS:      cloneTS[i],
				PTableID:     pTblIds[i],
				TableDeleted: tableDeleted[i],
			})
		}
		return true
	})

	return databranchutils.NewDAG(rowData), nil
}
