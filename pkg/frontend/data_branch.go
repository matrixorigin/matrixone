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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

const dataBranchMetadataIDBatchSize = 512

func isDataBranchUserVisibleColumn(col *plan.ColDef) bool {
	if col == nil {
		return false
	}
	if col.Hidden {
		return false
	}
	switch col.Name {
	case catalog.Row_ID, catalog.FakePrimaryKeyColName, catalog.CPrimaryKeyColName:
		return false
	default:
		return true
	}
}

func dataBranchFakePKColIdxes(tblDef *plan.TableDef) []int {
	idxes := make([]int, 0, len(tblDef.Cols))
	for i, col := range tblDef.Cols {
		if isDataBranchUserVisibleColumn(col) {
			idxes = append(idxes, i)
		}
	}
	return idxes
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
) (statistic.StatsArray, error) {
	var stats statistic.StatsArray
	stats.Reset()

	switch st := stmt.(type) {
	case *tree.DataBranchCreateTable:
		return stats, dataBranchCreateTable(execCtx, ses, st)
	case *tree.DataBranchCreateDatabase:
		return dataBranchCreateDatabase(execCtx, ses, st)
	case *tree.DataBranchDeleteTable:
		return stats, dataBranchDeleteTable(execCtx, ses, st)
	case *tree.DataBranchDeleteDatabase:
		return stats, dataBranchDeleteDatabase(execCtx, ses, st)
	case *tree.DataBranchDiff:
		return stats, handleBranchDiff(execCtx, ses, st)
	case *tree.DataBranchMerge:
		return stats, handleBranchMerge(execCtx, ses, st)
	case *tree.DataBranchPick:
		return stats, handleBranchPick(execCtx, ses, st)
	default:
		return stats, moerr.NewNotSupportedNoCtxf("data branch not supported: %v", st)
	}
}

func dataBranchCreateTable(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.DataBranchCreateTable,
) (err error) {
	var (
		bh        BackgroundExec
		deferred  func(error) error
		receipt   cloneReceipt
		cloneStmt *tree.CloneTable
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

	execCtx.reqCtx = context.WithValue(execCtx.reqCtx, tree.CloneLevelCtxKey{}, tree.NormalCloneLevelTable)

	if receipt, err = handleCloneTable(execCtx, ses, cloneStmt, bh); err != nil {
		return
	}

	if err = updateBranchMetaTable(execCtx.reqCtx, ses, bh, &receipt); err != nil {
		return
	}

	if err = createBranchProtectSnapshot(execCtx.reqCtx, ses, bh, &receipt); err != nil {
		return
	}

	return nil
}

func dataBranchCreateDatabase(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.DataBranchCreateDatabase,
) (stats statistic.StatsArray, err error) {
	var (
		bh        BackgroundExec
		deferred  func(error) error
		receipts  []cloneReceipt
		source    cloneDatabaseSource
		authStats statistic.StatsArray
	)
	stats.Reset()

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

	if !skipDataBranchPrivilegeCheck(ses) {
		if authStats, err = authenticateDataBranchCreateDatabase(execCtx.reqCtx, ses, stmt); err != nil {
			stats.Add(&authStats)
			return
		}
		stats.Add(&authStats)
	}

	if source, err = collectCloneDatabaseSource(execCtx.reqCtx, ses, bh, &stmt.CloneDatabase); err != nil {
		return
	}

	if authStats, err = authenticateDataBranchCreateDatabaseSourceTables(execCtx.reqCtx, ses, stmt, source); err != nil {
		stats.Add(&authStats)
		return
	}
	stats.Add(&authStats)

	if receipts, err = handleCloneDatabaseWithSource(execCtx, ses, bh, &stmt.CloneDatabase, &source); err != nil {
		return
	}

	if err = checkBranchQuota(execCtx.reqCtx, ses, bh, int64(len(receipts))); err != nil {
		return
	}

	for i := range receipts {
		if err = updateBranchMetaTable(execCtx.reqCtx, ses, bh, &receipts[i]); err != nil {
			return
		}
		if err = createBranchProtectSnapshot(execCtx.reqCtx, ses, bh, &receipts[i]); err != nil {
			return
		}
	}

	return
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

	for start := 0; start < len(tableIDs); start += dataBranchMetadataIDBatchSize {
		end := start + dataBranchMetadataIDBatchSize
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
		dbName  string
		tblName string
		accId   uint32
		tblID   uint64
	)

	if dbName, tblName, err = branchTableName(execCtx.reqCtx, ses, stmt.TableName); err != nil {
		return
	}

	if accId, err = defines.GetAccountId(execCtx.reqCtx); err != nil {
		return
	}

	if tblID, err = validateDataBranchDeleteTableTarget(execCtx.reqCtx, ses, bh, dbName, tblName); err != nil {
		return
	}

	{
		var dropRet executor.Result
		defer func() {
			dropRet.Close()
		}()

		dropSQL := fmt.Sprintf(
			"drop table %s.%s",
			quoteIdentifierForSQL(dbName),
			quoteIdentifierForSQL(tblName),
		)
		if dropRet, err = runSql(execCtx.reqCtx, ses, bh, dropSQL, nil, nil); err != nil {
			return
		}
	}

	if err = markBranchTablesDeleted(execCtx.reqCtx, ses, bh, accId, []uint64{tblID}); err != nil {
		return
	}

	if err = reclaimBranchSnapshotsWithBH(execCtx.reqCtx, ses, bh, []uint64{tblID}); err != nil {
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
		tableIDs []uint64
	)

	if accId, err = defines.GetAccountId(execCtx.reqCtx); err != nil {
		return
	}

	if tableIDs, err = validateDataBranchDeleteDatabaseTarget(execCtx.reqCtx, ses, bh, dbName.String()); err != nil {
		return
	}

	{
		var dropRet executor.Result
		defer func() {
			dropRet.Close()
		}()

		dropSQL := fmt.Sprintf("drop database %s", quoteIdentifierForSQL(dbName.String()))
		if dropRet, err = runSql(execCtx.reqCtx, ses, bh, dropSQL, nil, nil); err != nil {
			return
		}
	}

	if err = markBranchTablesDeleted(execCtx.reqCtx, ses, bh, accId, tableIDs); err != nil {
		return
	}

	if err = reclaimBranchSnapshotsWithBH(execCtx.reqCtx, ses, bh, tableIDs); err != nil {
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
	outputCh := retBatCh

	go func() {
		defer wg.Done()
		if diffStmt != nil {
			// 1. all rows
			// 2. limit rows
			// 3. count
			// 4. as table
			// 5. as file

			if err2 := satisfyDiffOutputOpt(
				ctx, cancel, stop, ses, bh, diffStmt, dagInfo, tblStuff, outputCh,
			); err2 != nil {
				outputErr.Store(err2)
			}
		} else if pickStmt != nil {
			if err2 := pickMergeDiffs(
				ctx, cancel, ses, bh, pickStmt, dagInfo, tblStuff, outputCh,
			); err2 != nil {
				outputErr.Store(err2)
			}
		} else {
			if err2 := mergeDiffs(
				ctx, cancel, ses, bh, mergeStmt, dagInfo, tblStuff, outputCh,
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

	if diffStmt.OutputOpt != nil && diffStmt.OutputOpt.As.ObjectName != "" {
		return moerr.NewNotSupportedNoCtx("DATA BRANCH DIFF OUTPUT AS")
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

		if isDataBranchUserVisibleColumn(col) {
			tblStuff.def.visibleIdxes = append(tblStuff.def.visibleIdxes, i)
		}
	}
	if tblStuff.def.pkKind == fakeKind {
		tblStuff.def.pkColIdxes = dataBranchFakePKColIdxes(baseTblDef)
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

	if dagInfo.hasLCA() {
		switch dagInfo.lcaTableId {
		case tblStuff.tarRel.GetTableID(ctx):
			tblStuff.lcaRel = tblStuff.tarRel
		case tblStuff.baseRel.GetTableID(ctx):
			tblStuff.lcaRel = tblStuff.baseRel
		default:
			tarSp, baseSp := tblStuff.resolvedSnapshots(ses)
			probe := dagInfo.lcaProbeSnapshot(tarSp, baseSp)
			lcaSnapshot := &plan2.Snapshot{
				Tenant: &plan.SnapshotTenant{TenantID: ses.GetAccountId()},
				TS:     &timestamp.Timestamp{PhysicalTime: probe.Physical()},
			}
			if tblStuff.lcaRel, err = getRelationById(
				ctx, ses, bh, dagInfo.lcaTableId, lcaSnapshot); err != nil {
				return
			}
		}
	}

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
		if pkFilter != nil && pkFilter.Valid() {
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
		tarCTS  types.TS
		baseCTS types.TS

		tblCommitTS []types.TS

		tarTableID  = tables.tarRel.GetTableID(ctx)
		baseTableID = tables.baseRel.GetTableID(ctx)
	)

	tarSp, baseSp := tables.resolvedSnapshots(ses)

	if tblCommitTS, err = getTablesCreationCommitTS(
		ctx, ses, tables.tarRel, tables.baseRel,
		[]types.TS{tarSp, baseSp},
	); err != nil {
		return
	}

	tarCTS = tblCommitTS[0]
	baseCTS = tblCommitTS[1]

	// ============================================================
	// Running example used throughout the comments below.
	//
	// A multi-fork branch tree (depth 4, 19 nodes, 10 leaves):
	//
	//                          t0  (root)
	//                       /   |   \
	//                     t1   t2   t3
	//                    /  \   |   /  \
	//                  t4   t5 t6  t7   t8
	//                 / \   /\ /\  /\   /\
	//                t9 t10 ... t15 t16 ... t18
	//
	// Every non-root node was cloned from its parent at some
	// CloneTS, then performed its own insert/update/delete writes.
	// The root t0 also performed extra writes after the whole tree
	// was built. The `data branch diff` query we need to answer:
	//
	//     data branch diff t9 against t0
	//
	// `t9 against t0` exercises a 3-edge skip: the LCA of (t9, t0)
	// is t0 itself; the path from LCA down to the target endpoint
	// is path[LCA → tar] = [t0, t1, t4, t9]; the path on the base
	// side is path[LCA → base] = [t0]. With this geometry, the
	// invariants below let us derive the correct collect ranges
	// for any pair of nodes in any tree shape.
	//
	// Boundary semantics:
	// 1. childCreateCommitTS (child.CTS) marks when the child table
	//    itself becomes visible. The cloned rows are materialized
	//    by that DDL, so child-owned change collection must start
	//    from child.CTS.Next() to avoid re-reading inherited rows.
	// 2. CloneTS marks the source snapshot on the parent side used
	//    by the clone. Parent-side incremental comparison ending at
	//    the next child's CloneTS captures every parent write that
	//    is still observable on this side (later parent writes are
	//    invisible — the fork already happened).
	// ============================================================

	// case 0: tar and base resolve to the same table id.
	// Diff degenerates into a snapshot-vs-snapshot comparison on
	// that single relation; the DAG model does not apply.
	//   -|-----------|------------|----
	//   cts          sp1          sp2
	//   diff t(sp1) against t(sp2) → diff empty against (sp1, sp2]
	//   diff t(sp2) against t(sp1) → diff (sp1, sp2] against empty
	if tarTableID == baseTableID {
		if tarSp.LE(&baseSp) {
			// tar collect nothing
			baseCollectRange = collectRange{
				from: []types.TS{tarSp.Next()},
				end:  []types.TS{baseSp},
				rel:  []engine.Relation{tables.baseRel},
			}
		} else {
			tarCollectRange = collectRange{
				from: []types.TS{baseSp.Next()},
				end:  []types.TS{tarSp},
				rel:  []engine.Relation{tables.tarRel},
			}
			// base collect nothing
		}
		return
	}

	// case 1: tar and base have no LCA (two unrelated tables).
	// Whole-history diff: each side reads everything it has from
	// the start of time up to its snapshot. There is no shared
	// ancestor to anchor the comparison, so the DAG model does
	// not apply either.
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

	// ============================================================
	// case 2: tar and base share an LCA in the DAG. This is the
	// general path that must work uniformly for any tree shape and
	// any depth. We unify what used to be three separate sub-cases
	// (LCA == base, LCA == tar, LCA == third node) into one model
	// driven by the LCA→endpoint paths captured in dagInfo.
	//
	// Logical state of an endpoint X at some snapshot tX:
	//
	//     state(X, tX) = ⋃ over each ancestor A on path[root, X] of
	//                       writes_on(A) within window_for(A, X, tX)
	//
	// Two sides of the diff therefore differ only in writes that
	// happen on path[LCA, endpoint] of each side — every ancestor
	// strictly above LCA is shared and contributes the same rows
	// (folded into both sides' clone-materialization), so it
	// cancels out and never needs to be read.
	//
	// For each ancestor A on path[LCA, endpoint] this side's own
	// "mutation window" is:
	//
	//     windowFrom_A = A.CTS + 1                        (A's own
	//                                                      writes,
	//                                                      excluding
	//                                                      clone)
	//     windowEnd_A  = nextChildOnPath.CloneTS          (A is on
	//                                                      the path
	//                                                      but is not
	//                                                      the
	//                                                      endpoint)
	//                  | endpointSP                       (A is the
	//                                                      endpoint)
	//
	// Walked through on the running example for `diff t9 against t0`:
	//
	//   path[LCA → tar]  = [t0, t1, t4, t9]
	//   path[LCA → base] = [t0]
	//
	//   For tar (t9):
	//     A = t0 (LCA): own writes that *t9* sees =
	//         (t0.CTS, t1.CloneTS]  ← t0 writes BEFORE t1 was
	//                                  forked are the ones t9
	//                                  inherits via the t0→t1→t4→t9
	//                                  clone chain. After t1 was
	//                                  forked, t0 mutations are
	//                                  invisible to t9.
	//     A = t1 (intermediate): (t1.CTS, t4.CloneTS]
	//     A = t4 (intermediate): (t4.CTS, t9.CloneTS]
	//     A = t9 (endpoint):     (t9.CTS, tarSP]
	//
	//   For base (t0): A = t0 (endpoint = LCA): (t0.CTS, baseSP]
	//
	// Symmetric prefix on the LCA. Both endpoints inherit the LCA's
	// state cloned at *their* fork moment. The prefix
	// (LCA.CTS, min(thisFirstChildCloneTS, otherFirstChildCloneTS)]
	// is therefore identical on both sides and would simply cancel
	// inside the hash-diff. We clamp this side's LCA window lower
	// bound up to otherFork.Next() so the IO is skipped entirely.
	//
	// Concretely for `diff t9 against t0`: tar's LCA window is
	// (t0.CTS, t1.CloneTS]. Base IS the LCA, so otherFork = baseSP
	// (= "now"); since baseSP > t1.CloneTS, the clamp does not move
	// windowFrom and the full window is collected. Conversely for
	// base (which is the LCA), tar's first child is t1, so
	// otherFork = t1.CloneTS and base collects only
	// (t1.CloneTS, baseSP] — t0's pre-fork writes that t9 already
	// inherits are not collected on either side.
	//
	// The two sides' segment lists feed the hash-diff algorithm,
	// which still reconciles whatever remaining symmetric writes
	// reach both sides (e.g. the LCA's own post-fork writes that
	// some descendants share). The unified model thus generalizes
	// the historical case-2/3/4 logic into a single walk regardless
	// of depth or which side hosts the LCA.
	// ============================================================
	tarPath := dagInfo.pathFromLCAToTar
	tarPathTS := dagInfo.pathFromLCAToTarTS
	basePath := dagInfo.pathFromLCAToBase
	basePathTS := dagInfo.pathFromLCAToBaseTS
	if len(tarPath) == 0 || len(basePath) == 0 {
		err = moerr.NewInternalErrorNoCtxf(
			"data branch diff: missing DAG path (tar=%d base=%d lca=%d)",
			tarTableID, baseTableID, dagInfo.lcaTableId,
		)
		return
	}

	if tarCollectRange, err = buildSideCollectRange(
		ctx, ses, bh, tables, tarSp, tarCTS,
		tarPath, tarPathTS, basePathTS, baseSp,
	); err != nil {
		return
	}
	if baseCollectRange, err = buildSideCollectRange(
		ctx, ses, bh, tables, baseSp, baseCTS,
		basePath, basePathTS, tarPathTS, tarSp,
	); err != nil {
		return
	}
	return
}

// buildSideCollectRange materializes one side of `data branch diff`
// using the unified ancestor-walk described above. It walks selfPath
// (LCA → endpoint, top-down) and emits one collect segment per
// ancestor.
//
// Per-ancestor window (A is the i-th node on selfPath):
//
//	windowFrom_A = A.CTS.Next()
//	windowEnd_A  = selfPath[i+1].CloneTS  if A is intermediate
//	             | endpointSP             if A is endpoint
//
// LCA prune (i == 0):
//
//	windowFrom_A = max(windowFrom_A, otherFork.Next())
//	where otherFork = otherPath[1].CloneTS if otherEndpoint != LCA
//	                | otherSP              if otherEndpoint == LCA
//
// Walked through on the running example `diff t9 against t0` for the
// tar side (selfPath = [t0, t1, t4, t9], otherPath = [t0]):
//
//	i=0  A = t0 (LCA)
//	     windowFrom = t0.CTS + 1
//	     windowEnd  = t1.CloneTS                       (selfPath[1])
//	     LCA prune:  otherEndpoint == LCA, so otherFork = otherSP
//	                 (= baseSP). baseSP > t1.CloneTS so no clamp.
//	     emit (t0_rel, t0.CTS+1, t1.CloneTS]
//
//	i=1  A = t1 (intermediate)
//	     windowFrom = t1.CTS + 1
//	     windowEnd  = t4.CloneTS                       (selfPath[2])
//	     emit (t1_rel, t1.CTS+1, t4.CloneTS]
//
//	i=2  A = t4 (intermediate)
//	     windowFrom = t4.CTS + 1
//	     windowEnd  = t9.CloneTS                       (selfPath[3])
//	     emit (t4_rel, t4.CTS+1, t9.CloneTS]
//
//	i=3  A = t9 (endpoint)
//	     windowFrom = t9.CTS + 1
//	     windowEnd  = tarSP
//	     emit (t9_rel, t9.CTS+1, tarSP]
//
// Empty windows (windowFrom > windowEnd) are dropped silently.
//
// Relations are looked up once per node (cache-hit on tar/base/lca,
// one fresh fetch per intermediate). CTSes are resolved lazily and
// reuse the already-known endpointCTS for the endpoint.
func buildSideCollectRange(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tables tableStuff,
	endpointSP types.TS,
	endpointCTS types.TS,
	selfPath []uint64,
	selfPathTS []types.TS,
	otherPathTS []types.TS,
	otherSP types.TS,
) (cr collectRange, err error) {

	endpointID := selfPath[len(selfPath)-1]
	for i, nodeID := range selfPath {
		var (
			rel        engine.Relation
			windowFrom types.TS
			windowEnd  types.TS
			nodeCTS    types.TS
		)

		// Resolve the relation handle for this node.
		switch {
		case nodeID == tables.tarRel.GetTableID(ctx):
			rel = tables.tarRel
		case nodeID == tables.baseRel.GetTableID(ctx):
			rel = tables.baseRel
		case tables.lcaRel != nil && nodeID == tables.lcaRel.GetTableID(ctx):
			rel = tables.lcaRel
		default:
			if rel, err = getRelationById(
				ctx, ses, bh, nodeID, &plan2.Snapshot{
					Tenant: &plan.SnapshotTenant{TenantID: ses.GetAccountId()},
					TS:     &timestamp.Timestamp{PhysicalTime: endpointSP.Physical()},
				},
			); err != nil {
				return
			}
		}

		// Resolve creation commit TS so the window starts after the
		// clone-materialization commit. Reuse endpointCTS for the
		// endpoint to avoid a redundant lookup.
		if nodeID == endpointID {
			nodeCTS = endpointCTS
		} else {
			ctsList, err2 := getTablesCreationCommitTS(
				ctx, ses, rel, rel,
				[]types.TS{endpointSP, endpointSP},
			)
			if err2 != nil {
				err = err2
				return
			}
			if len(ctsList) == 0 {
				err = moerr.NewInternalErrorNoCtxf(
					"data branch: failed to resolve creation TS for table %d", nodeID)
				return
			}
			nodeCTS = ctsList[0]
		}

		// Window upper bound.
		if i == len(selfPath)-1 {
			windowEnd = endpointSP
		} else {
			windowEnd = selfPathTS[i+1]
		}

		// Window lower bound.
		windowFrom = nodeCTS.Next()

		// LCA prune (i == 0): clamp to skip the prefix that the
		// other side inherits identically via clone.
		if i == 0 {
			var otherFork types.TS
			if len(otherPathTS) > 1 {
				otherFork = otherPathTS[1]
			} else {
				// Other endpoint IS the LCA; observation goes up to
				// otherSP.
				otherFork = otherSP
			}
			otherForkNext := otherFork.Next()
			if otherForkNext.GT(&windowFrom) {
				windowFrom = otherForkNext
			}
		}

		if windowFrom.GT(&windowEnd) {
			continue
		}
		cr.rel = append(cr.rel, rel)
		cr.from = append(cr.from, windowFrom)
		cr.end = append(cr.end, windowEnd)
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
	dbLowerBounds := make(map[dbCreatedTimeLowerBoundKey]types.TS)
	tableCTS := make(map[tableCreationCommitTSKey]types.TS)

	snapFor := func(idx int) types.TS {
		if idx < len(snapshot) && !snapshot[idx].IsEmpty() {
			return snapshot[idx]
		}
		return txnSnap
	}

	resolve := func(targetRel engine.Relation, snap types.TS) (types.TS, error) {
		totalStart := time.Now()
		tableID := targetRel.GetTableID(ctx)
		lowerBound, err := getDatabaseCreatedTimeLowerBound(
			ctx, ses, targetRel, snap, dbLowerBounds,
		)
		if err != nil {
			return types.TS{}, err
		}

		key := tableCreationCommitTSKey{
			tableID:    tableID,
			snapshot:   snap,
			lowerBound: lowerBound,
		}
		if ts, ok := tableCTS[key]; ok {
			return ts, nil
		}

		collectStart := time.Now()
		ts, err := getTableCreationCommitTSByCollectChanges(
			ctx, ses, targetRel, tableID, snap, lowerBound,
		)
		if err == nil {
			tableCTS[key] = ts
			logutil.Info("DataBranch-TableCTS-Resolve-Done",
				zap.Uint64("table-id", tableID),
				zap.String("snapshot", snap.ToString()),
				zap.String("lower-bound", lowerBound.ToString()),
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
			zap.String("lower-bound", lowerBound.ToString()),
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
				zap.String("lower-bound", lowerBound.ToString()),
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
			zap.String("lower-bound", lowerBound.ToString()),
			zap.Duration("collectchanges-cost", collectCost),
			zap.Duration("reader-cost", time.Since(readerStart)),
			zap.Duration("total-cost", time.Since(totalStart)),
		)
		tableCTS[key] = ts
		return ts, nil
	}

	tarCTS, err := resolve(tar, snapFor(0))
	if err != nil {
		return nil, err
	}
	baseCTS, err := resolve(base, snapFor(1))
	if err != nil {
		return nil, err
	}
	return []types.TS{tarCTS, baseCTS}, nil
}

type dbCreatedTimeLowerBoundKey struct {
	accountID  uint32
	databaseID uint64
	database   string
	snapshot   types.TS
}

type tableCreationCommitTSKey struct {
	tableID    uint64
	snapshot   types.TS
	lowerBound types.TS
}

func getDatabaseCreatedTimeLowerBound(
	ctx context.Context,
	ses *Session,
	targetRel engine.Relation,
	snapshotTS types.TS,
	cache map[dbCreatedTimeLowerBoundKey]types.TS,
) (types.TS, error) {
	if targetRel == nil {
		return types.TS{}, moerr.NewInternalErrorNoCtx(
			"getDatabaseCreatedTimeLowerBound: missing target relation",
		)
	}
	targetDef := targetRel.GetTableDef(ctx)
	if targetDef == nil || targetDef.DbName == "" {
		return types.TS{}, moerr.NewInternalErrorNoCtx(
			"getDatabaseCreatedTimeLowerBound: missing target database name",
		)
	}
	accountID := ses.GetAccountId()
	databaseID := targetRel.GetDBID(ctx)
	key := dbCreatedTimeLowerBoundKey{
		accountID:  accountID,
		databaseID: databaseID,
		database:   targetDef.DbName,
		snapshot:   snapshotTS,
	}
	if cache != nil {
		if lowerBound, ok := cache[key]; ok {
			return lowerBound, nil
		}
	}

	lowerBound, err := getDatabaseCreatedTimeLowerBoundByPK(
		ctx, ses, accountID, databaseID, targetDef.DbName, snapshotTS,
	)
	if err != nil {
		return types.TS{}, err
	}
	if cache != nil {
		cache[key] = lowerBound
	}
	return lowerBound, nil
}

func getDatabaseCreatedTimeLowerBoundByPK(
	ctx context.Context,
	ses *Session,
	accountID uint32,
	databaseID uint64,
	databaseName string,
	snapshotTS types.TS,
) (types.TS, error) {
	mp := ses.proc.Mp()

	cpKey := packMoDatabaseCPKey(accountID, databaseName)
	filterVec := vector.NewVec(types.T_varchar.ToType())
	defer filterVec.Free(mp)
	if err := vector.AppendBytes(filterVec, cpKey, false, mp); err != nil {
		return types.TS{}, err
	}

	attrs := []string{catalog.SystemDBAttr_ID, catalog.SystemDBAttr_CreateAt}
	colTypes := []types.Type{types.T_uint64.ToType(), types.T_timestamp.ToType()}
	filterExpr := readutil.ConstructInExpr(ctx, catalog.SystemDBAttr_CPKey, filterVec)

	found := false
	result := types.TS{}

	err := scanSnapshotRelationByID(
		ctx, "database-created-time-lower-bound", ses,
		catalog.MO_DATABASE_ID, snapshotTS,
		attrs, colTypes, filterExpr, 1,
		func(bat *batch.Batch) error {
			dbIDs := vector.MustFixedColWithTypeCheck[uint64](bat.Vecs[0])
			createdTimes := vector.MustFixedColWithTypeCheck[types.Timestamp](bat.Vecs[1])
			for i, dbID := range dbIDs {
				if dbID != databaseID || bat.Vecs[1].IsNull(uint64(i)) {
					continue
				}
				lowerBound, err := databaseCreatedTimeToCollectLowerBound(createdTimes[i])
				if err != nil {
					return err
				}
				if !found || lowerBound.LT(&result) {
					result = lowerBound
				}
				found = true
			}
			return nil
		},
	)
	if err != nil {
		return types.TS{}, err
	}
	if !found {
		return types.TS{}, moerr.NewInternalErrorNoCtxf(
			"cannot find database %d (%s) created_time at snapshot %s",
			databaseID, databaseName, snapshotTS.ToString(),
		)
	}
	logutil.Info("DataBranch-DBCreatedTime-LowerBound-Done",
		zap.Uint32("account-id", accountID),
		zap.Uint64("database-id", databaseID),
		zap.String("database-name", databaseName),
		zap.String("snapshot", snapshotTS.ToString()),
		zap.String("lower-bound", result.ToString()),
	)
	return result, nil
}

func databaseCreatedTimeToCollectLowerBound(createdTime types.Timestamp) (types.TS, error) {
	unixMicros := int64(createdTime) - types.GetUnixEpochSecs()
	if unixMicros <= 0 {
		return types.TS{}, moerr.NewInternalErrorNoCtxf(
			"invalid database created_time %s", createdTime.String(),
		)
	}
	return types.BuildTS(unixMicros*int64(time.Microsecond), 0).Prev(), nil
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
// to find the creation commit_ts.  The caller supplies a lower bound derived
// from the owning database's created_time to avoid scanning the whole catalog
// history from BuildTS(0,1).
func getTableCreationCommitTSByCollectChanges(
	ctx context.Context,
	ses *Session,
	targetRel engine.Relation,
	tableID uint64,
	snapshotTS types.TS,
	lowerBound types.TS,
) (types.TS, error) {
	storage := ses.GetTxnHandler().GetStorage()
	txnOp := ses.GetTxnHandler().GetTxn()
	mp := ses.proc.Mp()
	start := time.Now()
	var (
		found             bool
		result            types.TS
		pkFilter          *engine.PKFilter
		pkFilterFallback  string
		dataBatchCnt      int
		dataRowCnt        int
		tombstoneBatchCnt int
		tombstoneRowCnt   int
	)

	defer func() {
		fields := []zap.Field{
			zap.Uint64("table-id", tableID),
			zap.String("snapshot", snapshotTS.ToString()),
			zap.String("lower-bound", lowerBound.ToString()),
			zap.Bool("found", found),
			zap.Bool("has-pk-filter", pkFilter != nil),
			zap.Int("data-batch-cnt", dataBatchCnt),
			zap.Int("data-row-cnt", dataRowCnt),
			zap.Int("tombstone-batch-cnt", tombstoneBatchCnt),
			zap.Int("tombstone-row-cnt", tombstoneRowCnt),
			zap.Duration("duration", time.Since(start)),
		}
		if pkFilterFallback != "" {
			fields = append(fields, zap.String("pk-filter-fallback", pkFilterFallback))
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

	pkFilter, pkFilterFallback, err = tryBuildCurrentMoTablesCPKeyFilter(
		ctx, ses.GetAccountId(), targetRel, rel, mp,
	)
	if err != nil {
		return types.TS{}, err
	}
	if pkFilter != nil {
		ctx = engine.WithPKFilter(ctx, pkFilter)
	}
	ctx = engine.WithCollectChangesDebugLabel(ctx, "data-branch-table-cts")

	handle, err := rel.CollectChanges(ctx, lowerBound, snapshotTS, true, mp)
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

func tryBuildCurrentMoTablesCPKeyFilter(
	ctx context.Context,
	accountID uint32,
	targetRel engine.Relation,
	moTablesRel engine.Relation,
	mp *mpool.MPool,
) (*engine.PKFilter, string, error) {
	switch {
	case targetRel == nil:
		return nil, "missing-target-rel", nil
	case moTablesRel == nil:
		return nil, "missing-mo-tables-rel", nil
	}

	targetDef := targetRel.GetTableDef(ctx)
	if targetDef == nil {
		return nil, "missing-target-table-def", nil
	}
	if targetDef.DbName == "" {
		return nil, "missing-target-db-name", nil
	}
	tableName := targetRel.GetTableName()
	if tableName == "" {
		return nil, "missing-target-table-name", nil
	}

	moTablesDef := moTablesRel.GetTableDef(ctx)
	if moTablesDef == nil {
		return nil, "missing-mo-tables-table-def", nil
	}

	cpkeyIdx, ok := moTablesDef.Name2ColIndex[strings.ToLower(catalog.SystemRelAttr_CPKey)]
	if !ok {
		cpkeyIdx, ok = moTablesDef.Name2ColIndex[catalog.SystemRelAttr_CPKey]
	}
	if !ok || cpkeyIdx < 0 || int(cpkeyIdx) >= len(moTablesDef.Cols) {
		return nil, "missing-mo-tables-cpkey-col", nil
	}

	packedKey := packMoTablesCPKey(accountID, targetDef.DbName, tableName)
	replayPacker := types.NewPacker()
	defer replayPacker.Close()
	replayKey := append([]byte(nil), readutil.EncodePrimaryKey(packedKey, replayPacker)...)

	filterVec := vector.NewVec(types.T_varchar.ToType())
	defer filterVec.Free(mp)
	if err := vector.AppendBytes(filterVec, packedKey, false, mp); err != nil {
		return nil, "", err
	}

	pkFilter := buildPKFilterFromVec(filterVec, types.T_varchar.ToType(), int(moTablesDef.Cols[cpkeyIdx].Seqnum))
	if pkFilter == nil {
		return nil, "empty-mo-tables-cpkey-filter", nil
	}
	pkFilter.ReplaySpec = &engine.PKReplaySpec{
		Op:   function.EQUAL,
		Keys: [][]byte{replayKey},
	}
	return pkFilter, "", nil
}

func packMoDatabaseCPKey(accountID uint32, databaseName string) []byte {
	return packCatalogCPKey(accountID, databaseName)
}

func packMoTablesCPKey(accountID uint32, databaseName, tableName string) []byte {
	return packCatalogCPKey(accountID, databaseName, tableName)
}

func packCatalogCPKey(accountID uint32, values ...string) []byte {
	packer := types.NewPacker()
	defer packer.Close()
	packer.EncodeUint32(accountID)
	for _, value := range values {
		packer.EncodeStringType([]byte(value))
	}
	return append([]byte(nil), packer.Bytes()...)
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

// decideLCABranchTSFromBranchDAG resolves the lineage relationship
// between tar and base by consulting the branch DAG. It populates
// branchMetaInfo.lcaTableId and the pathFromLCAToTar /
// pathFromLCAToBase chains. All downstream consumers (collect-range
// builder, hash-diff, tombstone resolver) derive whatever per-side
// snapshot or LCA classification they need from these paths via the
// helper methods on branchMetaInfo.
//
// The DAG only answers lineage questions: the CloneTS attached to an
// outgoing edge is the *source* snapshot used by the clone operation
// on the parent, not the creation commit timestamp of the child.
//
// Special case: when tar and base resolve to the same table id (so
// the diff is purely snapshot-vs-snapshot on a single relation), no
// DAG-derived chain is needed but tombstone resolution still wants
// to probe the table at the earlier of the two snapshots. We model
// that with synthetic single-node paths so the LCA-snapshot helpers
// uniformly fall back to the other side's SP, matching the historic
// case-0 semantics.
func decideLCABranchTSFromBranchDAG(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tblStuff tableStuff,
) (
	branchInfo branchMetaInfo,
	err error,
) {

	tarTableID := tblStuff.tarRel.GetTableID(ctx)
	baseTableID := tblStuff.baseRel.GetTableID(ctx)

	dag, err := constructBranchDAG(ctx, ses, bh)
	if err != nil {
		return
	}

	if lcaTableID, _, _, hasLca := dag.FindLCA(tarTableID, baseTableID); hasLca {
		tarIDs, tarTSs, ok := dag.PathFromAncestor(tarTableID, lcaTableID)
		if !ok {
			err = moerr.NewInternalErrorNoCtxf(
				"data branch: DAG path broken from LCA %d to tar %d",
				lcaTableID, tarTableID)
			return
		}
		baseIDs, baseTSs, ok := dag.PathFromAncestor(baseTableID, lcaTableID)
		if !ok {
			err = moerr.NewInternalErrorNoCtxf(
				"data branch: DAG path broken from LCA %d to base %d",
				lcaTableID, baseTableID)
			return
		}
		branchInfo.lcaTableId = lcaTableID
		branchInfo.pathFromLCAToTar = tarIDs
		branchInfo.pathFromLCAToTarTS = buildTSs(tarTSs)
		branchInfo.pathFromLCAToBase = baseIDs
		branchInfo.pathFromLCAToBaseTS = buildTSs(baseTSs)
		return
	}

	// No DAG-derived LCA, but if tar and base resolve to the same
	// table id we still need to feed downstream tombstone resolution
	// with a probe snapshot. Synthetic single-node paths give the
	// branchMetaInfo helpers exactly that.
	if tarTableID == baseTableID {
		branchInfo.lcaTableId = tarTableID
		branchInfo.pathFromLCAToTar = []uint64{tarTableID}
		branchInfo.pathFromLCAToTarTS = []types.TS{{}}
		branchInfo.pathFromLCAToBase = []uint64{baseTableID}
		branchInfo.pathFromLCAToBaseTS = []types.TS{{}}
	}
	return
}

// buildTSs lifts a slice of int64 physical timestamps into the
// types.TS form expected by branchMetaInfo.
func buildTSs(in []int64) []types.TS {
	out := make([]types.TS, len(in))
	for i, ts := range in {
		out[i] = types.BuildTS(ts, 0)
	}
	return out
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
