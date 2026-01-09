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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/panjf2000/ants/v2"
)

const (
	fakeKind = iota
	normalKind
	compositeKind
)

const (
	diffInsert = "INSERT"
	diffDelete = "DELETE"
	diffUpdate = "UPDATE"
)

const (
	lcaEmpty = iota
	lcaOther
	lcaLeft
	lcaRight
)

const (
	maxSqlBatchCnt  = objectio.BlockMaxRows * 10
	maxSqlBatchSize = mpool.MB * 48
)

func acquireBuffer(pool *sync.Pool) *bytes.Buffer {
	if pool == nil {
		return &bytes.Buffer{}
	}
	buf := pool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func releaseBuffer(pool *sync.Pool, buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	buf.Reset()
	if pool != nil {
		pool.Put(buf)
	}
}

type collectRange struct {
	from []types.TS
	end  []types.TS
	rel  []engine.Relation
}

type branchMetaInfo struct {
	lcaType      int
	lcaTableId   uint64
	tarBranchTS  types.TS
	baseBranchTS types.TS
}

type tableStuff struct {
	tarRel   engine.Relation
	baseRel  engine.Relation
	tarSnap  *plan.Snapshot
	baseSnap *plan.Snapshot

	lcaRel engine.Relation

	def struct {
		colNames     []string     // all columns
		colTypes     []types.Type // all columns
		visibleIdxes []int
		pkColIdx     int
		pkColIdxes   []int // expanded pk columns
		pkKind       int
	}

	worker *ants.Pool

	retPool *retBatchList

	bufPool *sync.Pool
}

type batchWithKind struct {
	name  string
	kind  string
	batch *batch.Batch
}

type emitFunc func(batchWithKind) (stop bool, err error)

func newEmitter(
	ctx context.Context, stopCh <-chan struct{}, retCh chan batchWithKind,
) emitFunc {
	return func(wrapped batchWithKind) (bool, error) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-stopCh:
			return true, nil
		default:
		}

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-stopCh:
			return true, nil
		case retCh <- wrapped:
			return false, nil
		}
	}
}

func emitBatch(
	emit emitFunc, wrapped batchWithKind, forTombstone bool, pool *retBatchList,
) (bool, error) {
	stop, err := emit(wrapped)
	if stop || err != nil {
		pool.releaseRetBatch(wrapped.batch, forTombstone)
		return stop, err
	}
	return false, nil
}

type retBatchList struct {
	mu sync.Mutex
	// 0: data
	// 1: tombstone
	dList []*batch.Batch
	tList []*batch.Batch

	pinned map[*batch.Batch]struct{}
	//debug  map[*batch.Batch]retBatchDebug

	dataVecCnt    int
	tombVecCnt    int
	dataTypes     []types.Type
	tombstoneType types.Type
}

//type retBatchDebug struct {
//	acquire string
//	release string
//}

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
	//if retBatchPool.debug == nil {
	//	retBatchPool.debug = make(map[*batch.Batch]retBatchDebug)
	//}

	if retBatchPool.dataVecCnt == 0 {
		retBatchPool.dataVecCnt = len(tblStuff.def.colNames)
		retBatchPool.tombVecCnt = 1
		retBatchPool.dataTypes = tblStuff.def.colTypes
		retBatchPool.tombstoneType = tblStuff.def.colTypes[tblStuff.def.pkColIdx]
	}

	if forTombstone {
		if len(retBatchPool.tList) == 0 {
			bat = batch.NewWithSize(retBatchPool.tombVecCnt)
			bat.Vecs[0] = vector.NewVec(retBatchPool.tombstoneType)
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
	//retBatchPool.debug[bat] = retBatchDebug{acquire: string(debug.Stack())}
	return bat
}

func (retBatchPool *retBatchList) releaseRetBatch(bat *batch.Batch, forTombstone bool) {
	if retBatchPool == nil {
		panic(moerr.NewInternalErrorNoCtx("retBatchPool is nil"))
	}

	retBatchPool.mu.Lock()
	defer retBatchPool.mu.Unlock()

	//trace := retBatchPool.debug[bat]

	if _, ok := retBatchPool.pinned[bat]; !ok {
		msg := "retBatchPool: release unknown or already released batch"
		//if trace.acquire != "" || trace.release != "" {
		//	msg = fmt.Sprintf("%s (acquired at: %s) (last release at: %s)", msg, trace.acquire, trace.release)
		//}
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

	//retBatchPool.debug[bat] = retBatchDebug{
	//	acquire: trace.acquire,
	//	release: string(debug.Stack()),
	//}

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
	//retBatchPool.debug = nil

}

type compositeOption struct {
	conflictOpt *tree.ConflictOpt
}

func runSql(
	ctx context.Context, ses *Session, bh BackgroundExec, sql string,
	streamChan chan executor.Result, errChan chan error,
) (sqlRet executor.Result, err error) {

	useBackExec := false
	trimmedLower := strings.ToLower(strings.TrimSpace(sql))
	if strings.HasPrefix(trimmedLower, "drop database") {
		// Internal executor does not support DROP DATABASE (IsPublishing panics).
		useBackExec = true
	} else if strings.Contains(strings.ToLower(snapConditionRegex.FindString(sql)), "snapshot") {
		// SQLExecutor cannot resolve snapshot by name.
		useBackExec = true
	}

	var exec executor.SQLExecutor
	if !useBackExec {
		rt := moruntime.ServiceRuntime(ses.service)
		if rt == nil {
			useBackExec = true
		} else {
			val, exist := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
			if !exist {
				useBackExec = true
			} else {
				exec = val.(executor.SQLExecutor)
			}
		}
	}

	if useBackExec {
		// export as CSV need this
		// bh.(*backExec).backSes.SetMysqlResultSet(&MysqlResultSet{})
		//for range tblStuff.def.visibleIdxes {
		//	bh.(*backExec).backSes.mrs.AddColumn(&MysqlColumn{})
		//}
		if err = bh.Exec(ctx, sql); err != nil {
			return
		}
		if _, ok := bh.(*backExec); ok {
			bh.ClearExecResultSet()
			sqlRet.Mp = ses.proc.Mp()
			sqlRet.Batches = bh.GetExecResultBatches()
			return
		}

		rs := bh.GetExecResultSet()
		bh.ClearExecResultSet()
		if len(rs) == 0 || rs[0] == nil {
			return
		}
		mrs, ok := rs[0].(*MysqlResultSet)
		if !ok {
			return sqlRet, moerr.NewInternalError(ctx, "unexpected result set type")
		}
		if len(mrs.Columns) == 0 {
			return
		}
		var bat *batch.Batch
		bat, _, err = convertRowsIntoBatch(ses.proc.Mp(), mrs.Columns, mrs.Data)
		if err != nil {
			return sqlRet, err
		}
		sqlRet.Mp = ses.proc.Mp()
		sqlRet.Batches = []*batch.Batch{bat}
		return
	}

	// we do not use the bh.Exec here, it's too slow.
	// use internal sql instead.

	backSes := bh.(*backExec).backSes

	opts := executor.Options{}.
		WithDisableIncrStatement().
		WithTxn(backSes.GetTxnHandler().GetTxn()).
		WithKeepTxnAlive().
		WithTimeZone(ses.GetTimeZone())

	if streamChan != nil && errChan != nil {
		opts = opts.WithStreaming(streamChan, errChan)
	}

	if sqlRet, err = exec.Exec(ctx, sql, opts); err != nil {
		return sqlRet, err
	}

	return sqlRet, nil
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

	return nil
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
		tableIDs []uint64
	)

	if accId, err = defines.GetAccountId(execCtx.reqCtx); err != nil {
		return
	}

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

	//ctx = fileservice.WithParallelMode(execCtx.reqCtx, fileservice.ParallelForce)
	ctx, cancel = context.WithCancel(execCtx.reqCtx)

	var (
		dagInfo   branchMetaInfo
		tblStuff  tableStuff
		copt      compositeOption
		ok        bool
		diffStmt  *tree.DataBranchDiff
		mergeStmt *tree.DataBranchMerge
	)

	defer func() {
		cancel()
	}()

	if diffStmt, ok = stmt.(*tree.DataBranchDiff); !ok {
		if mergeStmt, ok = stmt.(*tree.DataBranchMerge); !ok {
			return moerr.NewNotSupportedNoCtxf("data branch not supported: %v", stmt)
		}
	}

	if diffStmt != nil {
		if diffStmt.OutputOpt != nil && len(diffStmt.OutputOpt.DirPath) != 0 {
			copt.conflictOpt = &tree.ConflictOpt{
				Opt: tree.CONFLICT_ACCEPT,
			}
		}
		if tblStuff, err = getTableStuff(
			ctx, ses, bh, diffStmt.TargetTable, diffStmt.BaseTable,
		); err != nil {
			return
		}
	} else {
		copt.conflictOpt = mergeStmt.ConflictOpt
		if tblStuff, err = getTableStuff(
			ctx, ses, bh, mergeStmt.SrcTable, mergeStmt.DstTable,
		); err != nil {
			return
		}
	}

	if dagInfo, err = decideLCABranchTSFromBranchDAG(
		ctx, ses, bh, tblStuff.tarRel, tblStuff.baseRel,
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
		} else {
			if err2 := mergeDiffs(
				ctx, cancel, ses, bh, mergeStmt, dagInfo, tblStuff, retBatCh,
			); err2 != nil {
				outputErr.Store(err2)
			}
		}
	}()

	if err = diffOnBase(
		ctx, ses, bh, wg, dagInfo, tblStuff, copt, emit,
	); err != nil {
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

func tryFlushDeletesOrReplace(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tblStuff tableStuff,
	newKind string,
	newValsLen int,
	newRowCnt int,
	deleteCnt *int,
	deletesBuf *bytes.Buffer,
	replaceCnt *int,
	replaceBuf *bytes.Buffer,
	writeFile func([]byte) error,
) (err error) {

	flushDeletes := func() error {
		if err = flushSqlValues(
			ctx, ses, bh, tblStuff, deletesBuf, true, writeFile,
		); err != nil {
			return err
		}

		*deleteCnt = 0
		deletesBuf.Reset()
		return nil
	}

	flushReplace := func() error {

		if err = flushSqlValues(
			ctx, ses, bh, tblStuff, replaceBuf, false, writeFile,
		); err != nil {
			return err
		}

		*replaceCnt = 0
		replaceBuf.Reset()
		return nil
	}

	// if wrapped is nil, means force flush
	if newKind != "" {
		if newKind == diffDelete {
			if deletesBuf.Len()+newValsLen >= maxSqlBatchSize ||
				*deleteCnt+newRowCnt >= maxSqlBatchCnt {
				return flushDeletes()
			}
		} else {
			if replaceBuf.Len()+newValsLen >= maxSqlBatchSize ||
				*replaceCnt+newRowCnt >= maxSqlBatchCnt {
				return flushReplace()
			}
		}
		return nil
	}

	if *replaceCnt > 0 {
		if err = flushReplace(); err != nil {
			return err
		}
	}

	if *deleteCnt > 0 {
		if err = flushDeletes(); err != nil {
			return err
		}
	}

	return nil
}

type sqlValuesAppender struct {
	ctx        context.Context
	ses        *Session
	bh         BackgroundExec
	tblStuff   tableStuff
	deleteCnt  *int
	deleteBuf  *bytes.Buffer
	replaceCnt *int
	replaceBuf *bytes.Buffer
	writeFile  func([]byte) error
}

func (sva sqlValuesAppender) appendRow(kind string, rowValues []byte) error {
	var (
		targetBuf *bytes.Buffer
		rowCnt    *int
	)

	if kind == diffDelete {
		targetBuf = sva.deleteBuf
		rowCnt = sva.deleteCnt
	} else {
		targetBuf = sva.replaceBuf
		rowCnt = sva.replaceCnt
	}

	newValsLen := len(rowValues)
	if targetBuf.Len() > 0 {
		newValsLen++
	}

	if err := tryFlushDeletesOrReplace(
		sva.ctx, sva.ses, sva.bh, sva.tblStuff, kind, newValsLen, 1,
		sva.deleteCnt, sva.deleteBuf, sva.replaceCnt, sva.replaceBuf, sva.writeFile,
	); err != nil {
		return err
	}

	if targetBuf.Len() > 0 {
		targetBuf.WriteString(",")
	}
	targetBuf.Write(rowValues)
	*rowCnt++
	return nil
}

func (sva sqlValuesAppender) flushAll() error {
	return tryFlushDeletesOrReplace(
		sva.ctx, sva.ses, sva.bh, sva.tblStuff, "",
		0, 0, sva.deleteCnt, sva.deleteBuf, sva.replaceCnt, sva.replaceBuf, sva.writeFile,
	)
}

func writeReplaceRowValues(
	ses *Session,
	tblStuff tableStuff,
	row []any,
	buf *bytes.Buffer,
) error {
	buf.WriteString("(")
	for i, idx := range tblStuff.def.visibleIdxes {
		if err := formatValIntoString(ses, row[idx], tblStuff.def.colTypes[idx], buf); err != nil {
			return err
		}
		if i != len(tblStuff.def.visibleIdxes)-1 {
			buf.WriteString(",")
		}
	}
	buf.WriteString(")")

	return nil
}

func writeDeleteRowValues(
	ses *Session,
	tblStuff tableStuff,
	row []any,
	buf *bytes.Buffer,
) error {
	if len(tblStuff.def.pkColIdxes) > 1 {
		buf.WriteString("(")
	}
	for i, colIdx := range tblStuff.def.pkColIdxes {
		if err := formatValIntoString(ses, row[colIdx], tblStuff.def.colTypes[colIdx], buf); err != nil {
			return err
		}
		if i != len(tblStuff.def.pkColIdxes)-1 {
			buf.WriteString(",")
		}
	}
	if len(tblStuff.def.pkColIdxes) > 1 {
		buf.WriteString(")")
	}

	return nil
}

func appendBatchRowsAsSQLValues(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	wrapped batchWithKind,
	tmpValsBuffer *bytes.Buffer,
	appender sqlValuesAppender,
) (err error) {

	//seenCols := make(map[int]struct{}, len(tblStuff.def.visibleIdxes))
	row := make([]any, len(tblStuff.def.colNames))

	for rowIdx := range wrapped.batch.RowCount() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		for _, colIdx := range tblStuff.def.visibleIdxes {
			//seenCols[colIdx] = struct{}{}
			vec := wrapped.batch.Vecs[colIdx]
			if vec.GetNulls().Contains(uint64(rowIdx)) {
				row[colIdx] = nil
				continue
			}

			switch vec.GetType().Oid {
			case types.T_datetime, types.T_timestamp, types.T_decimal64,
				types.T_decimal128, types.T_time:
				bb := vec.GetRawBytesAt(rowIdx)
				row[colIdx] = types.DecodeValue(bb, vec.GetType().Oid)
			default:
				if err = extractRowFromVector(
					ctx, ses, vec, colIdx, row, rowIdx, false,
				); err != nil {
					return
				}
			}
		}

		//for _, pkIdx := range tblStuff.def.pkColIdxes {
		//	if _, ok := seenCols[pkIdx]; ok {
		//		continue
		//	}
		//	vec := wrapped.batch.Vecs[pkIdx]
		//	if vec.GetNulls().Contains(uint64(rowIdx)) {
		//		row[pkIdx] = nil
		//		continue
		//	}
		//
		//	switch vec.GetType().Oid {
		//	case types.T_datetime, types.T_timestamp, types.T_decimal64,
		//		types.T_decimal128, types.T_time:
		//		bb := vec.GetRawBytesAt(rowIdx)
		//		row[pkIdx] = types.DecodeValue(bb, vec.GetType().Oid)
		//	default:
		//		if err = extractRowFromVector(
		//			ctx, ses, vec, pkIdx, row, rowIdx, false,
		//		); err != nil {
		//			return
		//		}
		//	}
		//}

		tmpValsBuffer.Reset()
		if wrapped.kind == diffDelete {
			if err = writeDeleteRowValues(ses, tblStuff, row, tmpValsBuffer); err != nil {
				return
			}
		} else {
			if err = writeReplaceRowValues(ses, tblStuff, row, tmpValsBuffer); err != nil {
				return
			}
		}

		if tmpValsBuffer.Len() == 0 {
			continue
		}

		if err = appender.appendRow(wrapped.kind, tmpValsBuffer.Bytes()); err != nil {
			return
		}
	}

	return nil
}

func mergeDiffs(
	ctx context.Context,
	cancel context.CancelFunc,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.DataBranchMerge,
	dagInfo branchMetaInfo,
	tblStuff tableStuff,
	retCh chan batchWithKind,
) (err error) {

	var (
		replaceCnt int
		deleteCnt  int

		replaceIntoVals = acquireBuffer(tblStuff.bufPool)
		deleteFromVals  = acquireBuffer(tblStuff.bufPool)
		firstErr        error
		tmpValsBuffer   = acquireBuffer(tblStuff.bufPool)
	)

	defer func() {
		releaseBuffer(tblStuff.bufPool, replaceIntoVals)
		releaseBuffer(tblStuff.bufPool, deleteFromVals)
		releaseBuffer(tblStuff.bufPool, tmpValsBuffer)
	}()

	defer func() {
		cancel()
	}()

	appender := sqlValuesAppender{
		ctx:        ctx,
		ses:        ses,
		bh:         bh,
		tblStuff:   tblStuff,
		deleteCnt:  &deleteCnt,
		deleteBuf:  deleteFromVals,
		replaceCnt: &replaceCnt,
		replaceBuf: replaceIntoVals,
	}

	// conflict option should be pushed down to the hash phase,
	// so the batch we received is conflict-free.
	for wrapped := range retCh {

		if firstErr != nil || ctx.Err() != nil {
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			cancel()
			tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
			continue
		}

		if err = appendBatchRowsAsSQLValues(
			ctx, ses, tblStuff, wrapped, tmpValsBuffer, appender,
		); err != nil {
			firstErr = err
			cancel()
			tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
			continue
		}

		tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
	}

	if err = appender.flushAll(); err != nil {
		if firstErr == nil {
			firstErr = err
		}
	}

	if firstErr != nil {
		return firstErr
	}
	return
}

func satisfyDiffOutputOpt(
	ctx context.Context,
	cancel context.CancelFunc,
	stop func(),
	ses *Session,
	bh BackgroundExec,
	stmt *tree.DataBranchDiff,
	dagInfo branchMetaInfo,
	tblStuff tableStuff,
	retCh chan batchWithKind,
) (err error) {

	var (
		mrs      = ses.GetMysqlResultSet()
		first    error
		hitLimit bool
	)

	defer func() {
		cancel()
	}()

	if stmt.OutputOpt == nil || stmt.OutputOpt.Limit != nil {
		var (
			rows = make([][]any, 0, 100)
		)

		for wrapped := range retCh {
			if first != nil {
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}
			if hitLimit {
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}
			if ctx.Err() != nil {
				first = ctx.Err()
				cancel()
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}

			for rowIdx := range wrapped.batch.RowCount() {
				var (
					row = make([]any, len(tblStuff.def.visibleIdxes)+2)
				)
				row[0] = wrapped.name
				row[1] = wrapped.kind

				for _, colIdx := range tblStuff.def.visibleIdxes {
					vec := wrapped.batch.Vecs[colIdx]
					if err = extractRowFromVector(
						ctx, ses, vec, colIdx+2, row, rowIdx, false,
					); err != nil {
						return
					}
				}

				rows = append(rows, row)
				if stmt.OutputOpt != nil && stmt.OutputOpt.Limit != nil &&
					int64(len(rows)) >= *stmt.OutputOpt.Limit {
					// hit limit, cancel producers but keep draining the channel
					hitLimit = true
					stop()
					break
				}
			}
			tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
		}

		slices.SortFunc(rows, func(a, b []any) int {
			for _, idx := range tblStuff.def.pkColIdxes {
				if cmp := types.CompareValue(
					a[idx+2], b[idx+2],
				); cmp != 0 {
					return cmp
				}
			}
			return 0
		})

		for _, row := range rows {
			mrs.AddRow(row)
		}

	} else if stmt.OutputOpt.Count {
		cnt := int64(0)
		for wrapped := range retCh {
			if first != nil {
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}
			if ctx.Err() != nil {
				first = ctx.Err()
				cancel()
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}

			cnt += int64(wrapped.batch.RowCount())
			tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
		}
		mrs.AddRow([]any{cnt})

	} else if len(stmt.OutputOpt.DirPath) != 0 {
		var (
			replaceCnt int
			deleteCnt  int

			deleteFromValsBuffer  = acquireBuffer(tblStuff.bufPool)
			replaceIntoValsBuffer = acquireBuffer(tblStuff.bufPool)
			tmpValsBuffer         = acquireBuffer(tblStuff.bufPool)

			fileHint     string
			fullFilePath string
			writeFile    func([]byte) error
			release      func()
			cleanup      func()
			succeeded    bool
		)

		defer func() {
			if !succeeded && cleanup != nil {
				cleanup()
			}
			if release != nil {
				release()
			}
			releaseBuffer(tblStuff.bufPool, deleteFromValsBuffer)
			releaseBuffer(tblStuff.bufPool, replaceIntoValsBuffer)
			releaseBuffer(tblStuff.bufPool, tmpValsBuffer)
		}()

		if fullFilePath, fileHint, writeFile, release, cleanup, err = prepareFSForDiffAsFile(
			ctx, ses, stmt, tblStuff,
		); err != nil {
			return
		}

		appender := sqlValuesAppender{
			ctx:        ctx,
			ses:        ses,
			bh:         bh,
			tblStuff:   tblStuff,
			deleteCnt:  &deleteCnt,
			deleteBuf:  deleteFromValsBuffer,
			replaceCnt: &replaceCnt,
			replaceBuf: replaceIntoValsBuffer,
			writeFile:  writeFile,
		}
		if writeFile != nil {
			// Make generated SQL runnable in one transaction.
			if err = writeFile([]byte("BEGIN;\n")); err != nil {
				return
			}
		}

		for wrapped := range retCh {
			if first != nil {
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}
			if ctx.Err() != nil {
				first = ctx.Err()
				cancel()
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}

			if wrapped.name == tblStuff.tarRel.GetTableName() {
				if err = appendBatchRowsAsSQLValues(
					ctx, ses, tblStuff, wrapped, tmpValsBuffer, appender,
				); err != nil {
					first = err
					cancel()
					tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
					continue
				}
			}

			tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
		}

		if first != nil {
			return first
		}

		if err = appender.flushAll(); err != nil {
			first = err
			cancel()
		}
		if first == nil && writeFile != nil {
			if err = writeFile([]byte("COMMIT;\n")); err != nil {
				return err
			}
		}

		succeeded = true
		mrs.AddRow([]any{fullFilePath, fileHint})
	}

	if first != nil {
		return first
	}
	if hitLimit {
		return trySaveQueryResult(context.Background(), ses, mrs)
	}
	return trySaveQueryResult(ctx, ses, mrs)
}

func makeFileName(
	baseAtTsExpr *tree.AtTimeStamp,
	tarAtTsExpr *tree.AtTimeStamp,
	tblStuff tableStuff,
) string {
	var (
		srcName  = tblStuff.tarRel.GetTableName()
		baseName = tblStuff.baseRel.GetTableName()
	)

	if baseAtTsExpr != nil {
		baseName = fmt.Sprintf("%s_%s", baseName, baseAtTsExpr.SnapshotName)
	}

	if tarAtTsExpr != nil {
		srcName = fmt.Sprintf("%s_%s", srcName, tarAtTsExpr.SnapshotName)
	}

	return fmt.Sprintf(
		"diff_%s_%s_%s",
		srcName, baseName,
		time.Now().UTC().Format("20060102_150405"),
	)
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

func validateOutputDirPath(ctx context.Context, ses *Session, dirPath string) (err error) {
	if len(dirPath) == 0 {
		return nil
	}

	var (
		stagePath    string
		ok           bool
		inputDirPath = dirPath
	)

	if stagePath, ok, err = tryDecodeStagePath(ses, dirPath); err != nil {
		return
	} else if ok {
		dirPath = stagePath
	}

	var fsPath fileservice.Path
	if fsPath, err = fileservice.ParsePath(dirPath); err != nil {
		return
	}

	if fsPath.Service == "" {
		var info os.FileInfo
		if info, err = os.Stat(dirPath); err != nil {
			if os.IsNotExist(err) {
				return moerr.NewInvalidInputNoCtxf("output directory %s does not exist", inputDirPath)
			}
			return
		}
		if !info.IsDir() {
			return moerr.NewInvalidInputNoCtxf("output directory %s is not a directory", inputDirPath)
		}
		return nil
	}

	var (
		targetFS   fileservice.FileService
		targetPath string
		entry      *fileservice.DirEntry
	)

	if fsPath.Service == defines.SharedFileServiceName {
		targetFS = getPu(ses.GetService()).FileService
		targetPath = dirPath
	} else {
		var etlFS fileservice.ETLFileService
		if etlFS, targetPath, err = fileservice.GetForETL(ctx, nil, dirPath); err != nil {
			return
		}
		targetFS = etlFS
		defer targetFS.Close(ctx)
	}

	if len(strings.Trim(targetPath, "/")) == 0 {
		// service root: try listing to ensure the bucket/root is reachable
		for _, err = range targetFS.List(ctx, targetPath) {
			if err != nil {
				if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
					return moerr.NewInvalidInputNoCtxf("output directory %s does not exist", inputDirPath)
				}
				return err
			}
			// any entry means list works
			break
		}
		return nil
	}

	if entry, err = targetFS.StatFile(ctx, targetPath); err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			// fallthrough to List-based directory detection
		} else {
			return
		}
	}

	// StatFile succeeded: reject if it's a file, accept if the FS can stat directories.
	if entry != nil {
		if !entry.IsDir {
			return moerr.NewInvalidInputNoCtxf("output directory %s is not a directory", inputDirPath)
		}
		return nil
	}

	// StatFile can't prove directory existence (common for object storage). Use parent List to
	// detect the child entry and its type.
	trimmedPath := strings.TrimRight(targetPath, "/")
	if len(trimmedPath) == 0 {
		// root of the service, treat as directory
		return nil
	}
	parent, base := path.Split(trimmedPath)
	parent = strings.TrimRight(parent, "/")
	if len(base) == 0 {
		// target is root
		return nil
	}

	for entry, err = range targetFS.List(ctx, parent) {
		if err != nil {
			return err
		}
		if entry.Name != base {
			continue
		}
		if !entry.IsDir {
			return moerr.NewInvalidInputNoCtxf("output directory %s is not a directory", inputDirPath)
		}
		return nil
	}

	return moerr.NewInvalidInputNoCtxf("output directory %s does not exist", inputDirPath)
}

func prepareFSForDiffAsFile(
	ctx context.Context,
	ses *Session,
	stmt *tree.DataBranchDiff,
	tblStuff tableStuff,
) (
	sqlRetPath, sqlRetHint string,
	writeFile func([]byte) error,
	release func(),
	cleanup func(),
	err error,
) {
	var (
		ok           bool
		stagePath    string
		fileName     string
		fullFilePath string
	)

	fileName = makeFileName(stmt.BaseTable.AtTsExpr, stmt.TargetTable.AtTsExpr, tblStuff)
	fileName += ".sql"

	sqlRetPath = path.Join(stmt.OutputOpt.DirPath, fileName)

	if stagePath, ok, err = tryDecodeStagePath(ses, stmt.OutputOpt.DirPath); err != nil {
		return
	} else if ok {
		sqlRetPath = strings.Replace(sqlRetPath, "stage:/", "stage://", 1)
		fullFilePath = path.Join(stagePath, fileName)
	} else {
		fullFilePath = path.Join(stmt.OutputOpt.DirPath, fileName)
	}

	sqlRetHint = fmt.Sprintf(
		"DELETE FROM %s.%s, REPLACE INTO %s.%s",
		tblStuff.baseRel.GetTableDef(ctx).DbName, tblStuff.baseRel.GetTableName(),
		tblStuff.baseRel.GetTableDef(ctx).DbName, tblStuff.baseRel.GetTableName(),
	)

	var (
		targetFS   fileservice.FileService
		targetPath string
		fsPath     fileservice.Path
	)

	if fsPath, err = fileservice.ParsePath(fullFilePath); err != nil {
		return
	}

	if fsPath.Service == defines.SharedFileServiceName {
		targetFS = getPu(ses.GetService()).FileService
		targetPath = fullFilePath
	} else {
		// ensure local dir exists when using implicit local ETL path
		if fsPath.Service == "" {
			if mkErr := os.MkdirAll(filepath.Dir(fullFilePath), 0o755); mkErr != nil {
				err = mkErr
				return
			}
		}

		var etlFS fileservice.ETLFileService
		if etlFS, targetPath, err = fileservice.GetForETL(ctx, nil, fullFilePath); err != nil {
			return
		}
		targetFS = etlFS
	}

	cleanup = func() {
		_ = targetFS.Delete(context.Background(), targetPath)
	}

	defer func() {
		if err != nil {
			if cleanup != nil {
				cleanup()
			}
			targetFS.Close(ctx)
		}
	}()

	if err = targetFS.Delete(ctx, targetPath); err != nil && !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
		return
	}

	if mfs, ok := targetFS.(fileservice.MutableFileService); ok {
		// SQL diff needs append, so pre-create the file and return an appender.
		if err = targetFS.Write(ctx, fileservice.IOVector{
			FilePath: targetPath,
			Entries: []fileservice.IOEntry{
				{Size: 0, Data: []byte{}},
			},
		}); err != nil && !moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) {
			return
		}

		var mut fileservice.Mutator
		if mut, err = mfs.NewMutator(ctx, targetPath); err != nil {
			return
		}

		writeFile = func(fileContent []byte) error {
			return mut.Append(ctx, fileservice.IOEntry{
				Size: int64(len(fileContent)),
				Data: fileContent,
			})
		}

		release = func() {
			_ = mut.Close()
			targetFS.Close(ctx)
		}
	} else {
		if writeFile, release, err = newSingleWriteAppender(
			ctx, tblStuff.worker, targetFS, targetPath, cleanup,
		); err != nil {
			return
		}
	}

	return
}

func newSingleWriteAppender(
	ctx context.Context,
	worker *ants.Pool,
	targetFS fileservice.FileService,
	targetPath string,
	onError func(),
) (writeFile func([]byte) error, release func(), err error) {
	pr, pw := io.Pipe()
	done := make(chan error, 1)

	if worker == nil {
		err = moerr.NewInternalErrorNoCtx("worker pool is nil")
		return
	}

	if err = worker.Submit(func() {
		defer close(done)
		vec := fileservice.IOVector{
			FilePath: targetPath,
			Entries: []fileservice.IOEntry{
				{
					ReaderForWrite: pr,
					Size:           -1,
				},
			},
		}
		if wErr := targetFS.Write(ctx, vec); wErr != nil {
			_ = pr.CloseWithError(wErr)
			done <- wErr
			return
		}
		done <- pr.Close()
	}); err != nil {
		_ = pr.Close()
		_ = pw.Close()
		return
	}

	writeFile = func(fileContent []byte) error {
		if len(fileContent) == 0 {
			return nil
		}
		_, err := pw.Write(fileContent)
		return err
	}

	release = func() {
		_ = pw.Close()
		if wErr := <-done; wErr != nil && onError != nil {
			onError()
		}
		targetFS.Close(ctx)
	}

	return
}

func removeFileIgnoreError(ctx context.Context, service, filePath string) {
	if len(filePath) == 0 {
		return
	}

	fsPath, err := fileservice.ParsePath(filePath)
	if err != nil {
		return
	}

	var (
		targetFS   fileservice.FileService
		targetPath string
	)

	if fsPath.Service == defines.SharedFileServiceName {
		targetFS = getPu(service).FileService
		targetPath = filePath
	} else {
		var etlFS fileservice.ETLFileService
		if etlFS, targetPath, err = fileservice.GetForETL(ctx, nil, filePath); err != nil {
			return
		}
		targetFS = etlFS
	}

	_ = targetFS.Delete(ctx, targetPath)
	targetFS.Close(ctx)
}

// if `writeFile` is not nil, the sql will be flushed down into this file, or
// the sql will be executed by bh.
func flushSqlValues(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tblStuff tableStuff,
	buf *bytes.Buffer,
	isDeleteFrom bool,
	writeFile func([]byte) error,
) (err error) {

	sqlBuffer := acquireBuffer(tblStuff.bufPool)
	defer releaseBuffer(tblStuff.bufPool, sqlBuffer)

	initReplaceIntoBuf := func() {
		sqlBuffer.WriteString(fmt.Sprintf(
			"replace into %s.%s values ",
			tblStuff.baseRel.GetTableDef(ctx).DbName,
			tblStuff.baseRel.GetTableDef(ctx).Name,
		))
	}

	initDeleteFromBuf := func() {
		if len(tblStuff.def.pkColIdxes) == 1 {
			sqlBuffer.WriteString(fmt.Sprintf(
				"delete from %s.%s where %s in (",
				tblStuff.baseRel.GetTableDef(ctx).DbName,
				tblStuff.baseRel.GetTableDef(ctx).Name,
				tblStuff.def.colNames[tblStuff.def.pkColIdx],
			))
		} else {
			pkNames := make([]string, len(tblStuff.def.pkColIdxes))
			for i, pkColIdx := range tblStuff.def.pkColIdxes {
				pkNames[i] = tblStuff.def.colNames[pkColIdx]
			}
			sqlBuffer.WriteString(fmt.Sprintf(
				"delete from %s.%s where (%s) in (",
				tblStuff.baseRel.GetTableDef(ctx).DbName,
				tblStuff.baseRel.GetTableDef(ctx).Name,
				strings.Join(pkNames, ","),
			))
		}
	}

	if isDeleteFrom {
		initDeleteFromBuf()
		sqlBuffer.Write(buf.Bytes())
		sqlBuffer.WriteString(")")
	} else {
		initReplaceIntoBuf()
		sqlBuffer.Write(buf.Bytes())
	}

	sqlBuffer.WriteString(";\n")

	if writeFile != nil {
		err = writeFile(sqlBuffer.Bytes())
	} else {
		var (
			ret executor.Result
		)

		defer func() {
			ret.Close()
		}()
		ret, err = runSql(ctx, ses, bh, sqlBuffer.String(), nil, nil)
	}

	return err
}

func buildOutputSchema(
	ctx context.Context,
	ses *Session,
	stmt *tree.DataBranchDiff,
	tblStuff tableStuff,
) (err error) {

	var (
		mrs      = ses.GetMysqlResultSet()
		showCols []*MysqlColumn
	)
	ses.ClearAllMysqlResultSet()
	ses.ClearResultBatches()

	if stmt.OutputOpt == nil || stmt.OutputOpt.Limit != nil {
		// output all rows OR
		// output limited rows (is this can be pushed down to hash-phase?)
		showCols = make([]*MysqlColumn, 0, 2)
		showCols = append(showCols, new(MysqlColumn), new(MysqlColumn))
		showCols[0].SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		showCols[0].SetName(fmt.Sprintf(
			"diff %s against %s", tblStuff.tarRel.GetTableName(), tblStuff.baseRel.GetTableName()),
		)
		showCols[1].SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		showCols[1].SetName("flag")

		for _, idx := range tblStuff.def.visibleIdxes {
			nCol := new(MysqlColumn)
			if err = convertEngineTypeToMysqlType(ctx, tblStuff.def.colTypes[idx].Oid, nCol); err != nil {
				return
			}

			nCol.SetName(tblStuff.def.colNames[idx])
			showCols = append(showCols, nCol)
		}

	} else if stmt.OutputOpt.Count {
		// count(*) of diff rows
		showCols = append(showCols, &MysqlColumn{})
		showCols[0].SetName("COUNT(*)")
		showCols[0].SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	} else if len(stmt.OutputOpt.DirPath) != 0 {
		// output as file
		col1 := new(MysqlColumn)
		col1.SetName("FILE SAVED TO")
		col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

		col2 := new(MysqlColumn)
		col2.SetName("HINT")
		col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

		showCols = append(showCols, col1, col2)
	} else {
		return moerr.NewNotSupportedNoCtx(fmt.Sprintf("%v", stmt.OutputOpt))
	}

	for _, col := range showCols {
		mrs.AddColumn(col)
	}

	return nil
}

func tryDiffAsCSV(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.DataBranchDiff,
	tblStuff tableStuff,
) (bool, error) {

	if stmt.OutputOpt == nil {
		return false, nil
	}

	if len(stmt.OutputOpt.DirPath) == 0 {
		return false, nil
	}

	sql := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s.%s",
		tblStuff.baseRel.GetTableDef(ctx).DbName,
		tblStuff.baseRel.GetTableDef(ctx).Name,
	)

	if tblStuff.baseSnap != nil {
		sql += fmt.Sprintf("{snapshot='%s'}", tblStuff.baseSnap.ExtraInfo.Name)
	}

	var (
		err    error
		sqlRet executor.Result
	)

	if sqlRet, err = runSql(ctx, ses, bh, sql, nil, nil); err != nil {
		return false, err
	}

	if len(sqlRet.Batches) != 1 &&
		sqlRet.Batches[0].RowCount() != 1 &&
		sqlRet.Batches[0].VectorCount() != 1 {
		return false, moerr.NewInternalErrorNoCtxf("cannot get count(*) of base table")
	}

	if vector.GetFixedAtWithTypeCheck[uint64](sqlRet.Batches[0].Vecs[0], 0) != 0 {
		return false, nil
	}

	sqlRet.Close()

	return true, writeCSV(ctx, ses, tblStuff, bh, stmt)
}

func writeCSV(
	inputCtx context.Context,
	ses *Session,
	tblStuff tableStuff,
	bh BackgroundExec,
	stmt *tree.DataBranchDiff,
) (err error) {

	ctx, cancelCtx := context.WithCancel(inputCtx)
	defer cancelCtx()

	// SQLExecutor do not support snapshot read, we must use the MO_TS
	snap := ""
	if tblStuff.tarSnap != nil {
		snap = fmt.Sprintf("{MO_TS=%d}", tblStuff.tarSnap.TS.PhysicalTime)
	}

	// output as csv
	sql := fmt.Sprintf("SELECT * FROM %s.%s%s;",
		tblStuff.tarRel.GetTableDef(ctx).DbName,
		tblStuff.tarRel.GetTableDef(ctx).Name,
		snap,
	)

	var (
		stop       bool
		wg         sync.WaitGroup
		errChan    = make(chan error, 1)
		streamChan = make(chan executor.Result, runtime.NumCPU())

		writerWg  sync.WaitGroup
		workerWg  sync.WaitGroup
		writerErr = make(chan error, 1)
		closeByte sync.Once

		mrs        = &MysqlResultSet{}
		sqlRetHint string
		sqlRetPath string
		fileName   = makeFileName(stmt.BaseTable.AtTsExpr, stmt.TargetTable.AtTsExpr, tblStuff)
		cleanup    func()
	)

	ep := &ExportConfig{
		userConfig: newDiffCSVUserConfig(),
		service:    ses.service,
	}

	for range tblStuff.def.visibleIdxes {
		mrs.AddColumn(&MysqlColumn{})
	}

	ep.init()
	ep.ctx = ctx
	ep.mrs = mrs
	ep.ByteChan = make(chan *BatchByte, runtime.NumCPU())

	ep.DefaultBufSize = getPu(ses.GetService()).SV.ExportDataDefaultFlushSize
	initExportFileParam(ep, mrs)

	sqlRetHint = ep.userConfig.String()
	fileName += ".csv"

	var (
		ok        bool
		stagePath string
	)

	sqlRetPath = path.Join(stmt.OutputOpt.DirPath, fileName)

	if stagePath, ok, err = tryDecodeStagePath(ses, stmt.OutputOpt.DirPath); err != nil {
		return
	} else if ok {
		sqlRetPath = strings.Replace(sqlRetPath, "stage:/", "stage://", 1)
		ep.userConfig.StageFilePath = path.Join(stagePath, fileName)
		if err = openNewFile(ctx, ep, mrs); err != nil {
			return
		}
		cleanup = func() {
			removeFileIgnoreError(context.Background(), ep.service, ep.userConfig.StageFilePath)
		}
	} else {
		ep.userConfig.FilePath = path.Join(stmt.OutputOpt.DirPath, fileName)
		if err = openNewFile(ctx, ep, mrs); err != nil {
			return
		}
		cleanup = func() {
			removeFileIgnoreError(context.Background(), ep.service, ep.userConfig.FilePath)
		}
	}

	writerWg.Add(1)
	if err = tblStuff.worker.Submit(func() {
		defer writerWg.Done()
		for bb := range ep.ByteChan {
			if bb.err != nil {
				select {
				case writerErr <- bb.err:
				default:
				}
				return
			}
			ep.BatchMap[bb.index] = bb.writeByte
			for {
				value, ok := ep.BatchMap[ep.WriteIndex.Load()+1]
				if !ok {
					break
				}
				if err2 := writeToCSVFile(ep, value); err2 != nil {
					select {
					case writerErr <- err2:
					default:
					}
					return
				}
				ep.WriteIndex.Add(1)
				ep.BatchMap[ep.WriteIndex.Load()] = nil
			}
		}
		if ep.WriteIndex.Load() != ep.Index.Load() {
			if err2 := exportAllDataFromBatches(ep); err2 != nil {
				select {
				case writerErr <- err2:
				default:
				}
			}
		}
	}); err != nil {
		writerWg.Done()
		return err
	}

	defer func() {
		if err != nil && cleanup != nil {
			cleanup()
		}
	}()

	defer func() {
		closeByte.Do(func() {
			close(ep.ByteChan)
		})
		writerWg.Wait()
		closeErr := Close(ep)
		ep.Close()
		if err == nil {
			err = closeErr
		} else {
			err = errors.Join(err, closeErr)
		}
	}()

	wg.Add(1)
	if err = tblStuff.worker.Submit(func() {
		defer func() {
			wg.Done()
			close(streamChan)
			close(errChan)
		}()
		if _, err2 := runSql(ctx, ses, bh, sql, streamChan, errChan); err2 != nil {
			select {
			case errChan <- err2:
			default:
			}
		}
	}); err != nil {
		wg.Done()
		return err
	}

	streamOpen, errOpen := true, true
	for streamOpen || errOpen {
		select {
		case <-inputCtx.Done():
			err = errors.Join(err, inputCtx.Err())
			stop = true
		case e := <-writerErr:
			if e != nil {
				err = errors.Join(err, e)
			}
			stop = true
			cancelCtx()
		case e, ok := <-errChan:
			if !ok {
				errOpen = false
				continue
			}
			err = errors.Join(err, e)
			cancelCtx()
			stop = true
		case sqlRet, ok := <-streamChan:
			if !ok {
				streamOpen = false
				continue
			}
			if stop {
				sqlRet.Close()
				continue
			}
			for _, bat := range sqlRet.Batches {
				copied, _ := bat.Dup(ses.proc.Mp())
				idx := ep.Index.Add(1)
				workerWg.Add(1)
				if submitErr := tblStuff.worker.Submit(func() {
					defer workerWg.Done()
					constructByte(ctx, ses, copied, idx, ep.ByteChan, ep)
				}); submitErr != nil {
					workerWg.Done()
					err = errors.Join(err, submitErr)
					stop = true
					cancelCtx()
					break
				}
			}
			sqlRet.Close()
		}
	}

	wg.Wait()
	workerWg.Wait()
	closeByte.Do(func() {
		close(ep.ByteChan)
	})
	writerWg.Wait()

	select {
	case e := <-writerErr:
		err = errors.Join(err, e)
	default:
	}

	if err != nil {
		return err
	}

	mrs = ses.GetMysqlResultSet()
	mrs.AddRow([]any{sqlRetPath, sqlRetHint})

	return trySaveQueryResult(ctx, ses, mrs)
}

func newDiffCSVUserConfig() *tree.ExportParam {
	fields := tree.NewFields(
		tree.DefaultFieldsTerminated,
		false,
		tree.DefaultFieldsEnclosedBy[0],
		tree.DefaultFieldsEscapedBy[0],
	)
	return &tree.ExportParam{
		Outfile: true,
		Fields:  fields,
		Lines:   tree.NewLines("", "\n"),
		Header:  false,
	}
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
			_ = h.Close()
		}
		for _, h := range baseHandle {
			_ = h.Close()
		}
		tarHandle = nil
		baseHandle = nil
	}

	defer func() {
		closeHandle()
	}()

	//if true || dagInfo.lcaTableId == 0 {

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
		ctx, ses, bh, tblStuff, &dagInfo,
	); err != nil {
		return
	}

	if err = hashDiff(
		ctx, ses, bh, tblStuff, dagInfo,
		copt, emit, tarHandle, baseHandle,
	); err != nil {
		return
	}

	closeHandle()
	return
	//}

	//return
	// merge left into right
	//var (
	//	lcaRel      engine.Relation
	//	lcaSnapshot *plan.Snapshot
	//)
	//
	//lcaSnapshot = &plan2.Snapshot{
	//	Tenant: &plan.SnapshotTenant{
	//		TenantID: ses.GetAccountId(),
	//	},
	//}
	//
	//if dagInfo.lcaTableId == tblStuff.tarRel.GetTableID(ctx) {
	//	// left is the LCA
	//	lcaRel = tblStuff.tarRel
	//	lcaSnapshot.TS = &timestamp.Timestamp{PhysicalTime: dagInfo.baseBranchTS.Physical()}
	//	if tblStuff.tarSnap != nil && tblStuff.tarSnap.TS.Less(*lcaSnapshot.TS) {
	//		lcaSnapshot.TS = tblStuff.tarSnap.TS
	//	}
	//} else if dagInfo.lcaTableId == tblStuff.baseRel.GetTableID(ctx) {
	//	// right is the LCA
	//	lcaRel = tblStuff.baseRel
	//	lcaSnapshot.TS = &timestamp.Timestamp{PhysicalTime: dagInfo.tarBranchTS.Physical()}
	//	if tblStuff.baseSnap != nil && tblStuff.baseSnap.TS.Less(*lcaSnapshot.TS) {
	//		lcaSnapshot.TS = tblStuff.baseSnap.TS
	//	}
	//} else {
	//	// LCA is other table
	//	lcaSnapshot.TS = &timestamp.Timestamp{PhysicalTime: dagInfo.tarBranchTS.Physical()}
	//	if dagInfo.baseBranchTS.LT(&dagInfo.tarBranchTS) {
	//		lcaSnapshot.TS.PhysicalTime = dagInfo.baseBranchTS.Physical()
	//	}
	//
	//	if lcaRel, err = getRelationById(
	//		ctx, ses, bh, dagInfo.lcaTableId, lcaSnapshot); err != nil {
	//		return
	//	}
	//}
	//
	//tblStuff.lcaRel = lcaRel
	//
	//var (
	//	tmpPair1 = tblStuff
	//	tmpPair2 = tblStuff
	//
	//	dagInfo1 branchMetaInfo
	//	dagInfo2 branchMetaInfo
	//
	//	finalTblStuff tableStuff
	//	finalDagInfo  branchMetaInfo
	//)
	//
	//tmpPair1.tarRel = tblStuff.tarRel
	//tmpPair1.baseRel = lcaRel
	//tmpPair1.tarSnap = tblStuff.tarSnap
	//tmpPair1.baseSnap = lcaSnapshot
	//
	//if dagInfo1, err = decideLCABranchTSFromBranchDAG(
	//	ctx, ses, tmpPair1.tarRel, tmpPair1.baseRel,
	//); err != nil {
	//	return
	//}
	//
	//if dagInfo1.tarBranchTS.IsEmpty() {
	//	dagInfo1.tarBranchTS = types.TimestampToTS(*tmpPair1.baseSnap.TS)
	//}
	//
	//if tarHandle, _, err = constructChangeHandle(
	//	ctx, ses, bh, tmpPair1, &dagInfo1,
	//); err != nil {
	//	return
	//}
	//
	//tmpPair2.tarRel = tblStuff.baseRel
	//tmpPair2.baseRel = lcaRel
	//tmpPair2.tarSnap = tblStuff.baseSnap
	//tmpPair2.baseSnap = lcaSnapshot
	//
	//if dagInfo2, err = decideLCABranchTSFromBranchDAG(
	//	ctx, ses, tmpPair2.tarRel, tmpPair2.baseRel,
	//); err != nil {
	//	return
	//}
	//
	//if dagInfo2.tarBranchTS.IsEmpty() {
	//	dagInfo2.tarBranchTS = types.TimestampToTS(*tmpPair2.baseSnap.TS)
	//}
	//
	//if copt.conflictOpt == nil || copt.conflictOpt.Opt != tree.CONFLICT_ACCEPT {
	//	// if we got a conflict_accept option,
	//	// then we should ignore all inserts/deletes/updates from the base.
	//	if baseHandle, _, err = constructChangeHandle(
	//		ctx, ses, bh, tmpPair2, &dagInfo2,
	//	); err != nil {
	//		return
	//	}
	//}
	//
	//finalDagInfo.lcaType = dagInfo.lcaType
	//finalDagInfo.lcaTableId = dagInfo.lcaTableId
	//finalDagInfo.tarBranchTS = dagInfo1.tarBranchTS
	//finalDagInfo.baseBranchTS = dagInfo2.tarBranchTS
	//
	//finalTblStuff = tblStuff
	//finalTblStuff.tarRel = tmpPair1.tarRel
	//finalTblStuff.baseRel = tmpPair2.tarRel
	//finalTblStuff.tarSnap = tmpPair1.tarSnap
	//finalTblStuff.baseSnap = tmpPair2.tarSnap
	//
	//return hashDiff(ctx, ses, bh, finalTblStuff, finalDagInfo, retCh, copt, tarHandle, baseHandle)
}

func hashDiff(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tblStuff tableStuff,
	dagInfo branchMetaInfo,
	copt compositeOption,
	emit emitFunc,
	tarHandle []engine.ChangesHandle,
	baseHandle []engine.ChangesHandle,
) (
	err error,
) {

	var (
		baseDataHashmap      databranchutils.BranchHashmap
		baseTombstoneHashmap databranchutils.BranchHashmap

		tarDataHashmap      databranchutils.BranchHashmap
		tarTombstoneHashmap databranchutils.BranchHashmap
	)

	defer func() {
		if baseDataHashmap != nil {
			baseDataHashmap.Close()
		}
		if baseTombstoneHashmap != nil {
			baseTombstoneHashmap.Close()
		}
		if tarDataHashmap != nil {
			tarDataHashmap.Close()
		}
		if tarTombstoneHashmap != nil {
			tarTombstoneHashmap.Close()
		}
	}()

	if baseDataHashmap, baseTombstoneHashmap, err = buildHashmapForTable(
		ctx, ses.proc.Mp(), dagInfo.lcaType, tblStuff, baseHandle,
	); err != nil {
		return
	}

	if tarDataHashmap, tarTombstoneHashmap, err = buildHashmapForTable(
		ctx, ses.proc.Mp(), dagInfo.lcaType, tblStuff, tarHandle,
	); err != nil {
		return
	}

	if dagInfo.lcaType == lcaEmpty {
		if err = hashDiffIfNoLCA(
			ctx, ses, tblStuff, copt, emit,
			tarDataHashmap, tarTombstoneHashmap,
			baseDataHashmap, baseTombstoneHashmap,
		); err != nil {
			return
		}
	} else {
		if err = hashDiffIfHasLCA(
			ctx, ses, bh, dagInfo, tblStuff, copt, emit,
			tarDataHashmap, tarTombstoneHashmap,
			baseDataHashmap, baseTombstoneHashmap,
		); err != nil {
			return
		}
	}

	return
}

func hashDiffIfHasLCA(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	dagInfo branchMetaInfo,
	tblStuff tableStuff,
	copt compositeOption,
	emit emitFunc,
	tarDataHashmap databranchutils.BranchHashmap,
	tarTombstoneHashmap databranchutils.BranchHashmap,
	baseDataHashmap databranchutils.BranchHashmap,
	baseTombstoneHashmap databranchutils.BranchHashmap,
) (err error) {

	var (
		wg        sync.WaitGroup
		atomicErr atomic.Value

		baseDeleteBatches []batchWithKind
		baseUpdateBatches []batchWithKind
	)

	handleBaseDeleteAndUpdates := func(wrapped batchWithKind) error {
		if err2 := mergeutil.SortColumnsByIndex(
			wrapped.batch.Vecs, tblStuff.def.pkColIdx, ses.proc.Mp(),
		); err2 != nil {
			return err2
		}
		if wrapped.kind == diffDelete {
			baseDeleteBatches = append(baseDeleteBatches, wrapped)
		} else {
			baseUpdateBatches = append(baseUpdateBatches, wrapped)
		}

		return nil
	}

	handleTarDeleteAndUpdates := func(wrapped batchWithKind) (err2 error) {
		if len(baseUpdateBatches) == 0 && len(baseDeleteBatches) == 0 {
			// no need to check conflict
			if stop, e := emitBatch(emit, wrapped, false, tblStuff.retPool); e != nil {
				return e
			} else if stop {
				return nil
			}
			return nil
		}

		if err2 = mergeutil.SortColumnsByIndex(
			wrapped.batch.Vecs, tblStuff.def.pkColIdx, ses.proc.Mp(),
		); err2 != nil {
			return err2
		}

		checkConflict := func(tarWrapped, baseWrapped batchWithKind) (sels1, sels2 []int64, err3 error) {
			var (
				cmp     int
				tarVec  = tarWrapped.batch.Vecs[tblStuff.def.pkColIdx]
				baseVec = baseWrapped.batch.Vecs[tblStuff.def.pkColIdx]
			)

			i, j := 0, 0
			for i < tarVec.Length() && j < baseVec.Length() {
				if cmp, err3 = compareSingleValInVector(
					ctx, ses, i, j, tarVec, baseVec,
				); err3 != nil {
					return
				}

				if cmp == 0 {
					// conflict
					// tar and base both deleted on pk1 => empty
					// tar and base both updated on pk1 => we need compare the left columns, consider
					// the conflict option.
					if copt.conflictOpt == nil {
						if tarWrapped.kind == baseWrapped.kind {
							// compare the left column values
							if cmp, err3 = compareRowInWrappedBatches(
								ctx, ses, tblStuff, i, j, true,
								tarWrapped, baseWrapped,
							); err3 != nil {
								return
							} else if cmp == 0 {
								sels1 = append(sels1, int64(i))
								sels2 = append(sels2, int64(j))
							}
						}
						i++
						j++
					} else if copt.conflictOpt.Opt == tree.CONFLICT_ACCEPT {
						// only keep the rows from tar
						sels2 = append(sels2, int64(j))
						i++
						j++
					} else if copt.conflictOpt.Opt == tree.CONFLICT_SKIP {
						sels1 = append(sels1, int64(i))
						sels2 = append(sels2, int64(j))
						i++
						j++
					} else {
						// copt.conflictOpt == tree.CONFLICT_FAIL
						tarRow := make([]any, 1)
						if err3 = extractRowFromVector(
							ctx, ses, tarVec, 0, tarRow, i, false,
						); err3 != nil {
							return
						}

						buf := acquireBuffer(tblStuff.bufPool)
						if err3 = formatValIntoString(
							ses, tarRow[0], tblStuff.def.colTypes[tblStuff.def.pkColIdx], buf,
						); err3 != nil {
							releaseBuffer(tblStuff.bufPool, buf)
							return
						}

						err3 = moerr.NewInternalErrorNoCtxf(
							"conflict: %s %s and %s %s on pk(%v) with different values",
							tarWrapped.name, tarWrapped.kind,
							baseWrapped.name, baseWrapped.kind, buf.String(),
						)
						releaseBuffer(tblStuff.bufPool, buf)
						return
					}
				} else if cmp < 0 {
					// tarVal < baseVal
					i++
				} else {
					// tarVal > baseVal
					j++
				}
			}

			return
		}

		foo := func(baseWrappedList []batchWithKind) (err3 error) {
			for _, baseWrapped := range baseWrappedList {
				var (
					sels1 []int64
					sels2 []int64
				)
				if sels1, sels2, err3 = checkConflict(wrapped, baseWrapped); err3 != nil {
					return
				} else {
					if len(sels1) != 0 {
						wrapped.batch.Shrink(sels1, true)
					}

					if len(sels2) != 0 {
						baseWrapped.batch.Shrink(sels2, true)
					}
				}

				if wrapped.batch.RowCount() == 0 {
					break
				}
			}

			return
		}

		if err2 = foo(baseDeleteBatches); err2 != nil {
			return
		}

		baseDeleteBatches = plan2.RemoveIf(baseDeleteBatches, func(t batchWithKind) bool {
			if t.batch.RowCount() == 0 {
				tblStuff.retPool.releaseRetBatch(t.batch, false)
				return true
			}
			return false
		})

		if err2 = foo(baseUpdateBatches); err2 != nil {
			return
		}

		baseUpdateBatches = plan2.RemoveIf(baseUpdateBatches, func(t batchWithKind) bool {
			if t.batch.RowCount() == 0 {
				tblStuff.retPool.releaseRetBatch(t.batch, false)
				return true
			}
			return false
		})

		if wrapped.batch.RowCount() == 0 {
			tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
			return
		}

		stop, e := emitBatch(emit, wrapped, false, tblStuff.retPool)
		if e != nil {
			return e
		}
		if stop {
			return nil
		}

		return
	}

	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	asyncDelsAndUpdatesHandler := func(forBase bool, tmpCh chan batchWithKind) (err2 error) {
		var (
			branchTS = dagInfo.baseBranchTS
			hashmap1 = baseDataHashmap
			hashmap2 = baseTombstoneHashmap
			name     = tblStuff.baseRel.GetTableName()
		)

		if !forBase {
			branchTS = dagInfo.tarBranchTS
			hashmap1 = tarDataHashmap
			hashmap2 = tarTombstoneHashmap
			name = tblStuff.tarRel.GetTableName()
		}

		wg.Add(1)
		if err2 = tblStuff.worker.Submit(func() {
			defer func() {
				close(tmpCh)
				tmpCh = nil
				wg.Done()
			}()

			if err3 := findDeleteAndUpdateBat(
				newCtx, ses, bh, tblStuff, name,
				tmpCh, branchTS, hashmap1, hashmap2,
			); err3 != nil {
				atomicErr.Store(err3)
			}
		}); err2 != nil {
			wg.Done()
			return
		}
		return nil
	}

	stepHandler := func(forBase bool) (err2 error) {
		var (
			tmpCh = make(chan batchWithKind, 1)
			first error
		)

		if err2 = asyncDelsAndUpdatesHandler(forBase, tmpCh); err2 != nil {
			return err2
		}

		for wrapped := range tmpCh {
			if first != nil {
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}

			select {
			case <-ctx.Done():
				first = ctx.Err()
				cancel()
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			default:
				if forBase {
					err2 = handleBaseDeleteAndUpdates(wrapped)
				} else {
					err2 = handleTarDeleteAndUpdates(wrapped)
				}

				if errors.Is(err2, context.Canceled) {
					err2 = nil
					cancel()
				}
			}

			if err2 != nil {
				first = err2
				cancel()
			}
		}

		wg.Wait()
		if first != nil {
			return first
		}

		if atomicErr.Load() != nil {
			return atomicErr.Load().(error)
		}

		return first
	}

	// phase 1: handle base dels and updates on lca
	if err = stepHandler(true); err != nil {
		return
	}

	// phase2: handle tar dels and updates on lca
	if err = stepHandler(false); err != nil {
		return
	}

	// what can I do with these left base updates/inserts ?
	if copt.conflictOpt == nil {
		stopped := false
		for i, w := range baseUpdateBatches {
			var stop bool
			if stop, err = emitBatch(emit, w, false, tblStuff.retPool); err != nil {
				return err
			}
			if stop {
				stopped = true
				for j := i + 1; j < len(baseUpdateBatches); j++ {
					tblStuff.retPool.releaseRetBatch(baseUpdateBatches[j].batch, false)
				}
				for _, bw := range baseDeleteBatches {
					tblStuff.retPool.releaseRetBatch(bw.batch, false)
				}
				break
			}
		}

		if !stopped {
			for i, w := range baseDeleteBatches {
				var stop bool
				if stop, err = emitBatch(emit, w, false, tblStuff.retPool); err != nil {
					return err
				}
				if stop {
					for j := i + 1; j < len(baseDeleteBatches); j++ {
						tblStuff.retPool.releaseRetBatch(baseDeleteBatches[j].batch, false)
					}
					break
				}
			}
		}
	}

	return diffDataHelper(ctx, ses, copt, tblStuff, emit, tarDataHashmap, baseDataHashmap)
}

func hashDiffIfNoLCA(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	copt compositeOption,
	emit emitFunc,
	tarDataHashmap databranchutils.BranchHashmap,
	tarTombstoneHashmap databranchutils.BranchHashmap,
	baseDataHashmap databranchutils.BranchHashmap,
	baseTombstoneHashmap databranchutils.BranchHashmap,
) (err error) {

	if err = tarTombstoneHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		return cursor.ForEach(func(key []byte, rows [][]byte) error {
			_, err2 := tarDataHashmap.PopByEncodedKey(key, true)
			return err2
		})

	}, -1); err != nil {
		return
	}

	if err = baseTombstoneHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		return cursor.ForEach(func(key []byte, rows [][]byte) error {
			_, err2 := baseDataHashmap.PopByEncodedKey(key, true)
			return err2
		})

	}, -1); err != nil {
		return
	}

	return diffDataHelper(ctx, ses, copt, tblStuff, emit, tarDataHashmap, baseDataHashmap)
}

func compareRowInWrappedBatches(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	rowIdx1 int,
	rowIdx2 int,
	skipPKCols bool,
	wrapped1 batchWithKind,
	wrapped2 batchWithKind,
) (int, error) {
	if wrapped1.kind == wrapped2.kind && wrapped1.kind == diffDelete && skipPKCols {
		return 0, nil
	}

	for i, colIdx := range tblStuff.def.visibleIdxes {
		if skipPKCols {
			if slices.Index(tblStuff.def.pkColIdxes, colIdx) != -1 {
				continue
			}
		}

		var (
			vec1 = wrapped1.batch.Vecs[i]
			vec2 = wrapped2.batch.Vecs[i]
		)

		if cmp, err := compareSingleValInVector(
			ctx, ses, rowIdx1, rowIdx2, vec1, vec2,
		); err != nil {
			return 0, err
		} else if cmp != 0 {
			return cmp, nil
		}

	}

	return 0, nil
}

func findDeleteAndUpdateBat(
	ctx context.Context, ses *Session, bh BackgroundExec,
	tblStuff tableStuff, tblName string, tmpCh chan batchWithKind, branchTS types.TS,
	dataHashmap, tombstoneHashmap databranchutils.BranchHashmap,
) (err error) {

	if err = tombstoneHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		var (
			err2      error
			tuple     types.Tuple
			dBat      *batch.Batch
			tBat1     = tblStuff.retPool.acquireRetBatch(tblStuff, true)
			tBat2     *batch.Batch
			updateBat *batch.Batch
			checkRet  []databranchutils.GetResult
		)

		send := func(bwk batchWithKind) error {
			select {
			case <-ctx.Done():
				tblStuff.retPool.releaseRetBatch(bwk.batch, false)
				return ctx.Err()
			case tmpCh <- bwk:
				return nil
			}
		}

		if err2 = cursor.ForEach(func(key []byte, rows [][]byte) error {
			for range rows {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				if tuple, _, err2 = tombstoneHashmap.DecodeRow(key); err2 != nil {
					return err2
				} else {
					if err2 = vector.AppendAny(tBat1.Vecs[0], tuple[0], false, ses.proc.Mp()); err2 != nil {
						return err2
					}

					tBat1.SetRowCount(tBat1.Vecs[0].Length())
				}
			}
			return nil
		}); err2 != nil {
			return err2
		}

		if tBat1.RowCount() == 0 {
			tblStuff.retPool.releaseRetBatch(tBat1, true)
			return nil
		}

		if dBat, err2 = handleDelsOnLCA(
			ctx, ses, bh, tBat1, tblStuff, branchTS.ToTimestamp(),
		); err2 != nil {
			return err2
		}

		// merge inserts and deletes on the tar
		// this deletes is not on the lca
		if tBat1.RowCount() > 0 {
			if _, err2 = dataHashmap.PopByVectors(
				[]*vector.Vector{tBat1.Vecs[0]}, false,
			); err2 != nil {
				return err2
			}
			tblStuff.retPool.releaseRetBatch(tBat1, true)
		}

		// find update
		if dBat.RowCount() > 0 {
			tBat2 = tblStuff.retPool.acquireRetBatch(tblStuff, false)
			if checkRet, err2 = dataHashmap.PopByVectors(
				[]*vector.Vector{dBat.Vecs[tblStuff.def.pkColIdx]}, false,
			); err2 != nil {
				return err2
			}

			for i, check := range checkRet {
				if check.Exists {
					// delete on lca and insert into tar ==> update
					if updateBat == nil {
						updateBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
					}

					if tuple, _, err2 = dataHashmap.DecodeRow(check.Rows[0]); err2 != nil {
						return err2
					}

					if err2 = appendTupleToBat(ses, updateBat, tuple, tblStuff); err2 != nil {
						return err2
					}

				} else {
					// delete on lca
					if err2 = tBat2.UnionOne(dBat, int64(i), ses.proc.Mp()); err2 != nil {
						return err2
					}
				}
			}

			tblStuff.retPool.releaseRetBatch(dBat, false)
			tBat2.SetRowCount(tBat2.Vecs[0].Length())

			if updateBat != nil {
				updateBat.SetRowCount(updateBat.Vecs[0].Length())
				if err2 = send(batchWithKind{
					name:  tblName,
					batch: updateBat,
					kind:  diffUpdate,
				}); err2 != nil {
					return err2
				}
			}

			if err2 = send(batchWithKind{
				name:  tblName,
				batch: tBat2,
				kind:  diffDelete,
			}); err2 != nil {
				return err2
			}
		}

		return nil

	}, -1); err != nil {
		return
	}

	return nil
}

func appendTupleToBat(ses *Session, bat *batch.Batch, tuple types.Tuple, tblStuff tableStuff) error {
	for j, val := range tuple {
		vec := bat.Vecs[j]

		if err := vector.AppendAny(
			vec, val, val == nil, ses.proc.Mp(),
		); err != nil {
			return err
		}
	}

	bat.SetRowCount(bat.Vecs[0].Length())

	return nil
}

func checkConflictAndAppendToBat(
	ses *Session, copt compositeOption, tblStuff tableStuff,
	tarBat, baseBat *batch.Batch, tarTuple, baseTuple types.Tuple,
) (err2 error) {
	if copt.conflictOpt != nil {
		switch copt.conflictOpt.Opt {
		case tree.CONFLICT_FAIL:
			buf := acquireBuffer(tblStuff.bufPool)
			for i, idx := range tblStuff.def.pkColIdxes {
				if err2 = formatValIntoString(ses, tarTuple[idx], tblStuff.def.colTypes[idx], buf); err2 != nil {
					releaseBuffer(tblStuff.bufPool, buf)
					return err2
				}
				if i < len(tblStuff.def.pkColIdxes)-1 {
					buf.WriteString(",")
				}
			}

			msg := buf.String()
			releaseBuffer(tblStuff.bufPool, buf)
			return moerr.NewInternalErrorNoCtxf(
				"conflict: %s %s and %s %s on pk(%v) with different values",
				tblStuff.tarRel.GetTableName(), diffInsert,
				tblStuff.baseRel.GetTableName(), diffInsert,
				msg,
			)
		case tree.CONFLICT_SKIP:
			return
		case tree.CONFLICT_ACCEPT:
			// accept the tar's insert
			return appendTupleToBat(ses, tarBat, tarTuple, tblStuff)
		}
	} else {
		if err2 = appendTupleToBat(ses, tarBat, tarTuple, tblStuff); err2 != nil {
			return err2
		}
		if err2 = appendTupleToBat(ses, baseBat, baseTuple, tblStuff); err2 != nil {
			return err2
		}
	}
	return
}

func diffDataHelper(
	ctx context.Context,
	ses *Session,
	copt compositeOption,
	tblStuff tableStuff,
	emit emitFunc,
	tarDataHashmap databranchutils.BranchHashmap,
	baseDataHashmap databranchutils.BranchHashmap,
) (err error) {

	// if no pk, we cannot use the fake pk to probe.
	// must probe with full columns

	if tblStuff.def.pkKind == fakeKind {
		var (
			keyIdxes   = tblStuff.def.visibleIdxes
			newHashmap databranchutils.BranchHashmap
		)

		if newHashmap, err = baseDataHashmap.Migrate(keyIdxes, -1); err != nil {
			return err
		}
		if err = baseDataHashmap.Close(); err != nil {
			return err
		}
		baseDataHashmap = newHashmap
	}

	if err = tarDataHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		var (
			err2      error
			tarBat    *batch.Batch
			baseBat   *batch.Batch
			tarTuple  types.Tuple
			baseTuple types.Tuple
			checkRet  databranchutils.GetResult
		)

		tarBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
		baseBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)

		if err2 = cursor.ForEach(func(key []byte, rows [][]byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			for _, row := range rows {
				if tblStuff.def.pkKind == fakeKind {
					if checkRet, err2 = baseDataHashmap.PopByEncodedFullValue(row, false); err2 != nil {
						return err2
					}
				} else {
					if checkRet, err2 = baseDataHashmap.PopByEncodedKey(key, false); err2 != nil {
						return err2
					}
				}

				if !checkRet.Exists {
					if tarTuple, _, err2 = tarDataHashmap.DecodeRow(row); err2 != nil {
						return err2
					}

					if err2 = appendTupleToBat(ses, tarBat, tarTuple, tblStuff); err2 != nil {
						return err2
					}

				} else {
					// both has the key, we continue compare the left columns,
					// if all columns are equal, exactly the same row, ignore.
					if tblStuff.def.pkKind == fakeKind {
						// all columns already compared.
						// ignore
					} else {
						if tarTuple, _, err2 = tarDataHashmap.DecodeRow(row); err2 != nil {
							return err2
						}

						if baseTuple, _, err2 = baseDataHashmap.DecodeRow(checkRet.Rows[0]); err2 != nil {
							return err2
						}

						notSame := false
						for _, idx := range tblStuff.def.visibleIdxes {
							if slices.Index(tblStuff.def.pkColIdxes, idx) != -1 {
								// pk columns already compared
								continue
							}

							if types.CompareValue(
								tarTuple[idx], baseTuple[idx],
							) != 0 {
								notSame = true
								break
							}
						}

						if notSame {
							// conflict happened
							if err2 = checkConflictAndAppendToBat(
								ses, copt, tblStuff, tarBat, baseBat, tarTuple, baseTuple,
							); err2 != nil {
								return err2
							}
						}
					}
				}
			}
			return nil
		}); err2 != nil {
			return err2
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if stop, err3 := emitBatch(emit, batchWithKind{
			batch: tarBat,
			kind:  diffInsert,
			name:  tblStuff.tarRel.GetTableName(),
		}, false, tblStuff.retPool); err3 != nil {
			return err3
		} else if stop {
			return nil
		}

		if stop, err3 := emitBatch(emit, batchWithKind{
			batch: baseBat,
			kind:  diffInsert,
			name:  tblStuff.baseRel.GetTableName(),
		}, false, tblStuff.retPool); err3 != nil {
			return err3
		} else if stop {
			return nil
		}

		return nil
	}, -1); err != nil {
		return
	}

	if copt.conflictOpt != nil {
		// merge doesnt need the base data
		return nil
	}

	if err = baseDataHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		var (
			err2  error
			tuple types.Tuple
			bat   *batch.Batch
		)

		bat = tblStuff.retPool.acquireRetBatch(tblStuff, false)

		if err2 = cursor.ForEach(func(key []byte, rows [][]byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			for _, row := range rows {
				if tuple, _, err2 = baseDataHashmap.DecodeRow(row); err2 != nil {
					return err2
				}

				if err2 = appendTupleToBat(ses, bat, tuple, tblStuff); err2 != nil {
					return err2
				}
			}
			return nil
		}); err2 != nil {
			return err2
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if stop, err3 := emitBatch(emit, batchWithKind{
			batch: bat,
			kind:  diffInsert,
			name:  tblStuff.baseRel.GetTableName(),
		}, false, tblStuff.retPool); err3 != nil {
			return err3
		} else if stop {
			return nil
		}
		return nil
	}, -1); err != nil {
		return
	}

	return nil
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

// should read the LCA table to get all column values.
func handleDelsOnLCA(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tBat *batch.Batch,
	tblStuff tableStuff,
	snapshot timestamp.Timestamp,
) (dBat *batch.Batch, err error) {

	if snapshot.PhysicalTime == 0 {
		return nil, moerr.NewInternalErrorNoCtxf("invalid branch ts: %s", snapshot.DebugString())
	}

	var (
		sql    string
		sqlRet executor.Result
		mots   = fmt.Sprintf("{MO_TS=%d} ", snapshot.PhysicalTime)

		sqlBuf  = acquireBuffer(tblStuff.bufPool)
		valsBuf = acquireBuffer(tblStuff.bufPool)

		lcaTblDef  = tblStuff.lcaRel.GetTableDef(ctx)
		baseTblDef = tblStuff.baseRel.GetTableDef(ctx)

		colTypes           = tblStuff.def.colTypes
		expandedPKColIdxes = tblStuff.def.pkColIdxes
	)

	defer func() {
		releaseBuffer(tblStuff.bufPool, sqlBuf)
		releaseBuffer(tblStuff.bufPool, valsBuf)
	}()

	pkNames := lcaTblDef.Pkey.Names

	// composite pk
	if baseTblDef.Pkey.CompPkeyCol != nil {
		var (
			tuple types.Tuple
		)

		cols, area := vector.MustVarlenaRawData(tBat.Vecs[0])
		for i := range cols {
			b := cols[i].GetByteSlice(area)
			if tuple, err = types.Unpack(b); err != nil {
				return nil, err
			}

			valsBuf.WriteString(fmt.Sprintf("row(%d,", i))

			for j := range tuple {
				if err = formatValIntoString(
					ses, tuple[j], colTypes[expandedPKColIdxes[j]], valsBuf,
				); err != nil {
					return nil, err
				}
				if j != len(tuple)-1 {
					valsBuf.WriteString(", ")
				}
			}

			valsBuf.WriteString(")")

			if i != len(cols)-1 {
				valsBuf.WriteString(", ")
			}
		}

		// fake pk
	} else if baseTblDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {

		pks := vector.MustFixedColNoTypeCheck[uint64](tBat.Vecs[0])
		for i := range pks {
			valsBuf.WriteString(fmt.Sprintf("row(%d,%d)", i, pks[i]))
			if i != len(pks)-1 {
				valsBuf.WriteString(", ")
			}
		}

		// real pk
	} else {
		valsBuf.Reset()
		for i := range tBat.Vecs[0].Length() {
			valsBuf.WriteString(fmt.Sprintf("row(%d,", i))
			b := tBat.Vecs[0].GetRawBytesAt(i)
			val := types.DecodeValue(b, tBat.Vecs[0].GetType().Oid)
			switch x := val.(type) {
			case []byte:
				valsBuf.WriteString("'")
				valsBuf.WriteString(string(x))
				valsBuf.WriteString("'")
			case string:
				valsBuf.WriteString("'")
				valsBuf.WriteString(string(x))
				valsBuf.WriteString("'")
			default:
				valsBuf.WriteString(fmt.Sprintf("%v", x))
			}
			valsBuf.WriteString(")")

			if i != tBat.Vecs[0].Length()-1 {
				valsBuf.WriteString(",")
			}
		}
	}

	sqlBuf.Reset()
	sqlBuf.WriteString(fmt.Sprintf(
		"select pks.__idx_, lca.* from %s.%s%s as lca ",
		lcaTblDef.DbName, lcaTblDef.Name, mots),
	)

	sqlBuf.WriteString(fmt.Sprintf(
		"right join (values %s) as pks(__idx_,%s) on ",
		valsBuf.String(), strings.Join(pkNames, ",")),
	)

	for i := range pkNames {
		sqlBuf.WriteString(fmt.Sprintf("lca.%s = ", pkNames[i]))
		switch typ := colTypes[expandedPKColIdxes[i]]; typ.Oid {
		case types.T_int32:
			sqlBuf.WriteString(fmt.Sprintf("cast(pks.%s as INT)", pkNames[i]))
		case types.T_int64:
			sqlBuf.WriteString(fmt.Sprintf("cast(pks.%s as BIGINT)", pkNames[i]))
		case types.T_uint32:
			sqlBuf.WriteString(fmt.Sprintf("cast(pks.%s as INT UNSIGNED)", pkNames[i]))
		case types.T_uint64:
			sqlBuf.WriteString(fmt.Sprintf("cast(pks.%s as BIGINT UNSIGNED)", pkNames[i]))
		case types.T_float32:
			sqlBuf.WriteString(fmt.Sprintf("cast(pks.%s as FLOAT)", pkNames[i]))
		case types.T_float64:
			sqlBuf.WriteString(fmt.Sprintf("cast(pks.%s as DOUBLE)", pkNames[i]))
		case types.T_varchar:
			sqlBuf.WriteString(fmt.Sprintf("cast(pks.%s as VARCHAR)", pkNames[i]))
		default:
			sqlBuf.WriteString(fmt.Sprintf("pks.%s", pkNames[i]))
		}
		if i != len(pkNames)-1 {
			sqlBuf.WriteString(" AND ")
		}
	}

	sqlBuf.WriteString(" order by pks.__idx_")

	sql = sqlBuf.String()
	if sqlRet, err = runSql(ctx, ses, bh, sql, nil, nil); err != nil {
		return
	}

	notExist := func(cols []*vector.Vector, r int) bool {
		exist := false
		for _, col := range cols {
			if !col.GetNulls().Contains(uint64(r)) {
				exist = true
				break
			}
		}
		return !exist
	}

	dBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)

	endIdx := dBat.VectorCount() - 1

	sels := make([]int64, 0, 100)
	sqlRet.ReadRows(func(rowCnt int, cols []*vector.Vector) bool {
		for i := range rowCnt {
			if notExist(cols[1:], i) {
				idx := vector.GetFixedAtNoTypeCheck[int64](cols[0], i)
				sels = append(sels, int64(idx))
				continue
			}

			for j := 1; j < len(cols); j++ {
				if err = dBat.Vecs[j-1].UnionOne(cols[j], int64(i), ses.proc.Mp()); err != nil {
					return false
				}
			}

			if tblStuff.def.pkKind != normalKind {
				if err = dBat.Vecs[endIdx].UnionOne(tBat.Vecs[0], int64(i), ses.proc.Mp()); err != nil {
					return false
				}
			}
		}

		dBat.SetRowCount(dBat.Vecs[0].Length())
		return true
	})

	sqlRet.Close()

	if len(sels) == 0 {
		tBat.Vecs[0].CleanOnlyData()
		tBat.SetRowCount(0)
	} else {
		tBat.Vecs[0].Shrink(sels, false)
		tBat.SetRowCount(tBat.Vecs[0].Length())
	}

	return
}

func formatValIntoString(ses *Session, val any, t types.Type, buf *bytes.Buffer) error {
	if val == nil {
		buf.WriteString("NULL")
		return nil
	}

	var scratch [64]byte

	writeInt := func(v int64) {
		buf.Write(strconv.AppendInt(scratch[:0], v, 10))
	}

	writeUint := func(v uint64) {
		buf.Write(strconv.AppendUint(scratch[:0], v, 10))
	}

	writeFloat := func(v float64, bitSize int) {
		buf.Write(strconv.AppendFloat(scratch[:0], v, 'g', -1, bitSize))
	}

	writeBool := func(v bool) {
		buf.WriteString(strconv.FormatBool(v))
	}

	switch t.Oid {
	case types.T_varchar, types.T_text, types.T_json, types.T_char, types.
		T_varbinary, types.T_binary:
		if t.Oid == types.T_json {
			var strVal string
			switch x := val.(type) {
			case bytejson.ByteJson:
				strVal = x.String()
			case *bytejson.ByteJson:
				if x == nil {
					return moerr.NewInternalErrorNoCtx("formatValIntoString: nil *bytejson.ByteJson")
				}
				strVal = x.String()
			case []byte:
				strVal = string(x)
			case string:
				strVal = x
			default:
				return moerr.NewInternalErrorNoCtxf("formatValIntoString: unexpected json type %T", val)
			}
			jsonLiteral := escapeJSONControlBytes([]byte(strVal))
			if !json.Valid(jsonLiteral) {
				return moerr.NewInternalErrorNoCtxf("formatValIntoString: invalid json input %q", strVal)
			}
			writeEscapedSQLString(buf, jsonLiteral)
			return nil
		}
		switch x := val.(type) {
		case []byte:
			writeEscapedSQLString(buf, x)
		case string:
			writeEscapedSQLString(buf, []byte(x))
		default:
			return moerr.NewInternalErrorNoCtxf("formatValIntoString: unexpected string type %T", val)
		}
	case types.T_timestamp:
		buf.WriteString("'")
		buf.WriteString(val.(types.Timestamp).String2(ses.timeZone, t.Scale))
		buf.WriteString("'")
	case types.T_datetime:
		buf.WriteString("'")
		buf.WriteString(val.(types.Datetime).String2(t.Scale))
		buf.WriteString("'")
	case types.T_time:
		buf.WriteString("'")
		buf.WriteString(val.(types.Time).String2(t.Scale))
		buf.WriteString("'")
	case types.T_date:
		buf.WriteString("'")
		buf.WriteString(val.(types.Date).String())
		buf.WriteString("'")
	case types.T_year:
		buf.WriteString(val.(types.MoYear).String())
	case types.T_decimal64:
		buf.WriteString(val.(types.Decimal64).Format(t.Scale))
	case types.T_decimal128:
		buf.WriteString(val.(types.Decimal128).Format(t.Scale))
	case types.T_decimal256:
		buf.WriteString(val.(types.Decimal256).Format(t.Scale))
	case types.T_bool:
		writeBool(val.(bool))
	case types.T_uint8:
		writeUint(uint64(val.(uint8)))
	case types.T_uint16:
		writeUint(uint64(val.(uint16)))
	case types.T_uint32:
		writeUint(uint64(val.(uint32)))
	case types.T_uint64:
		writeUint(val.(uint64))
	case types.T_int8:
		writeInt(int64(val.(int8)))
	case types.T_int16:
		writeInt(int64(val.(int16)))
	case types.T_int32:
		writeInt(int64(val.(int32)))
	case types.T_int64:
		writeInt(val.(int64))
	case types.T_float32:
		writeFloat(float64(val.(float32)), 32)
	case types.T_float64:
		writeFloat(val.(float64), 64)
	case types.T_array_float32:
		buf.WriteString("'")
		buf.WriteString(types.ArrayToString[float32](val.([]float32)))
		buf.WriteString("'")
	case types.T_array_float64:
		buf.WriteString("'")
		buf.WriteString(types.ArrayToString[float64](val.([]float64)))
		buf.WriteString("'")
	default:
		return moerr.NewNotSupportedNoCtxf("formatValIntoString: not support type %v", t.Oid)
	}

	return nil
}

// writeEscapedSQLString escapes special and control characters for SQL literal output.
func writeEscapedSQLString(buf *bytes.Buffer, b []byte) {
	buf.WriteByte('\'')
	for _, c := range b {
		switch c {
		case '\'':
			buf.WriteString("\\'")
		//case '"':
		//	buf.WriteString("\\\"")
		case '\\':
			buf.WriteString("\\\\")
		case 0:
			buf.WriteString("\\0")
		case '\b':
			buf.WriteString("\\b")
		case '\n':
			buf.WriteString("\\n")
		case '\r':
			buf.WriteString("\\r")
		case '\t':
			buf.WriteString("\\t")
		case 0x1A:
			buf.WriteString("\\Z")
		default:
			if c < 0x20 || c == 0x7f {
				buf.WriteString("\\x")
				buf.WriteString(hex.EncodeToString([]byte{c}))
			} else {
				buf.WriteByte(c)
			}
		}
	}
	buf.WriteByte('\'')
}

// escapeJSONControlChars converts control characters to JSON-compliant \u00XX escapes.
func escapeJSONControlBytes(b []byte) []byte {
	var out []byte
	hexDigits := "0123456789abcdef"

	for i, c := range b {
		if c < 0x20 || c == 0x7f {
			if out == nil {
				out = make([]byte, 0, len(b)+8)
				out = append(out, b[:i]...)
			}
			out = append(out, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
			continue
		}
		if out != nil {
			out = append(out, c)
		}
	}

	if out == nil {
		return b
	}
	return out
}

func buildHashmapForTable(
	ctx context.Context,
	mp *mpool.MPool,
	lcaType int,
	tblStuff tableStuff,
	handles []engine.ChangesHandle,
) (
	dataHashmap databranchutils.BranchHashmap,
	tombstoneHashmap databranchutils.BranchHashmap,
	err error,
) {

	var (
		atomicErr    atomic.Value
		dataBat      *batch.Batch
		tombstoneBat *batch.Batch
		wg           sync.WaitGroup
	)

	defer func() {
		wg.Wait()

		if dataBat != nil {
			dataBat.Clean(mp)
		}

		if tombstoneBat != nil {
			tombstoneBat.Clean(mp)
		}
	}()

	if dataHashmap, err = databranchutils.NewBranchHashmap(); err != nil {
		return
	}

	if tombstoneHashmap, err = databranchutils.NewBranchHashmap(); err != nil {
		return
	}

	putVectors := func(bat *batch.Batch, isTombstone bool) error {
		if bat == nil {
			return nil
		}

		wg.Add(1)

		if err = tblStuff.worker.Submit(func() {
			defer wg.Done()

			ll := bat.VectorCount()
			var taskErr error
			if isTombstone {
				taskErr = tombstoneHashmap.PutByVectors(bat.Vecs[:ll-1], []int{0})
			} else {
				taskErr = dataHashmap.PutByVectors(bat.Vecs[:ll-1], []int{tblStuff.def.pkColIdx})
			}

			bat.Clean(mp)
			if taskErr != nil {
				atomicErr.Store(taskErr)
			}
		}); err != nil {
			wg.Done()
		}

		return err
	}

	for _, handle := range handles {
		for {
			if dataBat, tombstoneBat, _, err = handle.Next(
				ctx, mp,
			); err != nil {
				return
			} else if dataBat == nil && tombstoneBat == nil {
				// out of data
				break
			}

			if atomicErr.Load() != nil {
				err = atomicErr.Load().(error)
				return
			}

			if err = putVectors(dataBat, false); err != nil {
				return
			}

			if err = putVectors(tombstoneBat, true); err != nil {
				return
			}
		}
	}

	wg.Wait()

	if atomicErr.Load() != nil {
		err = atomicErr.Load().(error)
	}

	return
}

func getRelationById(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tableId uint64,
	snapshot *plan.Snapshot,
) (rel engine.Relation, err error) {

	txnOp := bh.(*backExec).backSes.GetTxnHandler().txnOp

	if snapshot != nil && snapshot.TS != nil {
		txnOp = txnOp.CloneSnapshotOp(*snapshot.TS)
	}

	_, _, rel, err = ses.GetTxnHandler().GetStorage().GetRelationById(ctx, txnOp, tableId)
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

	eng := ses.proc.GetSessionInfo().StorageEngine
	if tarDB, err = eng.Database(ctx, tarDBName, txnOpA); err != nil {
		return
	}

	if tarRel, err = tarDB.Relation(ctx, tarTblName, nil); err != nil {
		return
	}

	if baseDB, err = eng.Database(ctx, baseDBName, txnOpB); err != nil {
		return
	}

	if baseRel, err = baseDB.Relation(ctx, baseTblName, nil); err != nil {
		return
	}

	return
}

func constructChangeHandle(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tables tableStuff,
	branchInfo *branchMetaInfo,
) (
	tarHandle []engine.ChangesHandle,
	baseHandle []engine.ChangesHandle,
	err error,
) {
	var (
		handle    engine.ChangesHandle
		tarRange  collectRange
		baseRange collectRange
	)

	if tarRange, baseRange, err = decideCollectRange(
		ctx, ses, bh, tables, branchInfo,
	); err != nil {
		return
	}

	for i := range tarRange.rel {
		if handle, err = databranchutils.CollectChanges(
			ctx,
			tarRange.rel[i],
			tarRange.from[i],
			tarRange.end[i],
			ses.proc.Mp(),
		); err != nil {
			return
		}

		tarHandle = append(tarHandle, handle)
	}

	for i := range baseRange.rel {
		if handle, err = databranchutils.CollectChanges(
			ctx,
			baseRange.rel[i],
			baseRange.from[i],
			baseRange.end[i],
			ses.proc.Mp(),
		); err != nil {
			return
		}

		//if branchInfo.lcaType != lcaEmpty {
		//	// collect nothing from base
		//	if handle != nil {
		//		err = moerr.NewInternalErrorNoCtx("the LCA collect range is not empty")
		//		return
		//	}
		//	continue
		//}

		baseHandle = append(baseHandle, handle)
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

		mp    = ses.proc.Mp()
		eng   = ses.proc.GetSessionInfo().StorageEngine
		txnOp = ses.GetTxnHandler().GetTxn()

		txnSnapshot = types.TimestampToTS(txnOp.SnapshotTS())
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
		ctx, ses, eng, mp,
		tables.tarRel, tables.baseRel,
		[]types.TS{tarSp, baseSp},
		txnOp,
	); err != nil {
		return
	}

	tarCTS = tblCommitTS[0]
	baseCTS = tblCommitTS[1]

	// Note That:
	// 1. the branchTS+1 cannot skip the cloned data, we need get the clone commitTS (the table creation commitTS)
	//

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
			} else {
				baseCollectRange.rel = append(baseCollectRange.rel, lcaRel)
				baseCollectRange.from = append(baseCollectRange.from, dagInfo.tarBranchTS.Next())
				baseCollectRange.end = append(baseCollectRange.end, dagInfo.baseBranchTS)
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

func getTablesCreationCommitTS(
	ctx context.Context,
	ses *Session,
	eng engine.Engine,
	mp *mpool.MPool,
	tar engine.Relation,
	base engine.Relation,
	snapshot []types.TS,
	txnOp client.TxnOperator,
) (commitTS []types.TS, err error) {

	var (
		sqlRet        executor.Result
		data          *batch.Batch
		tombstone     *batch.Batch
		moTableRel    engine.Relation
		moTableHandle engine.ChangesHandle

		from types.TS
		end  types.TS
	)

	defer func() {
		if data != nil {
			data.Clean(mp)
		}
		if moTableHandle != nil {
			moTableHandle.Close()
		}
	}()

	if _, _, moTableRel, err = eng.GetRelationById(
		ctx, txnOp, catalog.MO_TABLES_ID,
	); err != nil {
		return
	}

	buf := &bytes.Buffer{}
	defer func() {
		sqlRet.Close()
	}()

	// pk ==> account_id and reldatabase and relname
	buf.WriteString("select min(created_time) from mo_catalog.mo_tables where ")
	buf.WriteString(fmt.Sprintf(
		"(account_id = %d and reldatabase = '%s' and relname = '%s')",
		ses.accountId, tar.GetTableDef(ctx).DbName, tar.GetTableName(),
	))
	buf.WriteString(" OR ")
	buf.WriteString(fmt.Sprintf(
		"(account_id = %d and reldatabase = '%s' and relname = '%s')",
		ses.accountId, base.GetTableDef(ctx).DbName, base.GetTableName(),
	))

	if sqlRet, err = sqlexec.RunSql(sqlexec.NewSqlProcess(ses.proc), buf.String()); err != nil {
		return
	}

	if len(sqlRet.Batches) != 1 && sqlRet.Batches[0].RowCount() != 1 {
		return nil, moerr.NewInternalErrorNoCtxf(
			"get table created time for (%s, %s) failed(%s)",
			tar.GetTableName(), base.GetTableName(), buf.String(),
		)
	}

	ts := vector.GetFixedAtWithTypeCheck[types.Timestamp](sqlRet.Batches[0].Vecs[0], 0)
	from = types.BuildTS(
		ts.ToDatetime(ses.timeZone).ConvertToGoTime(ses.timeZone).UnixNano(),
		0,
	)

	end = slices.MaxFunc(snapshot, func(a, b types.TS) int { return a.Compare(&b) })

	if moTableHandle, err = moTableRel.CollectChanges(
		ctx, from, end, true, mp,
	); err != nil {
		return
	}

	commitTS = make([]types.TS, 2)

	for commitTS[0].IsEmpty() || commitTS[1].IsEmpty() {
		if data, tombstone, _, err = moTableHandle.Next(ctx, mp); err != nil {
			return
		} else if data == nil && tombstone == nil {
			break
		}

		if tombstone != nil {
			tombstone.Clean(mp)
		}

		if data != nil {
			relIdCol := vector.MustFixedColNoTypeCheck[uint64](data.Vecs[0])
			commitTSCol := vector.MustFixedColNoTypeCheck[types.TS](data.Vecs[len(data.Vecs)-1])

			if idx := slices.Index(relIdCol, tar.GetTableID(ctx)); idx != -1 {
				commitTS[0] = commitTSCol[idx]
			}

			if idx := slices.Index(relIdCol, base.GetTableID(ctx)); idx != -1 {
				commitTS[1] = commitTSCol[idx]
			}

			data.Clean(mp)
		}
	}

	if commitTS[0].IsEmpty() || commitTS[1].IsEmpty() {
		err = moerr.NewInternalErrorNoCtxf(
			"get table created time for (%s, %s) failed",
			tar.GetTableName(), base.GetTableName(),
		)
	}

	return commitTS, err
}

func decideLCABranchTSFromBranchDAG(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tarRel engine.Relation,
	baseRel engine.Relation,
) (
	branchInfo branchMetaInfo,
	err error,
) {

	var (
		dag *databranchutils.DataBranchDAG

		tarTS   int64
		baseTS  int64
		hasLca  bool
		lcaType int

		lcaTableID   uint64
		tarBranchTS  timestamp.Timestamp
		baseBranchTS timestamp.Timestamp
	)

	defer func() {
		branchInfo = branchMetaInfo{
			lcaType:      lcaType,
			lcaTableId:   lcaTableID,
			tarBranchTS:  types.TimestampToTS(tarBranchTS),
			baseBranchTS: types.TimestampToTS(baseBranchTS),
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
	// if a table is cloned table, the commit ts of the cloned data
	// should be the creation time of the table.
	if lcaTableID, tarTS, baseTS, hasLca = dag.FindLCA(
		tarRel.GetTableID(ctx), baseRel.GetTableID(ctx),
	); hasLca {
		if lcaTableID == baseRel.GetTableID(ctx) {
			ts := timestamp.Timestamp{PhysicalTime: tarTS}
			tarBranchTS = ts
			baseBranchTS = ts
			lcaType = lcaRight
		} else if lcaTableID == tarRel.GetTableID(ctx) {
			ts := timestamp.Timestamp{PhysicalTime: baseTS}
			tarBranchTS = ts
			baseBranchTS = ts
			lcaType = lcaLeft
		} else {
			lcaType = lcaOther
			tarBranchTS = timestamp.Timestamp{PhysicalTime: tarTS}
			baseBranchTS = timestamp.Timestamp{PhysicalTime: baseTS}
		}
	} else {
		lcaType = lcaEmpty
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
		fmt.Sprintf(scanBranchMetadataSql, catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA),
		nil, nil,
	); err != nil {
		return
	}

	rowData = make([]databranchutils.DataBranchMetadata, 0, sqlRet.AffectedRows)
	sqlRet.ReadRows(func(rows int, cols []*vector.Vector) bool {
		tblIds := vector.MustFixedColNoTypeCheck[uint64](cols[0])
		cloneTS := vector.MustFixedColNoTypeCheck[int64](cols[1])
		pTblIds := vector.MustFixedColNoTypeCheck[uint64](cols[2])
		for i := range tblIds {
			rowData = append(rowData, databranchutils.DataBranchMetadata{
				TableID:  tblIds[i],
				CloneTS:  cloneTS[i],
				PTableID: pTblIds[i],
			})
		}
		return true
	})

	return databranchutils.NewDAG(rowData), nil
}

func compareSingleValInVector(
	ctx context.Context,
	ses *Session,
	rowIdx1 int,
	rowIdx2 int,
	vec1 *vector.Vector,
	vec2 *vector.Vector,
) (int, error) {

	if !vec1.GetType().Eq(*vec2.GetType()) {
		return 0, moerr.NewInternalErrorNoCtxf(
			"type not matched: %v <-> %v",
			vec1.GetType().String(), vec2.GetType().String(),
		)
	}

	if vec1.IsConst() {
		rowIdx1 = 0
	}
	if vec2.IsConst() {
		rowIdx2 = 0
	}

	// Use raw values to avoid format conversions in extractRowFromVector.
	switch vec1.GetType().Oid {
	case types.T_json:
		return bytejson.CompareByteJson(
			types.DecodeJson(vec1.GetBytesAt(rowIdx1)),
			types.DecodeJson(vec2.GetBytesAt(rowIdx2)),
		), nil
	case types.T_bool:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[bool](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[bool](vec2, rowIdx2),
		), nil
	case types.T_bit:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[uint64](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[uint64](vec2, rowIdx2),
		), nil
	case types.T_int8:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[int8](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[int8](vec2, rowIdx2),
		), nil
	case types.T_uint8:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[uint8](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[uint8](vec2, rowIdx2),
		), nil
	case types.T_int16:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[int16](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[int16](vec2, rowIdx2),
		), nil
	case types.T_uint16:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[uint16](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[uint16](vec2, rowIdx2),
		), nil
	case types.T_int32:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[int32](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[int32](vec2, rowIdx2),
		), nil
	case types.T_uint32:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[uint32](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[uint32](vec2, rowIdx2),
		), nil
	case types.T_int64:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[int64](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[int64](vec2, rowIdx2),
		), nil
	case types.T_uint64:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[uint64](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[uint64](vec2, rowIdx2),
		), nil
	case types.T_float32:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[float32](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[float32](vec2, rowIdx2),
		), nil
	case types.T_float64:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[float64](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[float64](vec2, rowIdx2),
		), nil
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink:
		return bytes.Compare(
			vec1.GetBytesAt(rowIdx1),
			vec2.GetBytesAt(rowIdx2),
		), nil
	case types.T_array_float32:
		return types.CompareValue(
			vector.GetArrayAt[float32](vec1, rowIdx1),
			vector.GetArrayAt[float32](vec2, rowIdx2),
		), nil
	case types.T_array_float64:
		return types.CompareValue(
			vector.GetArrayAt[float64](vec1, rowIdx1),
			vector.GetArrayAt[float64](vec2, rowIdx2),
		), nil
	case types.T_date:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.Date](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.Date](vec2, rowIdx2),
		), nil
	case types.T_datetime:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.Datetime](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.Datetime](vec2, rowIdx2),
		), nil
	case types.T_time:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.Time](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.Time](vec2, rowIdx2),
		), nil
	case types.T_timestamp:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.Timestamp](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.Timestamp](vec2, rowIdx2),
		), nil
	case types.T_year:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.MoYear](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.MoYear](vec2, rowIdx2),
		), nil
	case types.T_decimal64:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.Decimal64](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.Decimal64](vec2, rowIdx2),
		), nil
	case types.T_decimal128:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.Decimal128](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.Decimal128](vec2, rowIdx2),
		), nil
	case types.T_uuid:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.Uuid](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.Uuid](vec2, rowIdx2),
		), nil
	case types.T_Rowid:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.Rowid](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.Rowid](vec2, rowIdx2),
		), nil
	case types.T_Blockid:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.Blockid](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.Blockid](vec2, rowIdx2),
		), nil
	case types.T_TS:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.TS](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.TS](vec2, rowIdx2),
		), nil
	case types.T_enum:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.Enum](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.Enum](vec2, rowIdx2),
		), nil
	default:
		return 0, moerr.NewInternalErrorNoCtxf("compareSingleValInVector : unsupported type %d", vec1.GetType().Oid)
	}
}
