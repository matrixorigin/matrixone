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
	"database/sql"
	"fmt"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	process2 "github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/shirou/gopsutil/v3/process"

	"github.com/panjf2000/ants/v2"
)

const (
	fakeKind = iota
	normalKind
	compositeKind
)

const (
	//diffAddedLine   = "I"
	//diffRemovedLine = "D"

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
	batchCnt = objectio.BlockMaxRows * 10
)

const (
	defaultSQLPoolSize = 8
)

type asyncSQLExecutor struct {
	pool   *ants.Pool
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	errMu sync.Mutex
	err   error
}

func newAsyncTaskExecutor(size int, parents ...context.Context) (*asyncSQLExecutor, error) {
	if size <= 0 {
		size = defaultSQLPoolSize
	}

	parent := context.Background()
	if len(parents) > 0 && parents[0] != nil {
		parent = parents[0]
	}

	ctx, cancel := context.WithCancel(parent)
	pool, err := ants.NewPool(size, ants.WithNonblocking(false))
	if err != nil {
		cancel()
		return nil, err
	}
	return &asyncSQLExecutor{
		pool:   pool,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

func (e *asyncSQLExecutor) Submit(task func(context.Context) error) error {
	if err := e.Err(); err != nil {
		return err
	}

	if e.ctx.Err() != nil {
		return e.ctx.Err()
	}

	e.wg.Add(1)
	if err := e.pool.Submit(func() {
		defer e.wg.Done()
		if e.ctx.Err() != nil {
			return
		}
		if err := task(e.ctx); err != nil {
			e.setError(err)
		}
	}); err != nil {
		e.wg.Done()
		e.setError(err)
		return err
	}

	return nil
}

func (e *asyncSQLExecutor) Wait() error {
	e.wg.Wait()
	return e.Err()
}

func (e *asyncSQLExecutor) Close() {
	e.cancel()
	if e.pool != nil {
		e.pool.Release()
	}
}

func (e *asyncSQLExecutor) Err() error {
	e.errMu.Lock()
	defer e.errMu.Unlock()
	return e.err
}

func (e *asyncSQLExecutor) setError(err error) {
	if err == nil {
		return
	}

	e.errMu.Lock()
	if e.err == nil {
		e.err = err
		e.cancel()
	}
	e.errMu.Unlock()
}

const (
	mergeDiffOutputNone = iota
	mergeDiffOutputSQLs
	mergeDiffOutputRows
	mergeDiffOutputBatch
)

var bufferPool = sync.Pool{
	New: func() any {
		return &bytes.Buffer{}
	},
}

func acquireBuffer() *bytes.Buffer {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func releaseBuffer(buf *bytes.Buffer) {
	buf.Reset()
	bufferPool.Put(buf)
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

	colNames           []string
	pkKind             int
	pkColIdx           int
	expandedPKColIdxes []int
	neededColIdxes     []int
	neededColTypes     []types.Type
}

type batchWithKind struct {
	name  string
	kind  string
	batch *batch.Batch
}

type retBatchList struct {
	mu sync.Mutex
	// 0: data
	// 1: tombstone
	dList []*batch.Batch
	tList []*batch.Batch

	pinned map[*batch.Batch]struct{}
}

var retBatchPool = retBatchList{}

func acquireRetBatch(tblStuff tableStuff, forTombstone bool) *batch.Batch {
	retBatchPool.mu.Lock()
	defer retBatchPool.mu.Unlock()

	var (
		bat *batch.Batch
	)

	if retBatchPool.pinned == nil {
		retBatchPool.pinned = make(map[*batch.Batch]struct{})
	}

	defer func() {
		retBatchPool.pinned[bat] = struct{}{}
	}()

	if forTombstone {
		if len(retBatchPool.tList) == 0 {
			bat = batch.NewWithSize(1)
			def := tblStuff.tarRel.GetTableDef(context.Background())
			bat.Vecs[0] = vector.NewVec(plan2.ExprType2Type(&def.Cols[tblStuff.pkColIdx].Typ))
			return bat
		}

		bat = retBatchPool.tList[0]
		retBatchPool.tList = retBatchPool.tList[1:]
		bat.CleanOnlyData()
		return bat

	} else {
		if len(retBatchPool.dList) == 0 {
			bat = batch.NewWithSize(len(tblStuff.colNames))
			for i := range tblStuff.colNames {
				bat.Vecs[i] = vector.NewVec(tblStuff.neededColTypes[i])
			}
			return bat
		}

		bat = retBatchPool.dList[0]
		retBatchPool.dList = retBatchPool.dList[1:]
		bat.CleanOnlyData()
		return bat
	}
}

func releaseRetBatch(bat *batch.Batch, forTombstone bool) {
	retBatchPool.mu.Lock()
	defer retBatchPool.mu.Unlock()

	list := retBatchPool.dList
	if forTombstone {
		list = retBatchPool.tList
	}

	bat.CleanOnlyData()
	list = append(list, bat)

	delete(retBatchPool.pinned, bat)
}

func freeAllRetBatches(mp *mpool.MPool) {
	for _, bat := range retBatchPool.dList {
		bat.Clean(mp)
	}
	for _, bat := range retBatchPool.tList {
		bat.Clean(mp)
	}
	for bat, _ := range retBatchPool.pinned {
		bat.Clean(mp)
	}
}

type compositeOption struct {
	conflictOpt *tree.ConflictOpt
	outputOpt   *tree.DiffOutputOpt
}

func validate(stmt tree.Statement) error {
	switch s := stmt.(type) {
	case *tree.DataBranchDiff:
		if s.OutputOpt != nil && len(s.OutputOpt.DirPath) != 0 {
			info, err := os.Stat(s.OutputOpt.DirPath)
			if err != nil {
				if os.IsNotExist(err) {
					return moerr.NewInternalErrorNoCtxf("diff output directory does not exist: %s", s.OutputOpt.DirPath)
				}
				return moerr.NewInternalErrorNoCtxf("diff output directory check failed: %v", err)
			}
			if !info.IsDir() {
				return moerr.NewInternalErrorNoCtxf("diff output file only accept a directory path")
			}
			return nil
		}
	}
	return nil
}

func runSql(
	ctx context.Context, ses *Session, bh BackgroundExec, sql string,
) (sqlRet executor.Result, err error) {

	var (
		val   any
		exist bool
		exec  executor.SQLExecutor
	)

	// we do not use the bh.Exec here, it's too slow.
	// use internal sql instead.
	if val, exist = moruntime.ServiceRuntime(ses.service).GetGlobalVariables(moruntime.InternalSQLExecutor); !exist {
		return sqlRet, moerr.NewInternalErrorNoCtxf("get internalExecutor failed")
	}

	exec = val.(executor.SQLExecutor)

	opts := executor.Options{}.
		WithDisableIncrStatement().
		WithTxn(bh.(*backExec).backSes.GetTxnHandler().GetTxn()).
		WithKeepTxnAlive().
		WithTimeZone(ses.GetTimeZone())

	return exec.Exec(ctx, sql, opts)
}

func handleDataBranch(
	execCtx *ExecCtx,
	ses *Session,
	stmt tree.Statement,
) error {

	switch st := stmt.(type) {
	case *tree.DataBranchCreateTable:
		//return dataBranchCreateTable(execCtx, ses, st)
	case *tree.DataBranchCreateDatabase:
	case *tree.DataBranchDeleteTable:
	case *tree.DataBranchDeleteDatabase:
	case *tree.DataBranchDiff:
		return handleBranchDiff(execCtx, ses, st)
	case *tree.DataBranchMerge:
		return handleBranchMerge(execCtx, ses, st)
	default:
		return moerr.NewNotSupportedNoCtxf("data branch not supported: %v", st)
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

	if err = validate(stmt); err != nil {
		return err
	}

	// do not open another transaction,
	// if the clone already executed within a transaction.
	if bh, deferred, err = getBackExecutor(execCtx.reqCtx, ses); err != nil {
		return
	}

	defer func() {
		if deferred != nil {
			err = deferred(err)
		}
	}()

	var (
		dagInfo     branchMetaInfo
		tblStuff    tableStuff
		copt        compositeOption
		ctx, cancel = context.WithCancel(execCtx.reqCtx)

		ok        bool
		diffStmt  *tree.DataBranchDiff
		mergeStmt *tree.DataBranchMerge
	)

	defer func() {
		cancel()
	}()

	if diffStmt, ok = stmt.(*tree.DataBranchDiff); !ok {
		mergeStmt, ok = stmt.(*tree.DataBranchMerge)
	}

	if diffStmt != nil {
		copt.outputOpt = diffStmt.OutputOpt
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
		ctx, ses, tblStuff.tarRel, tblStuff.baseRel,
	); err != nil {
		return
	}

	var (
		wg        = new(sync.WaitGroup)
		outputErr atomic.Value
		retBatCh  = make(chan batchWithKind, 10)
	)

	defer func() {
		freeAllRetBatches(ses.proc.Mp())
		close(retBatCh)
	}()

	if diffStmt != nil {
		if err = buildOutputSchema(ctx, ses, diffStmt, tblStuff); err != nil {
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

			if err2 := satisfyDiffOutputOpt2(
				ctx, cancel, ses, bh, diffStmt, dagInfo, tblStuff, retBatCh,
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
		execCtx.reqCtx, ses, bh, wg, dagInfo, tblStuff, retBatCh, copt,
	); err != nil {
		return
	}

	close(retBatCh)

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
	return diffMergeAgency(ses, execCtx, stmt)
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
		mrs         = ses.GetMysqlResultSet()
		conflictOpt = tree.CONFLICT_FAIL

		replaceCnt int
		deleteCnt  int

		replaceIntoVals = acquireBuffer()
		deleteFromVals  = acquireBuffer()
	)

	defer func() {
		cancel()
	}()

	if stmt.ConflictOpt != nil {
		conflictOpt = stmt.ConflictOpt
	}

	// conflict option should be pushed down to the hash phase,
	// so the batch we received is conflict-free.
	for wrapped := range retCh {

		if err = constructValsFromBatch(
			ctx, ses, tblStuff, wrapped, deleteFromVals, replaceIntoVals,
		); err != nil {
			return
		}

		if wrapped.kind == diffDelete {
			deleteCnt += wrapped.batch.RowCount()
		} else {
			replaceCnt += wrapped.batch.RowCount()
		}

		if deleteCnt >= batchCnt {
			deleteCnt = 0
			if err = flushSqlValues(
				ctx, ses, bh, tblStuff, deleteFromVals, true, nil,
			); err != nil {
				return
			}
			deleteFromVals.Reset()
		}

		if replaceCnt >= batchCnt {
			replaceCnt = 0
			if err = flushSqlValues(
				ctx, ses, bh, tblStuff, replaceIntoVals, false, nil,
			); err != nil {
				return
			}
			replaceIntoVals.Reset()
		}
		releaseRetBatch(wrapped.batch, false)
	}

	return
}

func satisfyDiffOutputOpt2(
	ctx context.Context,
	cancel context.CancelFunc,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.DataBranchDiff,
	dagInfo branchMetaInfo,
	tblStuff tableStuff,
	retCh chan batchWithKind,
) (err error) {

	var (
		mrs = ses.GetMysqlResultSet()
	)

	defer func() {
		cancel()
	}()

	if stmt.OutputOpt == nil || stmt.OutputOpt.Limit != nil {

		for wrapped := range retCh {
			for rowIdx := range wrapped.batch.RowCount() {
				var (
					row = []any{wrapped.name, wrapped.kind}
				)

				for colIdx, vec := range wrapped.batch.Vecs {
					if err = extractRowFromVector(
						ctx, ses, vec, colIdx+2, row, rowIdx, true,
					); err != nil {
						return
					}
				}

				mrs.AddRow(row)
				if stmt.OutputOpt.Limit != nil &&
					int64(mrs.GetRowCount()) >= *stmt.OutputOpt.Limit {
					break
				}
			}
			releaseRetBatch(wrapped.batch, false)
		}
	} else if stmt.OutputOpt.Count {
		cnt := int64(0)
		for wrapped := range retCh {
			cnt += int64(wrapped.batch.RowCount())
			releaseRetBatch(wrapped.batch, false)
		}
		mrs.AddRow([]any{cnt})

	} else if len(stmt.OutputOpt.DirPath) != 0 {
		var (
			replaceCnt int
			deleteCnt  int

			deleteFromValsBuffer  = acquireBuffer()
			replaceIntoValsBuffer = acquireBuffer()

			fileHint     string
			fullFilePath string
			writeFile    func([]byte) error
			release      func()
		)

		defer func() {
			if release != nil {
				release()
			}
			releaseBuffer(deleteFromValsBuffer)
			releaseBuffer(replaceIntoValsBuffer)
		}()

		if fullFilePath, fileHint, writeFile, release, err = prepareFSForDiffAsFile(
			ctx, ses, stmt, tblStuff, true,
		); err != nil {
			return
		}

		for wrapped := range retCh {
			if wrapped.name == tblStuff.tarRel.GetTableName() {
				if err = constructValsFromBatch(
					ctx, ses, tblStuff, wrapped, deleteFromValsBuffer, replaceIntoValsBuffer,
				); err != nil {
					return err
				}
				if wrapped.kind == diffDelete {
					deleteCnt += int(wrapped.batch.RowCount())
				} else {
					replaceCnt += int(wrapped.batch.RowCount())
				}
			}
			releaseRetBatch(wrapped.batch, false)

			if deleteCnt >= batchCnt {
				if err = flushSqlValues(
					ctx, ses, bh, tblStuff, deleteFromValsBuffer, true, writeFile,
				); err != nil {
					return
				}

				deleteCnt = 0
				deleteFromValsBuffer.Reset()
			}

			if replaceCnt >= batchCnt {
				if err = flushSqlValues(
					ctx, ses, bh, tblStuff, replaceIntoValsBuffer, false, writeFile,
				); err != nil {
					return
				}

				replaceCnt = 0
				replaceIntoValsBuffer.Reset()
			}
		}

		mrs.AddRow([]any{fullFilePath, fileHint})
	}

	return trySaveQueryResult(ctx, ses, mrs)
}

func prepareFSForDiffAsFile(
	ctx context.Context,
	ses *Session,
	stmt *tree.DataBranchDiff,
	tblStuff tableStuff,
	asSQLFile bool,
) (fullFilePath, hint string, writeFile func([]byte) error, release func(), err error) {
	var (
		fileName string
		srcName  = tblStuff.tarRel.GetTableName()
		baseName = tblStuff.baseRel.GetTableName()
	)

	if stmt.BaseTable.AtTsExpr != nil {
		baseName = fmt.Sprintf("%s_%s", baseName, stmt.BaseTable.AtTsExpr.SnapshotName)
	}

	if stmt.TargetTable.AtTsExpr != nil {
		srcName = fmt.Sprintf("%s_%s", srcName, stmt.TargetTable.AtTsExpr.SnapshotName)
	}

	fileName = fmt.Sprintf(
		"diff_%s_%s_%s",
		srcName, baseName,
		time.Now().UTC().Format("20060102_150405"),
	)

	if asSQLFile {
		fileName += ".sql"
	} else {
		fileName += ".csv"
	}

	if asSQLFile {
		hint = fmt.Sprintf(
			"DELETE FROM %s.%s, REPLACE INTO %s.%s",
			tblStuff.baseRel.GetTableDef(ctx).DbName, tblStuff.baseRel.GetTableName(),
			tblStuff.baseRel.GetTableDef(ctx).DbName, tblStuff.baseRel.GetTableName(),
		)
	}

	fullFilePath = path.Join(stmt.OutputOpt.DirPath, fileName)

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
		var etlFS fileservice.ETLFileService
		if etlFS, targetPath, err = fileservice.GetForETL(ctx, nil, fullFilePath); err != nil {
			return
		}
		targetFS = etlFS
	}

	if err = targetFS.Delete(ctx, targetPath); err != nil && !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
		return
	}

	writeFile = func(fileContent []byte) error {
		vec := fileservice.IOVector{
			FilePath: targetPath,
			Entries: []fileservice.IOEntry{
				{
					Size: int64(len(fileContent)),
					Data: fileContent,
				},
			},
		}

		return targetFS.Write(ctx, vec)
	}

	release = func() {
		targetFS.Close(ctx)
	}

	return
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

	sqlBuffer := acquireBuffer()
	defer releaseBuffer(sqlBuffer)

	initReplaceIntoBuf := func() {
		sqlBuffer.WriteString(fmt.Sprintf(
			"replace into %s.%s values ",
			tblStuff.baseRel.GetTableDef(ctx).DbName,
			tblStuff.baseRel.GetTableDef(ctx).Name,
		))
	}

	initDeleteFromBuf := func() {
		if len(tblStuff.expandedPKColIdxes) == 1 {
			sqlBuffer.WriteString(fmt.Sprintf(
				"delete from %s.%s where %s in (",
				tblStuff.baseRel.GetTableDef(ctx).DbName,
				tblStuff.baseRel.GetTableDef(ctx).Name,
				tblStuff.baseRel.GetTableDef(ctx).Cols[tblStuff.expandedPKColIdxes[0]].Name,
			))
		} else {
			pkNames := make([]string, len(tblStuff.expandedPKColIdxes))
			for i, pkColIdx := range tblStuff.expandedPKColIdxes {
				pkNames[i] = tblStuff.baseRel.GetTableDef(ctx).Cols[pkColIdx].Name
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

	if writeFile != nil {
		err = writeFile(sqlBuffer.Bytes())
	} else {
		var (
			ret executor.Result
		)

		defer func() {
			ret.Close()
		}()

		ret, err = runSql(ctx, ses, bh, sqlBuffer.String())
	}

	return err
}

func constructValsFromBatch(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	wrapped batchWithKind,
	deleteFromValsBuffer *bytes.Buffer,
	replaceIntoValsBuffer *bytes.Buffer,
) (err error) {

	writeReplaceInto := func(row []any) {
		if replaceIntoValsBuffer.Len() > 0 {
			replaceIntoValsBuffer.WriteString(",")
		}

		replaceIntoValsBuffer.WriteString("(")
		for j := 2; j < len(row); j++ {
			formatValIntoString(ses, row[j], tblStuff.neededColTypes[j-2], replaceIntoValsBuffer)
			if j != len(row)-1 {
				replaceIntoValsBuffer.WriteString(",")
			}
		}

		replaceIntoValsBuffer.WriteString(")")
	}

	writeDeleteFrom := func(row []any) {
		if deleteFromValsBuffer.Len() > 0 {
			deleteFromValsBuffer.WriteString(",")
		}

		if len(tblStuff.expandedPKColIdxes) > 1 {
			deleteFromValsBuffer.WriteString("(")
		}
		for idx, colIdx := range tblStuff.expandedPKColIdxes {
			formatValIntoString(ses, row[colIdx+2], tblStuff.neededColTypes[colIdx], deleteFromValsBuffer)
			if idx != len(tblStuff.expandedPKColIdxes)-1 {
				deleteFromValsBuffer.WriteString(",")
			}
		}
		if len(tblStuff.expandedPKColIdxes) > 1 {
			deleteFromValsBuffer.WriteString(")")
		}
	}

	var (
		row = make([]any, len(tblStuff.neededColIdxes))
	)

	for rowIdx := range wrapped.batch.RowCount() {
		for colIdx, vec := range wrapped.batch.Vecs {
			if err = extractRowFromVector(
				ctx, ses, vec, colIdx, row, rowIdx, true,
			); err != nil {
				return
			}
		}
	}
	if wrapped.kind == diffDelete {
		writeDeleteFrom(row)
	} else if wrapped.kind == diffInsert {
		writeReplaceInto(row)
	} else {
		writeReplaceInto(row)
	}

	return
}

func outputDiffAsFile(
	ctx context.Context,
	ses *Session,
	stmt *tree.DataBranchDiff,
	tblStuff tableStuff,
	retCh chan batchWithKind,
) (err error) {
	var (
		srcName  = tblStuff.tarRel.GetTableName()
		baseName = tblStuff.baseRel.GetTableName()
		fileName string
	)

	if tblStuff.tarSnap != nil {
		srcName = fmt.Sprintf("%s_%s", srcName, tblStuff.tarSnap.ExtraInfo.Name)
	}

	if tblStuff.baseSnap != nil {
		baseName = fmt.Sprintf("%s_%s", baseName, tblStuff.baseSnap.ExtraInfo.Name)
	}

	fileName = fmt.Sprintf(
		"diff_%s_%s_%s",
		srcName, baseName,
		time.Now().UTC().Format("20060102_150405"),
	)

	var (
		deleteFromCnt  int
		replaceIntoCnt int

		deleteFromSqls  []string
		replaceIntoSqls []string

		deleteFromBuf  = acquireBuffer()
		replaceIntoBuf = acquireBuffer()
	)

	defer func() {
		releaseBuffer(replaceIntoBuf)
		releaseBuffer(deleteFromBuf)
	}()

	initReplaceIntoBuf()
	initDeleteFromBuf()

	toSQLs(true, false, replaceIntoCnt, replaceIntoBuf)
	toSQLs(true, true, deleteFromCnt, deleteFromBuf)

	hint := fmt.Sprintf(
		"DELETE FROM %s.%s, REPLACE INTO %s.%s",
		tblStuff.baseRel.GetTableDef(ctx).DbName, tblStuff.baseRel.GetTableName(),
		tblStuff.baseRel.GetTableDef(ctx).DbName, tblStuff.baseRel.GetTableName(),
	)

	buf := acquireBuffer()
	defer releaseBuffer(buf)

	for i, s := range deleteFromSqls {
		if i > 0 {
			buf.WriteString("\n")
		}
		buf.WriteString(s)
		buf.WriteString(";")
	}

	var (
		fullFilePath = path.Join(stmt.OutputOpt.DirPath, fileName)
	)

	fsPath, err2 := fileservice.ParsePath(fullFilePath)
	if err2 != nil {
		err = err2
		return
	}

	var (
		targetFS   fileservice.FileService
		targetPath string
	)

	if fsPath.Service == defines.SharedFileServiceName {
		targetFS = getPu(ses.GetService()).FileService
		targetPath = fullFilePath
	} else {
		var etlFS fileservice.ETLFileService
		if etlFS, targetPath, err2 = fileservice.GetForETL(ctx, nil, fullFilePath); err2 != nil {
			err = err2
			return
		}
		targetFS = etlFS
	}

	if err2 = targetFS.Delete(ctx, targetPath); err2 != nil && !moerr.IsMoErrCode(err2, moerr.ErrFileNotFound) {
		err = err2
		return
	}

	vec := fileservice.IOVector{
		FilePath: targetPath,
		Entries: []fileservice.IOEntry{
			{
				Size: int64(buf.Len()),
				Data: buf.Bytes(),
			},
		},
	}

	if err2 = targetFS.Write(ctx, vec); err2 != nil {
		err = err2
	}

	mrs := ses.GetMysqlResultSet()
	mrs.AddRow([]any{fullFilePath, hint})

	return nil
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

		for i := range tblStuff.colNames {
			nCol := new(MysqlColumn)
			if err = convertEngineTypeToMysqlType(ctx, tblStuff.neededColTypes[i].Oid, nCol); err != nil {
				return
			}

			nCol.SetName(tblStuff.colNames[i])
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

func satisfyDiffOutputOpt(
	ctx context.Context,
	ses *Session,
	mrs *MysqlResultSet,
	stmt *tree.DataBranchDiff,
	rows1 [][]any,
	rows2 [][]any,
	srcRel engine.Relation,
	dstRel engine.Relation,
	lcaType int,
	colTypes []types.Type,
	pkColIdxes []int,
) (err error) {

	var (
		rows [][]any
	)

	merge := func(config int) error {
		if rows, _, _, err = mergeDiff(
			ctx, ses, srcRel, dstRel, rows1, rows2, lcaType,
			tree.CONFLICT_ACCEPT, colTypes, pkColIdxes, config,
		); err != nil {
			return err
		}
		return nil
	}

	fillMrsWithRows := func(limit int64) {
		if err = merge(mergeDiffOutputRows); err != nil {
			return
		}

		for i, row := range rows {
			if limit >= 0 && int64(i) >= limit {
				break
			}

			for j := 2; j < len(row); j++ {
				switch v := row[j].(type) {
				case types.Timestamp:
					row[j] = v.String2(ses.timeZone, colTypes[j-2].Scale)
				case types.Datetime:
					row[j] = v.String2(colTypes[j-2].Scale)
				case types.Decimal64:
					row[j] = v.Format(colTypes[j-2].Scale)
				case types.Decimal128:
					row[j] = v.Format(colTypes[j-2].Scale)
				case types.Decimal256:
					row[j] = v.Format(colTypes[j-2].Scale)
				}
			}
			mrs.AddRow(row)
		}
	}

	if stmt.OutputOpt == nil {
		fillMrsWithRows(-1)
		return trySaveQueryResult(ctx, ses, mrs)
	}

	if stmt.OutputOpt.Limit != nil {
		fillMrsWithRows(*stmt.OutputOpt.Limit)
		return trySaveQueryResult(ctx, ses, mrs)
	}

	if stmt.OutputOpt.Count {
		if err = merge(mergeDiffOutputRows); err != nil {
			return
		}
		ses.ClearAllMysqlResultSet()
		ses.SetMysqlResultSet(&MysqlResultSet{})
		mrs = ses.GetMysqlResultSet()

		col := new(MysqlColumn)
		col.SetName("Count(*)")
		col.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

		mrs.AddColumn(col)
		mrs.AddRow([]any{int64(len(rows))})

		return trySaveQueryResult(ctx, ses, mrs)
	}

	if len(stmt.OutputOpt.DirPath) != 0 {
		return diffOutputFile(
			ctx, ses, stmt, rows1, rows2, srcRel, dstRel,
			lcaType, colTypes, pkColIdxes,
		)
	}

	panic(fmt.Sprintf("unreachable: %v", stmt.OutputOpt))
}

func diffOutputFile(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.DataBranchDiff,
	rows1 [][]any,
	rows2 [][]any,
	srcRel engine.Relation,
	dstRel engine.Relation,
	lcaType int,
	colTypes []types.Type,
	pkColIdxes []int,
) (err error) {

	var (
		sql      string
		sqlRet   executor.Result
		fileName string

		bat     *batch.Batch
		sqls    []string
		content []byte

		hint string
		sp   *plan.Snapshot

		srcName  = srcRel.GetTableName()
		baseName = dstRel.GetTableName()
	)

	sql = fmt.Sprintf("select count(*) from %s.%s", dstRel.GetTableDef(ctx).DbName, dstRel.GetTableName())
	if stmt.BaseTable.AtTsExpr != nil {
		if sp, err = resolveSnapshot(ses, stmt.BaseTable.AtTsExpr); err != nil {
			return
		}
		sql += fmt.Sprintf("{mo_ts=%d}", sp.TS.PhysicalTime)
		baseName = fmt.Sprintf("%s_%s", baseName, sp.ExtraInfo.Name)
	}

	if stmt.TargetTable.AtTsExpr != nil {
		srcName = fmt.Sprintf("%s_%s", srcName, stmt.TargetTable.AtTsExpr.SnapshotName)
	}

	fileName = fmt.Sprintf(
		"diff_%s_%s_%s",
		srcName, baseName,
		time.Now().UTC().Format("20060102_150405"),
	)

	if sqlRet, err = runSql(ctx, ses, bh, sql); err != nil {
		return err
	}

	// output sql file
	config := mergeDiffOutputSQLs

	// output csv file
	if vector.GetFixedAtNoTypeCheck[uint64](sqlRet.Batches[0].Vecs[0], 0) == 0 {
		fileName += ".csv"
		config = mergeDiffOutputBatch
	} else {
		fileName += ".sql"
	}

	if _, sqls, bat, err = mergeDiff(
		ctx, ses, srcRel, dstRel, rows1, rows2, lcaType,
		tree.CONFLICT_ACCEPT, colTypes, pkColIdxes, config,
	); err != nil {
		return err
	}

	var (
		buf = acquireBuffer()
	)

	defer func() {
		releaseBuffer(buf)
	}()

	if len(sqls) != 0 {
		for i, s := range sqls {
			if i > 0 {
				buf.WriteByte('\n')
			}
			buf.WriteString(s)
			buf.WriteString(";")
		}

		content = append([]byte(nil), buf.Bytes()...)
		hint = fmt.Sprintf(
			"DELETE FROM %s.%s, REPLACE INTO %s.%s",
			dstRel.GetTableDef(ctx).DbName, dstRel.GetTableName(),
			dstRel.GetTableDef(ctx).DbName, dstRel.GetTableName(),
		)
	} else {
		defer func() {
			bat.Clean(ses.proc.Mp())
		}()

		if content, hint, err = constructDiffCSVContent(ctx, ses, bat); err != nil {
			return err
		}
	}

	var (
		fullFilePath = path.Join(stmt.OutputOpt.DirPath, fileName)
	)

	ses.ClearAllMysqlResultSet()
	ses.SetMysqlResultSet(&MysqlResultSet{})
	mrs := ses.GetMysqlResultSet()

	col1 := new(MysqlColumn)
	col1.SetName("FILE SAVED TO")
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	col2 := new(MysqlColumn)
	col2.SetName("HINT")
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddRow([]any{fullFilePath, hint})

	return trySaveQueryResult(ctx, ses, mrs)
}

func constructDiffCSVContent(
	ctx context.Context, ses *Session, bat *batch.Batch,
) ([]byte, string, error) {

	var (
		result     *BatchByte
		spiltRules string
	)

	if bat == nil || bat.RowCount() == 0 {
		return nil, "", nil
	}

	exportCfg := &ExportConfig{
		userConfig: newDiffCSVExportParam(),
		service:    ses.GetService(),
	}
	initDiffCSVExportConfig(exportCfg, len(bat.Vecs))

	spiltRules = exportCfg.userConfig.String()

	byteChan := make(chan *BatchByte, 1)
	constructByte(ctx, ses, bat, 0, byteChan, exportCfg)

	result = <-byteChan
	if result == nil {
		return nil, "", moerr.NewInternalError(ctx, "failed to construct csv content")
	}
	if result.err != nil {
		return nil, "", result.err
	}

	return result.writeByte, spiltRules, nil
}

func newDiffCSVExportParam() *tree.ExportParam {
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

func initDiffCSVExportConfig(ep *ExportConfig, columnCount int) {
	if columnCount <= 0 {
		ep.Symbol = nil
		ep.ColumnFlag = nil
		return
	}
	ep.Symbol = make([][]byte, columnCount)
	ep.ColumnFlag = make([]bool, columnCount)

	fieldTerminated := tree.DefaultFieldsTerminated
	if ep.userConfig != nil && ep.userConfig.Fields != nil && ep.userConfig.Fields.Terminated != nil {
		fieldTerminated = ep.userConfig.Fields.Terminated.Value
	}

	lineTerminated := "\n"
	if ep.userConfig != nil && ep.userConfig.Lines != nil && ep.userConfig.Lines.TerminatedBy != nil {
		lineTerminated = ep.userConfig.Lines.TerminatedBy.Value
	}

	for i := 0; i < columnCount-1; i++ {
		ep.Symbol[i] = []byte(fieldTerminated)
	}
	ep.Symbol[columnCount-1] = []byte(lineTerminated)
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

	if tblStuff.tarRel, tblStuff.baseRel, tblStuff.tarSnap, tblStuff.baseSnap, err = getRelations(
		ctx, ses, bh, srcTable, dstTable,
	); err != nil {
		return
	}

	tarTblDef = tblStuff.tarRel.GetTableDef(ctx)
	baseTblDef = tblStuff.tarRel.GetTableDef(ctx)

	if !isSchemaEquivalent(tarTblDef, baseTblDef) {
		err = moerr.NewInternalErrorNoCtx("the target table schema is not equivalent to the base table.")
		return
	}

	if baseTblDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
		tblStuff.pkKind = fakeKind
		for i, col := range baseTblDef.Cols {
			if col.Name != catalog.FakePrimaryKeyColName && col.Name != catalog.Row_ID {
				tblStuff.expandedPKColIdxes = append(tblStuff.expandedPKColIdxes, i)
			}
		}
	} else if baseTblDef.Pkey.CompPkeyCol != nil {
		// case 2: composite pk, combined all pks columns as the PK
		tblStuff.pkKind = compositeKind
		pkNames := baseTblDef.Pkey.Names
		for _, name := range pkNames {
			idx := int(baseTblDef.Name2ColIndex[name])
			tblStuff.expandedPKColIdxes = append(tblStuff.expandedPKColIdxes, idx)
		}
	} else {
		// normal pk
		tblStuff.pkKind = normalKind
		pkName := baseTblDef.Pkey.PkeyColName
		idx := int(baseTblDef.Name2ColIndex[pkName])
		tblStuff.expandedPKColIdxes = append(tblStuff.expandedPKColIdxes, idx)
	}

	tblStuff.pkColIdx = int(baseTblDef.Name2ColIndex[baseTblDef.Pkey.PkeyColName])

	for i, col := range tarTblDef.Cols {
		if col.Name == catalog.Row_ID ||
			col.Name == catalog.FakePrimaryKeyColName ||
			col.Name == catalog.CPrimaryKeyColName {
			continue
		}

		t := types.New(types.T(col.Typ.Id), col.Typ.Width, col.Typ.Scale)
		tblStuff.colNames = append(tblStuff.colNames, col.Name)
		tblStuff.neededColTypes = append(tblStuff.neededColTypes, t)
		tblStuff.neededColIdxes = append(tblStuff.neededColIdxes, i)
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
	retCh chan batchWithKind,
	copt compositeOption,
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

	if dagInfo.lcaTableId == 0 {
		// has no lca
		if tarHandle, baseHandle, err = constructChangeHandle(
			ctx, ses, bh, tblStuff, &dagInfo,
		); err != nil {
			return
		}

		if err = hashDiff(
			ctx, ses, bh, tblStuff, dagInfo, retCh,
			copt, tarHandle, baseHandle,
		); err != nil {
			return
		}

		closeHandle()
		return
	}

	// merge left into right
	var (
		lcaRel      engine.Relation
		lcaSnapshot *plan.Snapshot
	)

	lcaSnapshot = &plan2.Snapshot{
		Tenant: &plan.SnapshotTenant{
			TenantID: ses.GetAccountId(),
		},
	}

	if dagInfo.lcaTableId == tblStuff.tarRel.GetTableID(ctx) {
		// left is the LCA
		lcaRel = tblStuff.tarRel
		lcaSnapshot.TS = &timestamp.Timestamp{PhysicalTime: dagInfo.baseBranchTS.Physical()}
		if tblStuff.tarSnap != nil && tblStuff.tarSnap.TS.Less(*lcaSnapshot.TS) {
			lcaSnapshot.TS = tblStuff.tarSnap.TS
		}
	} else if dagInfo.lcaTableId == tblStuff.baseRel.GetTableID(ctx) {
		// right is the LCA
		lcaRel = tblStuff.baseRel
		lcaSnapshot.TS = &timestamp.Timestamp{PhysicalTime: dagInfo.tarBranchTS.Physical()}
		if tblStuff.baseSnap != nil && tblStuff.baseSnap.TS.Less(*lcaSnapshot.TS) {
			lcaSnapshot.TS = tblStuff.baseSnap.TS
		}
	} else {
		// LCA is other table
		lcaSnapshot.TS = &timestamp.Timestamp{PhysicalTime: dagInfo.tarBranchTS.Physical()}
		if dagInfo.baseBranchTS.LT(&dagInfo.tarBranchTS) {
			lcaSnapshot.TS.PhysicalTime = dagInfo.baseBranchTS.Physical()
		}

		if lcaRel, err = getRelationById(
			ctx, ses, bh, dagInfo.lcaTableId, lcaSnapshot); err != nil {
			return
		}
	}

	var (
		tmpPair1 = tblStuff
		tmpPair2 = tblStuff

		dagInfo1 branchMetaInfo
		dagInfo2 branchMetaInfo

		finalTblStuff tableStuff
		finalDagInfo  branchMetaInfo
	)

	tmpPair1.tarRel = tblStuff.tarRel
	tmpPair1.baseRel = lcaRel
	tmpPair1.tarSnap = tblStuff.tarSnap
	tmpPair1.baseSnap = lcaSnapshot

	if dagInfo1, err = decideLCABranchTSFromBranchDAG(
		ctx, ses, tmpPair1.tarRel, tmpPair1.baseRel,
	); err != nil {
		return
	}

	if dagInfo1.tarBranchTS.IsEmpty() {
		dagInfo1.tarBranchTS = types.TimestampToTS(*tmpPair1.baseSnap.TS)
	}

	if tarHandle, _, err = constructChangeHandle(
		ctx, ses, bh, tmpPair1, &dagInfo1,
	); err != nil {
		return
	}

	tmpPair2.tarRel = tblStuff.baseRel
	tmpPair2.baseRel = lcaRel
	tmpPair2.tarSnap = tblStuff.baseSnap
	tmpPair2.baseSnap = lcaSnapshot

	if dagInfo2, err = decideLCABranchTSFromBranchDAG(
		ctx, ses, tmpPair2.tarRel, tmpPair2.baseRel,
	); err != nil {
		return
	}

	if dagInfo2.tarBranchTS.IsEmpty() {
		dagInfo2.tarBranchTS = types.TimestampToTS(*tmpPair2.baseSnap.TS)
	}

	if baseHandle, _, err = constructChangeHandle(
		ctx, ses, bh, tmpPair2, &dagInfo2,
	); err != nil {
		return
	}

	finalDagInfo.lcaType = dagInfo.lcaType
	finalDagInfo.lcaTableId = dagInfo.lcaTableId
	finalDagInfo.tarBranchTS = dagInfo1.tarBranchTS
	finalDagInfo.baseBranchTS = dagInfo2.tarBranchTS

	finalTblStuff = tblStuff
	finalTblStuff.tarRel = tmpPair1.tarRel
	finalTblStuff.baseRel = tmpPair2.tarRel
	finalTblStuff.tarSnap = tmpPair1.tarSnap
	finalTblStuff.tarSnap = tmpPair2.tarSnap

	return hashDiff(ctx, ses, bh, finalTblStuff, finalDagInfo, retCh, copt, tarHandle, baseHandle)
}

func mergeDiff(
	ctx context.Context,
	ses *Session,
	srcRel engine.Relation,
	dstRel engine.Relation,
	rows1 [][]any,
	rows2 [][]any,
	lcaType int,
	conflictOpt int,
	colTypes []types.Type,
	pkColIdxes []int,
	outputConf int,
) (retRows [][]any, retSQLs []string, bat *batch.Batch, err error) {

	var (
		replaceCnt int
		deleteCnt  int

		replaceIntoBuf *bytes.Buffer
		deleteFromBuf  *bytes.Buffer
	)

	initReplaceIntoBuf := func() {}
	initDeleteFromBuf := func() {}
	toSQLs := func(force bool, isDelete bool, cnt int, buf *bytes.Buffer) {}

	if outputConf != mergeDiffOutputRows {
		replaceIntoBuf = acquireBuffer()
		deleteFromBuf = acquireBuffer()

		initReplaceIntoBuf = func() {
			replaceCnt = 0
			replaceIntoBuf.Reset()
			replaceIntoBuf.WriteString(fmt.Sprintf(
				"replace into %s.%s values ",
				dstRel.GetTableDef(ctx).DbName,
				dstRel.GetTableDef(ctx).Name,
			))
		}

		initDeleteFromBuf = func() {
			deleteCnt = 0
			deleteFromBuf.Reset()
			if len(pkColIdxes) == 1 {
				deleteFromBuf.WriteString(fmt.Sprintf(
					"delete from %s.%s where %s in (",
					dstRel.GetTableDef(ctx).DbName,
					dstRel.GetTableDef(ctx).Name,
					dstRel.GetTableDef(ctx).Cols[pkColIdxes[0]].Name,
				))
			} else {
				pkNames := make([]string, len(pkColIdxes))
				for i, pkColIdx := range pkColIdxes {
					pkNames[i] = dstRel.GetTableDef(ctx).Cols[pkColIdx].Name
				}
				deleteFromBuf.WriteString(fmt.Sprintf(
					"delete from %s.%s where (%s) in (",
					dstRel.GetTableDef(ctx).DbName,
					dstRel.GetTableDef(ctx).Name,
					strings.Join(pkNames, ","),
				))
			}
		}

		initReplaceIntoBuf()
		initDeleteFromBuf()

		toSQLs = func(force bool, isDelete bool, cnt int, buf *bytes.Buffer) {
			if cnt == 0 || outputConf != mergeDiffOutputSQLs {
				return
			}

			if force || cnt >= batchCnt/2 {
				if isDelete {
					buf.WriteString(")")
					retSQLs = append(retSQLs, buf.String())
					initDeleteFromBuf()
				} else {
					retSQLs = append(retSQLs, buf.String())
					initReplaceIntoBuf()
				}
			}
		}

		defer func() {
			releaseBuffer(replaceIntoBuf)
			releaseBuffer(deleteFromBuf)
		}()
	}

	writeReplaceInto := func(row []any) {
		if outputConf == mergeDiffOutputRows {
			return
		}
		if replaceCnt > 0 {
			replaceIntoBuf.WriteString(",")
		}
		replaceCnt++
		replaceIntoBuf.WriteString("(")
		for j := 2; j < len(row); j++ {
			formatValIntoString(ses, row[j], colTypes[j-2], replaceIntoBuf)
			if j != len(row)-1 {
				replaceIntoBuf.WriteString(",")
			}
		}
		replaceIntoBuf.WriteString(")")
		toSQLs(false, false, replaceCnt, replaceIntoBuf)
	}

	writeDeleteFrom := func(row []any) {
		if outputConf == mergeDiffOutputRows {
			return
		}
		if deleteCnt > 0 {
			deleteFromBuf.WriteString(",")
		}
		deleteCnt++
		if len(pkColIdxes) > 1 {
			deleteFromBuf.WriteString("(")
		}
		for idx, colIdx := range pkColIdxes {
			formatValIntoString(ses, row[colIdx+2], colTypes[colIdx], deleteFromBuf)
			if idx != len(pkColIdxes)-1 {
				deleteFromBuf.WriteString(",")
			}
		}
		if len(pkColIdxes) > 1 {
			deleteFromBuf.WriteString(")")
		}
		toSQLs(false, true, deleteCnt, deleteFromBuf)
	}

	writeRow := func(row []any) {
		if outputConf == mergeDiffOutputRows {
			retRows = append(retRows, row)
			return
		} else if outputConf == mergeDiffOutputBatch {
			if bat == nil {
				bat = batch.NewWithSize(len(colTypes))
				for i := range colTypes {
					bat.Vecs[i] = vector.NewVec(colTypes[i])
				}
			}

			for i := range colTypes {
				vector.AppendAny(bat.Vecs[i], row[i+2], row[i+2] == nil, ses.proc.Mp())
			}
			bat.SetRowCount(bat.Vecs[0].Length())

		} else {
			// output sql, none
			if row[1].(string) == diffDelete {
				writeDeleteFrom(row)
			} else {
				writeReplaceInto(row)
			}
		}
	}

	conflictErr := func(a, b []any) error {
		pkStr := func(c []any) string {
			buf := acquireBuffer()
			defer releaseBuffer(buf)

			buf.WriteString("pk(")
			for i, idx := range pkColIdxes {
				formatValIntoString(ses, c[idx+2], colTypes[idx], buf)
				if i != len(pkColIdxes)-1 {
					buf.WriteString(",")
				}
			}
			buf.WriteString(")")
			return buf.String()
		}

		return moerr.NewInternalErrorNoCtxf(
			"merge conflict: %v %v %s <=> %v %v %s",
			a[0], a[1], pkStr(a),
			b[0], b[1], pkStr(b),
		)
	}

	if lcaType == lcaEmpty {
		var prevRow []any
		for _, row := range rows1 {
			if outputConf == mergeDiffOutputRows {
				retRows = append(retRows, row)
				continue
			}

			if len(prevRow) == 0 || row[0] == prevRow[0] {
				prevRow = row
				if row[0].(string) == srcRel.GetTableName() {
					//writeReplaceInto(row)
					writeRow(row)
				}
			} else {
				if compareRows(row, prevRow, pkColIdxes, colTypes, true) == 0 {
					switch conflictOpt {
					case tree.CONFLICT_FAIL:
						return nil, nil, nil, conflictErr(row, prevRow)
					case tree.CONFLICT_SKIP:
					// do nothing
					case tree.CONFLICT_ACCEPT:
						if row[0].(string) == srcRel.GetTableName() {
							//writeReplaceInto(row)
							writeRow(row)
						}
					}
				} else {
					if row[0].(string) == srcRel.GetTableName() {
						//writeReplaceInto(row)
						writeRow(row)
					}
				}
			}
		}
	} else {
		i, j := 0, 0
		for i < len(rows1) && j < len(rows2) {
			if cmp := compareRows(
				rows1[i], rows2[j], pkColIdxes, colTypes, true,
			); cmp == 0 {
				if rows1[i][1] == rows2[j][1] {
					// insert = insert
					// delete = delete
					// update = update
					if compareRows(rows1[i], rows2[j], pkColIdxes, colTypes, false) == 0 {
						// insert/delete/update the same row, skip
						i++
						j++
						continue
					}

					if outputConf == mergeDiffOutputRows {
						retRows = append(retRows, rows1[i])
						retRows = append(retRows, rows2[j])
						i++
						j++
						continue
					}
					switch conflictOpt {
					case tree.CONFLICT_FAIL:
						return nil, nil, nil, conflictErr(rows1[i], rows2[j])
					case tree.CONFLICT_SKIP:
						i++
						j++
					case tree.CONFLICT_ACCEPT:
						writeRow(rows1[i])
						i++
						j++
					}
				}
			} else {
				if cmp > 0 {
					if outputConf == mergeDiffOutputRows {
						retRows = append(retRows, rows2[j])
					}
					j++
				} else {
					writeRow(rows1[i])
					//if outputConf == mergeDiffOutputRows {
					//	retRows = append(retRows, rows1[i])
					//} else {
					//	writeRow(rows1[i])
					//}
					i++
				}
			}
		}

		for ; i < len(rows1); i++ {
			writeRow(rows1[i])
			//if outputConf == mergeDiffOutputRows {
			//	retRows = append(retRows, rows1[i])
			//} else {
			//	writeRow(rows1[i])
			//}
		}

		if outputConf == mergeDiffOutputRows {
			for ; j < len(rows2); j++ {
				retRows = append(retRows, rows2[j])
			}
		}
	}

	switch outputConf {
	case mergeDiffOutputNone:
		err = sqlexec.RunTxn(sqlexec.NewSqlProcess(ses.proc), func(txnExecutor executor.TxnExecutor) error {
			var (
				err2 error
				ret  executor.Result
			)

			if deleteCnt > 0 {
				deleteFromBuf.WriteString(")")
				if ret, err2 = txnExecutor.Exec(
					deleteFromBuf.String(), executor.StatementOption{},
				); err2 != nil {
					return err2
				}
				ret.Close()
			}

			if replaceCnt > 0 {
				if ret, err2 = txnExecutor.Exec(
					replaceIntoBuf.String(), executor.StatementOption{},
				); err2 != nil {
					return err2
				}
				ret.Close()
			}

			return nil
		})
	case mergeDiffOutputSQLs:
		toSQLs(true, false, replaceCnt, replaceIntoBuf)
		toSQLs(true, true, deleteCnt, deleteFromBuf)
	}

	return
}

func compareRows(
	row1 []any,
	row2 []any,
	pkColIdxes []int,
	colTypes []types.Type,
	comparePK bool,
) int {

	if comparePK {
		for i, idx := range pkColIdxes {
			if cmp := types.CompareValues(
				row1[idx+2], row2[idx+2], colTypes[i].Oid,
			); cmp == 0 {
				continue
			} else {
				return cmp
			}
		}
		return 0
	}

	for i := 2; i < len(row1); i++ {
		if slices.Index(pkColIdxes, i-2) != -1 {
			// skip pk
			continue
		}

		if cmp := types.CompareValues(
			row1[i], row2[i], colTypes[i-2].Oid,
		); cmp == 0 {
			continue
		} else {
			return cmp
		}
	}

	return 0
}

func sortDiffResultRows(
	rows [][]any,
	pkColIdxes []int,
	colTypes []types.Type,
) (newRows [][]any) {

	twoRowsCompare := func(a, b []any, onlyByPK bool) int {
		return compareRows(a, b, pkColIdxes, colTypes, onlyByPK)
	}

	slices.SortFunc(rows, func(a, b []any) int {
		return twoRowsCompare(a, b, true)
	})

	appendNewRow := func(row []any) {
		newRows = append(newRows, row)
		if row[1].(string) == diffAddedLine {
			newRows[len(newRows)-1][1] = diffInsert
		} else {
			newRows[len(newRows)-1][1] = diffDelete
		}
	}

	mergeTwoRows := func(row1, row2 []any) {
		// merge two rows to a single row
		if row1[1].(string) == diffAddedLine {
			newRows = append(newRows, row1)
		} else {
			newRows = append(newRows, row2)
		}
		newRows[len(newRows)-1][1] = diffUpdate
	}

	for i := 0; i < len(rows); {
		if i+2 > len(rows) {
			appendNewRow(rows[i])
			break
		}

		// check if the row[i], r[i+1] has the same pk
		// if pk equal and flag not equal, means this is an update,
		// or duplicate insert with no pk table.
		if twoRowsCompare(rows[i], rows[i+1], true) == 0 &&
			rows[i][1] != rows[i+1][1] {
			mergeTwoRows(rows[i], rows[i+1])
			i += 2
		} else {
			appendNewRow(rows[i])
			i++
		}
	}

	return
}

func hashDiff(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tblStuff tableStuff,
	dagInfo branchMetaInfo,
	retCh chan batchWithKind,
	option compositeOption,
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
			ctx, ses, tblStuff, retCh,
			tarDataHashmap, tarTombstoneHashmap,
			baseDataHashmap, baseTombstoneHashmap,
		); err != nil {
			return
		}
	} else {
		if err = hashDiffIfHasLCA(
			ctx, ses, bh, dagInfo, tblStuff, retCh,
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
	retCh chan batchWithKind,
	tarDataHashmap databranchutils.BranchHashmap,
	tarTombstoneHashmap databranchutils.BranchHashmap,
	baseDataHashmap databranchutils.BranchHashmap,
	baseTombstoneHashmap databranchutils.BranchHashmap,
) (err error) {

	if err = tarTombstoneHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		var (
			err2      error
			tuple     types.Tuple
			dBat      *batch.Batch
			tBat      = acquireRetBatch(tblStuff, true)
			updateBat *batch.Batch
			checkRet  []databranchutils.GetResult
		)

		if err2 = cursor.ForEach(func(key []byte, rows [][]byte) error {
			if tuple, _, err2 = tarTombstoneHashmap.DecodeRow(key); err2 != nil {
				return err2
			} else {
				if err2 = vector.AppendAny(tBat.Vecs[0], tuple[0], false, ses.proc.Mp()); err2 != nil {
					return err2
				}

				tBat.SetRowCount(tBat.Vecs[0].Length())
			}
			return nil
		}); err2 != nil {
			return err2
		}

		if dBat, err2 = handleDelsOnLCA(
			ctx, ses, bh, tBat, tblStuff, dagInfo.tarBranchTS.ToTimestamp(),
		); err2 != nil {
			return err2
		}

		// merge inserts and deletes on the tar
		// this deletes is not on the lca
		if tBat.RowCount() > 0 {
			if _, err2 = tarDataHashmap.PopByVectors(
				[]*vector.Vector{tBat.Vecs[0]}, false,
			); err2 != nil {
				return err2
			}
			releaseRetBatch(tBat, true)
		}

		// find update
		if dBat.RowCount() > 0 {
			tBat = acquireRetBatch(tblStuff, false)
			if checkRet, err2 = tarDataHashmap.PopByVectors(
				[]*vector.Vector{dBat.Vecs[tblStuff.pkColIdx]}, false,
			); err2 != nil {
				return err2
			}

			for i, check := range checkRet {
				if check.Exists {
					// delete on lca and insert into tar ==> update
					if updateBat == nil {
						updateBat = acquireRetBatch(tblStuff, false)
					}

					if tuple, _, err2 = tarDataHashmap.DecodeRow(check.Rows[0]); err2 != nil {
						return err2
					}

					for j, idx := range tblStuff.neededColIdxes {
						vec := updateBat.Vecs[j]
						if err2 = vector.AppendAny(vec, tuple[idx], false, ses.proc.Mp()); err2 != nil {
							return err2
						}
					}

				} else {
					// delete on lca
					if err2 = tBat.UnionOne(dBat, int64(i), ses.proc.Mp()); err2 != nil {
						return err2
					}
				}
			}

			releaseRetBatch(dBat, true)
			tBat.SetRowCount(tBat.Vecs[0].Length())
			updateBat.SetRowCount(tBat.Vecs[0].Length())

			retCh <- batchWithKind{
				batch: updateBat,
				kind:  diffUpdate,
			}

			retCh <- batchWithKind{
				batch: tBat,
				kind:  diffDelete,
			}
		}

		return nil

	}, -1); err != nil {
		return
	}

	// insert on the lca
	return tarDataHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		var (
			err2  error
			tuple types.Tuple
			iBat  = acquireRetBatch(tblStuff, false)
		)

		if err2 = cursor.ForEach(func(key []byte, rows [][]byte) error {
			for _, row := range rows {
				if tuple, _, err2 = tarDataHashmap.DecodeRow(row); err2 != nil {
					return err2
				}

				for j, idx := range tblStuff.neededColIdxes {
					vec := iBat.Vecs[j]
					if err2 = vector.AppendAny(vec, tuple[idx], false, ses.proc.Mp()); err2 != nil {
						return err2
					}
				}
			}
			return nil
		}); err2 != nil {
			return err2
		}

		iBat.SetRowCount(iBat.Vecs[0].Length())
		retCh <- batchWithKind{
			batch: iBat,
			kind:  diffInsert,
		}

		return nil

	}, -1)
}

func hashDiffIfNoLCA(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	retCh chan batchWithKind,
	copt compositeOption,
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

	if err = tarDataHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		var (
			err2      error
			tarBat    *batch.Batch
			baseBat   *batch.Batch
			tarTuple  types.Tuple
			baseTuple types.Tuple
			checkRet  databranchutils.GetResult
		)

		if err2 = cursor.ForEach(func(key []byte, rows [][]byte) error {
			for _, row := range rows {
				if checkRet, err2 = baseDataHashmap.PopByEncodedKey(key, false); err2 != nil {
					return err2
				}

				if !checkRet.Exists {
					if tarBat == nil {
						tarBat = acquireRetBatch(tblStuff, false)
					}

					if tarTuple, _, err2 = tarDataHashmap.DecodeRow(row); err2 != nil {
						return err2
					}

					for j, idx := range tblStuff.neededColIdxes {
						vec := tarBat.Vecs[j]
						if err2 = vector.AppendAny(vec, tarTuple[idx], false, ses.proc.Mp()); err2 != nil {
							return err2
						}
					}

				} else {
					// both has the key, we continue compare the left columns,
					// if all columns are equal, exactly a same row, ignore.
					if tblStuff.pkKind == fakeKind {
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
						for j, idx := range tblStuff.neededColIdxes {
							if slices.Index(tblStuff.expandedPKColIdxes, idx) != -1 {
								// pk columns already compared
								continue
							}

							if types.CompareValues(tarTuple[idx], baseTuple[idx], tblStuff.neededColTypes[j].Oid) != 0 {
								notSame = true
								break
							}
						}

						if notSame {
							// conflict happened
							if copt.conflictOpt != nil {
								switch copt.conflictOpt.Opt {
								case tree.CONFLICT_FAIL:
									buf := acquireBuffer()
									for i, idx := range tblStuff.expandedPKColIdxes {
										formatValIntoString(ses, tarTuple[idx], tblStuff.neededColTypes[i], buf)
										if i < len(tblStuff.expandedPKColIdxes)-1 {
											buf.WriteString(",")
										}
									}

									msg := buf.String()
									releaseBuffer(buf)
									return moerr.NewInternalErrorNoCtxf(
										"conflict, both insert pk(%v) with different values", msg,
									)
								}
							}

							if tarBat == nil {
								tarBat = acquireRetBatch(tblStuff, false)
							}

							if baseBat == nil {
								baseBat = acquireRetBatch(tblStuff, false)
							}

							for j, idx := range tblStuff.neededColIdxes {
								vec1 := tarBat.Vecs[j]
								if err2 = vector.AppendAny(vec1, tarTuple[idx], false, ses.proc.Mp()); err2 != nil {
									return err2
								}

								vec2 := baseBat.Vecs[j]
								if err2 = vector.AppendAny(vec2, baseTuple[idx], false, ses.proc.Mp()); err2 != nil {
									return err2
								}
							}
						}
					}
				}
			}
			return nil
		}); err2 != nil {
			return err2
		}

		retCh <- batchWithKind{
			batch: tarBat,
			kind:  diffInsert,
			name:  tblStuff.tarRel.GetTableName(),
		}

		retCh <- batchWithKind{
			batch: baseBat,
			kind:  diffInsert,
			name:  tblStuff.baseRel.GetTableName(),
		}

		return nil
	}, -1); err != nil {
		return
	}

	if err = baseDataHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		var (
			err2  error
			tuple types.Tuple
			bat   *batch.Batch
		)

		if err2 = cursor.ForEach(func(key []byte, rows [][]byte) error {
			if bat == nil {
				bat = acquireRetBatch(tblStuff, false)
			}

			for _, row := range rows {
				if tuple, _, err2 = baseDataHashmap.DecodeRow(row); err2 != nil {
					return err2
				}

				for i, idx := range tblStuff.neededColIdxes {
					vec := bat.Vecs[i]
					if err2 = vector.AppendAny(vec, tuple[idx], false, ses.proc.Mp()); err2 != nil {
						return err2
					}
				}
			}
			return nil
		}); err2 != nil {
			return err2
		}

		retCh <- batchWithKind{
			batch: bat,
			kind:  diffInsert,
			name:  tblStuff.baseRel.GetTableName(),
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

	var (
		sql       string
		sqlRet    executor.Result
		lcaTblDef *plan.TableDef
		mots      = fmt.Sprintf("{MO_TS=%d} ", snapshot.PhysicalTime)

		sqlBuf  = acquireBuffer()
		valsBuf = acquireBuffer()

		baseTblDef = tblStuff.baseRel.GetTableDef(ctx)

		colTypes           = tblStuff.neededColTypes
		expandedPKColIdxes = tblStuff.expandedPKColIdxes
	)

	defer func() {
		releaseBuffer(sqlBuf)
		releaseBuffer(valsBuf)
	}()

	lcaTblDef = baseTblDef

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
				formatValIntoString(ses, tuple[j], colTypes[expandedPKColIdxes[j]], valsBuf)
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
	if sqlRet, err = runSql(ctx, ses, bh, sql); err != nil {
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

	dBat = acquireRetBatch(tblStuff, false)

	sels := make([]int64, 0, 100)
	sqlRet.ReadRows(func(rowCnt int, cols []*vector.Vector) bool {
		for i := range rowCnt {
			if notExist(cols[1:], i) {
				idx := vector.GetFixedAtNoTypeCheck[int64](cols[0], i)
				sels = append(sels, int64(idx))
				continue
			}

			for j := range len(colTypes) {
				if err = dBat.Vecs[j].Union(cols[j+1], nil, ses.proc.Mp()); err != nil {
					return false
				}
			}
		}
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

func formatValIntoString(ses *Session, val any, t types.Type, buf *bytes.Buffer) {
	if val == nil {
		buf.WriteString("NULL")
		return
	}

	switch t.Oid {
	case types.T_varchar, types.T_text, types.T_json, types.T_char, types.
		T_varbinary, types.T_binary, types.T_array_float32, types.T_array_float64:
		buf.WriteString("'")
		if x, ok := val.([]byte); ok {
			buf.WriteString(string(x))
		} else {
			buf.WriteString(val.(string))
		}
		buf.WriteString("'")
	case types.T_timestamp:
		buf.WriteString("'")
		buf.WriteString(val.(types.Timestamp).String2(ses.timeZone, t.Scale))
		buf.WriteString("'")
	case types.T_datetime:
		buf.WriteString("'")
		buf.WriteString(val.(types.Datetime).String2(t.Scale))
		buf.WriteString("'")
	case types.T_date:
		buf.WriteString("'")
		buf.WriteString(val.(types.Date).String())
		buf.WriteString("'")
	case types.T_decimal64:
		buf.WriteString(val.(types.Decimal64).Format(t.Scale))
	case types.T_decimal128:
		buf.WriteString(val.(types.Decimal128).Format(t.Scale))
	case types.T_decimal256:
		buf.WriteString(val.(types.Decimal256).Format(t.Scale))
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_int8,
		types.T_int16, types.T_int32, types.T_int64, types.T_float32, types.T_float64:
		buf.WriteString(fmt.Sprintf("%v", val))
	}
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
		dataBat      *batch.Batch
		tombstoneBat *batch.Batch
	)

	defer func() {
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

	pkIdxes := []int{tblStuff.pkColIdx}
	if lcaType == lcaEmpty && tblStuff.pkKind == fakeKind {
		pkIdxes = tblStuff.expandedPKColIdxes
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

			if dataBat != nil {
				if err = dataHashmap.PutByVectors(dataBat.Vecs, pkIdxes); err != nil {
					return
				}
				dataBat.Clean(mp)
			}

			if tombstoneBat != nil {
				if err = tombstoneHashmap.PutByVectors(tombstoneBat.Vecs, []int{0}); err != nil {
					return
				}
				tombstoneBat.Clean(mp)
			}
		}
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

		if branchInfo.lcaType != lcaEmpty {
			// collect nothing from base
			if handle != nil {
				err = moerr.NewInternalErrorNoCtx("the LCA collect range is not empty")
				return
			}
			continue
		}

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

	buf := acquireBuffer()
	defer func() {
		releaseBuffer(buf)
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
			"get table created time for (%s, %s) failed",
			tar.GetTableName(), base.GetTableName(),
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

	if dag, err = constructBranchDAG(ctx, ses); err != nil {
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

	if sqlRet, err = sqlexec.RunSql(
		sqlexec.NewSqlProcess(ses.proc),
		fmt.Sprintf(scanBranchMetadataSql, catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA),
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
