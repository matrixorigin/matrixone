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
	"os"
	"path"
	"runtime"
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
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
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

	lcaRel engine.Relation

	colNames           []string
	pkKind             int
	pkColIdx           int
	pkColType          types.Type
	expandedPKColIdxes []int
	neededColIdxes     []int
	neededColTypes     []types.Type

	worker *ants.Pool
}

func (t tableStuff) pkIdxInBat() int {
	sortColIdx := 0
	if t.pkKind != normalKind {
		sortColIdx = len(t.neededColIdxes)
	} else {
		for i, idx := range t.neededColIdxes {
			if idx == t.pkColIdx {
				sortColIdx = i
			}
		}
	}

	return sortColIdx
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
			bat.Vecs[0] = vector.NewVec(tblStuff.pkColType)
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

			if tblStuff.pkKind != normalKind {
				bat.Vecs = append(bat.Vecs, vector.NewVec(tblStuff.pkColType))
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
		if retBatCh != nil {
			close(retBatCh)
		}
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

			if err2 := satisfyDiffOutputOpt(
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
	retBatCh = nil

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

		replaceIntoVals = acquireBuffer()
		deleteFromVals  = acquireBuffer()
	)

	defer func() {
		cancel()
	}()

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

func satisfyDiffOutputOpt(
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
					row = make([]any, len(tblStuff.neededColIdxes)+2)
				)
				row[0] = wrapped.name
				row[1] = wrapped.kind

				for colIdx, _ := range tblStuff.neededColIdxes {
					vec := wrapped.batch.Vecs[colIdx]
					if err = extractRowFromVector(
						ctx, ses, vec, colIdx+2, row, rowIdx, true,
					); err != nil {
						return
					}
				}

				mrs.AddRow(row)
				if stmt.OutputOpt != nil && stmt.OutputOpt.Limit != nil &&
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
		t := types.New(types.T(col.Typ.Id), col.Typ.Width, col.Typ.Scale)
		if i == tblStuff.pkColIdx {
			tblStuff.pkColType = t
		}

		if col.Name == catalog.Row_ID ||
			col.Name == catalog.FakePrimaryKeyColName ||
			col.Name == catalog.CPrimaryKeyColName {
			continue
		}

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

	tblStuff.lcaRel = lcaRel

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

	if copt.conflictOpt == nil || copt.conflictOpt.Opt != tree.CONFLICT_ACCEPT {
		// if we got a conflict_accept option,
		// then we should ignore all inserts/deletes/updates from the base.
		if baseHandle, _, err = constructChangeHandle(
			ctx, ses, bh, tmpPair2, &dagInfo2,
		); err != nil {
			return
		}
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

func hashDiff(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tblStuff tableStuff,
	dagInfo branchMetaInfo,
	retCh chan batchWithKind,
	copt compositeOption,
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
			ctx, ses, tblStuff, retCh, copt,
			tarDataHashmap, tarTombstoneHashmap,
			baseDataHashmap, baseTombstoneHashmap,
		); err != nil {
			return
		}
	} else {
		if err = hashDiffIfHasLCA(
			ctx, ses, bh, dagInfo, tblStuff, retCh, copt,
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
	copt compositeOption,
	tarDataHashmap databranchutils.BranchHashmap,
	tarTombstoneHashmap databranchutils.BranchHashmap,
	baseDataHashmap databranchutils.BranchHashmap,
	baseTombstoneHashmap databranchutils.BranchHashmap,
) (err error) {

	var (
		wg        sync.WaitGroup
		atomicErr atomic.Value

		pkIdxInBat        = tblStuff.pkIdxInBat()
		baseDeleteBatches []batchWithKind
		baseUpdateBatches []batchWithKind
	)

	handleBaseDeleteAndUpdates := func(wrapped batchWithKind) error {
		if err2 := mergeutil.SortColumnsByIndex(
			wrapped.batch.Vecs, pkIdxInBat, ses.proc.Mp(),
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
			retCh <- wrapped
			return nil
		}

		if err2 = mergeutil.SortColumnsByIndex(
			wrapped.batch.Vecs, pkIdxInBat, ses.proc.Mp(),
		); err2 != nil {
			return err2
		}

		checkConflict := func(tarWrapped, baseWrapped batchWithKind) (sels1, sels2 []int64, err3 error) {
			var (
				tarRow  = make([]any, 1)
				baseRow = make([]any, 1)
				tarVec  = tarWrapped.batch.Vecs[pkIdxInBat]
				baseVec = baseWrapped.batch.Vecs[pkIdxInBat]
			)

			i, j := 0, 0
			for i < tarVec.Length() && j < baseVec.Length() {
				if err3 = extractRowFromVector(
					ctx, ses, tarVec, 0, tarRow, i, true,
				); err3 != nil {
					return
				}

				if err3 = extractRowFromVector(
					ctx, ses, baseVec, 0, baseRow, j, true,
				); err3 != nil {
					return
				}

				cmp := types.CompareValues(tarRow[0], baseRow[0], tblStuff.pkColType.Oid)
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
					} else if copt.conflictOpt.Opt == tree.CONFLICT_SKIP {
						sels1 = append(sels1, int64(i))
						sels2 = append(sels2, int64(j))
						i++
						j++
					} else {
						// copt.conflictOpt == tree.CONFLICT_FAIL
						buf := acquireBuffer()
						formatValIntoString(ses, tarRow[0], tblStuff.pkColType, buf)

						err3 = moerr.NewInternalErrorNoCtxf(
							"conflict: %s %s and %s %s on pk(%v)",
							tarWrapped.name, tarWrapped.kind,
							baseWrapped.name, baseWrapped.kind, buf.String(),
						)
						releaseBuffer(buf)
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
				} else if len(sels1) != 0 {
					wrapped.batch.Shrink(sels1, true)
					baseWrapped.batch.Shrink(sels2, true)
				}

				if wrapped.batch.RowCount() == 0 {
					break
				}
			}

			plan2.RemoveIf(baseWrappedList, func(t batchWithKind) bool {
				if t.batch.RowCount() == 0 {
					releaseRetBatch(t.batch, false)
					return true
				}
				return false
			})

			return
		}

		if err2 = foo(baseDeleteBatches); err2 != nil {
			return
		}

		if err2 = foo(baseUpdateBatches); err2 != nil {
			return
		}

		if wrapped.batch.RowCount() == 0 {
			releaseRetBatch(wrapped.batch, false)
			return
		}

		retCh <- wrapped

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
			return
		}
		return nil
	}

	stepHandler := func(forBase bool) (err2 error) {
		var (
			tmpCh = make(chan batchWithKind, 1)
		)

		if err2 = asyncDelsAndUpdatesHandler(forBase, tmpCh); err2 != nil {
			return err2
		}

		for wrapped := range tmpCh {
			select {
			case <-ctx.Done():
				cancel()
				break
			default:
				if forBase {
					err2 = handleBaseDeleteAndUpdates(wrapped)
				} else {
					err2 = handleTarDeleteAndUpdates(wrapped)
				}
				if err2 != nil {
					cancel()
					break
				}
			}
		}

		wg.Wait()
		if err2 != nil {
			return
		}

		if atomicErr.Load() != nil {
			return atomicErr.Load().(error)
		}

		return
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
		for _, w := range baseUpdateBatches {
			retCh <- w
		}

		for _, w := range baseDeleteBatches {
			retCh <- w
		}
	}

	return diffDataHelper(ctx, ses, copt, tblStuff, retCh, tarDataHashmap, baseDataHashmap)
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

	return diffDataHelper(ctx, ses, copt, tblStuff, retCh, tarDataHashmap, baseDataHashmap)
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

	for i, colIdx := range tblStuff.neededColIdxes {
		if slices.Index(tblStuff.expandedPKColIdxes, colIdx) != -1 {
			continue
		}

		var (
			err  error
			row1 = make([]any, 1)
			row2 = make([]any, 1)

			vec1 = wrapped1.batch.Vecs[i]
			vec2 = wrapped2.batch.Vecs[i]
		)

		if err = extractRowFromVector(
			ctx, ses, vec1, 0, row1, rowIdx1, true,
		); err != nil {
			return 0, err
		}

		if err = extractRowFromVector(
			ctx, ses, vec2, 0, row2, rowIdx2, true,
		); err != nil {
			return 0, err
		}

		if cmp := types.CompareValues(
			row1[0], row2[0], tblStuff.neededColTypes[i].Oid,
		); cmp != 0 {
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

	pkIdxInBat := tblStuff.pkIdxInBat()

	if err = tombstoneHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		var (
			err2      error
			tuple     types.Tuple
			dBat      *batch.Batch
			tBat      = acquireRetBatch(tblStuff, true)
			updateBat *batch.Batch
			checkRet  []databranchutils.GetResult
		)

		if err2 = cursor.ForEach(func(key []byte, rows [][]byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if tuple, _, err2 = tombstoneHashmap.DecodeRow(key); err2 != nil {
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

		if tBat.RowCount() == 0 {
			releaseRetBatch(tBat, true)
			return nil
		}

		if dBat, err2 = handleDelsOnLCA(
			ctx, ses, bh, tBat, tblStuff, branchTS.ToTimestamp(),
		); err2 != nil {
			return err2
		}

		// merge inserts and deletes on the tar
		// this deletes is not on the lca
		if tBat.RowCount() > 0 {
			if _, err2 = dataHashmap.PopByVectors(
				[]*vector.Vector{tBat.Vecs[0]}, false,
			); err2 != nil {
				return err2
			}
			releaseRetBatch(tBat, true)
		}

		// find update
		if dBat.RowCount() > 0 {
			tBat = acquireRetBatch(tblStuff, false)
			if checkRet, err2 = dataHashmap.PopByVectors(
				[]*vector.Vector{dBat.Vecs[pkIdxInBat]}, false,
			); err2 != nil {
				return err2
			}

			fmt.Println("AAA", tblName, len(checkRet), common.MoBatchToString(dBat, dBat.RowCount()))

			for i, check := range checkRet {
				if check.Exists {
					// delete on lca and insert into tar ==> update
					if updateBat == nil {
						updateBat = acquireRetBatch(tblStuff, false)
					}

					if tuple, _, err2 = dataHashmap.DecodeRow(check.Rows[0]); err2 != nil {
						return err2
					}

					if err2 = appendTupleToBat(ses, updateBat, tuple, tblStuff); err2 != nil {
						return err2
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

			if updateBat != nil {
				updateBat.SetRowCount(updateBat.Vecs[0].Length())
				fmt.Println("BBB", updateBat.RowCount(), tblName)
				tmpCh <- batchWithKind{
					name:  tblName,
					batch: updateBat,
					kind:  diffUpdate,
				}
			}

			tmpCh <- batchWithKind{
				name:  tblName,
				batch: tBat,
				kind:  diffDelete,
			}
		}

		return nil

	}, -1); err != nil {
		return
	}

	return nil
}

func appendTupleToBat(ses *Session, bat *batch.Batch, tuple types.Tuple, tblStuff tableStuff) error {
	for j, idx := range tblStuff.neededColIdxes {
		vec := bat.Vecs[j]
		if err := vector.AppendAny(
			vec, tuple[idx], false, ses.proc.Mp(),
		); err != nil {
			return err
		}
	}

	if tblStuff.pkKind != normalKind {
		if err := vector.AppendAny(
			bat.Vecs[bat.VectorCount()-1], tuple[tblStuff.pkColIdx], false, ses.proc.Mp(),
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
	retCh chan batchWithKind,
	tarDataHashmap databranchutils.BranchHashmap,
	baseDataHashmap databranchutils.BranchHashmap,
) (err error) {

	if err = tarDataHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		var (
			err2      error
			tarBat    *batch.Batch
			baseBat   *batch.Batch
			tarTuple  types.Tuple
			baseTuple types.Tuple
			checkRet  databranchutils.GetResult
		)

		tarBat = acquireRetBatch(tblStuff, false)
		baseBat = acquireRetBatch(tblStuff, false)

		if err2 = cursor.ForEach(func(key []byte, rows [][]byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			for _, row := range rows {
				if checkRet, err2 = baseDataHashmap.PopByEncodedKey(key, false); err2 != nil {
					return err2
				}

				if !checkRet.Exists {
					if tarTuple, _, err2 = tarDataHashmap.DecodeRow(row); err2 != nil {
						return err2
					}

					if err = appendTupleToBat(ses, tarBat, tarTuple, tblStuff); err != nil {
						return err
					}

				} else {
					// both has the key, we continue compare the left columns,
					// if all columns are equal, exactly the same row, ignore.
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

							if types.CompareValues(
								tarTuple[idx], baseTuple[idx], tblStuff.neededColTypes[j].Oid,
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

		bat = acquireRetBatch(tblStuff, false)

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

	if snapshot.PhysicalTime == 0 {
		return nil, moerr.NewInternalErrorNoCtxf("invalid branch ts: %s", snapshot.DebugString())
	}

	var (
		sql    string
		sqlRet executor.Result
		mots   = fmt.Sprintf("{MO_TS=%d} ", snapshot.PhysicalTime)

		sqlBuf  = acquireBuffer()
		valsBuf = acquireBuffer()

		lcaTblDef  = tblStuff.lcaRel.GetTableDef(ctx)
		baseTblDef = tblStuff.baseRel.GetTableDef(ctx)

		colTypes           = tblStuff.neededColTypes
		expandedPKColIdxes = tblStuff.expandedPKColIdxes
	)

	defer func() {
		releaseBuffer(sqlBuf)
		releaseBuffer(valsBuf)
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
		fmt.Println(sql)
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
				if err = dBat.Vecs[j].UnionOne(cols[j+1], int64(i), ses.proc.Mp()); err != nil {
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
		atomicErr    atomic.Value
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

	putVectors := func(bat *batch.Batch, isTombstone bool) error {
		if bat == nil {
			return nil
		}

		return tblStuff.worker.Submit(func() {
			if isTombstone {
				if err = tombstoneHashmap.PutByVectors(bat.Vecs, []int{0}); err != nil {
					return
				}
			} else {
				if err = dataHashmap.PutByVectors(bat.Vecs, []int{tblStuff.pkColIdx}); err != nil {
					return
				}
			}
			bat.Clean(mp)
			if err != nil {
				atomicErr.Store(err)
			}
		})
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

			if err = putVectors(tombstoneBat, false); err != nil {
				return
			}
		}
	}

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
