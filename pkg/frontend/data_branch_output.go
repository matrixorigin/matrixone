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
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/panjf2000/ants/v2"
)

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
		insertCnt int
		deleteCnt int

		insertIntoVals = acquireBuffer(tblStuff.bufPool)
		deleteFromVals = acquireBuffer(tblStuff.bufPool)
		firstErr       error
		tmpValsBuffer  = acquireBuffer(tblStuff.bufPool)
	)

	defer func() {
		releaseBuffer(tblStuff.bufPool, insertIntoVals)
		releaseBuffer(tblStuff.bufPool, deleteFromVals)
		releaseBuffer(tblStuff.bufPool, tmpValsBuffer)
	}()

	defer func() {
		cancel()
	}()

	appender := sqlValuesAppender{
		ctx:             ctx,
		ses:             ses,
		bh:              bh,
		tblStuff:        tblStuff,
		deleteByFullRow: tblStuff.def.pkKind == fakeKind,
		pkInfo:          newPKBatchInfo(ctx, ses, tblStuff),
		deleteCnt:       &deleteCnt,
		deleteBuf:       deleteFromVals,
		insertCnt:       &insertCnt,
		insertBuf:       insertIntoVals,
	}
	if err = initPKTables(ctx, ses, bh, appender.pkInfo, appender.writeFile); err != nil {
		return err
	}
	defer func() {
		if err2 := dropPKTables(ctx, ses, bh, appender.pkInfo, appender.writeFile); err2 != nil && err == nil {
			err = err2
		}
	}()

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

// resolveProjectedIdxes resolves the user-specified COLUMNS list to a subset
// of visibleIdxes. Returns nil if no COLUMNS clause was specified.
func resolveProjectedIdxes(columns tree.IdentifierList, tblStuff tableStuff) ([]int, error) {
	if columns == nil {
		return nil, nil
	}

	nameToIdx := make(map[string]int, len(tblStuff.def.visibleIdxes))
	for _, idx := range tblStuff.def.visibleIdxes {
		nameToIdx[strings.ToLower(tblStuff.def.colNames[idx])] = idx
	}

	projected := make([]int, 0, len(columns))
	seen := make(map[int]bool, len(columns))
	for _, col := range columns {
		idx, ok := nameToIdx[strings.ToLower(string(col))]
		if !ok {
			return nil, moerr.NewInternalErrorNoCtxf("column %q not found in table", string(col))
		}
		if seen[idx] {
			continue
		}
		seen[idx] = true
		projected = append(projected, idx)
	}

	return projected, nil
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

		// Resolve column projection (nil means show all visible columns).
		displayIdxes, resolveErr := resolveProjectedIdxes(stmt.Columns, tblStuff)
		if resolveErr != nil {
			return resolveErr
		}

		// Determine which columns to extract from batch vectors.
		// When projecting, only extract display columns + PK columns (for sorting).
		extractIdxes := tblStuff.def.visibleIdxes
		rowSize := len(tblStuff.def.visibleIdxes) + 2
		if displayIdxes != nil {
			seen := make(map[int]bool, len(displayIdxes)+len(tblStuff.def.pkColIdxes))
			extractIdxes = make([]int, 0, len(displayIdxes)+len(tblStuff.def.pkColIdxes))
			for _, idx := range displayIdxes {
				if !seen[idx] {
					seen[idx] = true
					extractIdxes = append(extractIdxes, idx)
				}
			}
			for _, idx := range tblStuff.def.pkColIdxes {
				if !seen[idx] {
					seen[idx] = true
					extractIdxes = append(extractIdxes, idx)
				}
			}
			// Sparse row: indexed by colIdx+2, so size must accommodate the largest index.
			rowSize = slices.Max(extractIdxes) + 3
		}

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
					row = make([]any, rowSize)
				)
				row[0] = wrapped.name
				row[1] = wrapped.kind

				for _, colIdx := range extractIdxes {
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

		if displayIdxes != nil {
			// Column projection: build compact rows with only projected columns.
			for _, row := range rows {
				projRow := make([]any, len(displayIdxes)+2)
				projRow[0] = row[0]
				projRow[1] = row[1]
				for j, colIdx := range displayIdxes {
					projRow[j+2] = row[colIdx+2]
				}
				mrs.AddRow(projRow)
			}
		} else {
			for _, row := range rows {
				mrs.AddRow(row)
			}
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

	} else if stmt.OutputOpt.Summary {
		var (
			targetInsertCnt int64
			targetDeleteCnt int64
			targetUpdateCnt int64
			baseInsertCnt   int64
			baseDeleteCnt   int64
			baseUpdateCnt   int64
		)

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

			var (
				targetMetric *int64
				baseMetric   *int64
			)

			switch wrapped.kind {
			case diffInsert:
				targetMetric = &targetInsertCnt
				baseMetric = &baseInsertCnt
			case diffDelete:
				targetMetric = &targetDeleteCnt
				baseMetric = &baseDeleteCnt
			case diffUpdate:
				targetMetric = &targetUpdateCnt
				baseMetric = &baseUpdateCnt
			default:
				first = moerr.NewInternalErrorNoCtxf("unknown diff kind %q", wrapped.kind)
				cancel()
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}

			cnt := int64(wrapped.batch.RowCount())
			switch wrapped.side {
			case diffSideTarget:
				*targetMetric += cnt
			case diffSideBase:
				*baseMetric += cnt
			default:
				first = moerr.NewInternalErrorNoCtxf("unknown diff side for summary, kind=%q, table=%q", wrapped.kind, wrapped.name)
				cancel()
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}

			tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
		}

		if first != nil {
			return first
		}

		mrs.AddRow([]any{"INSERTED", targetInsertCnt, baseInsertCnt})
		mrs.AddRow([]any{"DELETED", targetDeleteCnt, baseDeleteCnt})
		mrs.AddRow([]any{"UPDATED", targetUpdateCnt, baseUpdateCnt})

	} else if len(stmt.OutputOpt.DirPath) != 0 {
		var (
			insertCnt int
			deleteCnt int

			deleteFromValsBuffer = acquireBuffer(tblStuff.bufPool)
			insertIntoValsBuffer = acquireBuffer(tblStuff.bufPool)
			tmpValsBuffer        = acquireBuffer(tblStuff.bufPool)

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
			releaseBuffer(tblStuff.bufPool, insertIntoValsBuffer)
			releaseBuffer(tblStuff.bufPool, tmpValsBuffer)
		}()

		if fullFilePath, fileHint, writeFile, release, cleanup, err = prepareFSForDiffAsFile(
			ctx, ses, stmt, tblStuff,
		); err != nil {
			return
		}

		appender := sqlValuesAppender{
			ctx:             ctx,
			ses:             ses,
			bh:              bh,
			tblStuff:        tblStuff,
			deleteByFullRow: tblStuff.def.pkKind == fakeKind,
			pkInfo:          newPKBatchInfo(ctx, ses, tblStuff),
			deleteCnt:       &deleteCnt,
			deleteBuf:       deleteFromValsBuffer,
			insertCnt:       &insertCnt,
			insertBuf:       insertIntoValsBuffer,
			writeFile:       writeFile,
		}
		if writeFile != nil {
			// Make generated SQL runnable in one transaction.
			if err = writeFile([]byte("BEGIN;\n")); err != nil {
				return
			}
		}
		if err = initPKTables(ctx, ses, bh, appender.pkInfo, appender.writeFile); err != nil {
			return
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
			} else if wrapped.name == tblStuff.baseRel.GetTableName() {
				if wrapped.kind == diffInsert {
					wrapped.kind = diffDelete
				}
				if wrapped.kind == diffDelete {
					if err = appendBatchRowsAsSQLValues(
						ctx, ses, tblStuff, wrapped, tmpValsBuffer, appender,
					); err != nil {
						first = err
						cancel()
						tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
						continue
					}
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
		if first == nil {
			if err = dropPKTables(ctx, ses, bh, appender.pkInfo, appender.writeFile); err != nil {
				return err
			}
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

		displayIdxes := tblStuff.def.visibleIdxes
		if stmt.Columns != nil {
			if displayIdxes, err = resolveProjectedIdxes(stmt.Columns, tblStuff); err != nil {
				return
			}
		}

		showCols = make([]*MysqlColumn, 0, 2)
		showCols = append(showCols, new(MysqlColumn), new(MysqlColumn))
		showCols[0].SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		showCols[0].SetName(fmt.Sprintf(
			"diff %s against %s", tblStuff.tarRel.GetTableName(), tblStuff.baseRel.GetTableName()),
		)
		showCols[1].SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		showCols[1].SetName("flag")

		for _, idx := range displayIdxes {
			nCol := new(MysqlColumn)
			if err = convertEngineTypeToMysqlType(ctx, tblStuff.def.colTypes[idx].Oid, nCol); err != nil {
				return
			}

			nCol.SetName(tblStuff.def.colNames[idx])
			showCols = append(showCols, nCol)
		}

	} else if stmt.OutputOpt.Summary {
		targetName := tree.StringWithOpts(&stmt.TargetTable, dialect.MYSQL, tree.WithSingleQuoteString())
		baseName := tree.StringWithOpts(&stmt.BaseTable, dialect.MYSQL, tree.WithSingleQuoteString())

		showCols = append(showCols, &MysqlColumn{}, &MysqlColumn{}, &MysqlColumn{})
		showCols[0].SetName("metric")
		showCols[0].SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		showCols[1].SetName(targetName)
		showCols[1].SetColumnType(defines.MYSQL_TYPE_LONGLONG)
		showCols[2].SetName(baseName)
		showCols[2].SetColumnType(defines.MYSQL_TYPE_LONGLONG)

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

	if tblStuff.baseSnap != nil && tblStuff.baseSnap.TS != nil {
		sql += fmt.Sprintf("{mo_ts=%d}", tblStuff.baseSnap.TS.PhysicalTime)
	}

	var (
		err    error
		sqlRet executor.Result
	)

	if sqlRet, err = runSql(ctx, ses, bh, sql, nil, nil); err != nil {
		return false, err
	}
	defer sqlRet.Close()

	ok, err := shouldDiffAsCSV(sqlRet)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	return true, writeCSV(ctx, ses, tblStuff, bh, stmt)
}

func shouldDiffAsCSV(sqlRet executor.Result) (bool, error) {
	if len(sqlRet.Batches) != 1 ||
		sqlRet.Batches[0] == nil ||
		sqlRet.Batches[0].RowCount() != 1 ||
		sqlRet.Batches[0].VectorCount() != 1 ||
		len(sqlRet.Batches[0].Vecs) != 1 ||
		sqlRet.Batches[0].Vecs[0] == nil {
		return false, moerr.NewInternalErrorNoCtxf("cannot get count(*) of base table")
	}

	return vector.GetFixedAtWithTypeCheck[uint64](sqlRet.Batches[0].Vecs[0], 0) == 0, nil
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

func writeDeleteRowSQLFull(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	row []any,
	buf *bytes.Buffer,
) error {
	// Use NULL-aware equality and LIMIT 1 to preserve duplicate-row semantics.
	buf.WriteString(fmt.Sprintf(
		"delete from %s.%s where ",
		tblStuff.baseRel.GetTableDef(ctx).DbName,
		tblStuff.baseRel.GetTableDef(ctx).Name,
	))
	for i, idx := range tblStuff.def.visibleIdxes {
		if i > 0 {
			buf.WriteString(" and ")
		}
		colName := tblStuff.def.colNames[idx]
		if row[idx] == nil {
			buf.WriteString(colName)
			buf.WriteString(" is null")
		} else {
			buf.WriteString(colName)
			buf.WriteString(" = ")
			if err := formatValIntoString(ses, row[idx], tblStuff.def.colTypes[idx], buf); err != nil {
				return err
			}
		}
	}
	buf.WriteString(" limit 1;\n")
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
			if rowIdx >= vec.Length() {
				return moerr.NewInternalErrorNoCtxf(
					"data branch output batch shape mismatch: row=%d batchRows=%d col=%d vecLen=%d type=%s",
					rowIdx, wrapped.batch.RowCount(), colIdx, vec.Length(), vec.GetType().String(),
				)
			}
			if vec.GetNulls().Contains(uint64(rowIdx)) {
				row[colIdx] = nil
				continue
			}

			switch vec.GetType().Oid {
			case types.T_datetime, types.T_timestamp, types.T_decimal64,
				types.T_decimal128, types.T_time:
				row[colIdx] = types.DecodeValue(vec.GetRawBytesAt(rowIdx), vec.GetType().Oid)
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
			if appender.deleteByFullRow {
				if err = writeDeleteRowSQLFull(ctx, ses, tblStuff, row, tmpValsBuffer); err != nil {
					return
				}
			} else if appender.pkInfo != nil {
				if err = writeDeleteRowValuesAsTuple(ses, tblStuff, row, tmpValsBuffer); err != nil {
					return
				}
			} else {
				if err = writeDeleteRowValues(ses, tblStuff, row, tmpValsBuffer); err != nil {
					return
				}
			}
		} else {
			if err = writeInsertRowValues(ses, tblStuff, row, tmpValsBuffer); err != nil {
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
		"DELETE FROM %s.%s, INSERT INTO %s.%s",
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

func tryFlushDeletesOrInserts(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tblStuff tableStuff,
	newKind string,
	newValsLen int,
	newRowCnt int,
	deleteByFullRow bool,
	pkInfo *pkBatchInfo,
	deleteCnt *int,
	deletesBuf *bytes.Buffer,
	insertCnt *int,
	insertBuf *bytes.Buffer,
	writeFile func([]byte) error,
) (err error) {

	flushDeletes := func() error {
		if err = flushSqlValues(
			ctx, ses, bh, tblStuff, deletesBuf, true, deleteByFullRow, pkInfo, writeFile,
		); err != nil {
			return err
		}

		*deleteCnt = 0
		deletesBuf.Reset()
		return nil
	}

	flushInserts := func() error {
		if err = flushSqlValues(
			ctx, ses, bh, tblStuff, insertBuf, false, false, pkInfo, writeFile,
		); err != nil {
			return err
		}

		*insertCnt = 0
		insertBuf.Reset()
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
			if insertBuf.Len()+newValsLen >= maxSqlBatchSize ||
				*insertCnt+newRowCnt >= maxSqlBatchCnt {
				if *deleteCnt > 0 {
					if err = flushDeletes(); err != nil {
						return err
					}
				}
				return flushInserts()
			}
		}
		return nil
	}

	if *deleteCnt > 0 {
		if err = flushDeletes(); err != nil {
			return err
		}
	}

	if *insertCnt > 0 {
		if err = flushInserts(); err != nil {
			return err
		}
	}

	return nil
}

type pkBatchInfo struct {
	dbName       string
	baseTable    string
	deleteTable  string
	insertTable  string
	pkNames      []string
	visibleNames []string
}

func newPKBatchInfo(ctx context.Context, ses *Session, tblStuff tableStuff) *pkBatchInfo {
	if tblStuff.def.pkKind == fakeKind {
		return nil
	}

	pkNames := make([]string, len(tblStuff.def.pkColIdxes))
	for i, idx := range tblStuff.def.pkColIdxes {
		pkNames[i] = tblStuff.def.colNames[idx]
	}

	visibleNames := make([]string, len(tblStuff.def.visibleIdxes))
	for i, idx := range tblStuff.def.visibleIdxes {
		visibleNames[i] = tblStuff.def.colNames[idx]
	}

	seq := atomic.AddUint64(&diffTempTableSeq, 1)
	sessionTag := strings.ReplaceAll(ses.GetUUIDString(), "-", "")
	return &pkBatchInfo{
		dbName:       tblStuff.baseRel.GetTableDef(ctx).DbName,
		baseTable:    tblStuff.baseRel.GetTableName(),
		deleteTable:  fmt.Sprintf("__mo_diff_del_%s_%d", sessionTag, seq),
		insertTable:  fmt.Sprintf("__mo_diff_ins_%s_%d", sessionTag, seq),
		pkNames:      pkNames,
		visibleNames: visibleNames,
	}
}

type sqlValuesAppender struct {
	ctx             context.Context
	ses             *Session
	bh              BackgroundExec
	tblStuff        tableStuff
	deleteByFullRow bool
	pkInfo          *pkBatchInfo
	deleteCnt       *int
	deleteBuf       *bytes.Buffer
	insertCnt       *int
	insertBuf       *bytes.Buffer
	writeFile       func([]byte) error
}

func (sva sqlValuesAppender) flushAll() error {
	return tryFlushDeletesOrInserts(
		sva.ctx, sva.ses, sva.bh, sva.tblStuff, "",
		0, 0, sva.deleteByFullRow, sva.pkInfo, sva.deleteCnt, sva.deleteBuf, sva.insertCnt, sva.insertBuf, sva.writeFile,
	)
}

func writeInsertRowValues(
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

func writeDeleteRowValuesAsTuple(
	ses *Session,
	tblStuff tableStuff,
	row []any,
	buf *bytes.Buffer,
) error {
	buf.WriteString("(")
	for i, colIdx := range tblStuff.def.pkColIdxes {
		if err := formatValIntoString(ses, row[colIdx], tblStuff.def.colTypes[colIdx], buf); err != nil {
			return err
		}
		if i != len(tblStuff.def.pkColIdxes)-1 {
			buf.WriteString(",")
		}
	}
	buf.WriteString(")")
	return nil
}

func (sva sqlValuesAppender) appendRow(kind string, rowValues []byte) error {
	var (
		targetBuf *bytes.Buffer
		rowCnt    *int
	)

	if kind == diffDelete {
		targetBuf = sva.deleteBuf
		rowCnt = sva.deleteCnt
		if sva.deleteByFullRow {
			newValsLen := len(rowValues)
			if err := tryFlushDeletesOrInserts(
				sva.ctx, sva.ses, sva.bh, sva.tblStuff, kind, newValsLen, 1, sva.deleteByFullRow,
				sva.pkInfo, sva.deleteCnt, sva.deleteBuf, sva.insertCnt, sva.insertBuf, sva.writeFile,
			); err != nil {
				return err
			}

			targetBuf.Write(rowValues)
			*rowCnt++
			return nil
		}
	} else {
		targetBuf = sva.insertBuf
		rowCnt = sva.insertCnt
	}

	newValsLen := len(rowValues)
	if targetBuf.Len() > 0 {
		newValsLen++
	}

	if err := tryFlushDeletesOrInserts(
		sva.ctx, sva.ses, sva.bh, sva.tblStuff, kind, newValsLen, 1, sva.deleteByFullRow,
		sva.pkInfo, sva.deleteCnt, sva.deleteBuf, sva.insertCnt, sva.insertBuf, sva.writeFile,
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

func qualifiedTableName(dbName, tableName string) string {
	return fmt.Sprintf("%s.%s", dbName, tableName)
}

func execSQLStatements(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	writeFile func([]byte) error,
	stmts []string,
) error {
	for _, stmt := range stmts {
		if stmt == "" {
			continue
		}
		if writeFile != nil {
			if err := writeFile([]byte(stmt + ";\n")); err != nil {
				return err
			}
			continue
		}
		ret, err := runSql(ctx, ses, bh, stmt, nil, nil)
		if len(ret.Batches) > 0 && ret.Mp != nil {
			ret.Close()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func initPKTables(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	pkInfo *pkBatchInfo,
	writeFile func([]byte) error,
) error {
	if pkInfo == nil {
		return nil
	}

	baseTable := qualifiedTableName(pkInfo.dbName, pkInfo.baseTable)
	deleteTable := qualifiedTableName(pkInfo.dbName, pkInfo.deleteTable)
	insertTable := qualifiedTableName(pkInfo.dbName, pkInfo.insertTable)

	deleteCols := strings.Join(pkInfo.pkNames, ",")
	insertCols := strings.Join(pkInfo.visibleNames, ",")

	stmts := []string{
		fmt.Sprintf("drop table if exists %s", deleteTable),
		fmt.Sprintf("drop table if exists %s", insertTable),
		fmt.Sprintf("create table %s as select %s from %s where 1=0", deleteTable, deleteCols, baseTable),
		fmt.Sprintf("create table %s as select %s from %s where 1=0", insertTable, insertCols, baseTable),
	}

	return execSQLStatements(ctx, ses, bh, writeFile, stmts)
}

func dropPKTables(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	pkInfo *pkBatchInfo,
	writeFile func([]byte) error,
) error {
	if pkInfo == nil {
		return nil
	}

	deleteTable := qualifiedTableName(pkInfo.dbName, pkInfo.deleteTable)
	insertTable := qualifiedTableName(pkInfo.dbName, pkInfo.insertTable)

	stmts := []string{
		fmt.Sprintf("drop table if exists %s", deleteTable),
		fmt.Sprintf("drop table if exists %s", insertTable),
	}
	return execSQLStatements(ctx, ses, bh, writeFile, stmts)
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
	deleteByFullRow bool,
	pkInfo *pkBatchInfo,
	writeFile func([]byte) error,
) (err error) {

	if buf.Len() == 0 {
		return nil
	}

	if isDeleteFrom && deleteByFullRow {
		if writeFile != nil {
			return writeFile(buf.Bytes())
		}

		statements := bytes.Split(buf.Bytes(), []byte(";\n"))
		for _, stmt := range statements {
			stmt = bytes.TrimSpace(stmt)
			if len(stmt) == 0 {
				continue
			}
			var ret executor.Result
			ret, err = runSql(ctx, ses, bh, string(stmt)+";", nil, nil)
			ret.Close()
			if err != nil {
				return err
			}
		}
		return nil
	}

	if pkInfo != nil {
		baseTable := qualifiedTableName(pkInfo.dbName, pkInfo.baseTable)
		deleteTable := qualifiedTableName(pkInfo.dbName, pkInfo.deleteTable)
		insertTable := qualifiedTableName(pkInfo.dbName, pkInfo.insertTable)

		if isDeleteFrom {
			insertStmt := fmt.Sprintf("insert into %s values %s", deleteTable, buf.String())
			pkExpr := pkInfo.pkNames[0]
			if len(pkInfo.pkNames) > 1 {
				pkExpr = fmt.Sprintf("(%s)", strings.Join(pkInfo.pkNames, ","))
			}
			deleteStmt := fmt.Sprintf(
				"delete from %s where %s in (select %s from %s)",
				baseTable, pkExpr, strings.Join(pkInfo.pkNames, ","), deleteTable,
			)
			clearStmt := fmt.Sprintf("delete from %s", deleteTable)
			return execSQLStatements(ctx, ses, bh, writeFile, []string{insertStmt, deleteStmt, clearStmt})
		}

		insertStmt := fmt.Sprintf("insert into %s values %s", insertTable, buf.String())
		cols := strings.Join(pkInfo.visibleNames, ",")
		applyStmt := fmt.Sprintf(
			"insert into %s (%s) select %s from %s",
			baseTable, cols, cols, insertTable,
		)
		clearStmt := fmt.Sprintf("delete from %s", insertTable)
		return execSQLStatements(ctx, ses, bh, writeFile, []string{insertStmt, applyStmt, clearStmt})
	}

	sqlBuffer := acquireBuffer(tblStuff.bufPool)
	defer releaseBuffer(tblStuff.bufPool, sqlBuffer)

	initInsertIntoBuf := func() {
		sqlBuffer.WriteString(fmt.Sprintf(
			"insert into %s.%s values ",
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
		initInsertIntoBuf()
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
