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
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"go.uber.org/zap"
)

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
		sqlRet executor.Result

		lcaTblDef  = tblStuff.lcaRel.GetTableDef(ctx)
		baseTblDef = tblStuff.baseRel.GetTableDef(ctx)

		colTypes           = tblStuff.def.colTypes
		expandedPKColIdxes = tblStuff.def.pkColIdxes
		snapshotTS         = types.TimestampToTS(snapshot)
	)

	forceReaderProbe := tblStuff.lcaReaderProbeMode != nil && tblStuff.lcaReaderProbeMode.Load()
	if forceReaderProbe {
		sqlRet, err = runLCAProbeWithReaderFallback(ctx, ses, tBat, tblStuff, snapshotTS)
		if err != nil {
			return nil, err
		}
	} else {
		sqlBuf := acquireBuffer(tblStuff.bufPool)
		valsBuf := acquireBuffer(tblStuff.bufPool)
		defer func() {
			releaseBuffer(tblStuff.bufPool, sqlBuf)
			releaseBuffer(tblStuff.bufPool, valsBuf)
		}()

		// Keep SQL as the fast path because it is much cheaper than reader-based
		// probing. However, this path still relies on name-based time travelling
		// like db.table{MO_TS=...}. After GC, if no snapshot or PITR history was
		// created for the corresponding account/db/table, catalog lookup at that
		// timestamp may fail with unknown db/table even though the caller already
		// knows the stable table ID.
		mots := fmt.Sprintf("{MO_TS=%d} ", snapshot.PhysicalTime)
		pkNames := lcaTblDef.Pkey.Names

		// composite pk
		if baseTblDef.Pkey.CompPkeyCol != nil {
			var tuple types.Tuple
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
		} else if baseTblDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
			// fake pk
			pks := vector.MustFixedColNoTypeCheck[uint64](tBat.Vecs[0])
			for i := range pks {
				valsBuf.WriteString(fmt.Sprintf("row(%d,%d)", i, pks[i]))
				if i != len(pks)-1 {
					valsBuf.WriteString(", ")
				}
			}
		} else {
			// real pk
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

		if sqlRet, err = runSql(ctx, ses, bh, sqlBuf.String(), nil, nil); err != nil {
			logutil.Error(
				"DataBranch-LCA-SQL-Error",
				zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
				zap.String("db-name", lcaTblDef.DbName),
				zap.String("table-name", lcaTblDef.Name),
				zap.Int("input-pk-rows", tBat.RowCount()),
				zap.String("snapshot-ts", snapshotTS.ToString()),
				zap.Error(err),
			)
			if shouldUseLCAReaderFallback(err) {
				if tblStuff.lcaReaderProbeMode != nil {
					tblStuff.lcaReaderProbeMode.Store(true)
				}
				logutil.Info(
					"DataBranch-LCA-SQL-Fallback-Start",
					zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
					zap.String("table-name", lcaTblDef.Name),
					zap.Int("input-pk-rows", tBat.RowCount()),
					zap.String("snapshot-ts", snapshotTS.ToString()),
					zap.Error(err),
				)
				sqlRet, err = runLCAProbeWithReaderFallback(ctx, ses, tBat, tblStuff, snapshotTS)
				if err != nil {
					logutil.Error(
						"DataBranch-LCA-SQL-Fallback-Error",
						zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
						zap.String("table-name", lcaTblDef.Name),
						zap.Int("input-pk-rows", tBat.RowCount()),
						zap.String("snapshot-ts", snapshotTS.ToString()),
						zap.Error(err),
					)
					return nil, err
				}
				logutil.Info(
					"DataBranch-LCA-SQL-Fallback-Done",
					zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
					zap.String("table-name", lcaTblDef.Name),
					zap.Int("input-pk-rows", tBat.RowCount()),
					zap.Int("fallback-batches", len(sqlRet.Batches)),
				)
			} else {
				return nil, err
			}
		}
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

			// For composite/fake PK tables, the hidden PK column (__cpkey__
			// or __mo_fake_pk_col) may not be returned by SQL "select *".
			// Fill it from the tombstone batch when the loop above didn't
			// cover endIdx.  The reader fallback returns ALL columns so the
			// loop already fills endIdx — skip the extra append to avoid
			// doubling Vecs[endIdx].Length() vs RowCount().
			if tblStuff.def.pkKind != normalKind && len(cols)-2 < endIdx {
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

// runLCAProbeWithReaderFallback reconstructs the same row shape as
// "select pks.__idx_, lca.* ... right join(values ...)" using snapshot readers.
// It returns an executor.Result so handleDelsOnLCA can reuse the same post-SQL
// processing logic regardless of whether SQL or reader fallback produced rows.
func runLCAProbeWithReaderFallback(
	ctx context.Context,
	ses *Session,
	tBat *batch.Batch,
	tblStuff tableStuff,
	snapshotTS types.TS,
) (sqlRet executor.Result, err error) {
	start := time.Now()
	sqlRet.Mp = ses.proc.Mp()
	if tBat == nil {
		return sqlRet, nil
	}
	rowCount := tBat.RowCount()
	if rowCount == 0 {
		bat := batch.NewWithSize(len(tblStuff.def.colNames) + 1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		for i, typ := range tblStuff.def.colTypes {
			bat.Vecs[i+1] = vector.NewVec(typ)
		}
		bat.SetRowCount(0)
		sqlRet.Batches = []*batch.Batch{bat}
		return sqlRet, nil
	}

	mp := ses.proc.Mp()
	pkVec := tBat.Vecs[0]
	var (
		prepareCost        time.Duration
		scanCost           time.Duration
		outputCost         time.Duration
		callbackCost       time.Duration
		callbackFilterCost time.Duration
		callbackUnionCost  time.Duration
	)
	inputKeys := make(map[string]struct{}, rowCount)
	for i := 0; i < rowCount; i++ {
		inputKeys[string(pkVec.GetRawBytesAt(i))] = struct{}{}
	}
	lcaTblDef := tblStuff.lcaRel.GetTableDef(ctx)
	// Build a sorted IN vector for reader-side PK filtering.
	// The sorted-search path uses binary search over the IN value array and
	// assumes the array is ordered; an unsorted IN vector can drop valid hits.
	filterVec := vector.NewVec(*pkVec.GetType())
	defer filterVec.Free(mp)
	if err = filterVec.UnionBatch(pkVec, 0, rowCount, nil, mp); err != nil {
		return executor.Result{}, err
	}
	filterVec.InplaceSort()
	pkFilterExpr := readutil.ConstructInExpr(ctx, lcaTblDef.Pkey.PkeyColName, filterVec)
	prepareCost = time.Since(start)

	tmp := batch.NewWithSize(len(tblStuff.def.colNames))
	for i, typ := range tblStuff.def.colTypes {
		tmp.Vecs[i] = vector.NewVec(typ)
	}
	defer tmp.Clean(mp)
	tmpRowByKey := make(map[string]int, rowCount)
	var callbackMu sync.Mutex
	var (
		scannedBatchCnt int
		scannedRowCnt   int
		acceptedRowCnt  int
		duplicateRows   int
	)
	logutil.Info(
		"DataBranch-LCA-ReaderFallback-ScanStart",
		zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
		zap.String("table-name", lcaTblDef.Name),
		zap.Int("input-pk-rows", rowCount),
		zap.Int("input-unique-keys", len(inputKeys)),
		zap.String("snapshot-ts", snapshotTS.ToString()),
	)

	defer func() {
		fields := []zap.Field{
			zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
			zap.String("table-name", lcaTblDef.Name),
			zap.Int("input-pk-rows", rowCount),
			zap.Int("input-unique-keys", len(inputKeys)),
			zap.Int("scan-batch-cnt", scannedBatchCnt),
			zap.Int("scan-row-cnt", scannedRowCnt),
			zap.Int("accepted-row-cnt", acceptedRowCnt),
			zap.Int("dedup-row-cnt", duplicateRows),
			zap.Int("accepted-key-cnt", len(tmpRowByKey)),
			zap.Duration("prepare-cost", prepareCost),
			zap.Duration("scan-cost", scanCost),
			zap.Duration("callback-cost", callbackCost),
			zap.Duration("callback-filter-cost", callbackFilterCost),
			zap.Duration("callback-union-cost", callbackUnionCost),
			zap.Duration("output-cost", outputCost),
			zap.Duration("total-cost", time.Since(start)),
		}
		if err != nil {
			fields = append(fields, zap.Error(err))
			logutil.Warn("DataBranch-LCA-ReaderFallback-Error", fields...)
			return
		}
		logutil.Info("DataBranch-LCA-ReaderFallback-Done", fields...)
	}()

	scanStart := time.Now()
	err = scanSnapshotRelationByID(
		ctx,
		"lca-reader-fallback",
		ses,
		tblStuff.lcaRel.GetTableID(ctx),
		snapshotTS,
		tblStuff.def.colNames,
		tblStuff.def.colTypes,
		pkFilterExpr,
		0,
		func(readBatch *batch.Batch) error {
			callbackStart := time.Now()
			callbackMu.Lock()
			defer func() {
				callbackCost += time.Since(callbackStart)
				callbackMu.Unlock()
			}()
			scannedBatchCnt++
			scannedRowCnt += readBatch.RowCount()
			readPK := readBatch.Vecs[tblStuff.def.pkColIdx]
			sels := make([]int64, 0, readBatch.RowCount())
			keys := make([]string, 0, readBatch.RowCount())
			filterStart := time.Now()
			for row := 0; row < readBatch.RowCount(); row++ {
				key := string(readPK.GetRawBytesAt(row))
				if _, ok := inputKeys[key]; !ok {
					continue
				}
				if _, exists := tmpRowByKey[key]; exists {
					duplicateRows++
					continue
				}
				sels = append(sels, int64(row))
				keys = append(keys, key)
			}
			callbackFilterCost += time.Since(filterStart)
			if len(sels) == 0 {
				return nil
			}
			base := tmp.Vecs[0].Length()
			unionStart := time.Now()
			for colIdx := range tblStuff.def.colNames {
				if err = tmp.Vecs[colIdx].Union(readBatch.Vecs[colIdx], sels, mp); err != nil {
					return err
				}
			}
			for i, key := range keys {
				tmpRowByKey[key] = base + i
			}
			acceptedRowCnt += len(sels)
			callbackUnionCost += time.Since(unionStart)
			return nil
		},
	)
	scanCost = time.Since(scanStart)
	if err != nil {
		return executor.Result{}, err
	}
	logutil.Info(
		"DataBranch-LCA-ReaderFallback-ScanDone",
		zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
		zap.String("table-name", lcaTblDef.Name),
		zap.Int("scan-batch-cnt", scannedBatchCnt),
		zap.Int("scan-row-cnt", scannedRowCnt),
		zap.Int("accepted-row-cnt", acceptedRowCnt),
		zap.Int("dedup-row-cnt", duplicateRows),
		zap.Int("accepted-key-cnt", len(tmpRowByKey)),
	)

	out := batch.NewWithSize(len(tblStuff.def.colNames) + 1)
	out.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	for i, typ := range tblStuff.def.colTypes {
		out.Vecs[i+1] = vector.NewVec(typ)
	}
	defer func() {
		if err != nil {
			out.Clean(mp)
		}
	}()

	outputStart := time.Now()
	orderedIdx := make([]int64, rowCount)
	sels := make([]int64, rowCount)
	missedRows := make([]uint64, 0)
	hasAnyHit := false
	for i := 0; i < rowCount; i++ {
		orderedIdx[i] = int64(i)
		if rowIdx, ok := tmpRowByKey[string(pkVec.GetRawBytesAt(i))]; ok {
			sels[i] = int64(rowIdx)
			hasAnyHit = true
		} else {
			missedRows = append(missedRows, uint64(i))
		}
	}

	if err = vector.AppendFixedList(out.Vecs[0], orderedIdx, nil, mp); err != nil {
		return executor.Result{}, err
	}

	if !hasAnyHit {
		for colIdx := range tblStuff.def.colNames {
			nullConst := vector.NewConstNull(tblStuff.def.colTypes[colIdx], rowCount, mp)
			err = out.Vecs[colIdx+1].UnionBatch(nullConst, 0, rowCount, nil, mp)
			nullConst.Free(mp)
			if err != nil {
				return executor.Result{}, err
			}
		}
	} else if len(missedRows) == 0 {
		for colIdx := range tblStuff.def.colNames {
			if err = out.Vecs[colIdx+1].UnionBatch(tmp.Vecs[colIdx], 0, rowCount, nil, mp); err != nil {
				return executor.Result{}, err
			}
		}
	} else {
		for colIdx := range tblStuff.def.colNames {
			if err = out.Vecs[colIdx+1].Union(tmp.Vecs[colIdx], sels, mp); err != nil {
				return executor.Result{}, err
			}
			for _, rowIdx := range missedRows {
				nulls.Add(out.Vecs[colIdx+1].GetNulls(), rowIdx)
			}
		}
	}
	out.SetRowCount(rowCount)
	sqlRet.Batches = []*batch.Batch{out}
	outputCost = time.Since(outputStart)
	logutil.Info(
		"DataBranch-LCA-ReaderFallback-Output",
		zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
		zap.String("table-name", lcaTblDef.Name),
		zap.Int("output-rows", rowCount),
		zap.Bool("has-any-hit", hasAnyHit),
		zap.Int("missed-rows", len(missedRows)),
		zap.Int("matched-keys", len(tmpRowByKey)),
	)
	return sqlRet, nil
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
	start := time.Now()
	var (
		buildBaseCost   time.Duration
		buildTargetCost time.Duration
		diffCost        time.Duration
	)

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

		if err != nil {
			logutil.Warn("DataBranch-HashDiff-Error",
				zap.Int("base-handle-cnt", len(baseHandle)),
				zap.Int("target-handle-cnt", len(tarHandle)),
				zap.Int("lca-type", dagInfo.lcaType),
				zap.Duration("build-base-cost", buildBaseCost),
				zap.Duration("build-target-cost", buildTargetCost),
				zap.Duration("diff-cost", diffCost),
				zap.Duration("total-cost", time.Since(start)),
				zap.Error(err),
			)
		}
	}()

	buildBaseStart := time.Now()
	if baseDataHashmap, baseTombstoneHashmap, err = buildHashmapForTable(
		ctx, ses.proc.Mp(), dagInfo.lcaType, &tblStuff, baseHandle, "base",
	); err != nil {
		return
	}
	buildBaseCost = time.Since(buildBaseStart)

	buildTargetStart := time.Now()
	if tarDataHashmap, tarTombstoneHashmap, err = buildHashmapForTable(
		ctx, ses.proc.Mp(), dagInfo.lcaType, &tblStuff, tarHandle, "target",
	); err != nil {
		return
	}
	buildTargetCost = time.Since(buildTargetStart)

	diffStart := time.Now()
	if dagInfo.lcaType == lcaEmpty {
		if err = hashDiffIfNoLCA(
			ctx, ses, tblStuff, dagInfo.lcaType, copt, emit,
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
	diffCost = time.Since(diffStart)

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
		wrapped.side = diffSideBase
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
		wrapped.side = diffSideTarget
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
				func() int {
					if forBase {
						return diffSideBase
					}
					return diffSideTarget
				}(),
				tmpCh, branchTS, copt.expandUpdate, hashmap1, hashmap2,
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

	return diffDataHelper(ctx, ses, dagInfo.lcaType, copt, tblStuff, emit, tarDataHashmap, baseDataHashmap)
}

func hashDiffIfNoLCA(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	lcaType int,
	copt compositeOption,
	emit emitFunc,
	tarDataHashmap databranchutils.BranchHashmap,
	tarTombstoneHashmap databranchutils.BranchHashmap,
	baseDataHashmap databranchutils.BranchHashmap,
	baseTombstoneHashmap databranchutils.BranchHashmap,
) (err error) {

	if err = tarTombstoneHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		return cursor.ForEach(func(key []byte, _ []byte) error {
			_, err2 := tarDataHashmap.PopByEncodedKey(key, true)
			return err2
		})

	}, -1); err != nil {
		return
	}

	if err = baseTombstoneHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		return cursor.ForEach(func(key []byte, _ []byte) error {
			_, err2 := baseDataHashmap.PopByEncodedKey(key, true)
			return err2
		})

	}, -1); err != nil {
		return
	}

	return diffDataHelper(ctx, ses, lcaType, copt, tblStuff, emit, tarDataHashmap, baseDataHashmap)
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
	tblStuff tableStuff, tblName string, side int, tmpCh chan batchWithKind, branchTS types.TS,
	expandUpdate bool,
	dataHashmap, tombstoneHashmap databranchutils.BranchHashmap,
) (err error) {

	maxTombstoneBatchCnt := tblStuff.maxTombstoneBatchCnt
	if maxTombstoneBatchCnt <= 0 {
		maxTombstoneBatchCnt = maxSqlBatchCnt
	}

	if err = tombstoneHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		var (
			err2  error
			tuple types.Tuple
			tBat1 = tblStuff.retPool.acquireRetBatch(tblStuff, true)
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

		processBatch := func(tombBat *batch.Batch) error {
			if tombBat.RowCount() == 0 {
				tblStuff.retPool.releaseRetBatch(tombBat, true)
				return nil
			}

			dBat, err2 := handleDelsOnLCA(
				ctx, ses, bh, tombBat, tblStuff, branchTS.ToTimestamp(),
			)
			if err2 != nil {
				tblStuff.retPool.releaseRetBatch(tombBat, true)
				return err2
			}

			// merge inserts and deletes on the tar
			// this deletes is not on the lca
			if tombBat.RowCount() > 0 {
				if _, err2 = dataHashmap.PopByVectorsStream(
					[]*vector.Vector{tombBat.Vecs[0]}, false, nil,
				); err2 != nil {
					tblStuff.retPool.releaseRetBatch(tombBat, true)
					tblStuff.retPool.releaseRetBatch(dBat, false)
					return err2
				}
				tblStuff.retPool.releaseRetBatch(tombBat, true)
			}

			// find update
			if dBat.RowCount() > 0 {
				pkVecLen := dBat.Vecs[tblStuff.def.pkColIdx].Length()
				if pkVecLen != dBat.RowCount() {
					logutil.Error(
						"DataBranch-findDeleteAndUpdateBat-VecLenMismatch",
						zap.String("table-name", tblName),
						zap.Int("dBat-row-count", dBat.RowCount()),
						zap.Int("pkVec-length", pkVecLen),
						zap.Int("pkColIdx", tblStuff.def.pkColIdx),
						zap.Int("vec-count", dBat.VectorCount()),
					)
				}
				tBat2 := tblStuff.retPool.acquireRetBatch(tblStuff, false)
				seen := make([]bool, dBat.RowCount())
				var updateBat *batch.Batch
				var updateDeleteBat *batch.Batch
				if _, err2 = dataHashmap.PopByVectorsStream(
					[]*vector.Vector{dBat.Vecs[tblStuff.def.pkColIdx]}, false,
					func(idx int, _ []byte, row []byte) error {
						seen[idx] = true
						// delete on lca and insert into tar ==> update
						if updateBat == nil {
							updateBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
						}
						if expandUpdate && updateDeleteBat == nil {
							updateDeleteBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
						}

						if tuple, _, err2 = dataHashmap.DecodeRow(row); err2 != nil {
							return err2
						}

						if expandUpdate {
							if err2 = updateDeleteBat.UnionOne(dBat, int64(idx), ses.proc.Mp()); err2 != nil {
								return err2
							}
						}

						if err2 = appendTupleToBat(ses, updateBat, tuple, tblStuff); err2 != nil {
							return err2
						}
						return nil
					},
				); err2 != nil {
					tblStuff.retPool.releaseRetBatch(dBat, false)
					tblStuff.retPool.releaseRetBatch(tBat2, false)
					if updateBat != nil {
						tblStuff.retPool.releaseRetBatch(updateBat, false)
					}
					if updateDeleteBat != nil {
						tblStuff.retPool.releaseRetBatch(updateDeleteBat, false)
					}
					return err2
				}

				for i := 0; i < dBat.RowCount(); i++ {
					if seen[i] {
						continue
					}
					// delete on lca
					if err2 = tBat2.UnionOne(dBat, int64(i), ses.proc.Mp()); err2 != nil {
						tblStuff.retPool.releaseRetBatch(dBat, false)
						tblStuff.retPool.releaseRetBatch(tBat2, false)
						if updateBat != nil {
							tblStuff.retPool.releaseRetBatch(updateBat, false)
						}
						if updateDeleteBat != nil {
							tblStuff.retPool.releaseRetBatch(updateDeleteBat, false)
						}
						return err2
					}
				}

				tblStuff.retPool.releaseRetBatch(dBat, false)
				tBat2.SetRowCount(tBat2.Vecs[0].Length())

				if updateBat != nil {
					updateBat.SetRowCount(updateBat.Vecs[0].Length())
					if expandUpdate {
						if updateDeleteBat != nil {
							updateDeleteBat.SetRowCount(updateDeleteBat.Vecs[0].Length())
							if err2 = send(batchWithKind{
								name:  tblName,
								side:  side,
								batch: updateDeleteBat,
								kind:  diffDelete,
							}); err2 != nil {
								return err2
							}
						}
						if err2 = send(batchWithKind{
							name:  tblName,
							side:  side,
							batch: updateBat,
							kind:  diffInsert,
						}); err2 != nil {
							return err2
						}
					} else {
						if err2 = send(batchWithKind{
							name:  tblName,
							side:  side,
							batch: updateBat,
							kind:  diffUpdate,
						}); err2 != nil {
							return err2
						}
					}
				}

				if err2 = send(batchWithKind{
					name:  tblName,
					side:  side,
					batch: tBat2,
					kind:  diffDelete,
				}); err2 != nil {
					return err2
				}
				return nil
			}

			tblStuff.retPool.releaseRetBatch(dBat, false)
			return nil
		}

		if err2 = cursor.ForEach(func(key []byte, _ []byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if tuple, _, err2 = tombstoneHashmap.DecodeRow(key); err2 != nil {
				return err2
			}
			if err2 = vector.AppendAny(tBat1.Vecs[0], tuple[0], false, ses.proc.Mp()); err2 != nil {
				return err2
			}

			tBat1.SetRowCount(tBat1.Vecs[0].Length())
			if tBat1.RowCount() >= maxTombstoneBatchCnt {
				tombBat := tBat1
				tBat1 = nil
				if err2 = processBatch(tombBat); err2 != nil {
					return err2
				}
				tBat1 = tblStuff.retPool.acquireRetBatch(tblStuff, true)
			}
			return nil
		}); err2 != nil {
			if tBat1 != nil {
				tblStuff.retPool.releaseRetBatch(tBat1, true)
			}
			return err2
		}

		if tBat1 != nil {
			return processBatch(tBat1)
		}
		return nil

	}, -1); err != nil {
		return
	}

	return nil
}

func appendTupleToBat(ses *Session, bat *batch.Batch, tuple types.Tuple, tblStuff tableStuff) error {
	limit := len(tuple)
	if limit == bat.VectorCount()+1 {
		// Some change-collection paths keep commit ts in the encoded payload while
		// others only materialize visible columns. Trim the trailing commit ts only
		// when it is actually present.
		limit--
	}
	if limit != bat.VectorCount() {
		return moerr.NewInternalErrorNoCtxf(
			"unexpected tuple width %d for batch with %d vectors on table %s",
			len(tuple), bat.VectorCount(), tblStuff.tarRel.GetTableName(),
		)
	}
	for j, val := range tuple[:limit] {
		vec := bat.Vecs[j]
		if err := appendTupleValueToVector(vec, val, ses.proc.Mp()); err != nil {
			return err
		}
	}

	bat.SetRowCount(bat.Vecs[0].Length())

	return nil
}

func appendTupleValueToVector(vec *vector.Vector, val any, mp *mpool.MPool) error {
	if val == nil {
		return vector.AppendAny(vec, nil, true, mp)
	}
	if raw, ok := val.([]byte); ok {
		if !vec.GetType().IsVarlen() {
			return moerr.NewInternalErrorNoCtxf(
				"unexpected byte slice for fixed-width column type %s",
				vec.GetType().String(),
			)
		}
		return vector.AppendBytes(vec, raw, false, mp)
	}
	return vector.AppendAny(vec, val, false, mp)
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
	lcaType int,
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
			err2          error
			tarBat        *batch.Batch
			baseBat       *batch.Batch
			baseDeleteBat *batch.Batch
			tarTuple      types.Tuple
			baseTuple     types.Tuple
			checkRet      databranchutils.GetResult
		)

		tarBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
		baseBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)

		if err2 = cursor.ForEach(func(key []byte, row []byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

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
						if copt.conflictOpt != nil &&
							copt.conflictOpt.Opt == tree.CONFLICT_ACCEPT &&
							copt.expandUpdate {
							if baseDeleteBat == nil {
								baseDeleteBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
							}
							if err2 = appendTupleToBat(ses, baseDeleteBat, baseTuple, tblStuff); err2 != nil {
								return err2
							}
							if err2 = appendTupleToBat(ses, tarBat, tarTuple, tblStuff); err2 != nil {
								return err2
							}
						} else {
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

		if baseDeleteBat != nil {
			if stop, err3 := emitBatch(emit, batchWithKind{
				batch: baseDeleteBat,
				kind:  diffDelete,
				name:  tblStuff.baseRel.GetTableName(),
				side:  diffSideBase,
			}, false, tblStuff.retPool); err3 != nil {
				return err3
			} else if stop {
				return nil
			}
		}

		if stop, err3 := emitBatch(emit, batchWithKind{
			batch: tarBat,
			kind:  diffInsert,
			name:  tblStuff.tarRel.GetTableName(),
			side:  diffSideTarget,
		}, false, tblStuff.retPool); err3 != nil {
			return err3
		} else if stop {
			return nil
		}

		if stop, err3 := emitBatch(emit, batchWithKind{
			batch: baseBat,
			kind:  diffInsert,
			name:  tblStuff.baseRel.GetTableName(),
			side:  diffSideBase,
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
		if !copt.outputSQL {
			// merge doesnt need the base data
			return nil
		}
	}

	if err = baseDataHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		var (
			err2  error
			tuple types.Tuple
			bat   *batch.Batch
		)

		bat = tblStuff.retPool.acquireRetBatch(tblStuff, false)

		if err2 = cursor.ForEach(func(_ []byte, row []byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if tuple, _, err2 = baseDataHashmap.DecodeRow(row); err2 != nil {
				return err2
			}

			if err2 = appendTupleToBat(ses, bat, tuple, tblStuff); err2 != nil {
				return err2
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
			side:  diffSideBase,
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
func buildHashmapForTable(
	ctx context.Context,
	mp *mpool.MPool,
	lcaType int,
	tblStuff *tableStuff,
	handles []engine.ChangesHandle,
	side string,
) (
	dataHashmap databranchutils.BranchHashmap,
	tombstoneHashmap databranchutils.BranchHashmap,
	err error,
) {
	var (
		atomicErr          atomic.Value
		dataBat            *batch.Batch
		tombstoneBat       *batch.Batch
		wg                 sync.WaitGroup
		totalRows          int64
		totalBytes         int64
		totalTombstoneRows int64
		start              = time.Now()
	)

	defer func() {
		wg.Wait()

		if dataBat != nil {
			dataBat.Clean(mp)
		}

		if tombstoneBat != nil {
			tombstoneBat.Clean(mp)
		}

		if err != nil {
			logutil.Warn("DataBranch-Hashmap-Build-Error",
				zap.String("side", side),
				zap.Duration("duration", time.Since(start)),
				zap.Error(err),
			)
		}
	}()

	if dataHashmap, err = databranchutils.NewBranchHashmap(
		databranchutils.WithBranchHashmapAllocator(tblStuff.hashmapAllocator),
	); err != nil {
		return
	}

	if tombstoneHashmap, err = databranchutils.NewBranchHashmap(
		databranchutils.WithBranchHashmapAllocator(tblStuff.hashmapAllocator),
	); err != nil {
		return
	}

	putVectors := func(bat *batch.Batch, isTombstone bool) error {
		if bat == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			bat.Clean(mp)
			return ctx.Err()
		default:
		}

		wg.Add(1)

		if err = tblStuff.worker.Submit(func() {
			defer wg.Done()
			defer bat.Clean(mp)

			ll := bat.VectorCount()
			var taskErr error
			select {
			case <-ctx.Done():
				taskErr = ctx.Err()
			default:
				if isTombstone {
					// keep the commit ts
					taskErr = tombstoneHashmap.PutByVectors(bat.Vecs[:ll], []int{0})
				} else {
					// keep the commit ts
					taskErr = dataHashmap.PutByVectors(bat.Vecs[:ll], []int{tblStuff.def.pkColIdx})
				}
			}
			if taskErr != nil {
				atomicErr.Store(taskErr)
			}
		}); err != nil {
			wg.Done()
			bat.Clean(mp)
		}

		return err
	}

	for _, handle := range handles {
		for {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				return
			default:
			}
			if dataBat, tombstoneBat, _, err = handle.Next(
				ctx, mp,
			); err != nil {
				return
			} else if dataBat == nil && tombstoneBat == nil {
				// out of data
				break
			}

			if dataBat != nil && dataBat.RowCount() > 0 {
				totalRows += int64(dataBat.RowCount())
				totalBytes += int64(dataBat.Size())
			}
			if tombstoneBat != nil && tombstoneBat.RowCount() > 0 {
				totalTombstoneRows += int64(tombstoneBat.RowCount())
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
	} else {
		select {
		case <-ctx.Done():
			err = ctx.Err()
		default:
		}
	}

	if err != nil {
		return
	}

	if tombstoneHashmap.ItemCount() == 0 && dataHashmap.ItemCount() == 0 {
		return
	}

	if tblStuff.maxTombstoneBatchCnt == 0 {
		rowBytes := defaultRowBytes
		if totalRows > 0 {
			rowBytes = totalBytes / totalRows
			if rowBytes <= 0 {
				rowBytes = defaultRowBytes
			}
		}
		rowBytes = rowBytes * tombstoneRowMult / tombstoneRowDiv

		available := tblStuff.hashmapAllocator.Available() / 3

		shardCount := 0
		if tombstoneHashmap != nil {
			shardCount = tombstoneHashmap.ShardCount()
		} else if dataHashmap != nil {
			shardCount = dataHashmap.ShardCount()
		}

		maxByMem := int(available / int64(shardCount) / rowBytes)
		if maxByMem < 1 {
			maxByMem = 1
		} else if maxByMem > maxSqlBatchCnt {
			maxByMem = maxSqlBatchCnt
		}

		tblStuff.maxTombstoneBatchCnt = maxByMem

	}

	// For fake PK tables, each row version has a unique fake PK, so there are
	// no same-key duplicates. However, the dedup phase also pairs tombstones
	// with their corresponding data entries. For example, INSERT(1,1) with
	// FK_old then UPDATE→(1,11) produces tombstone FK_old + stale data FK_old
	// + live data FK_new. Without cleanup, the stale data (1,1) would survive
	// and appear in the diff output with the wrong value.

	/*
		case 1:
			     insert    update    update     update
				   pk1		 pk1       pk1        pk1   ....
				|------|-------------------------------------|
		               |           collect range             |
			1.1. the last one is an update: only keep the last update op
			1.2. the last one is a deletion: only keep the last deletion op

		case 2:
			           insert    update     update
				   		 pk1       pk1        pk1   ....
				|------|-------------------------------------|
		               |           collect range             |
		     2.1. the last one is an update: only keep the last update op
		     2.2. the last one is a deletion: only keep the last deletion op

		case 3:
				 insert    delete    update     update
		           pk1		 pk1       pk1        pk1   ....
				|------|-------------------------------------|
		               |           collect range             |
				3.1. the last one is an update: only keep the last update op
				3.2. the last one is a deletion: only keep the last deletion op
	*/

	if err = tombstoneHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		var (
			err2    error
			err3    error
			maxTs   types.TS
			maxIdx  int
			tuple   types.Tuple
			dataRet databranchutils.GetResult
		)
		if err2 = cursor.ForEach(func(key []byte, _ []byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			var tombstoneRet databranchutils.GetResult
			if tombstoneRet, err3 = cursor.GetByEncodedKey(key); err3 != nil {
				return err3
			}
			if len(tombstoneRet.Rows) == 0 {
				return nil
			}

			maxIdx = 0
			maxTs = types.MinTs()
			for i := 0; i < len(tombstoneRet.Rows); i++ {
				if tuple, _, err3 = tombstoneHashmap.DecodeRow(tombstoneRet.Rows[i]); err3 != nil {
					return err3
				}
				cur := types.TS(tuple[len(tuple)-1].([]uint8))
				if cur.GT(&maxTs) {
					maxTs = cur
					maxIdx = i
				}
			}

			totalTombstones := len(tombstoneRet.Rows)

			// Remove stale tombstone entries (only needed when multiple exist)
			if len(tombstoneRet.Rows) > 1 {
				for i := range tombstoneRet.Rows {
					if i != maxIdx {
						if _, err3 = cursor.PopByEncodedKeyValue(
							key, tombstoneRet.Rows[i], false,
						); err3 != nil {
							return err3
						}
					}
				}
			}

			// Remove data entries older than the latest tombstone.
			// Count how many stale data entries are removed — each one
			// "consumes" one tombstone from an intermediate operation
			// (e.g., insert→update creates 1 tombstone + 1 stale data entry).
			staleDataCount := 0
			if dataRet, err3 = dataHashmap.GetByEncodedKey(key); err3 != nil {
				return err3
			}

			if dataRet.Exists {
				for i := range dataRet.Rows {
					if tuple, _, err3 = dataHashmap.DecodeRow(dataRet.Rows[i]); err3 != nil {
						return err3
					}
					cur := types.TS(tuple[len(tuple)-1].([]uint8))
					if cur.LT(&maxTs) {
						if _, err3 = dataHashmap.PopByEncodedKeyValue(
							key, dataRet.Rows[i], false,
						); err3 != nil {
							return err3
						}
						staleDataCount++
					}
				}
			}

			// If all tombstones are consumed by stale data entries, the net
			// effect is a pure INSERT (not a delete). Remove the remaining
			// tombstone so that findDeleteAndUpdateBat doesn't incorrectly
			// cancel the surviving data entry.
			//
			// Example: insert(1,1) → update→(1,11) produces 1 tombstone +
			// 1 stale data (1,1) + 1 live data (1,11). staleDataCount=1
			// matches totalTombstones=1, so the tombstone is intermediate.
			//
			// Contrast: update LCA row (0,0)→(0,10) produces 1 tombstone +
			// 0 stale data (the original (0,0) is outside the change range).
			// staleDataCount=0 < totalTombstones=1, so the tombstone stays
			// and findDeleteAndUpdateBat correctly detects UPDATE on the LCA.
			if staleDataCount >= totalTombstones {
				if _, err3 = cursor.PopByEncodedKeyValue(
					key, tombstoneRet.Rows[maxIdx], false,
				); err3 != nil {
					return err3
				}
			}
			return nil
		}); err2 != nil {
			return err2
		}
		return nil
	}, -1); err != nil {
		return nil, nil, err
	}

	return
}
