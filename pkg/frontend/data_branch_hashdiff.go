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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"go.uber.org/zap"
)

type lcaProbeLayout struct {
	attrs       []string
	types       []types.Type
	targetIdxes []int
}

func lcaProbeResultTargetIndexes(
	layout lcaProbeLayout,
	targetColumnCount int,
	resultColumnCount int,
	fullTargetLayout bool,
) ([]int, error) {
	if fullTargetLayout {
		if resultColumnCount != targetColumnCount {
			return nil, moerr.NewInternalErrorNoCtxf(
				"unexpected LCA probe result width %d for full target layout with %d columns",
				resultColumnCount, targetColumnCount,
			)
		}
		idxes := make([]int, targetColumnCount)
		for i := range idxes {
			idxes[i] = i
		}
		return idxes, nil
	}
	if resultColumnCount != len(layout.targetIdxes) {
		return nil, moerr.NewInternalErrorNoCtxf(
			"unexpected LCA probe result width %d for projected layout with %d columns",
			resultColumnCount, len(layout.targetIdxes),
		)
	}
	return append([]int(nil), layout.targetIdxes...), nil
}

// lcaProbeColumnLayout selects only columns that exist in both the LCA and
// target layouts. The caller reconstructs omitted target-only columns as NULL,
// because they cannot exist in an ancestor snapshot.
func lcaProbeColumnLayout(lcaDef *plan2.TableDef, targetColNames []string, targetColTypes []types.Type) lcaProbeLayout {
	if len(lcaDef.Cols) == 0 {
		layout := lcaProbeLayout{
			attrs: append([]string(nil), targetColNames...),
			types: append([]types.Type(nil), targetColTypes...),
		}
		for i := range targetColNames {
			layout.targetIdxes = append(layout.targetIdxes, i)
		}
		return layout
	}
	targetByName := make(map[string]int, len(targetColNames))
	for idx, name := range targetColNames {
		targetByName[strings.ToLower(name)] = idx
	}

	layout := lcaProbeLayout{}
	for _, col := range lcaDef.Cols {
		if col.Name == catalog.Row_ID {
			continue
		}
		targetIdx, ok := targetByName[strings.ToLower(col.Name)]
		if !ok {
			continue
		}
		layout.attrs = append(layout.attrs, col.Name)
		layout.types = append(layout.types, types.New(types.T(col.Typ.Id), col.Typ.Width, col.Typ.Scale))
		layout.targetIdxes = append(layout.targetIdxes, targetIdx)
	}
	return layout
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
		sqlRet           executor.Result
		fullTargetLayout bool

		lcaTblDef    = tblStuff.lcaRel.GetTableDef(ctx)
		baseTblDef   = tblStuff.baseRel.GetTableDef(ctx)
		targetTblDef = tblStuff.tarRel.GetTableDef(ctx)

		colTypes           = tblStuff.def.colTypes
		expandedPKColIdxes = tblStuff.def.pkColIdxes
		snapshotTS         = types.TimestampToTS(snapshot)
	)
	lcaLayout := lcaProbeColumnLayout(lcaTblDef, tblStuff.def.colNames, tblStuff.def.colTypes)

	forceReaderProbe := tblStuff.lcaReaderProbeMode != nil && tblStuff.lcaReaderProbeMode.Load()
	if forceReaderProbe {
		sqlRet, err = runLCAProbeWithReaderFallback(ctx, ses, tBat, tblStuff, snapshotTS)
		if err != nil {
			return nil, err
		}
		fullTargetLayout = true
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
			pkType := colTypes[expandedPKColIdxes[0]]
			for i := range tBat.Vecs[0].Length() {
				valsBuf.WriteString(fmt.Sprintf("row(%d,", i))
				b := tBat.Vecs[0].GetRawBytesAt(i)
				val := types.DecodeValue(b, tBat.Vecs[0].GetType().Oid)
				if err = formatValIntoString(ses, val, pkType, valsBuf); err != nil {
					return nil, err
				}
				valsBuf.WriteString(")")
				if i != tBat.Vecs[0].Length()-1 {
					valsBuf.WriteString(",")
				}
			}
		}

		selectCols := make([]string, len(lcaLayout.attrs)+1)
		selectCols[0] = "pks.__idx_"
		for i, colName := range lcaLayout.attrs {
			selectCols[i+1] = fmt.Sprintf("lca.%s", quoteIdentifierForSQL(colName))
		}

		sqlBuf.Reset()
		sqlBuf.WriteString(fmt.Sprintf(
			"select %s from %s.%s%s as lca ",
			strings.Join(selectCols, ", "),
			quoteIdentifierForSQL(lcaTblDef.DbName),
			quoteIdentifierForSQL(lcaTblDef.Name),
			mots),
		)
		sqlBuf.WriteString(fmt.Sprintf(
			"right join (values %s) as pks(__idx_,%s) on ",
			valsBuf.String(), strings.Join(pkNames, ",")),
		)

		for i := range pkNames {
			sqlBuf.WriteString(fmt.Sprintf("lca.%s = ", pkNames[i]))
			if castType, ok := lcaProbeJoinCastType(colTypes[expandedPKColIdxes[i]]); ok {
				sqlBuf.WriteString(fmt.Sprintf("cast(pks.%s as %s)", pkNames[i], castType))
			} else {
				sqlBuf.WriteString(fmt.Sprintf("pks.%s", pkNames[i]))
			}
			if i != len(pkNames)-1 {
				sqlBuf.WriteString(" AND ")
			}
		}
		sqlBuf.WriteString(" order by pks.__idx_")

		sqlPreview := sqlBuf.String()
		if len(sqlPreview) > 512 {
			sqlPreview = sqlPreview[:512]
		}
		logutil.Debug(
			"DataBranch-LCA-SQL-Start",
			zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
			zap.String("db-name", lcaTblDef.DbName),
			zap.String("table-name", lcaTblDef.Name),
			zap.Int("input-pk-rows", tBat.RowCount()),
			zap.String("snapshot-ts", snapshotTS.ToString()),
			zap.Int("sql-len", sqlBuf.Len()),
			zap.String("sql-preview", sqlPreview),
		)

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
				fullTargetLayout = true
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
	defer sqlRet.Close()

	if forceReaderProbe {
		logutil.Debug(
			"DataBranch-LCA-ReaderProbe-Direct",
			zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
			zap.String("table-name", lcaTblDef.Name),
			zap.Int("input-pk-rows", tBat.RowCount()),
			zap.String("snapshot-ts", snapshotTS.ToString()),
		)
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
	defer func() {
		if err != nil && dBat != nil {
			tblStuff.retPool.releaseRetBatch(dBat, false)
			dBat = nil
		}
	}()

	sels := make([]int64, 0, 100)
	joinedRows := 0
	lcaHitRows := 0
	lcaMissRows := 0
	sqlRet.ReadRows(func(rowCnt int, cols []*vector.Vector) bool {
		if len(cols) == 0 {
			err = moerr.NewInternalErrorNoCtx("LCA probe returned a batch without index column")
			return false
		}
		var resultTargetIdxes []int
		if resultTargetIdxes, err = lcaProbeResultTargetIndexes(
			lcaLayout, len(tblStuff.def.colNames), len(cols)-1, fullTargetLayout,
		); err != nil {
			return false
		}
		joinedRows += rowCnt
		for i := range rowCnt {
			if notExist(cols[1:], i) {
				idx := vector.GetFixedAtNoTypeCheck[int64](cols[0], i)
				sels = append(sels, int64(idx))
				lcaMissRows++
				continue
			}

			for j := 1; j < len(cols); j++ {
				targetIdx := resultTargetIdxes[j-1]
				if err = dBat.Vecs[targetIdx].UnionOne(cols[j], int64(i), ses.proc.Mp()); err != nil {
					return false
				}
			}
			lcaHitRows++

		}

		return true
	})
	if err != nil {
		return nil, err
	}
	selectedTargetIdxes := make(map[int]struct{}, len(tblStuff.def.colNames))
	if fullTargetLayout {
		for targetIdx := range tblStuff.def.colNames {
			selectedTargetIdxes[targetIdx] = struct{}{}
		}
	} else {
		for _, targetIdx := range lcaLayout.targetIdxes {
			selectedTargetIdxes[targetIdx] = struct{}{}
		}
	}
	for targetIdx, typ := range tblStuff.def.colTypes {
		if _, selected := selectedTargetIdxes[targetIdx]; selected {
			continue
		}
		nullVec := vector.NewConstNull(typ, lcaHitRows, ses.proc.Mp())
		if err = dBat.Vecs[targetIdx].UnionBatch(nullVec, 0, lcaHitRows, nil, ses.proc.Mp()); err != nil {
			nullVec.Free(ses.proc.Mp())
			return nil, err
		}
		nullVec.Free(ses.proc.Mp())
	}
	dBat.SetRowCount(lcaHitRows)
	logutil.Debug(
		"DataBranch-LCA-Join-Result",
		zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
		zap.String("table-name", lcaTblDef.Name),
		zap.Int("joined-rows", joinedRows),
		zap.Int("lca-hit-rows", lcaHitRows),
		zap.Int("lca-miss-rows", lcaMissRows),
		zap.Int("delete-candidate-rows", len(sels)),
		zap.Int("output-dbat-rows", dBat.RowCount()),
	)

	if len(sels) == 0 {
		for i := range tBat.Vecs {
			tBat.Vecs[i].CleanOnlyData()
		}
		tBat.SetRowCount(0)
	} else {
		for i := range tBat.Vecs {
			tBat.Vecs[i].Shrink(sels, false)
		}
		tBat.SetRowCount(tBat.Vecs[0].Length())
	}
	logutil.Debug(
		"DataBranch-LCA-PostProcess-Done",
		zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
		zap.String("table-name", lcaTblDef.Name),
		zap.Int("remaining-tbat-rows", tBat.RowCount()),
		zap.Int("remaining-dbat-rows", dBat.RowCount()),
	)

	return
}

// appendLCAProbeValue appends a column returned by the LCA SQL probe to the
// corresponding physical table vector. A RIGHT JOIN exposes ENUM columns as
// VARCHAR labels, so they need to be restored to their physical enum codes
// before the batch enters the diff pipeline. Other columns must retain their
// physical type; copying mismatched vector storage would silently corrupt data.
func appendLCAProbeValue(
	dst, src *vector.Vector,
	row int,
	enumValues string,
	mp *mpool.MPool,
) error {
	if dst.GetType().Oid == src.GetType().Oid {
		return dst.UnionOne(src, int64(row), mp)
	}

	if dst.GetType().Oid != types.T_enum ||
		(src.GetType().Oid != types.T_varchar && src.GetType().Oid != types.T_char && src.GetType().Oid != types.T_text) {
		return moerr.NewInternalErrorNoCtxf(
			"unexpected LCA probe type conversion: source=%s target=%s",
			src.GetType().String(), dst.GetType().String(),
		)
	}

	if src.IsConstNull() || src.GetNulls().Contains(uint64(row)) {
		return vector.AppendNull(dst, mp)
	}
	if src.IsConst() {
		row = 0
	}

	val, err := types.ParseEnum(enumValues, src.UnsafeGetStringAt(row))
	if err != nil {
		return err
	}
	return vector.AppendFixed(dst, val, false, mp)
}

func lcaProbeJoinCastType(typ types.Type) (string, bool) {
	switch typ.Oid {
	case types.T_bit:
		return typ.DescString(), true
	case types.T_int8, types.T_int16, types.T_int32:
		return "INT", true
	case types.T_int64:
		return "BIGINT", true
	case types.T_uint8, types.T_uint16, types.T_uint32:
		return "INT UNSIGNED", true
	case types.T_uint64:
		return "BIGINT UNSIGNED", true
	case types.T_float32:
		return "FLOAT", true
	case types.T_float64:
		return "DOUBLE", true
	case types.T_char, types.T_varchar, types.T_text:
		return "VARCHAR", true
	case types.T_binary, types.T_varbinary:
		return "VARBINARY", true
	case types.T_decimal64, types.T_decimal128, types.T_decimal256:
		return typ.DescString(), true
	case types.T_date, types.T_datetime, types.T_time, types.T_timestamp, types.T_year:
		return typ.String(), true
	default:
		return "", false
	}
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
	lcaLayout := lcaProbeColumnLayout(lcaTblDef, tblStuff.def.colNames, tblStuff.def.colTypes)
	lcaPKIdx := slices.IndexFunc(lcaLayout.attrs, func(name string) bool {
		return strings.EqualFold(name, lcaTblDef.Pkey.PkeyColName)
	})
	if lcaPKIdx < 0 {
		return executor.Result{}, moerr.NewInternalErrorNoCtxf(
			"data branch: LCA primary key column %q is absent from probe layout",
			lcaTblDef.Pkey.PkeyColName,
		)
	}
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

	tmp := batch.NewWithSize(len(lcaLayout.attrs))
	for i, typ := range lcaLayout.types {
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
	err = scanSnapshotRelationByIDWithFallback(
		ctx,
		"lca-reader-fallback",
		ses,
		tblStuff.lcaRel.GetTableID(ctx),
		snapshotTS,
		tblStuff.lcaRel,
		lcaLayout.attrs,
		lcaLayout.types,
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
			readPK := readBatch.Vecs[lcaPKIdx]
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
			for colIdx := range lcaLayout.attrs {
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

	sourceIdxByTarget := make([]int, len(tblStuff.def.colNames))
	for i := range sourceIdxByTarget {
		sourceIdxByTarget[i] = -1
	}
	for sourceIdx, targetIdx := range lcaLayout.targetIdxes {
		sourceIdxByTarget[targetIdx] = sourceIdx
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
		for targetIdx, sourceIdx := range sourceIdxByTarget {
			if sourceIdx < 0 {
				nullConst := vector.NewConstNull(tblStuff.def.colTypes[targetIdx], rowCount, mp)
				err = out.Vecs[targetIdx+1].UnionBatch(nullConst, 0, rowCount, nil, mp)
				nullConst.Free(mp)
				if err != nil {
					return executor.Result{}, err
				}
				continue
			}
			if err = out.Vecs[targetIdx+1].UnionBatch(tmp.Vecs[sourceIdx], 0, rowCount, nil, mp); err != nil {
				return executor.Result{}, err
			}
		}
	} else {
		for targetIdx, sourceIdx := range sourceIdxByTarget {
			if sourceIdx < 0 {
				nullConst := vector.NewConstNull(tblStuff.def.colTypes[targetIdx], rowCount, mp)
				err = out.Vecs[targetIdx+1].UnionBatch(nullConst, 0, rowCount, nil, mp)
				nullConst.Free(mp)
				if err != nil {
					return executor.Result{}, err
				}
				continue
			}
			if err = out.Vecs[targetIdx+1].Union(tmp.Vecs[sourceIdx], sels, mp); err != nil {
				return executor.Result{}, err
			}
			for _, rowIdx := range missedRows {
				nulls.Add(out.Vecs[targetIdx+1].GetNulls(), rowIdx)
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
	pickKeyHashmap databranchutils.BranchHashmap,
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

		fields := []zap.Field{
			zap.Int("base-handle-cnt", len(baseHandle)),
			zap.Int("target-handle-cnt", len(tarHandle)),
			zap.Bool("has-lca", dagInfo.hasLCA()),
			zap.Duration("build-base-cost", buildBaseCost),
			zap.Duration("build-target-cost", buildTargetCost),
			zap.Duration("diff-cost", diffCost),
			zap.Duration("total-cost", time.Since(start)),
		}
		if err != nil {
			fields = append(fields, zap.Error(err))
			logutil.Warn("DataBranch-HashDiff-Error", fields...)
			return
		}
		logutil.Info("DataBranch-HashDiff-Done", fields...)
	}()

	buildBaseStart := time.Now()
	if baseDataHashmap, baseTombstoneHashmap, err = buildHashmapForTable(
		ctx, ses.proc.Mp(), &tblStuff, baseHandle, "base", pickKeyHashmap,
	); err != nil {
		return
	}
	buildBaseCost = time.Since(buildBaseStart)

	buildTargetStart := time.Now()
	if tarDataHashmap, tarTombstoneHashmap, err = buildHashmapForTable(
		ctx, ses.proc.Mp(), &tblStuff, tarHandle, "target", pickKeyHashmap,
	); err != nil {
		return
	}
	buildTargetCost = time.Since(buildTargetStart)

	diffStart := time.Now()
	if !dagInfo.hasLCA() {
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

	// Resolve per-side LCA snapshots once: each side's tombstone
	// resolution probes the LCA at "the moment THIS side forked off
	// the LCA" (or, when the side IS the LCA, at the OTHER side's
	// snapshot — see branchMetaInfo helper docs).
	tarSP, baseSP := tblStuff.resolvedSnapshots(ses)
	tarLCAProbe := dagInfo.tarLCASnapshot(baseSP)
	baseLCAProbe := dagInfo.baseLCASnapshot(tarSP)

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
		var pickConflictBat *batch.Batch
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

		appendPickConflict := func(baseWrapped batchWithKind, rowIdx int) error {
			if !copt.preservePickConflicts || copt.conflictOpt == nil ||
				copt.conflictOpt.Opt != tree.CONFLICT_ACCEPT {
				return nil
			}
			if pickConflictBat == nil {
				pickConflictBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
			}
			return pickConflictBat.UnionOne(baseWrapped.batch, int64(rowIdx), ses.proc.Mp())
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
						if tarWrapped.kind == diffDelete &&
							baseWrapped.kind == diffDelete &&
							!tarWrapped.fromUpdate && !baseWrapped.fromUpdate {
							if cmp, err3 = compareRowInWrappedBatches(
								ctx, ses, tblStuff, i, j, true,
								tarWrapped, baseWrapped,
							); err3 != nil {
								return
							} else if cmp == 0 {
								sels1 = append(sels1, int64(i))
								sels2 = append(sels2, int64(j))
								i++
								j++
								continue
							}
						}
						if err3 = appendPickConflict(baseWrapped, j); err3 != nil {
							return
						}
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

		if pickConflictBat != nil {
			if stop, e := emitBatch(emit, batchWithKind{
				batch: pickConflictBat,
				kind:  diffDelete,
				name:  tblStuff.baseRel.GetTableName(),
				side:  diffSideBase,
			}, false, tblStuff.retPool); e != nil {
				return e
			} else if stop {
				return nil
			}
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
			branchTS = baseLCAProbe
			hashmap1 = baseDataHashmap
			hashmap2 = baseTombstoneHashmap
			name     = tblStuff.baseRel.GetTableName()
		)

		if !forBase {
			branchTS = tarLCAProbe
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

	return diffDataHelper(ctx, ses, copt, tblStuff, emit, tarDataHashmap, baseDataHashmap)
}

func hashDiffIfNoLCA(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	copt compositeOption,
	emit emitFunc,
	tarDataHashmap databranchutils.BranchHashmap,
	_ databranchutils.BranchHashmap,
	baseDataHashmap databranchutils.BranchHashmap,
	_ databranchutils.BranchHashmap,
) (err error) {
	// buildHashmapForTable already reconciles data versions with tombstones by
	// commit timestamp. Any data row left for a key is the latest live version.
	// In particular, a bounded UPDATE produces a same-commit data+tombstone
	// pair; removing the data merely because the marker remains would erase the
	// update before the two independent tables are compared.
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

	compareIdxes := tblStuff.def.commonIdxes
	if len(compareIdxes) == 0 {
		compareIdxes = tblStuff.def.visibleIdxes // backward compat when commonIdxes not set
	}
	for _, colIdx := range compareIdxes {
		if skipPKCols {
			if slices.Index(tblStuff.def.pkColIdxes, colIdx) != -1 {
				continue
			}
		}

		var (
			vec1 = wrapped1.batch.Vecs[colIdx]
			vec2 = wrapped2.batch.Vecs[colIdx]
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

func compareTupleWithBatchRow(
	tblStuff tableStuff,
	tuple types.Tuple,
	bat *batch.Batch,
	rowIdx int,
	skipPKCols bool,
) (int, error) {
	compareIdxes := tblStuff.def.commonIdxes
	if len(compareIdxes) == 0 {
		compareIdxes = tblStuff.def.visibleIdxes // backward compat when commonIdxes not set
	}
	for _, colIdx := range compareIdxes {
		if skipPKCols && slices.Index(tblStuff.def.pkColIdxes, colIdx) != -1 {
			continue
		}
		if colIdx < 0 || colIdx >= bat.VectorCount() {
			return 0, moerr.NewInternalErrorNoCtxf(
				"column index %d out of range for batch width %d",
				colIdx, bat.VectorCount(),
			)
		}
		val, err := getTupleColumnValue(tuple, tblStuff, colIdx)
		if err != nil {
			return 0, err
		}
		cmp, err := compareTupleValueWithVector(val, bat.Vecs[colIdx], rowIdx)
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
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
				removedLiveRows := 0
				tombRowIDs := vector.MustFixedColNoTypeCheck[types.Rowid](tombBat.Vecs[1])
				for i := 0; i < tombBat.RowCount(); i++ {
					keyBytes := tombBat.Vecs[2].GetBytesAt(i)
					var dataRet databranchutils.GetResult
					if dataRet, err2 = dataHashmap.GetByEncodedKey(keyBytes); err2 != nil {
						tblStuff.retPool.releaseRetBatch(tombBat, true)
						tblStuff.retPool.releaseRetBatch(dBat, false)
						return err2
					}
					if !dataRet.Exists {
						continue
					}
					var matchedRow []byte
					for _, row := range dataRet.Rows {
						var liveTuple types.Tuple
						if liveTuple, _, err2 = dataHashmap.DecodeRow(row); err2 != nil {
							tblStuff.retPool.releaseRetBatch(tombBat, true)
							tblStuff.retPool.releaseRetBatch(dBat, false)
							return err2
						}
						liveRowIDBytes, ok := liveTuple[0].([]uint8)
						if !ok {
							continue
						}
						liveRowID := types.DecodeFixed[types.Rowid](liveRowIDBytes)
						if !liveRowID.EQ(&tombRowIDs[i]) {
							continue
						}
						matchedRow = row
						break
					}
					if matchedRow == nil {
						continue
					}
					var removed int
					if removed, err2 = dataHashmap.PopByEncodedKeyValue(keyBytes, matchedRow, false); err2 != nil {
						tblStuff.retPool.releaseRetBatch(tombBat, true)
						tblStuff.retPool.releaseRetBatch(dBat, false)
						return err2
					}
					removedLiveRows += removed
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

						if tuple, _, err2 = dataHashmap.DecodeRow(row); err2 != nil {
							return err2
						}

						var cmp int
						skipPKCols := tblStuff.def.pkKind != fakeKind
						if cmp, err2 = compareTupleWithBatchRow(
							tblStuff, tuple, dBat, idx, skipPKCols,
						); err2 != nil {
							return err2
						}
						if cmp == 0 {
							return nil
						}

						// delete on lca and insert into tar ==> update
						if updateBat == nil {
							updateBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
						}
						if expandUpdate && updateDeleteBat == nil {
							updateDeleteBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
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
						updateDeleteBat.SetRowCount(updateDeleteBat.Vecs[0].Length())
						if err2 = send(batchWithKind{
							name:       tblName,
							side:       side,
							fromUpdate: true,
							batch:      updateDeleteBat,
							kind:       diffDelete,
						}); err2 != nil {
							return err2
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

		if err2 = cursor.ForEach(func(key []byte, row []byte) error {
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
			var tombTuple types.Tuple
			if tombTuple, _, err2 = tombstoneHashmap.DecodeRow(row); err2 != nil {
				return err2
			}
			rowIDBytes, ok := tombTuple[0].([]uint8)
			if !ok {
				return moerr.NewInternalErrorNoCtx("tombstone row missing rowid in hashmap payload")
			}
			rowID := types.DecodeFixed[types.Rowid](rowIDBytes)
			if err2 = vector.AppendFixed(tBat1.Vecs[1], rowID, false, ses.proc.Mp()); err2 != nil {
				return err2
			}
			if err2 = vector.AppendBytes(tBat1.Vecs[2], append([]byte(nil), key...), false, ses.proc.Mp()); err2 != nil {
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
	if bat.VectorCount() != len(tblStuff.def.colNames) {
		return moerr.NewInternalErrorNoCtxf(
			"unexpected batch width %d for table %s with %d physical columns",
			bat.VectorCount(), tblStuff.tarRel.GetTableName(), len(tblStuff.def.colNames),
		)
	}
	for colIdx := range tblStuff.def.colNames {
		val, err := getTupleColumnValue(tuple, tblStuff, colIdx)
		if err != nil {
			return err
		}
		vec := bat.Vecs[colIdx]
		if err := appendTupleValueToVector(vec, val, ses.proc.Mp()); err != nil {
			return err
		}
	}

	bat.SetRowCount(bat.Vecs[0].Length())

	return nil
}

func getTupleColumnValue(tuple types.Tuple, tblStuff tableStuff, colIdx int) (any, error) {
	totalColCnt := len(tblStuff.def.colTypes)
	if colIdx < 0 || colIdx >= totalColCnt {
		return nil, moerr.NewInternalErrorNoCtxf(
			"column index %d out of range for table %s with %d columns",
			colIdx, tblStuff.tarRel.GetTableName(), totalColCnt,
		)
	}
	switch len(tuple) {
	case totalColCnt:
		return normalizeTupleColumnValue(tuple[colIdx], tblStuff.def.colTypes[colIdx])
	case totalColCnt + 1, totalColCnt + 2:
		return normalizeTupleColumnValue(tuple[colIdx+1], tblStuff.def.colTypes[colIdx])
	default:
		return nil, moerr.NewInternalErrorNoCtxf(
			"unexpected tuple width %d for table %s with %d visible columns",
			len(tuple), tblStuff.tarRel.GetTableName(), len(tblStuff.def.visibleIdxes),
		)
	}
}

func fakePKTupleKeyIdxes(tblStuff tableStuff) []int {
	keyColIdxes := tblStuff.def.commonVisibleIdxes
	if len(keyColIdxes) == 0 {
		keyColIdxes = tblStuff.def.visibleIdxes
	}

	idxes := make([]int, len(keyColIdxes))
	for i, colIdx := range keyColIdxes {
		idxes[i] = colIdx + 1
	}
	return idxes
}

func batchSampleRowsForLog(bat *batch.Batch, limit int) []string {
	if bat == nil || bat.RowCount() == 0 || limit <= 0 {
		return nil
	}
	if limit > bat.RowCount() {
		limit = bat.RowCount()
	}
	rows := make([]string, 0, limit)
	for rowIdx := 0; rowIdx < limit; rowIdx++ {
		cols := make([]string, 0, bat.VectorCount())
		for _, vec := range bat.Vecs {
			if vec == nil {
				cols = append(cols, "<nil>")
				continue
			}
			if rowIdx >= vec.Length() {
				cols = append(cols, "<oob>")
				continue
			}
			if vec.GetNulls().Contains(uint64(rowIdx)) {
				cols = append(cols, "NULL")
				continue
			}
			cols = append(cols, fmt.Sprintf("%v", types.DecodeValue(vec.GetRawBytesAt(rowIdx), vec.GetType().Oid)))
		}
		rows = append(rows, strings.Join(cols, ", "))
	}
	return rows
}

func validateLeadingRowID(side, tableName string, isTombstone bool, bat *batch.Batch) error {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	buildFields := func() []zap.Field {
		return []zap.Field{
			zap.String("side", side),
			zap.String("table-name", tableName),
			zap.Bool("tombstone", isTombstone),
			zap.Int("row-cnt", bat.RowCount()),
			zap.Int("vec-cnt", bat.VectorCount()),
			zap.Strings("attrs", append([]string(nil), bat.Attrs...)),
			zap.Strings("samples", batchSampleRowsForLog(bat, 4)),
		}
	}
	fail := func(msg string, extra ...zap.Field) error {
		logutil.Error(msg, append(buildFields(), extra...)...)
		return moerr.NewInternalErrorNoCtx(msg)
	}
	if bat.VectorCount() == 0 || bat.Vecs[0] == nil {
		return fail("DataBranch-CollectChanges-MissingRowID")
	}
	rowIDVec := bat.Vecs[0]
	if rowIDVec.GetType().Oid != types.T_Rowid {
		return fail("DataBranch-CollectChanges-InvalidRowIDVector",
			zap.String("rowid-vec-type", rowIDVec.GetType().String()),
		)
	}
	if rowIDVec.Length() != bat.RowCount() {
		return fail("DataBranch-CollectChanges-RowIDLenMismatch",
			zap.Int("rowid-vec-len", rowIDVec.Length()),
		)
	}
	rowIDs := vector.MustFixedColNoTypeCheck[types.Rowid](rowIDVec)
	for i := range rowIDs {
		if rowIDVec.GetNulls().Contains(uint64(i)) {
			return fail("DataBranch-CollectChanges-NullRowID", zap.Int("row-idx", i))
		}
		if rowIDs[i].EQ(&types.EmptyRowid) || rowIDs[i].BorrowBlockID().IsEmpty() {
			return fail("DataBranch-CollectChanges-InvalidRowID",
				zap.Int("row-idx", i),
				zap.String("rowid", rowIDs[i].String()),
			)
		}
	}
	return nil
}

func appendTupleValueToVector(vec *vector.Vector, val any, mp *mpool.MPool) error {
	val, err := normalizeTupleColumnValue(val, *vec.GetType())
	if err != nil {
		return err
	}
	if val == nil {
		return vector.AppendNull(vec, mp)
	}
	if raw, ok := val.([]byte); ok {
		return vector.AppendBytes(vec, raw, false, mp)
	}
	return vector.AppendAny(vec, val, false, mp)
}

func normalizeTupleColumnValue(val any, typ types.Type) (any, error) {
	if val == nil {
		return nil, nil
	}
	raw, ok := val.([]byte)
	if !ok {
		return val, nil
	}
	if typ.IsVarlen() {
		return raw, nil
	}
	fixedLen := typ.Oid.FixedLength()
	if fixedLen < 0 || len(raw) != fixedLen {
		return nil, moerr.NewInternalErrorNoCtxf(
			"unexpected byte slice for fixed-width column type %s",
			typ.String(),
		)
	}
	switch typ.Oid {
	case types.T_bool, types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_date, types.T_time, types.T_datetime, types.T_timestamp, types.T_year,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_uuid, types.T_TS, types.T_Rowid, types.T_Blockid,
		types.T_enum:
		return types.DecodeValue(raw, typ.Oid), nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf(
			"unexpected byte slice for fixed-width column type %s",
			typ.String(),
		)
	}
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
				var val any
				if val, err2 = getTupleColumnValue(tarTuple, tblStuff, idx); err2 != nil {
					releaseBuffer(tblStuff.bufPool, buf)
					return err2
				}
				if err2 = formatValIntoString(ses, val, tblStuff.def.colTypes[idx], buf); err2 != nil {
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
	var migratedHashmaps []databranchutils.BranchHashmap
	defer func() {
		for _, h := range migratedHashmaps {
			if closeErr := h.Close(); err == nil && closeErr != nil {
				err = closeErr
			}
		}
	}()

	if tblStuff.def.pkKind == fakeKind {
		var (
			keyIdxes   = fakePKTupleKeyIdxes(tblStuff)
			newHashmap databranchutils.BranchHashmap
		)

		if newHashmap, err = migrateAndCloseSourceHashmap(baseDataHashmap, keyIdxes, -1); err != nil {
			return err
		}
		baseDataHashmap = newHashmap
		migratedHashmaps = append(migratedHashmaps, baseDataHashmap)

		if newHashmap, err = migrateAndCloseSourceHashmap(tarDataHashmap, keyIdxes, -1); err != nil {
			return err
		}
		tarDataHashmap = newHashmap
		migratedHashmaps = append(migratedHashmaps, tarDataHashmap)
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
				if checkRet, err2 = baseDataHashmap.PopByEncodedKey(key, false); err2 != nil {
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
					compareIdxes := tblStuff.def.commonIdxes
					if len(compareIdxes) == 0 {
						compareIdxes = tblStuff.def.visibleIdxes // backward compat when commonIdxes not set
					}
					for _, idx := range compareIdxes {
						if slices.Index(tblStuff.def.pkColIdxes, idx) != -1 {
							// pk columns already compared
							continue
						}
						var tarVal, baseVal any
						if tarVal, err2 = getTupleColumnValue(tarTuple, tblStuff, idx); err2 != nil {
							return err2
						}
						if baseVal, err2 = getTupleColumnValue(baseTuple, tblStuff, idx); err2 != nil {
							return err2
						}

						if types.CompareValue(
							tarVal, baseVal,
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

func migrateAndCloseSourceHashmap(
	source databranchutils.BranchHashmap,
	keyIdxes []int,
	parallelism int,
) (databranchutils.BranchHashmap, error) {
	newHashmap, err := source.Migrate(keyIdxes, parallelism)
	if err != nil {
		return nil, err
	}
	if err = source.Close(); err != nil {
		_ = newHashmap.Close()
		return nil, err
	}
	return newHashmap, nil
}

func buildHashmapForTable(
	ctx context.Context,
	mp *mpool.MPool,
	tblStuff *tableStuff,
	handles []engine.ChangesHandle,
	side string,
	pickKeyHashmap databranchutils.BranchHashmap,
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

		fields := []zap.Field{
			zap.String("side", side),
			zap.Int("handle-cnt", len(handles)),
			zap.Int64("data-row-cnt", totalRows),
			zap.Int64("data-bytes", totalBytes),
			zap.Int64("tombstone-row-cnt", totalTombstoneRows),
			zap.Duration("duration", time.Since(start)),
		}
		if dataHashmap != nil {
			fields = append(fields,
				zap.Int64("data-item-cnt", dataHashmap.ItemCount()),
				zap.Int("data-shard-cnt", dataHashmap.ShardCount()),
			)
		}
		if tombstoneHashmap != nil {
			fields = append(fields,
				zap.Int64("tombstone-item-cnt", tombstoneHashmap.ItemCount()),
				zap.Int("tombstone-shard-cnt", tombstoneHashmap.ShardCount()),
			)
		}
		if err != nil {
			fields = append(fields, zap.Error(err))
			logutil.Warn("DataBranch-Hashmap-Build-Error", fields...)
			return
		}
		logutil.Info("DataBranch-Hashmap-Build-Done", fields...)
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
				// When a pickKeyHashmap is provided (PICK operation), filter
				// the batch to keep only rows whose PK exists in the hashmap.
				// This moves the precise PK filtering from the consumer
				// (appendPickedBatchRows) into the producer, reducing the
				// data that flows through hashDiff and the channel.
				if pickKeyHashmap != nil {
					pkIdx := tblStuff.def.pkColIdx
					if isTombstone {
						// Data branch keeps rowid in Vec[0] while building the
						// hashmaps, so tombstone PK lives at Vec[1].
						pkIdx = 1
					} else {
						pkIdx++
					}
					var results []databranchutils.GetResult
					results, taskErr = pickKeyHashmap.GetByVectors(
						[]*vector.Vector{bat.Vecs[pkIdx]},
					)
					if taskErr != nil {
						atomicErr.Store(taskErr)
						return
					}
					var sels []int64
					for i, r := range results {
						if r.Exists {
							sels = append(sels, int64(i))
						}
					}
					if len(sels) == 0 {
						return // all rows filtered out
					}
					if len(sels) < bat.RowCount() {
						bat.Shrink(sels, false)
						ll = bat.VectorCount()
					}
				}

				if isTombstone {
					taskErr = tombstoneHashmap.PutByVectors(bat.Vecs[:ll], []int{1})
				} else {
					taskErr = dataHashmap.PutByVectors(bat.Vecs[:ll], []int{tblStuff.def.pkColIdx + 1})
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

			tableName := ""
			if side == "base" && tblStuff.baseRel != nil {
				tableName = tblStuff.baseRel.GetTableName()
			} else if side == "target" && tblStuff.tarRel != nil {
				tableName = tblStuff.tarRel.GetTableName()
			}
			if err = validateLeadingRowID(side, tableName, false, dataBat); err != nil {
				return
			}
			if err = validateLeadingRowID(side, tableName, true, tombstoneBat); err != nil {
				return
			}

			dataBat = projectBaseBatchToTargetIfNeeded(side, dataBat, tblStuff, mp)

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

// projectBaseBatchToTarget projects a base-side data batch to match the target
// column layout. Ownership of moved vectors is transferred to the returned
// batch; any vectors left behind on baseBat are cleaned before returning.
func projectBaseBatchToTarget(
	baseBat *batch.Batch,
	tblStuff *tableStuff,
	mp *mpool.MPool,
) *batch.Batch {
	return projectDataBranchBatchToTarget(
		baseBat, tblStuff, tblStuff.def.baseColToTarIdx, mp,
	)
}

func projectBaseBatchToTargetIfNeeded(
	side string,
	dataBat *batch.Batch,
	tblStuff *tableStuff,
	mp *mpool.MPool,
) *batch.Batch {
	if side != "base" || dataBat == nil || dataBat.RowCount() == 0 ||
		dataBranchBatchHasTargetLayout(dataBat, tblStuff) {
		return dataBat
	}
	return projectBaseBatchToTarget(dataBat, tblStuff, mp)
}

func dataBranchSourceColToTargetIdx(
	sourceDef, targetDef *plan2.TableDef,
	targetColNames []string,
) ([]int, error) {
	if sourceDef == nil || targetDef == nil {
		return nil, moerr.NewInternalErrorNoCtx("missing schema for historical data branch projection")
	}
	if err := checkDataBranchPrimaryKeyCompatibility(targetDef, sourceDef); err != nil {
		return nil, moerr.NewNotSupportedNoCtxf(
			"historical data branch primary key is incompatible with the endpoint schema: %s",
			err.Error(),
		)
	}
	if sourceDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
		sourceNames := make([]string, 0, len(sourceDef.Cols))
		targetNames := make([]string, 0, len(targetDef.Cols))
		for _, col := range sourceDef.Cols {
			if col.Name != catalog.Row_ID {
				sourceNames = append(sourceNames, col.Name)
			}
		}
		for _, col := range targetDef.Cols {
			if col.Name != catalog.Row_ID {
				targetNames = append(targetNames, col.Name)
			}
		}
		if len(sourceNames) != len(targetNames) {
			return nil, moerr.NewNotSupportedNoCtx(
				"historical data branch fake primary key schema differs from the endpoint schema",
			)
		}
		for i := range sourceNames {
			if !strings.EqualFold(sourceNames[i], targetNames[i]) {
				return nil, moerr.NewNotSupportedNoCtx(
					"historical data branch fake primary key schema differs from the endpoint schema",
				)
			}
		}
	}
	targetCols := make(map[string]*plan2.ColDef, len(targetDef.Cols))
	for _, col := range targetDef.Cols {
		targetCols[strings.ToLower(col.Name)] = col
	}
	mapping := make([]int, 0, len(sourceDef.Cols))
	for _, col := range sourceDef.Cols {
		if col.Name == catalog.Row_ID {
			continue
		}
		targetIdx := dataBranchColumnIndexByName(targetColNames, col.Name)
		mapping = append(mapping, targetIdx)
		if targetIdx < 0 {
			continue
		}
		targetCol, ok := targetCols[strings.ToLower(col.Name)]
		if !ok || col.Typ.Id != targetCol.Typ.Id ||
			!dataBranchColumnTypeAttributesEqual(col.Typ, targetCol.Typ) {
			return nil, moerr.NewNotSupportedNoCtxf(
				"historical data branch column %s has a different type from the endpoint schema",
				col.Name,
			)
		}
	}
	return mapping, nil
}

// dataBranchNeedsHistoricalProjection reports whether change rows from an
// older physical table generation have a different data-column layout from
// the endpoint. A different table ID alone can also mean an ordinary branch
// ancestor with an identical schema; projecting and hydrating those rows would
// collapse intermediate UPDATE versions to endpoint values.
func dataBranchNeedsHistoricalProjection(sourceToTarget []int, targetColCount int) bool {
	if len(sourceToTarget) != targetColCount {
		return true
	}
	for sourceIdx, targetIdx := range sourceToTarget {
		if sourceIdx != targetIdx {
			return true
		}
	}
	return false
}

func dataBranchTargetLayoutAttrs(tblStuff *tableStuff, hasCommitTS bool) []string {
	attrs := make([]string, 0, len(tblStuff.def.colNames)+2)
	attrs = append(attrs, catalog.Row_ID)
	attrs = append(attrs, tblStuff.def.colNames...)
	if hasCommitTS {
		attrs = append(attrs, objectio.DefaultCommitTS_Attr)
	}
	return attrs
}

func dataBranchBatchHasTargetLayout(bat *batch.Batch, tblStuff *tableStuff) bool {
	if bat == nil || len(bat.Attrs) != bat.VectorCount() {
		return false
	}
	hasCommitTS := bat.VectorCount() == len(tblStuff.def.colNames)+2
	if !hasCommitTS && bat.VectorCount() != len(tblStuff.def.colNames)+1 {
		return false
	}
	want := dataBranchTargetLayoutAttrs(tblStuff, hasCommitTS)
	for i := range want {
		if !strings.EqualFold(bat.Attrs[i], want[i]) {
			return false
		}
	}
	return true
}

func overlayDataBranchProbeResult(
	projected *batch.Batch,
	probe executor.Result,
	pkTargetIdx int,
	mp *mpool.MPool,
) (err error) {
	if projected == nil || projected.RowCount() == 0 {
		return nil
	}
	prepared := false
	probe.ReadRows(func(rowCount int, cols []*vector.Vector) bool {
		if len(cols) < 2 || pkTargetIdx < 0 || pkTargetIdx+1 >= len(cols) {
			err = moerr.NewInternalErrorNoCtx("invalid endpoint probe layout for historical data branch batch")
			return false
		}
		targetColCount := len(cols) - 1
		if projected.VectorCount() < targetColCount+1 {
			err = moerr.NewInternalErrorNoCtxf(
				"historical data branch batch has %d vectors for %d target columns",
				projected.VectorCount(), targetColCount,
			)
			return false
		}
		if !prepared {
			// Projection represents columns absent from the historical schema as
			// constant-NULL vectors. Vector.Copy requires a writable flat
			// destination, so materialize only those constant data columns before
			// overlaying endpoint values. RowID and commit_ts stay untouched.
			for targetIdx := 0; targetIdx < targetColCount; targetIdx++ {
				dst := projected.Vecs[targetIdx+1]
				if !dst.IsConst() {
					continue
				}
				flat := vector.NewVec(*dst.GetType())
				if err = flat.UnionBatch(dst, 0, projected.RowCount(), nil, mp); err != nil {
					flat.Free(mp)
					return false
				}
				dst.Free(mp)
				projected.Vecs[targetIdx+1] = flat
			}
			prepared = true
		}
		for row := 0; row < rowCount; row++ {
			projectedRow := vector.GetFixedAtNoTypeCheck[int64](cols[0], row)
			if projectedRow < 0 || projectedRow >= int64(projected.RowCount()) {
				err = moerr.NewInternalErrorNoCtxf(
					"endpoint probe row index %d out of range %d",
					projectedRow, projected.RowCount(),
				)
				return false
			}
			if cols[pkTargetIdx+1].IsNull(uint64(row)) {
				continue
			}
			for targetIdx := 0; targetIdx < targetColCount; targetIdx++ {
				if err = projected.Vecs[targetIdx+1].Copy(
					cols[targetIdx+1], projectedRow, int64(row), mp,
				); err != nil {
					return false
				}
			}
		}
		return true
	})
	return err
}

func hydrateHistoricalDataBranchBatch(
	ctx context.Context,
	ses *Session,
	projected *batch.Batch,
	tblStuff tableStuff,
	endpointRel engine.Relation,
	endpointSnapshot types.TS,
) (err error) {
	if projected == nil || projected.RowCount() == 0 {
		return nil
	}
	pkVecIdx := tblStuff.def.pkColIdx + 1 // projected data keeps RowID at Vec[0]
	if pkVecIdx <= 0 || pkVecIdx >= projected.VectorCount() {
		return moerr.NewInternalErrorNoCtxf(
			"historical data branch PK index %d out of range", pkVecIdx,
		)
	}
	tBat := batch.NewWithSize(1)
	tBat.Vecs[0] = vector.NewVec(*projected.Vecs[pkVecIdx].GetType())
	defer tBat.Clean(ses.proc.Mp())
	if err = tBat.Vecs[0].UnionBatch(
		projected.Vecs[pkVecIdx], 0, projected.RowCount(), nil, ses.proc.Mp(),
	); err != nil {
		return err
	}
	tBat.SetRowCount(projected.RowCount())

	probeStuff := tblStuff
	probeStuff.lcaRel = endpointRel
	probe, err := runLCAProbeWithReaderFallback(
		ctx, ses, tBat, probeStuff, endpointSnapshot,
	)
	if err != nil {
		return err
	}
	defer probe.Close()
	return overlayDataBranchProbeResult(
		projected, probe, tblStuff.def.pkColIdx, ses.proc.Mp(),
	)
}

type historicalDataBranchChangesHandle struct {
	inner            engine.ChangesHandle
	sourceMapping    []int
	tblStuff         tableStuff
	ses              *Session
	endpointRel      engine.Relation
	endpointSnapshot types.TS
	hydrate          bool
}

func (h *historicalDataBranchChangesHandle) Next(
	ctx context.Context,
	mp *mpool.MPool,
) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	data, tombstone, hint, err := h.inner.Next(ctx, mp)
	if err != nil || data == nil || data.RowCount() == 0 {
		return data, tombstone, hint, err
	}
	data = projectDataBranchBatchToTarget(data, &h.tblStuff, h.sourceMapping, mp)
	if !h.hydrate {
		return data, tombstone, hint, nil
	}
	if err = hydrateHistoricalDataBranchBatch(
		ctx, h.ses, data, h.tblStuff, h.endpointRel, h.endpointSnapshot,
	); err != nil {
		data.Clean(mp)
		if tombstone != nil {
			tombstone.Clean(mp)
		}
		return nil, nil, hint, err
	}
	return data, tombstone, hint, nil
}

func (h *historicalDataBranchChangesHandle) Close() error {
	return h.inner.Close()
}

func projectDataBranchBatchToTarget(
	sourceBat *batch.Batch,
	tblStuff *tableStuff,
	sourceColToTargetIdx []int,
	mp *mpool.MPool,
) *batch.Batch {
	// CollectChanges data batches are laid out as [RowID, data..., commit_ts].
	// Identify the optional trailing vector from the schema-derived data-column
	// count so that it cannot be mistaken for a column that needs projection.
	hasCommitTS := sourceBat.VectorCount() == len(sourceColToTargetIdx)+2
	outColCount := len(tblStuff.def.colNames) + 1
	if hasCommitTS {
		outColCount++
	}
	out := batch.NewWithSize(outColCount)
	out.Vecs[0] = sourceBat.Vecs[0] // RowID
	sourceBat.Vecs[0] = nil
	sourceColCount := sourceBat.VectorCount() - 1 // subtract RowID
	if hasCommitTS {
		sourceColCount--
		commitTSIdx := sourceBat.VectorCount() - 1
		out.Vecs[out.VectorCount()-1] = sourceBat.Vecs[commitTSIdx]
		sourceBat.Vecs[commitTSIdx] = nil
	}
	if sourceColCount > len(sourceColToTargetIdx) {
		sourceColCount = len(sourceColToTargetIdx)
	}
	for sourceColIdx := 0; sourceColIdx < sourceColCount; sourceColIdx++ {
		tarColIdx := sourceColToTargetIdx[sourceColIdx]
		if tarColIdx >= 0 && tarColIdx < len(tblStuff.def.colNames) {
			out.Vecs[tarColIdx+1] = sourceBat.Vecs[sourceColIdx+1]
			sourceBat.Vecs[sourceColIdx+1] = nil
		}
	}
	for i := range tblStuff.def.colNames {
		if out.Vecs[i+1] == nil {
			out.Vecs[i+1] = vector.NewConstNull(tblStuff.def.colTypes[i], sourceBat.RowCount(), mp)
		}
	}
	out.SetRowCount(sourceBat.RowCount())
	out.SetAttributes(dataBranchTargetLayoutAttrs(tblStuff, hasCommitTS))
	sourceBat.Clean(mp)
	return out
}
