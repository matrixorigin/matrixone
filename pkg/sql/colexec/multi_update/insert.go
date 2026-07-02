// Copyright 2021-2024 Matrix Origin
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

package multi_update

import (
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// debugCpkeyAssert controls the main-table __mo_cpkey_col consistency check,
// via env MO_DEBUG_CPKEY_ASSERT. Disabled by default (no per-row overhead):
//   - unset / anything else (default): off
//   - "log": log an error on mismatch, keep running
//   - "panic": log + panic on mismatch (dumps stack to locate the cause)
var debugCpkeyAssert = os.Getenv("MO_DEBUG_CPKEY_ASSERT")

// debugCheckMainCpkey verifies, for a composite-PK main table, that the
// __mo_cpkey_col value of every row in insertBatch equals serial(pk parts)
// recomputed from the same row. A mismatch means the cpkey column got
// row-misaligned with the data columns somewhere upstream (the genesis of the
// duplicate-primary-key bug). Cheap no-op unless MO_DEBUG_CPKEY_ASSERT is set.
func debugCheckMainCpkey(proc *process.Process, updateCtx *MultiUpdateCtx, insertBatch *batch.Batch) {
	if debugCpkeyAssert != "log" && debugCpkeyAssert != "panic" {
		return
	}
	tableDef := updateCtx.TableDef
	if tableDef.Pkey == nil || tableDef.Pkey.PkeyColName != catalog.CPrimaryKeyColName {
		return // only composite PK has a separate __mo_cpkey_col
	}
	// insertBatch.Vecs[i] corresponds to tableDef.Cols[i] (rowid is last & skipped)
	cpkeyVecIdx := -1
	for i, col := range tableDef.Cols {
		if col.Name == catalog.CPrimaryKeyColName {
			cpkeyVecIdx = i
			break
		}
	}
	if cpkeyVecIdx < 0 || cpkeyVecIdx >= len(insertBatch.Vecs) {
		return
	}
	pkPartIdx := make([]int, 0, len(tableDef.Pkey.Names))
	for _, name := range tableDef.Pkey.Names {
		idx := -1
		for i, col := range tableDef.Cols {
			if col.Name == name {
				idx = i
				break
			}
		}
		if idx < 0 || idx >= len(insertBatch.Vecs) {
			return
		}
		pkPartIdx = append(pkPartIdx, idx)
	}

	packer := types.NewPacker()
	defer packer.Close()
	cpkeyVec := insertBatch.Vecs[cpkeyVecIdx]
	rows := insertBatch.RowCount()
	for r := 0; r < rows; r++ {
		packer.Reset()
		ok := true
		for _, idx := range pkPartIdx {
			if !encodePackForAssert(insertBatch.Vecs[idx], r, packer) {
				ok = false
				break
			}
		}
		if !ok {
			return // unsupported pk type, skip rather than false-positive
		}
		want := packer.GetBuf()
		got := cpkeyVec.GetBytesAt(r)
		if string(want) != string(got) {
			msg := fmt.Sprintf("CPKEY MISALIGN detected on table %s row %d/%d: "+
				"recomputed serial(pk)=%s but stored __mo_cpkey_col=%s",
				tableDef.Name, r, rows, hex.EncodeToString(want), hex.EncodeToString(got))
			logutil.Errorf("%s", msg)
			if debugCpkeyAssert == "panic" {
				panic(msg)
			}
			return
		}
	}
}

// encodePackForAssert mirrors getPackFun in pkg/sql/plan/function for the PK
// column types; returns false for unsupported types so the caller can skip.
func encodePackForAssert(v *vector.Vector, idx int, ps *types.Packer) bool {
	switch v.GetType().Oid {
	case types.T_uuid:
		ps.EncodeUuid(vector.GetFixedAtNoTypeCheck[types.Uuid](v, idx))
	case types.T_int8:
		ps.EncodeInt8(vector.GetFixedAtNoTypeCheck[int8](v, idx))
	case types.T_int16:
		ps.EncodeInt16(vector.GetFixedAtNoTypeCheck[int16](v, idx))
	case types.T_int32:
		ps.EncodeInt32(vector.GetFixedAtNoTypeCheck[int32](v, idx))
	case types.T_int64:
		ps.EncodeInt64(vector.GetFixedAtNoTypeCheck[int64](v, idx))
	case types.T_uint8:
		ps.EncodeUint8(vector.GetFixedAtNoTypeCheck[uint8](v, idx))
	case types.T_uint16:
		ps.EncodeUint16(vector.GetFixedAtNoTypeCheck[uint16](v, idx))
	case types.T_uint32:
		ps.EncodeUint32(vector.GetFixedAtNoTypeCheck[uint32](v, idx))
	case types.T_uint64:
		ps.EncodeUint64(vector.GetFixedAtNoTypeCheck[uint64](v, idx))
	case types.T_date:
		ps.EncodeDate(vector.GetFixedAtNoTypeCheck[types.Date](v, idx))
	case types.T_datetime:
		ps.EncodeDatetime(vector.GetFixedAtNoTypeCheck[types.Datetime](v, idx))
	case types.T_timestamp:
		ps.EncodeTimestamp(vector.GetFixedAtNoTypeCheck[types.Timestamp](v, idx))
	case types.T_decimal64:
		ps.EncodeDecimal64(vector.GetFixedAtNoTypeCheck[types.Decimal64](v, idx))
	case types.T_decimal128:
		ps.EncodeDecimal128(vector.GetFixedAtNoTypeCheck[types.Decimal128](v, idx))
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
		types.T_blob, types.T_text, types.T_json, types.T_datalink:
		ps.EncodeStringType(v.GetBytesAt(idx))
	default:
		return false
	}
	return true
}

//@todo add test case: only insert hidden table

func (update *MultiUpdate) insert_main_table(
	proc *process.Process,
	analyzer process.Analyzer,
	tableIndex int,
	inputBatch *batch.Batch) (err error) {
	ctr := &update.ctr
	updateCtx := update.MultiUpdateCtx[tableIndex]

	// init buffer
	if ctr.insertBuf[tableIndex] == nil {
		bat := batch.NewOffHeapWithSize(len(updateCtx.InsertCols))
		attrs := make([]string, 0, len(updateCtx.TableDef.Cols)-1)
		for _, col := range updateCtx.TableDef.Cols {
			if col.Name == catalog.Row_ID {
				continue
			}
			bat.Vecs[len(attrs)] = vector.NewOffHeapVecWithType(plan.MakeTypeByPlan2Type(col.Typ))
			attrs = append(attrs, col.GetOriginCaseName())
		}
		bat.SetAttributes(attrs)
		ctr.insertBuf[tableIndex] = bat
	}

	if updateCtx.SkipInsertOnNullPk {
		pkColIdx, ok := mainTableInsertPkInputIdx(updateCtx)
		if !ok || pkColIdx >= len(inputBatch.Vecs) {
			return moerr.NewInternalError(proc.Ctx, "invalid main table insert pk column index")
		}
		if inputBatch.Vecs[pkColIdx].HasNull() {
			return update.check_null_and_insert_main_table(proc, analyzer, updateCtx, inputBatch, ctr.insertBuf[tableIndex])
		}
	}

	// preinsert: check not null column
	for insertIdx, inputIdx := range updateCtx.InsertCols {
		col := updateCtx.TableDef.Cols[insertIdx]
		if col.Default != nil && !col.Default.NullAbility && !strings.HasPrefix(col.Name, catalog.PrefixCBColName) {
			if inputBatch.Vecs[inputIdx].HasNull() {
				return moerr.NewConstraintViolation(proc.Ctx, fmt.Sprintf("Column '%s' cannot be null", col.Name))
			}
		}
	}

	// insert
	err = update.insert_table(proc, analyzer, updateCtx, inputBatch, ctr.insertBuf[tableIndex])
	return
}

func (update *MultiUpdate) insert_unique_index_table(
	proc *process.Process,
	analyzer process.Analyzer,
	tableIndex int,
	inputBatch *batch.Batch) (err error) {
	ctr := &update.ctr
	updateCtx := update.MultiUpdateCtx[tableIndex]

	// init buffer
	if ctr.insertBuf[tableIndex] == nil {
		ctr.insertBuf[tableIndex] = batch.NewOffHeapWithSize(2)
		ctr.insertBuf[tableIndex].Attrs = []string{catalog.IndexTableIndexColName, catalog.IndexTablePrimaryColName}
		for insertIdx, inputIdx := range updateCtx.InsertCols {
			ctr.insertBuf[tableIndex].Vecs[insertIdx] = vector.NewOffHeapVecWithType(*inputBatch.Vecs[inputIdx].GetType())
		}
	}

	idxPkPos := updateCtx.InsertCols[0]
	if inputBatch.Vecs[idxPkPos].HasNull() {
		err = update.check_null_and_insert_table(proc, analyzer, updateCtx, inputBatch, ctr.insertBuf[tableIndex])
	} else {
		err = update.insert_table(proc, analyzer, updateCtx, inputBatch, ctr.insertBuf[tableIndex])
	}
	return
}

func (update *MultiUpdate) check_null_and_insert_main_table(
	proc *process.Process,
	analyzer process.Analyzer,
	updateCtx *MultiUpdateCtx,
	inputBatch *batch.Batch,
	insertBatch *batch.Batch,
) (err error) {
	pkPos, ok := mainTableInsertPkInputIdx(updateCtx)
	if !ok || pkPos >= len(inputBatch.Vecs) {
		return moerr.NewInternalError(proc.Ctx, "invalid main table insert pk column index")
	}
	pkNulls := inputBatch.Vecs[pkPos].GetNulls()

	insertBatch.CleanOnlyData()
	rowCount := uint64(inputBatch.RowCount())
	for i := uint64(0); i < rowCount; i++ {
		if pkNulls.Contains(i) {
			continue
		}
		for insertIdx, inputIdx := range updateCtx.InsertCols {
			if err = insertBatch.Vecs[insertIdx].UnionOne(inputBatch.Vecs[inputIdx], int64(i), proc.Mp()); err != nil {
				return err
			}
		}
	}

	newRowCount := insertBatch.Vecs[0].Length()
	if newRowCount == 0 {
		return nil
	}
	insertBatch.SetRowCount(newRowCount)
	if err = checkMainTableNotNull(proc, updateCtx, insertBatch); err != nil {
		return err
	}
	debugCheckMainCpkey(proc, updateCtx, insertBatch)
	tableType := update.ctr.updateCtxInfos[updateCtx.TableDef.Name].tableType
	update.addInsertAffectRows(tableType, uint64(newRowCount))
	source := update.ctr.updateCtxInfos[updateCtx.TableDef.Name].Source

	crs := analyzer.GetOpCounterSet()
	newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)
	if err = source.Write(newCtx, insertBatch); err != nil {
		return err
	}
	analyzer.AddWrittenRows(int64(newRowCount))
	analyzer.AddS3RequestCount(crs)
	analyzer.AddFileServiceCacheInfo(crs)
	analyzer.AddDiskIO(crs)
	return nil
}

func (update *MultiUpdate) insert_secondary_index_table(
	proc *process.Process,
	analyzer process.Analyzer,
	tableIndex int,
	inputBatch *batch.Batch) (err error) {
	ctr := &update.ctr
	updateCtx := update.MultiUpdateCtx[tableIndex]

	// init buf
	if ctr.insertBuf[tableIndex] == nil {
		attrs := make([]string, 0, len(update.MultiUpdateCtx[tableIndex].TableDef.Cols))
		for _, col := range update.MultiUpdateCtx[tableIndex].TableDef.Cols {
			if col.Name != catalog.Row_ID {
				attrs = append(attrs, col.Name)
			}
		}

		ctr.insertBuf[tableIndex] = batch.NewOffHeap(attrs)
		for insertIdx, inputIdx := range updateCtx.InsertCols {
			ctr.insertBuf[tableIndex].Vecs[insertIdx] = vector.NewOffHeapVecWithType(*inputBatch.Vecs[inputIdx].GetType())
		}
	}

	idxPkPos := updateCtx.InsertCols[0]
	if inputBatch.Vecs[idxPkPos].HasNull() {
		err = update.check_null_and_insert_table(proc, analyzer, updateCtx, inputBatch, ctr.insertBuf[tableIndex])
	} else {
		err = update.insert_table(proc, analyzer, updateCtx, inputBatch, ctr.insertBuf[tableIndex])
	}
	return
}

func (update *MultiUpdate) insert_table(
	proc *process.Process,
	analyzer process.Analyzer,
	updateCtx *MultiUpdateCtx,
	inputBatch *batch.Batch,
	insertBatch *batch.Batch,
) (err error) {
	rowCount := inputBatch.RowCount()
	if rowCount == 0 {
		return
	}

	info := update.ctr.updateCtxInfos[updateCtx.TableDef.Name]
	writeBatch := insertBatch
	if info.isContiguous {
		if info.refBatch == nil {
			info.refBatch = batch.NewOffHeapWithSize(len(updateCtx.InsertCols))
			info.refBatch.SetAttributes(insertBatch.Attrs)
		}
		for insertIdx, inputIdx := range updateCtx.InsertCols {
			info.refBatch.Vecs[insertIdx] = inputBatch.Vecs[inputIdx]
		}
		info.refBatch.SetRowCount(rowCount)
		writeBatch = info.refBatch
	} else {
		insertBatch.CleanOnlyData()
		for insertIdx, inputIdx := range updateCtx.InsertCols {
			err = insertBatch.Vecs[insertIdx].UnionBatch(inputBatch.Vecs[inputIdx], 0, inputBatch.Vecs[inputIdx].Length(), nil, proc.GetMPool())
			if err != nil {
				return err
			}
		}
		insertBatch.SetRowCount(insertBatch.Vecs[0].Length())
	}

	update.addInsertAffectRows(info.tableType, uint64(writeBatch.RowCount()))

	crs := analyzer.GetOpCounterSet()
	newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)
	err = info.Source.Write(newCtx, writeBatch)
	if err != nil {
		return err
	}
	analyzer.AddWrittenRows(int64(writeBatch.RowCount()))
	analyzer.AddS3RequestCount(crs)
	analyzer.AddFileServiceCacheInfo(crs)
	analyzer.AddDiskIO(crs)
	return
}

func isContiguousMapping(cols []int) bool {
	if len(cols) == 0 {
		return false
	}
	base := cols[0]
	for i, col := range cols {
		if col != base+i {
			return false
		}
	}
	return true
}

func mainTableInsertPkInputIdx(updateCtx *MultiUpdateCtx) (int, bool) {
	if updateCtx.InsertPkColIdx < 0 || updateCtx.InsertPkColIdx >= len(updateCtx.InsertCols) {
		return 0, false
	}
	return updateCtx.InsertCols[updateCtx.InsertPkColIdx], true
}

func (update *MultiUpdate) check_null_and_insert_table(
	proc *process.Process,
	analyzer process.Analyzer,
	updateCtx *MultiUpdateCtx,
	inputBatch *batch.Batch,
	insertBatch *batch.Batch) (err error) {

	idxPkPos := updateCtx.InsertCols[0]
	mainPkPos := updateCtx.InsertCols[1]
	idxPkVec := inputBatch.Vecs[idxPkPos]
	mainPkVec := inputBatch.Vecs[mainPkPos]
	idxPkNulls := inputBatch.Vecs[updateCtx.InsertCols[0]].GetNulls()

	insertBatch.CleanOnlyData()
	rowCount := uint64(inputBatch.RowCount())
	for i := uint64(0); i < rowCount; i++ {
		if !idxPkNulls.Contains(i) {
			err = insertBatch.Vecs[0].UnionOne(idxPkVec, int64(i), proc.Mp())
			if err != nil {
				return err
			}

			err = insertBatch.Vecs[1].UnionOne(mainPkVec, int64(i), proc.Mp())
			if err != nil {
				return err
			}
		}
	}

	newRowCount := insertBatch.Vecs[0].Length()
	if newRowCount > 0 {
		insertBatch.SetRowCount(newRowCount)
		tableType := update.ctr.updateCtxInfos[updateCtx.TableDef.Name].tableType
		update.addInsertAffectRows(tableType, uint64(newRowCount))
		source := update.ctr.updateCtxInfos[updateCtx.TableDef.Name].Source

		crs := analyzer.GetOpCounterSet()
		newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)
		err = source.Write(newCtx, insertBatch)
		if err != nil {
			return err
		}
		analyzer.AddWrittenRows(int64(insertBatch.RowCount()))
		analyzer.AddS3RequestCount(crs)
		analyzer.AddFileServiceCacheInfo(crs)
		analyzer.AddDiskIO(crs)
	}
	return
}
