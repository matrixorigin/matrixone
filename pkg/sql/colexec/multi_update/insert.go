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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

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
		bat := batch.NewWithSize(len(updateCtx.InsertCols))
		attrs := make([]string, 0, len(updateCtx.TableDef.Cols)-1)
		for _, col := range updateCtx.TableDef.Cols {
			if col.Name == catalog.Row_ID {
				continue
			}
			bat.Vecs[len(attrs)] = vector.NewVec(plan.MakeTypeByPlan2Type(col.Typ))
			attrs = append(attrs, col.GetOriginCaseName())
		}
		bat.SetAttributes(attrs)
		ctr.insertBuf[tableIndex] = bat
	}

	// preinsert: check not null column
	for insertIdx, inputIdx := range updateCtx.InsertCols {
		col := updateCtx.TableDef.Cols[insertIdx]
		if col.Default != nil && !col.Default.NullAbility {
			if inputBatch.Vecs[inputIdx].HasNull() {
				return moerr.NewConstraintViolation(proc.Ctx, fmt.Sprintf("Column '%s' cannot be null", col.Name))
			}
		}
	}

	// insert
	err = update.insert_table(proc, analyzer, updateCtx, inputBatch, ctr.insertBuf[tableIndex])
	return
}

func (update *MultiUpdate) insert_uniuqe_index_table(
	proc *process.Process,
	analyzer process.Analyzer,
	tableIndex int,
	inputBatch *batch.Batch) (err error) {
	ctr := &update.ctr
	updateCtx := update.MultiUpdateCtx[tableIndex]

	// init buffer
	if ctr.insertBuf[tableIndex] == nil {
		ctr.insertBuf[tableIndex] = batch.NewWithSize(2)
		ctr.insertBuf[tableIndex].Attrs = []string{catalog.IndexTableIndexColName, catalog.IndexTablePrimaryColName}
		for insertIdx, inputIdx := range updateCtx.InsertCols {
			ctr.insertBuf[tableIndex].Vecs[insertIdx] = vector.NewVec(*inputBatch.Vecs[inputIdx].GetType())
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
		ctr.insertBuf[tableIndex] = batch.New(attrs)
		for insertIdx, inputIdx := range updateCtx.InsertCols {
			ctr.insertBuf[tableIndex].Vecs[insertIdx] = vector.NewVec(*inputBatch.Vecs[inputIdx].GetType())
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
	insertBatch.CleanOnlyData()
	for insertIdx, inputIdx := range updateCtx.InsertCols {
		err = insertBatch.Vecs[insertIdx].UnionBatch(inputBatch.Vecs[inputIdx], 0, inputBatch.Vecs[inputIdx].Length(), nil, proc.GetMPool())
		if err != nil {
			return err
		}
	}
	rowCount := insertBatch.Vecs[0].Length()
	if rowCount > 0 {
		insertBatch.SetRowCount(rowCount)
		tableType := update.ctr.updateCtxInfos[updateCtx.TableDef.Name].tableType
		update.addInsertAffectRows(tableType, uint64(rowCount))
		source := update.ctr.updateCtxInfos[updateCtx.TableDef.Name].Sources[0]

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
		source := update.ctr.updateCtxInfos[updateCtx.TableDef.Name].Sources[0]

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
