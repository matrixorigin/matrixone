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
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (update *MultiUpdate) insert_main_table(
	proc *process.Process,
	tableIndex int,
	inputBatch *batch.Batch) (err error) {
	ctr := &update.ctr
	updateCtx := update.MultiUpdateCtx[tableIndex]

	// init buffer
	if ctr.insertBuf[tableIndex] == nil {
		bat := batch.NewWithSize(len(updateCtx.insertCols))
		attrs := make([]string, 0, len(updateCtx.tableDef.Cols)-1)
		for _, col := range updateCtx.tableDef.Cols {
			if col.Name == catalog.Row_ID {
				continue
			}
			bat.Vecs[len(attrs)] = vector.NewVec(plan.MakeTypeByPlan2Type(col.Typ))
			attrs = append(attrs, col.Name)
		}
		bat.SetAttributes(attrs)
		ctr.insertBuf[tableIndex] = bat
	}

	// preinsert: check not null column
	for insertIdx, inputIdx := range updateCtx.insertCols {
		col := updateCtx.tableDef.Cols[insertIdx]
		if col.Default != nil && !col.Default.NullAbility {
			if inputBatch.Vecs[inputIdx].HasNull() {
				return moerr.NewConstraintViolation(proc.Ctx, fmt.Sprintf("Column '%s' cannot be null", col.Name))
			}
		}
	}

	// insert
	err = update.insert_table(proc, updateCtx, inputBatch, ctr.insertBuf[tableIndex])
	return
}

func (update *MultiUpdate) insert_uniuqe_index_table(
	proc *process.Process,
	tableIndex int,
	inputBatch *batch.Batch) (err error) {
	ctr := &update.ctr
	updateCtx := update.MultiUpdateCtx[tableIndex]

	// init buffer
	if ctr.insertBuf[tableIndex] == nil {
		ctr.insertBuf[tableIndex] = batch.NewWithSize(2)
		ctr.insertBuf[tableIndex].Attrs = []string{catalog.IndexTableIndexColName, catalog.IndexTablePrimaryColName}
		for insertIdx, inputIdx := range updateCtx.insertCols {
			ctr.insertBuf[tableIndex].Vecs[insertIdx] = vector.NewVec(*inputBatch.Vecs[inputIdx].GetType())
		}
	}

	idxPkPos := updateCtx.insertCols[0]
	if inputBatch.Vecs[idxPkPos].HasNull() {
		err = update.check_null_and_insert_table(proc, updateCtx, inputBatch, ctr.insertBuf[tableIndex])
	} else {
		err = update.insert_table(proc, updateCtx, inputBatch, ctr.insertBuf[tableIndex])
	}
	return
}

func (update *MultiUpdate) insert_secondary_index_table(
	proc *process.Process,
	tableIndex int,
	inputBatch *batch.Batch) (err error) {
	ctr := &update.ctr
	updateCtx := update.MultiUpdateCtx[tableIndex]

	// init buf
	if ctr.insertBuf[tableIndex] == nil {
		ctr.insertBuf[tableIndex] = batch.NewWithSize(2)
		ctr.insertBuf[tableIndex].Attrs = []string{catalog.IndexTableIndexColName, catalog.IndexTablePrimaryColName}
		for insertIdx, inputIdx := range updateCtx.insertCols {
			ctr.insertBuf[tableIndex].Vecs[insertIdx] = vector.NewVec(*inputBatch.Vecs[inputIdx].GetType())
		}
	}

	idxPkPos := updateCtx.insertCols[0]
	if inputBatch.Vecs[idxPkPos].HasNull() {
		err = update.check_null_and_insert_table(proc, updateCtx, inputBatch, ctr.insertBuf[tableIndex])
	} else {
		err = update.insert_table(proc, updateCtx, inputBatch, ctr.insertBuf[tableIndex])
	}
	return
}

func (update *MultiUpdate) insert_table(
	proc *process.Process,
	updateCtx *MultiUpdateCtx,
	inputBatch *batch.Batch,
	insertBatch *batch.Batch) (err error) {
	if len(updateCtx.partitionTableIDs) > 0 {
		for partIdx := range len(updateCtx.partitionTableIDs) {
			insertBatch.CleanOnlyData()
			expected := int32(partIdx)
			partTableIDs := vector.MustFixedColWithTypeCheck[int32](inputBatch.Vecs[updateCtx.partitionIdx])
			partTableNulls := inputBatch.Vecs[updateCtx.partitionIdx].GetNulls()

			for i, partition := range partTableIDs {
				if !partTableNulls.Contains(uint64(i)) {
					if partition == -1 {
						return moerr.NewInvalidInput(proc.Ctx, "Table has no partition for value from column_list")
					} else if partition == expected {
						for insertIdx, inputIdx := range updateCtx.insertCols {
							err = insertBatch.Vecs[insertIdx].UnionOne(inputBatch.Vecs[inputIdx], int64(i), proc.Mp())
							if err != nil {
								return err
							}
						}
					}
				}
			}

			err = updateCtx.partitionSources[partIdx].Write(proc.Ctx, insertBatch)
			if err != nil {
				return err
			}
		}
	} else {
		insertBatch.CleanOnlyData()
		for insertIdx, inputIdx := range updateCtx.insertCols {
			err = insertBatch.Vecs[insertIdx].UnionBatch(inputBatch.Vecs[inputIdx], 0, inputBatch.Vecs[inputIdx].Length(), nil, proc.GetMPool())
			if err != nil {
				return err
			}
		}
		err = updateCtx.source.Write(proc.Ctx, insertBatch)
	}
	return
}

func (update *MultiUpdate) check_null_and_insert_table(
	proc *process.Process,
	updateCtx *MultiUpdateCtx,
	inputBatch *batch.Batch,
	insertBatch *batch.Batch) (err error) {

	idxPkPos := updateCtx.insertCols[0]
	mainPkPos := updateCtx.insertCols[1]
	idxPkVec := inputBatch.Vecs[idxPkPos]
	mainPkVec := inputBatch.Vecs[mainPkPos]
	idxPkNulls := inputBatch.Vecs[updateCtx.insertCols[0]].GetNulls()

	if len(updateCtx.partitionTableIDs) > 0 {
		for partIdx := range len(updateCtx.partitionTableIDs) {
			insertBatch.CleanOnlyData()
			expected := int32(partIdx)
			partTableIDs := vector.MustFixedColWithTypeCheck[int32](inputBatch.Vecs[updateCtx.partitionIdx])
			partTableNulls := inputBatch.Vecs[updateCtx.partitionIdx].GetNulls()

			for i, partition := range partTableIDs {
				if !partTableNulls.Contains(uint64(i)) {
					if partition == -1 {
						return moerr.NewInvalidInput(proc.Ctx, "Table has no partition for value from column_list")
					} else if partition == expected {
						if !idxPkNulls.Contains(uint64(i)) {
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
				}
			}

			err = updateCtx.partitionSources[partIdx].Write(proc.Ctx, insertBatch)
			if err != nil {
				return err
			}
		}
	} else {
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

		err = updateCtx.source.Write(proc.Ctx, insertBatch)
	}
	return
}
