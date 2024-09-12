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
	// init buffer
	ctr := &update.ctr
	updateCtx := update.MultiUpdateCtx[tableIndex]

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

	// preinsert:  fill autoIncr & check not null column

	// insert
	update.insert_table(proc, updateCtx, inputBatch, ctr.insertBuf[tableIndex])
	return
}

func (update *MultiUpdate) insert_uniuqe_index_table(
	proc *process.Process,
	tableIndex int,
	inputBatch *batch.Batch) (err error) {
	// init buffer

	// preinsert: build uniuqe index table's PK

	// insert

	return nil
}

func (update *MultiUpdate) insert_secondary_index_table(
	proc *process.Process,
	tableIndex int,
	inputBatch *batch.Batch) (err error) {
	// preinsert:

	// insert

	return nil
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

			for i, partition := range partTableIDs {
				if !inputBatch.Vecs[updateCtx.partitionIdx].GetNulls().Contains(uint64(i)) {
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
