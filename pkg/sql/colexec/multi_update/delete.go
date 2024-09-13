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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (update *MultiUpdate) delete_table(
	proc *process.Process,
	updateCtx *MultiUpdateCtx,
	inputBatch *batch.Batch,
	deleteBatch *batch.Batch) (err error) {
	deleteBatch.CleanOnlyData()

	rowIdIdx := updateCtx.deleteCols[0]

	if len(updateCtx.partitionTableIDs) > 0 {
		for partIdx := range len(updateCtx.partitionTableIDs) {
			deleteBatch.CleanOnlyData()
			expected := int32(partIdx)
			partTableIDs := vector.MustFixedColWithTypeCheck[int32](inputBatch.Vecs[updateCtx.partitionIdx])
			rowIdNulls := inputBatch.Vecs[rowIdIdx].GetNulls()

			for i, partition := range partTableIDs {
				if !inputBatch.Vecs[updateCtx.partitionIdx].GetNulls().Contains(uint64(i)) {
					if partition == -1 {
						return moerr.NewInvalidInput(proc.Ctx, "Table has no partition for value from column_list")
					} else if partition == expected {
						if !rowIdNulls.Contains(uint64(i)) {
							for deleteIdx, inputIdx := range updateCtx.deleteCols {
								err = deleteBatch.Vecs[deleteIdx].UnionOne(inputBatch.Vecs[inputIdx], int64(i), proc.Mp())
								if err != nil {
									return err
								}
							}
						}
					}
				}
			}

			err = updateCtx.partitionSources[partIdx].Delete(proc.Ctx, deleteBatch, catalog.Row_ID)
			if err != nil {
				return err
			}
		}
	} else {
		deleteBatch.CleanOnlyData()

		if inputBatch.Vecs[rowIdIdx].HasNull() {
			// multi delete
			rowIdNulls := inputBatch.Vecs[rowIdIdx].GetNulls()
			for i := 0; i < inputBatch.RowCount(); i++ {
				if !rowIdNulls.Contains(uint64(i)) {
					for deleteIdx, inputIdx := range updateCtx.deleteCols {
						err = deleteBatch.Vecs[deleteIdx].UnionOne(inputBatch.Vecs[inputIdx], int64(i), proc.Mp())
						if err != nil {
							return err
						}
					}
				}
			}

		} else {
			for deleteIdx, inputIdx := range updateCtx.deleteCols {
				err = deleteBatch.Vecs[deleteIdx].UnionBatch(inputBatch.Vecs[inputIdx], 0, inputBatch.Vecs[inputIdx].Length(), nil, proc.GetMPool())
				if err != nil {
					return err
				}
			}
		}
		err = updateCtx.source.Delete(proc.Ctx, deleteBatch, catalog.Row_ID)
	}

	return
}
