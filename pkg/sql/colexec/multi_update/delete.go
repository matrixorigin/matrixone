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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (update *MultiUpdate) delete_table(
	proc *process.Process,
	analyzer process.Analyzer,
	updateCtx *MultiUpdateCtx,
	inputBatch *batch.Batch,
	idx int,
) (err error) {

	// init buf
	ctr := &update.ctr
	if ctr.deleteBuf[idx] == nil {
		mainPkIdx := updateCtx.DeleteCols[1]
		ctr.deleteBuf[idx] = newDeleteBatch(inputBatch, mainPkIdx)
	}
	deleteBatch := ctr.deleteBuf[idx]
	rowIdIdx := updateCtx.DeleteCols[0]
	rowIdVec := inputBatch.Vecs[rowIdIdx]
	if rowIdVec.IsConstNull() {
		return
	}
	rowCount := inputBatch.RowCount()

	if len(updateCtx.PartitionTableIDs) > 0 {
		partTableNulls := inputBatch.Vecs[updateCtx.OldPartitionIdx].GetNulls()
		partTableIDs := vector.MustFixedColWithTypeCheck[int32](inputBatch.Vecs[updateCtx.OldPartitionIdx])

		for partIdx := range len(updateCtx.PartitionTableIDs) {
			rowIdNulls := rowIdVec.GetNulls()
			if rowIdNulls.Count() == rowCount {
				continue
			}

			deleteBatch.CleanOnlyData()
			expected := int32(partIdx)

			for i, partition := range partTableIDs {
				if !partTableNulls.Contains(uint64(i)) {
					if partition == -1 {
						return moerr.NewInvalidInput(proc.Ctx, "Table has no partition for value from column_list")
					} else if partition == expected {
						if !rowIdNulls.Contains(uint64(i)) {
							for deleteIdx, inputIdx := range updateCtx.DeleteCols {
								err = deleteBatch.Vecs[deleteIdx].UnionOne(inputBatch.Vecs[inputIdx], int64(i), proc.Mp())
								if err != nil {
									return err
								}
							}
						}
					}
				}
			}

			rowCount := deleteBatch.Vecs[0].Length()
			if rowCount > 0 {
				deleteBatch.SetRowCount(rowCount)
				tableType := update.ctr.updateCtxInfos[updateCtx.TableDef.Name].tableType
				update.addDeleteAffectRows(tableType, uint64(rowCount))
				source := update.ctr.updateCtxInfos[updateCtx.TableDef.Name].Sources[partIdx]

				crs := analyzer.GetOpCounterSet()
				newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)
				err = source.Delete(newCtx, deleteBatch, catalog.Row_ID)
				if err != nil {
					return err
				}
				analyzer.AddDeletedRows(int64(deleteBatch.RowCount()))
				analyzer.AddS3RequestCount(crs)
				analyzer.AddDiskIO(crs)
			}
		}
	} else {
		deleteBatch.CleanOnlyData()
		if rowIdVec.HasNull() {
			// multi delete or delete unique table with null value
			rowIdNulls := rowIdVec.GetNulls()
			if rowIdNulls.Count() == rowCount {
				return
			}

			for i := 0; i < rowCount; i++ {
				if !rowIdNulls.Contains(uint64(i)) {
					for deleteIdx, inputIdx := range updateCtx.DeleteCols {
						err = deleteBatch.Vecs[deleteIdx].UnionOne(inputBatch.Vecs[inputIdx], int64(i), proc.Mp())
						if err != nil {
							return err
						}
					}
				}
			}

		} else {
			for deleteIdx, inputIdx := range updateCtx.DeleteCols {
				err = deleteBatch.Vecs[deleteIdx].UnionBatch(inputBatch.Vecs[inputIdx], 0, inputBatch.Vecs[inputIdx].Length(), nil, proc.GetMPool())
				if err != nil {
					return err
				}
			}
		}
		rowCount := deleteBatch.Vecs[0].Length()
		if rowCount > 0 {
			deleteBatch.SetRowCount(rowCount)
			tableType := update.ctr.updateCtxInfos[updateCtx.TableDef.Name].tableType
			update.addDeleteAffectRows(tableType, uint64(rowCount))
			source := update.ctr.updateCtxInfos[updateCtx.TableDef.Name].Sources[0]

			crs := analyzer.GetOpCounterSet()
			newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)
			err = source.Delete(newCtx, deleteBatch, catalog.Row_ID)
			if err != nil {
				return err
			}
			analyzer.AddDeletedRows(int64(deleteBatch.RowCount()))
			analyzer.AddS3RequestCount(crs)
			analyzer.AddDiskIO(crs)
		}
	}

	return
}

func newDeleteBatch(inputBatch *batch.Batch, mainPkIdx int) *batch.Batch {
	buf := batch.New([]string{catalog.Row_ID, "pk"})
	buf.SetVector(0, vector.NewVec(types.T_Rowid.ToType()))
	buf.SetVector(1, vector.NewVec(*inputBatch.Vecs[mainPkIdx].GetType()))
	return buf
}
