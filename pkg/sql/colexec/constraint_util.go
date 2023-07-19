// Copyright 2022 Matrix Origin
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

package colexec

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func FilterRowIdForDel(proc *process.Process, bat *batch.Batch, idx int) (*batch.Batch, error) {
	retBatch := batch.New(true, []string{catalog.Row_ID})
	rowIdMap := make(map[types.Rowid]bool)
	vecNulls := bat.Vecs[idx].GetNulls()
	retVec := vector.NewVec(types.T_Rowid.ToType())
	err := retVec.PreExtend(bat.Vecs[idx].Length()-vecNulls.Count(), proc.Mp())
	if err != nil {
		return nil, err
	}
	row := 0
	for i, r := range vector.MustFixedCol[types.Rowid](bat.Vecs[idx]) {
		if !vecNulls.Contains(uint64(i)) {
			if rowIdMap[r] {
				continue
			}
			rowIdMap[r] = true
			if err = vector.AppendFixed(retVec, r, false, proc.Mp()); err != nil {
				retVec.Free(proc.Mp())
				return nil, err
			}
			row++
		}
	}
	retBatch.SetZs(retVec.Length(), proc.Mp())
	retBatch.SetVector(0, retVec)
	return retBatch, nil
}

// GroupByPartitionForDeleteS3: Group data based on partition and return batch array with the same length as the number of partitions.
// Data from the same partition is placed in the same batch
func GroupByPartitionForDeleteS3(proc *process.Process, bat *batch.Batch, rowIdIdx int, partitionIdx int, partitionNum int, pkIdx int) ([]*batch.Batch, error) {
	vecList := make([]*vector.Vector, partitionNum)
	pkList := make([]*vector.Vector, partitionNum)
	pkTyp := bat.Vecs[pkIdx].GetType()
	fun := vector.GetUnionOneFunction(*pkTyp, proc.Mp())
	for i := 0; i < partitionNum; i++ {
		//retVec := vector.NewVec(types.T_Rowid.ToType())
		retVec := proc.GetVector(types.T_Rowid.ToType())
		pkVec := proc.GetVector(*pkTyp)
		vecList[i] = retVec
		pkList[i] = pkVec
	}

	// Fill the data into the corresponding batch based on the different partitions to which the current `row_id` data
	for i, rowid := range vector.MustFixedCol[types.Rowid](bat.Vecs[rowIdIdx]) {
		if !bat.Vecs[rowIdIdx].GetNulls().Contains(uint64(i)) {
			partition := vector.MustFixedCol[int32](bat.Vecs[partitionIdx])[i]
			if partition == -1 {
				for _, vecElem := range vecList {
					vecElem.Free(proc.Mp())
				}
				//panic("partiton number is -1, the partition number is incorrect")
				return nil, moerr.NewInvalidInput(proc.Ctx, "Table has no partition for value from column_list")
			} else {
				vector.AppendFixed(vecList[partition], rowid, false, proc.Mp())
				err := fun(pkList[partition], bat.Vecs[pkIdx], int64(i))
				if err != nil {
					return nil, err
				}
			}
		}
	}
	// create a batch array equal to the number of partitions
	batches := make([]*batch.Batch, partitionNum)
	for i := range vecList {
		// initialize the vectors in each batch, the batch only contains a `row_id` column
		retBatch := batch.New(true, []string{catalog.Row_ID, "pk"})
		retBatch.SetZs(vecList[i].Length(), proc.Mp())
		retBatch.SetVector(0, vecList[i])
		retBatch.SetVector(1, pkList[i])
		batches[i] = retBatch
	}
	return batches, nil
}

// GroupByPartitionForDelete: Group data based on partition and return batch array with the same length as the number of partitions.
// Data from the same partition is placed in the same batch
func GroupByPartitionForDelete(proc *process.Process, bat *batch.Batch, rowIdIdx int, partitionIdx int, partitionNum int) ([]*batch.Batch, error) {
	vecList := make([]*vector.Vector, partitionNum)
	for i := 0; i < partitionNum; i++ {
		retVec := proc.GetVector(types.T_Rowid.ToType())
		vecList[i] = retVec
	}

	// Fill the data into the corresponding batch based on the different partitions to which the current `row_id` data
	for i, rowid := range vector.MustFixedCol[types.Rowid](bat.Vecs[rowIdIdx]) {
		if !bat.Vecs[rowIdIdx].GetNulls().Contains(uint64(i)) {
			partition := vector.MustFixedCol[int32](bat.Vecs[partitionIdx])[i]
			if partition == -1 {
				for _, vecElem := range vecList {
					vecElem.Free(proc.Mp())
				}
				//panic("partiton number is -1, the partition number is incorrect")
				return nil, moerr.NewInvalidInput(proc.Ctx, "Table has no partition for value from column_list")
			} else {
				vector.AppendFixed(vecList[partition], rowid, false, proc.Mp())
			}
		}
	}
	// create a batch array equal to the number of partitions
	batches := make([]*batch.Batch, partitionNum)
	for i := range vecList {
		// initialize the vectors in each batch, the batch only contains a `row_id` column
		retBatch := batch.New(true, []string{catalog.Row_ID})
		retBatch.SetZs(vecList[i].Length(), proc.Mp())
		retBatch.SetVector(0, vecList[i])
		batches[i] = retBatch
	}
	return batches, nil
}

// GroupByPartitionForInsert: Group data based on partition and return batch array with the same length as the number of partitions.
// Data from the same partition is placed in the same batch
func GroupByPartitionForInsert(proc *process.Process, bat *batch.Batch, attrs []string, pIdx int, partitionNum int) ([]*batch.Batch, error) {
	// create a batch array equal to the number of partitions
	batches := make([]*batch.Batch, partitionNum)
	for partIdx := 0; partIdx < partitionNum; partIdx++ {
		// initialize the vectors in each batch, corresponding to the original batch
		partitionBatch := batch.NewWithSize(len(attrs))
		partitionBatch.Attrs = attrs
		for i := range partitionBatch.Attrs {
			vecType := bat.GetVector(int32(i)).GetType()
			//retVec := vector.NewVec(*vecType)
			retVec := proc.GetVector(*vecType)
			partitionBatch.SetVector(int32(i), retVec)
		}
		batches[partIdx] = partitionBatch
	}

	// fill the data into the corresponding batch based on the different partitions to which the current row data belongs
	for i, partition := range vector.MustFixedCol[int32](bat.Vecs[pIdx]) {
		if !bat.Vecs[pIdx].GetNulls().Contains(uint64(i)) {
			if partition == -1 {
				for _, batchElem := range batches {
					batchElem.Clean(proc.Mp())
				}
				//panic("partiton number is -1, the partition number is incorrect")
				return nil, moerr.NewInvalidInput(proc.Ctx, "Table has no partition for value from column_list")
			} else {
				//  `i` corresponds to the row number of the batch data,
				//  `j` corresponds to the column number of the batch data
				for j := range attrs {
					batches[partition].GetVector(int32(j)).UnionOne(bat.Vecs[j], int64(i), proc.Mp())
				}
			}
		}
	}

	for partIdx := range batches {
		length := batches[partIdx].GetVector(0).Length()
		batches[partIdx].SetZs(length, proc.Mp())
	}
	return batches, nil
}

func BatchDataNotNullCheck(tmpBat *batch.Batch, tableDef *plan.TableDef, ctx context.Context) error {
	for j := range tmpBat.Vecs {
		if tmpBat.Vecs[j] == nil {
			continue
		}
		nsp := tmpBat.Vecs[j].GetNulls()
		if tableDef.Cols[j].Default != nil && !tableDef.Cols[j].Default.NullAbility && nulls.Any(nsp) {
			return moerr.NewConstraintViolation(ctx, fmt.Sprintf("Column '%s' cannot be null", tmpBat.Attrs[j]))
		}
	}
	return nil
}
