// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mergedelete

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "merge_delete"

func (mergeDelete *MergeDelete) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": MergeS3DeleteInfo ")
}

func (mergeDelete *MergeDelete) OpType() vm.OpType {
	return vm.MergeDelete
}

func (mergeDelete *MergeDelete) Prepare(proc *process.Process) error {
	mergeDelete.OpAnalyzer = process.NewAnalyzer(mergeDelete.GetIdx(), mergeDelete.IsFirst, mergeDelete.IsLast, "merge_delete")

	ref := mergeDelete.Ref
	eng := mergeDelete.Engine
	partitionNames := mergeDelete.PartitionTableNames
	rel, partitionRels, err := colexec.GetRelAndPartitionRelsByObjRef(proc.Ctx, proc, eng, ref, partitionNames)
	if err != nil {
		return err
	}
	mergeDelete.ctr.delSource = rel
	mergeDelete.ctr.partitionSources = partitionRels
	mergeDelete.ctr.affectedRows = 0
	return nil
}

func (mergeDelete *MergeDelete) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	//anal := proc.GetAnalyze(mergeDelete.GetIdx(), mergeDelete.GetParallelIdx(), mergeDelete.GetParallelMajor())
	//anal.Start()
	//defer anal.Stop()
	analyzer := mergeDelete.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	var err error
	var name string

	input, err := vm.ChildrenCall(mergeDelete.Children[0], proc, analyzer)
	if err != nil {
		return vm.CancelResult, err
	}

	if input.Batch == nil || input.Batch.IsEmpty() {
		return input, nil
	}
	bat := input.Batch

	// 	  blkId           deltaLoc                        type                                 partitionIdx
	// |----------|-----------------------------|-------------------------------------------|---------------------
	// |  blk_id  | batch.Marshal(deltaLoc)     | FlushDeltaLoc  (DN Block )                |  partitionIdx
	// |  blk_id  | batch.Marshal(int64 offset) | CNBlockOffset (CN Block )                 |  partitionIdx
	// |  blk_id  | batch.Marshal(rowId)        | RawRowIdBatch (DN Blcok )                 |  partitionIdx
	// |  blk_id  | batch.Marshal(int64 offset) | RawBatchOffset(RawBatch[in txn workspace])|  partitionIdx
	blkIds, area0 := vector.MustVarlenaRawData(bat.GetVector(0))
	deltaLocs, area1 := vector.MustVarlenaRawData(bat.GetVector(1))
	typs := vector.MustFixedCol[int8](bat.GetVector(2))

	// If the target table is a partition table, Traverse partition subtables for separate processing
	if len(mergeDelete.ctr.partitionSources) > 0 {
		partitionIdxs := vector.MustFixedCol[int32](bat.GetVector(3))
		for i := 0; i < bat.RowCount(); i++ {
			name = fmt.Sprintf("%s|%d", blkIds[i].UnsafeGetString(area0), typs[i])
			//@todo need new method for batch UnmarshalBinaryWithNoCopy(data []byte, enBatch *batch.EncodeBatch, mp *mpool.MPool)
			//then we can reuse mem
			bat := &batch.Batch{}
			if err := bat.UnmarshalBinary(deltaLocs[i].GetByteSlice(area1)); err != nil {
				return input, err
			}
			bat.Cnt = 1
			pIndex := partitionIdxs[i]
			err = mergeDelete.ctr.partitionSources[pIndex].Delete(proc.Ctx, bat, name)
			if err != nil {
				return input, err
			}
			bat.Clean(proc.Mp())
		}
	} else {
		// If the target table is a general table
		for i := 0; i < bat.RowCount(); i++ {
			name = fmt.Sprintf("%s|%d", blkIds[i], typs[i])
			//@todo need new method UnmarshalBinaryWithNoCopy(data []byte, enBatch *batch.EncodeBatch, mp *mpool.MPool) for batch
			//then we can reuse mem
			bat := &batch.Batch{}
			if err := bat.UnmarshalBinary(deltaLocs[i].GetByteSlice(area1)); err != nil {
				return input, err
			}
			bat.Cnt = 1
			err = mergeDelete.ctr.delSource.Delete(proc.Ctx, bat, name)
			if err != nil {
				return input, err
			}
			bat.Clean(proc.Mp())
		}
	}
	// and there are another attr used to record how many rows are deleted
	if mergeDelete.AddAffectedRows {
		mergeDelete.ctr.affectedRows += uint64(vector.GetFixedAt[uint32](bat.GetVector(4), 0))
	}

	analyzer.Output(input.Batch)
	return input, nil
}
