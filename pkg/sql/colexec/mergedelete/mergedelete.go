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

const argName = "merge_delete"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": MergeS3DeleteInfo ")
}

func (arg *Argument) Prepare(proc *process.Process) error {
	ref := arg.Ref
	eng := arg.Engine
	partitionNames := arg.PartitionTableNames
	rel, partitionRels, err := colexec.GetRelAndPartitionRelsByObjRef(proc.Ctx, proc, eng, ref, partitionNames)
	if err != nil {
		return err
	}
	arg.delSource = rel
	arg.partitionSources = partitionRels
	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	var err error
	var name string
	ap := arg

	result, err := arg.GetChildren(0).Call(proc)
	if err != nil {
		return result, err
	}

	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	if result.Batch == nil || result.Batch.IsEmpty() {
		return result, nil
	}
	bat := result.Batch

	// 	  blkId           deltaLoc                        type                                 partitionIdx
	// |----------|-----------------------------|-------------------------------------------|---------------------
	// |  blk_id  | batch.Marshal(deltaLoc)     | FlushDeltaLoc  (DN Block )                |  partitionIdx
	// |  blk_id  | batch.Marshal(int64 offset) | CNBlockOffset (CN Block )                 |  partitionIdx
	// |  blk_id  | batch.Marshal(rowId)        | RawRowIdBatch (DN Blcok )                 |  partitionIdx
	// |  blk_id  | batch.Marshal(int64 offset) | RawBatchOffset(RawBatch[in txn workspace])|  partitionIdx
	blkIds := vector.MustStrCol(bat.GetVector(0))
	deltaLocs := vector.MustBytesCol(bat.GetVector(1))
	typs := vector.MustFixedCol[int8](bat.GetVector(2))

	// If the target table is a partition table, Traverse partition subtables for separate processing
	if len(ap.partitionSources) > 0 {
		partitionIdxs := vector.MustFixedCol[int32](bat.GetVector(3))
		for i := 0; i < bat.RowCount(); i++ {
			name = fmt.Sprintf("%s|%d", blkIds[i], typs[i])
			bat := &batch.Batch{}
			if err := bat.UnmarshalBinary(deltaLocs[i]); err != nil {
				return result, err
			}
			bat.Cnt = 1
			pIndex := partitionIdxs[i]
			err = ap.partitionSources[pIndex].Delete(proc.Ctx, bat, name)
			if err != nil {
				return result, err
			}
		}
	} else {
		// If the target table is a general table
		for i := 0; i < bat.RowCount(); i++ {
			name = fmt.Sprintf("%s|%d", blkIds[i], typs[i])
			bat := &batch.Batch{}
			if err := bat.UnmarshalBinary(deltaLocs[i]); err != nil {
				return result, err
			}
			bat.Cnt = 1
			err = ap.delSource.Delete(proc.Ctx, bat, name)
			if err != nil {
				return result, err
			}
		}
	}
	// and there are another attr used to record how many rows are deleted
	ap.AffectedRows += uint64(vector.GetFixedAt[uint32](bat.GetVector(4), 0))
	return result, nil
}
