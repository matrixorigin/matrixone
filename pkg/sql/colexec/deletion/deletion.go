// Copyright 2021 Matrix Origin
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

package deletion

import (
	"bytes"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

//row id be divided into four types:
// 1. RawBatchOffset : belong to txn's workspace
// 2. CNBlockOffset  : belong to txn's workspace

// 3. RawRowIdBatch  : belong to txn's snapshot data.
// 4. FlushDeltaLoc   : belong to txn's snapshot data, which on S3 and pointed by delta location.
const (
	RawRowIdBatch = iota
	// remember that, for one block,
	// when it sends the info to mergedeletes,
	// either it's Compaction or not.
	Compaction
	CNBlockOffset
	RawBatchOffset
	FlushDeltaLoc
)

const opName = "deletion"

func (deletion *Deletion) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": delete rows")
}

func (deletion *Deletion) OpType() vm.OpType {
	return vm.Deletion
}

func (deletion *Deletion) Prepare(proc *process.Process) error {
	if deletion.RemoteDelete {
		if deletion.ctr.blockId_type == nil {
			deletion.ctr.blockId_type = make(map[types.Blockid]int8)
			deletion.ctr.blockId_bitmap = make(map[types.Blockid]*nulls.Nulls)
			deletion.ctr.pool = &BatchPool{pools: make([]*batch.Batch, 0, options.DefaultBlocksPerObject)}
			deletion.ctr.partitionId_blockId_rowIdBatch = make(map[int]map[types.Blockid]*batch.Batch)
			//deletion.ctr.partitionId_blockId_deltaLoc = make(map[int]map[types.Blockid]*batch.Batch)
			deletion.ctr.partitionId_tombstoneObjectStatsBats = make(map[int][]*batch.Batch)
		}
	} else {
		ref := deletion.DeleteCtx.Ref
		eng := deletion.DeleteCtx.Engine
		partitionNames := deletion.DeleteCtx.PartitionTableNames
		rel, partitionRels, err := colexec.GetRelAndPartitionRelsByObjRef(proc.Ctx, proc, eng, ref, partitionNames)
		if err != nil {
			return err
		}
		deletion.ctr.source = rel
		deletion.ctr.partitionSources = partitionRels
	}
	deletion.ctr.affectedRows = 0

	return nil
}

// the bool return value means whether it completed its work or not
func (deletion *Deletion) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	if deletion.RemoteDelete {
		return deletion.remoteDelete(proc)
	}
	return deletion.normalDelete(proc)
}

func (deletion *Deletion) remoteDelete(proc *process.Process) (vm.CallResult, error) {
	anal := proc.GetAnalyze(deletion.GetIdx(), deletion.GetParallelIdx(), deletion.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	var err error
	if deletion.ctr.state == vm.Build {
		for {
			result, err := vm.ChildrenCall(deletion.GetChildren(0), proc, anal)
			if err != nil {
				return result, err
			}
			if result.Batch == nil {
				deletion.ctr.state = vm.Eval
				break
			}
			if result.Batch.IsEmpty() {
				continue
			}
			anal.Input(result.Batch, deletion.IsFirst)

			if err = deletion.SplitBatch(proc, result.Batch); err != nil {
				return result, err
			}
		}
	}

	result := vm.NewCallResult()
	if deletion.ctr.state == vm.Eval {
		// ToDo: CNBlock Compaction
		// blkId,delta_metaLoc,type
		if deletion.ctr.resBat != nil {
			//Vecs[4] is constant， need free first
			deletion.ctr.resBat.Vecs[4].Free(proc.GetMPool())
			deletion.ctr.resBat.Vecs[4] = nil
			deletion.ctr.resBat.CleanOnlyData()
		} else {
			deletion.ctr.resBat = makeDelRemoteBatch()
		}

		for pidx, blockidRowidbatch := range deletion.ctr.partitionId_blockId_rowIdBatch {
			for blkid, bat := range blockidRowidbatch {
				if err = vector.AppendBytes(deletion.ctr.resBat.GetVector(0), blkid[:], false, proc.GetMPool()); err != nil {
					return result, err
				}
				bat.SetRowCount(bat.GetVector(0).Length())
				byts, err1 := bat.MarshalBinary()
				if err1 != nil {
					result.Status = vm.ExecStop
					return result, err1
				}
				if err = vector.AppendBytes(deletion.ctr.resBat.GetVector(1), byts, false, proc.GetMPool()); err != nil {
					return result, err
				}
				if err = vector.AppendFixed(deletion.ctr.resBat.GetVector(2), deletion.ctr.blockId_type[blkid], false, proc.GetMPool()); err != nil {
					return result, err
				}
				if err = vector.AppendFixed(deletion.ctr.resBat.GetVector(3), int32(pidx), false, proc.GetMPool()); err != nil {
					return result, err
				}
			}
		}

		// cn flushed s3 tombstone objects
		for pIdx, bats := range deletion.ctr.partitionId_tombstoneObjectStatsBats {
			for _, bat := range bats {
				data, area := vector.MustVarlenaRawData(bat.Vecs[0])
				stats := objectio.ObjectStats(data[0].GetByteSlice(area))

				if err = vector.AppendBytes(
					deletion.ctr.resBat.GetVector(0),
					stats.ObjectName().ObjectId()[:], false, proc.GetMPool()); err != nil {
					return result, err
				}

				batBytes, err := bat.MarshalBinary()
				if err != nil {
					result.Status = vm.ExecStop
					return result, err
				}

				if err = vector.AppendBytes(
					deletion.ctr.resBat.GetVector(1),
					batBytes, false, proc.GetMPool()); err != nil {
					return result, err
				}
				if err = vector.AppendFixed(
					deletion.ctr.resBat.GetVector(2),
					int8(FlushDeltaLoc), false, proc.GetMPool()); err != nil {
					return result, err
				}
				if err = vector.AppendFixed(
					deletion.ctr.resBat.GetVector(3),
					int32(pIdx), false, proc.GetMPool()); err != nil {
					return result, err
				}
			}
		}

		deletion.ctr.resBat.SetRowCount(deletion.ctr.resBat.Vecs[0].Length())
		deletion.ctr.resBat.Vecs[4], err = vector.NewConstFixed(
			types.T_uint32.ToType(), deletion.ctr.deleted_length, deletion.ctr.resBat.RowCount(), proc.GetMPool())
		if err != nil {
			result.Status = vm.ExecStop
			return result, err
		}
		result.Batch = deletion.ctr.resBat
		deletion.ctr.state = vm.End
		return result, nil
	}

	if deletion.ctr.state == vm.End {
		return vm.CancelResult, nil
	}

	panic("bug")

}

func (deletion *Deletion) normalDelete(proc *process.Process) (vm.CallResult, error) {
	anal := proc.GetAnalyze(deletion.GetIdx(), deletion.GetParallelIdx(), deletion.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	result, err := vm.ChildrenCall(deletion.GetChildren(0), proc, anal)
	if err != nil {
		return result, err
	}
	if result.Batch == nil || result.Batch.IsEmpty() {
		return result, nil
	}
	anal.Input(result.Batch, deletion.IsFirst)

	if deletion.ctr.resBat == nil {
		deletion.ctr.resBat = makeDelBatch(*result.Batch.GetVector(int32(deletion.DeleteCtx.PrimaryKeyIdx)).GetType())
	} else {
		deletion.ctr.resBat.CleanOnlyData()
	}

	bat := result.Batch

	var affectedRows uint64
	delCtx := deletion.DeleteCtx

	if len(delCtx.PartitionTableIDs) > 0 {
		//@todo need reuse delBatches
		delBatches, err := colexec.GroupByPartitionForDelete(proc, bat, delCtx.RowIdIdx, delCtx.PartitionIndexInBatch,
			len(delCtx.PartitionTableIDs), delCtx.PrimaryKeyIdx)
		if err != nil {
			return result, err
		}

		for i, delBatch := range delBatches {
			tempRows := uint64(delBatch.RowCount())
			if tempRows > 0 {
				affectedRows += tempRows
				err = deletion.ctr.partitionSources[i].Delete(proc.Ctx, delBatch, catalog.Row_ID)
				if err != nil {
					delBatch.Clean(proc.Mp())
					return result, err
				}
				delBatch.Clean(proc.Mp())
			}
		}
	} else {
		err := colexec.FilterRowIdForDel(proc, deletion.ctr.resBat, bat, delCtx.RowIdIdx,
			delCtx.PrimaryKeyIdx)
		if err != nil {
			return result, err
		}
		affectedRows = uint64(deletion.ctr.resBat.RowCount())
		if affectedRows > 0 {
			err = deletion.ctr.source.Delete(proc.Ctx, deletion.ctr.resBat, catalog.Row_ID)
			if err != nil {
				return result, err
			}
		}
	}

	if delCtx.AddAffectedRows {
		atomic.AddUint64(&deletion.ctr.affectedRows, affectedRows)
	}
	return result, nil
}
