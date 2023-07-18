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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
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

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("delete rows")
}

func Prepare(_ *process.Process, arg any) error {
	ap := arg.(*Argument)
	if ap.RemoteDelete {
		ap.ctr = new(container)
		ap.ctr.blockId_type = make(map[string]int8)
		ap.ctr.blockId_bitmap = make(map[string]*nulls.Nulls)
		ap.ctr.pool = &BatchPool{pools: make([]*batch.Batch, 0, options.DefaultBlocksPerSegment)}
		ap.ctr.partitionId_blockId_rowIdBatch = make(map[int]map[string]*batch.Batch)
		ap.ctr.partitionId_blockId_deltaLoc = make(map[int]map[string]*batch.Batch)
	}
	return nil
}

// the bool return value means whether it completed its work or not
func Call(_ int, proc *process.Process, arg any, isFirst bool, isLast bool) (process.ExecStatus, error) {
	p := arg.(*Argument)
	bat := proc.InputBatch()

	// last batch of block
	if bat == nil {
		if p.RemoteDelete {
			// ToDo: CNBlock Compaction
			// blkId,delta_metaLoc,type
			resBat := batch.NewWithSize(5)
			resBat.Attrs = []string{
				catalog.BlockMeta_Delete_ID,
				catalog.BlockMeta_DeltaLoc,
				catalog.BlockMeta_Type,
				catalog.BlockMeta_Partition,
				catalog.BlockMeta_Deletes_Length,
			}
			resBat.SetVector(0, vector.NewVec(types.T_text.ToType()))
			resBat.SetVector(1, vector.NewVec(types.T_text.ToType()))
			resBat.SetVector(2, vector.NewVec(types.T_int8.ToType()))
			resBat.SetVector(3, vector.NewVec(types.T_int32.ToType()))

			for pidx, blockId_rowIdBatch := range p.ctr.partitionId_blockId_rowIdBatch {
				for blkid, bat := range blockId_rowIdBatch {
					vector.AppendBytes(resBat.GetVector(0), []byte(blkid), false, proc.GetMPool())
					bat.SetZs(bat.GetVector(0).Length(), proc.GetMPool())
					bytes, err := bat.MarshalBinary()
					if err != nil {
						return process.ExecStop, err
					}
					vector.AppendBytes(resBat.GetVector(1), bytes, false, proc.GetMPool())
					vector.AppendFixed(resBat.GetVector(2), p.ctr.blockId_type[blkid], false, proc.GetMPool())
					vector.AppendFixed(resBat.GetVector(3), int32(pidx), false, proc.GetMPool())
				}
			}

			for pidx, blockId_deltaLoc := range p.ctr.partitionId_blockId_deltaLoc {
				for blkid, bat := range blockId_deltaLoc {
					vector.AppendBytes(resBat.GetVector(0), []byte(blkid), false, proc.GetMPool())
					//bat.Attrs = {catalog.BlockMeta_DeltaLoc}
					bat.SetZs(bat.GetVector(0).Length(), proc.GetMPool())
					bytes, err := bat.MarshalBinary()
					if err != nil {
						return process.ExecStop, err
					}
					vector.AppendBytes(resBat.GetVector(1), bytes, false, proc.GetMPool())
					vector.AppendFixed(resBat.GetVector(2), int8(FlushDeltaLoc), false, proc.GetMPool())
					vector.AppendFixed(resBat.GetVector(3), int32(pidx), false, proc.GetMPool())
				}
			}

			resBat.SetZs(resBat.Vecs[0].Length(), proc.GetMPool())
			resBat.SetVector(4, vector.NewConstFixed(types.T_uint32.ToType(), p.ctr.deleted_length, resBat.Length(), proc.GetMPool()))
			proc.SetInputBatch(resBat)
		} else {
			// ToDo: need ouyuaning to make sure there are only one table
			// in a deletion operator
			// do compaction here
			p.DeleteCtx.Source.Delete(proc.Ctx, nil, catalog.Row_ID)
		}
		return process.ExecStop, nil
	}

	// empty batch
	if bat.Length() == 0 {
		bat.Clean(proc.Mp())
		proc.SetInputBatch(batch.EmptyBatch)
		return process.ExecNext, nil
	}

	defer proc.PutBatch(bat)
	if p.RemoteDelete {
		// we will cache all rowId in memory,
		// when the size is too large we will
		// trigger write s3
		p.SplitBatch(proc, bat)
		proc.SetInputBatch(batch.EmptyBatch)
		return process.ExecNext, nil
	}

	var affectedRows uint64
	delCtx := p.DeleteCtx

	if len(delCtx.PartitionTableIDs) > 0 {
		delBatches, err := colexec.GroupByPartitionForDelete(proc, bat, delCtx.RowIdIdx, delCtx.PartitionIndexInBatch, len(delCtx.PartitionTableIDs))
		if err != nil {
			return process.ExecNext, err
		}

		for i, delBatch := range delBatches {
			tempRows := uint64(delBatch.Length())
			if tempRows > 0 {
				affectedRows += tempRows
				err = delCtx.PartitionSources[i].Delete(proc.Ctx, delBatch, catalog.Row_ID)
				if err != nil {
					delBatch.Clean(proc.Mp())
					return process.ExecNext, err
				}
				delBatch.Clean(proc.Mp())
			}
		}
	} else {
		delBatch, err := colexec.FilterRowIdForDel(proc, bat, delCtx.RowIdIdx)
		if err != nil {
			return process.ExecNext, err
		}
		affectedRows = uint64(delBatch.Length())
		if affectedRows > 0 {
			err = delCtx.Source.Delete(proc.Ctx, delBatch, catalog.Row_ID)
			if err != nil {
				delBatch.Clean(proc.GetMPool())
				return process.ExecNext, err
			}
		}
		delBatch.Clean(proc.GetMPool())
	}

	proc.SetInputBatch(batch.EmptyBatch)

	if delCtx.AddAffectedRows {
		atomic.AddUint64(&p.affectedRows, affectedRows)
	}
	return process.ExecNext, nil
}
