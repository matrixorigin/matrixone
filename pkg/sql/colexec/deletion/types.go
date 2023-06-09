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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	flushThreshold = 32 * mpool.MB
)

type BatchPool struct {
	pools []*batch.Batch
}

func (pool *BatchPool) put(bat *batch.Batch) {
	pool.pools = append(pool.pools, bat)
}

func (pool *BatchPool) get() *batch.Batch {
	if len(pool.pools) == 0 {
		return nil
	}
	bat := pool.pools[0]
	pool.pools = pool.pools[1:]
	return bat
}

// for now, we won't do compaction for cn block
type container struct {
	blockId_bitmap                 map[string]*nulls.Nulls
	partitionId_blockId_rowIdBatch map[int]map[string]*batch.Batch // PartitionId -> blockId -> RowIdBatch
	partitionId_blockId_metaLoc    map[int]map[string]*batch.Batch // PartitionId -> blockId -> MetaLocation
	// don't flush cn block rowId and rawBatch
	// we just do compaction for cn block in the
	// future
	blockId_type   map[string]int8
	batch_size     uint32
	deleted_length uint32
	pool           *BatchPool
	debug_len      uint32
}
type Argument struct {
	Ts           uint64
	DeleteCtx    *DeleteCtx
	affectedRows uint64

	// for delete filter below
	// mp[segmentName] = 1 => txnWorkSpace,mp[segmentName] = 2 => CN Block
	SegmentMap   map[string]int32
	RemoteDelete bool
	IBucket      uint32
	Nbucket      uint32
	ctr          *container
}

type DeleteCtx struct {
	CanTruncate           bool
	RowIdIdx              int               // The array index position of the rowid column
	PartitionTableIDs     []uint64          // Align array index with the partition number
	PartitionTableNames   []string          // Align array index with the partition number
	PartitionIndexInBatch int               // The array index position of the partition expression column
	PartitionSources      []engine.Relation // Align array index with the partition number
	Source                engine.Relation
	Ref                   *plan.ObjectRef
	AddAffectedRows       bool
}

// delete from t1 using t1 join t2 on t1.a = t2.a;
func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
	if arg.RemoteDelete {
		for _, blockId_rowIdBatch := range arg.ctr.partitionId_blockId_rowIdBatch {
			for _, bat := range blockId_rowIdBatch {
				bat.Clean(proc.GetMPool())
			}
		}

		for _, blockId_metaLoc := range arg.ctr.partitionId_blockId_metaLoc {
			for _, bat := range blockId_metaLoc {
				bat.Clean(proc.GetMPool())
			}
		}
		arg.SegmentMap = nil
		arg.ctr.blockId_bitmap = nil
		arg.ctr.partitionId_blockId_rowIdBatch = nil
		arg.ctr.partitionId_blockId_metaLoc = nil
		arg.ctr.blockId_type = nil
		arg.ctr.pool = nil
	}
}

func (arg *Argument) AffectedRows() uint64 {
	return arg.affectedRows
}

func (arg *Argument) SplitBatch(proc *process.Process, srcBat *batch.Batch) error {
	delCtx := arg.DeleteCtx
	// If the target table is a partition table, group and split the batch data
	if len(delCtx.PartitionSources) > 0 {
		delBatches, err := colexec.GroupByPartitionForDelete(proc, srcBat, delCtx.RowIdIdx, delCtx.PartitionIndexInBatch, len(delCtx.PartitionTableIDs))
		if err != nil {
			return err
		}
		for i, delBatch := range delBatches {
			collectBatchInfo(proc, arg, delBatch, 0, i)
		}
	} else {
		collectBatchInfo(proc, arg, srcBat, arg.DeleteCtx.RowIdIdx, 0)
	}
	// we will flush all
	if arg.ctr.batch_size >= flushThreshold {
		size, err := arg.ctr.flush(proc)
		if err != nil {
			return err
		}
		arg.ctr.batch_size -= size
	}
	return nil
}

func (ctr *container) flush(proc *process.Process) (uint32, error) {
	var err error
	resSize := uint32(0)
	for pidx, blockId_rowIdBatch := range ctr.partitionId_blockId_rowIdBatch {
		s3writer := &colexec.S3Writer{}
		s3writer.SetSortIdx(-1)
		_, err = s3writer.GenerateWriter(proc)
		if err != nil {
			return 0, err
		}
		blkids := make([]string, 0, len(blockId_rowIdBatch))
		for blkid, bat := range blockId_rowIdBatch {
			// don't flush cn block and RawBatch
			if ctr.blockId_type[blkid] != 0 {
				continue
			}
			err = s3writer.WriteBlock(bat)
			if err != nil {
				return 0, err
			}
			resSize += uint32(bat.Size())
			blkids = append(blkids, blkid)
			bat.CleanOnlyData()
			ctr.pool.put(bat)
			delete(blockId_rowIdBatch, blkid)
		}
		metaLocs, err := s3writer.WriteEndBlocks(proc)
		if err != nil {
			return 0, err
		}
		for i, metaLoc := range metaLocs {
			if _, has := ctr.partitionId_blockId_metaLoc[pidx]; !has {
				ctr.partitionId_blockId_metaLoc[pidx] = make(map[string]*batch.Batch)
			}
			blockId_metaLoc := ctr.partitionId_blockId_metaLoc[pidx]
			if _, ok := blockId_metaLoc[blkids[i]]; !ok {
				bat := batch.New(false, []string{catalog.BlockMeta_MetaLoc})
				bat.SetVector(0, vector.NewVec(types.T_text.ToType()))
				blockId_metaLoc[blkids[i]] = bat
			}
			bat := blockId_metaLoc[blkids[i]]
			vector.AppendBytes(bat.GetVector(0), []byte(metaLoc), false, proc.GetMPool())
		}
	}
	return resSize, nil
}

// Collect relevant information about intermediate batche
func collectBatchInfo(proc *process.Process, arg *Argument, destBatch *batch.Batch, rowIdIdx int, pIdx int) {
	vs := vector.MustFixedCol[types.Rowid](destBatch.GetVector(int32(rowIdIdx)))
	var bitmap *nulls.Nulls
	arg.ctr.debug_len += uint32(len(vs))
	for _, rowId := range vs {
		blkid := rowId.CloneBlockID()
		segid := rowId.CloneSegmentID()
		blkOffset := rowId.GetBlockOffset()
		rowOffset := rowId.GetRowOffset()
		if blkOffset%uint16(arg.Nbucket) != uint16(arg.IBucket) {
			continue
		}
		arg.ctr.deleted_length += 1
		// FIXME: string(blkid[:]) means heap allocation, just take the id type itself
		str := string(blkid[:])
		offsetFlag := false
		if arg.ctr.blockId_bitmap[str] == nil {
			arg.ctr.blockId_bitmap[str] = nulls.NewWithSize(int(options.DefaultBlockMaxRows))
		}
		bitmap = arg.ctr.blockId_bitmap[str]
		if bitmap.Contains(uint64(rowOffset)) {
			continue
		} else {
			bitmap.Add(uint64(rowOffset))
		}

		// FIXME: string(segid[:]) means heap allocation, just take the id type itself
		if arg.SegmentMap[string(segid[:])] == colexec.TxnWorkSpaceIdType {
			arg.ctr.blockId_type[str] = RawBatchOffset
			offsetFlag = true
		} else if arg.SegmentMap[string(segid[:])] == colexec.CnBlockIdType {
			arg.ctr.blockId_type[str] = CNBlockOffset
			offsetFlag = true
		} else {
			arg.ctr.blockId_type[str] = RawRowIdBatch
		}

		if _, ok := arg.ctr.partitionId_blockId_rowIdBatch[pIdx]; !ok {
			blockIdRowIdBatchMap := make(map[string]*batch.Batch)
			if !offsetFlag {
				var tmpBat *batch.Batch
				tmpBat = arg.ctr.pool.get()
				if tmpBat == nil {
					tmpBat = batch.New(false, []string{catalog.Row_ID})
					tmpBat.SetVector(0, vector.NewVec(types.T_Rowid.ToType()))
				}
				blockIdRowIdBatchMap[str] = tmpBat
			} else {
				tmpBat := batch.New(false, []string{catalog.BlockMetaOffset})
				tmpBat.SetVector(0, vector.NewVec(types.T_int64.ToType()))
				blockIdRowIdBatchMap[str] = tmpBat
			}
			arg.ctr.partitionId_blockId_rowIdBatch[pIdx] = blockIdRowIdBatchMap
		} else {
			blockIdRowIdBatchMap := arg.ctr.partitionId_blockId_rowIdBatch[pIdx]
			if _, ok := blockIdRowIdBatchMap[str]; !ok {
				if !offsetFlag {
					var bat *batch.Batch
					bat = arg.ctr.pool.get()
					if bat == nil {
						bat = batch.New(false, []string{catalog.Row_ID})
						bat.SetVector(0, vector.NewVec(types.T_Rowid.ToType()))
					}
					blockIdRowIdBatchMap[str] = bat
				} else {
					bat := batch.New(false, []string{catalog.BlockMetaOffset})
					bat.SetVector(0, vector.NewVec(types.T_int64.ToType()))
					blockIdRowIdBatchMap[str] = bat
				}
			}
		}

		rbat := arg.ctr.partitionId_blockId_rowIdBatch[pIdx][str]
		offset := rowId.GetRowOffset()
		if !offsetFlag {
			vector.AppendFixed(rbat.GetVector(0), rowId, false, proc.GetMPool())
		} else {
			vector.AppendFixed(rbat.GetVector(0), int64(offset), false, proc.GetMPool())
		}
		// add rowId size
		if offsetFlag {
			continue
		}
		arg.ctr.batch_size += 24
	}
}
