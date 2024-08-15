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
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Deletion)

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
	blockId_bitmap                 map[types.Blockid]*nulls.Nulls
	partitionId_blockId_rowIdBatch map[int]map[types.Blockid]*batch.Batch // PartitionId -> blockId -> RowIdBatch
	partitionId_blockId_deltaLoc   map[int]map[types.Blockid]*batch.Batch // PartitionId -> blockId -> MetaLocation
	// don't flush cn block rowId and rawBatch
	// we just do compaction for cn block in the
	// future
	blockId_type   map[types.Blockid]int8
	batch_size     uint32
	deleted_length uint32
	pool           *BatchPool
	// debug_len      uint32

	state            vm.CtrState
	resBat           *batch.Batch
	source           engine.Relation
	partitionSources []engine.Relation // Align array index with the partition number
}
type Deletion struct {
	ctr          *container
	DeleteCtx    *DeleteCtx
	affectedRows uint64

	// for delete filter below
	// mp[segmentId] = 1 => txnWorkSpace,mp[segmentId] = 2 => CN Block
	SegmentMap   map[string]int32
	RemoteDelete bool
	IBucket      uint32
	Nbucket      uint32

	vm.OperatorBase
}

func (deletion *Deletion) GetOperatorBase() *vm.OperatorBase {
	return &deletion.OperatorBase
}

func init() {
	reuse.CreatePool[Deletion](
		func() *Deletion {
			return &Deletion{}
		},
		func(a *Deletion) {
			*a = Deletion{}
		},
		reuse.DefaultOptions[Deletion]().
			WithEnableChecker(),
	)
}

func (deletion Deletion) TypeName() string {
	return opName
}

func NewArgument() *Deletion {
	return reuse.Alloc[Deletion](nil)
}

func (deletion *Deletion) Release() {
	if deletion != nil {
		reuse.Free[Deletion](deletion, nil)
	}
}

type DeleteCtx struct {
	CanTruncate           bool
	RowIdIdx              int      // The array index position of the rowid column
	PartitionTableIDs     []uint64 // Align array index with the partition number
	PartitionTableNames   []string // Align array index with the partition number
	PartitionIndexInBatch int      // The array index position of the partition expression column
	// PartitionSources      []engine.Relation // Align array index with the partition number
	// Source                engine.Relation
	Ref             *plan.ObjectRef
	AddAffectedRows bool // for hidden table, should not update affect Rows
	PrimaryKeyIdx   int

	Engine engine.Engine
}

func (deletion *Deletion) Reset(proc *process.Process, pipelineFailed bool, err error) {
	deletion.Free(proc, pipelineFailed, err)
}

// delete from t1 using t1 join t2 on t1.a = t2.a;
func (deletion *Deletion) Free(proc *process.Process, pipelineFailed bool, err error) {
	if deletion.RemoteDelete {
		deletion.SegmentMap = nil
		if deletion.ctr != nil {
			for _, blockId_rowIdBatch := range deletion.ctr.partitionId_blockId_rowIdBatch {
				for _, bat := range blockId_rowIdBatch {
					if bat != nil {
						bat.Clean(proc.GetMPool())
					}
				}
			}
			for _, blockId_metaLoc := range deletion.ctr.partitionId_blockId_deltaLoc {
				for _, bat := range blockId_metaLoc {
					if bat != nil {
						bat.Clean(proc.GetMPool())
					}
				}
			}
			deletion.ctr.blockId_bitmap = nil
			deletion.ctr.partitionId_blockId_rowIdBatch = nil
			deletion.ctr.partitionId_blockId_deltaLoc = nil
			deletion.ctr.blockId_type = nil
			deletion.ctr.pool = nil
		}
	}
	if deletion.ctr != nil {
		if deletion.ctr.resBat != nil {
			deletion.ctr.resBat.Clean(proc.Mp())
			deletion.ctr.resBat = nil
		}
		deletion.ctr.partitionSources = nil
		deletion.ctr.source = nil
		deletion.ctr = nil
	}
}

func (deletion *Deletion) AffectedRows() uint64 {
	return deletion.affectedRows
}

func (deletion *Deletion) SplitBatch(proc *process.Process, srcBat *batch.Batch) error {
	delCtx := deletion.DeleteCtx
	// If the target table is a partition table, group and split the batch data
	if len(deletion.ctr.partitionSources) > 0 {
		delBatches, err := colexec.GroupByPartitionForDelete(proc, srcBat, delCtx.RowIdIdx, delCtx.PartitionIndexInBatch, len(delCtx.PartitionTableIDs), delCtx.PrimaryKeyIdx)
		if err != nil {
			return err
		}
		for i, delBatch := range delBatches {
			collectBatchInfo(proc, deletion, delBatch, 0, i, 1)
			proc.PutBatch(delBatch)
		}
	} else {
		collectBatchInfo(proc, deletion, srcBat, deletion.DeleteCtx.RowIdIdx, 0, delCtx.PrimaryKeyIdx)
	}
	// we will flush all
	if deletion.ctr.batch_size >= flushThreshold {
		size, err := deletion.ctr.flush(proc)
		if err != nil {
			return err
		}
		deletion.ctr.batch_size -= size
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
		blkids := make([]types.Blockid, 0, len(blockId_rowIdBatch))
		for blkid, bat := range blockId_rowIdBatch {
			//Don't flush rowids belong to uncommitted cn block and raw data batch in txn's workspace.
			if ctr.blockId_type[blkid] != RawRowIdBatch {
				continue
			}
			err = s3writer.WriteBlock(bat, objectio.SchemaTombstone)
			if err != nil {
				return 0, err
			}
			resSize += uint32(bat.Size())
			blkids = append(blkids, blkid)
			bat.CleanOnlyData()
			ctr.pool.put(bat)
			delete(blockId_rowIdBatch, blkid)
		}
		blkInfos, _, err := s3writer.WriteEndBlocks(proc)
		if err != nil {
			return 0, err
		}
		for i, blkInfo := range blkInfos {
			if _, has := ctr.partitionId_blockId_deltaLoc[pidx]; !has {
				ctr.partitionId_blockId_deltaLoc[pidx] = make(map[types.Blockid]*batch.Batch)
			}
			blockId_deltaLoc := ctr.partitionId_blockId_deltaLoc[pidx]
			if _, ok := blockId_deltaLoc[blkids[i]]; !ok {
				bat := batch.New(false, []string{catalog.BlockMeta_DeltaLoc})
				bat.SetVector(0, proc.GetVector(types.T_text.ToType()))
				blockId_deltaLoc[blkids[i]] = bat
			}
			bat := blockId_deltaLoc[blkids[i]]
			vector.AppendBytes(bat.GetVector(0), []byte(blkInfo.MetaLocation().String()), false, proc.GetMPool())
		}
	}
	return resSize, nil
}

// Collect relevant information about intermediate batche
func collectBatchInfo(proc *process.Process, deletion *Deletion, destBatch *batch.Batch, rowIdIdx int, pIdx int, pkIdx int) {
	vs := vector.MustFixedCol[types.Rowid](destBatch.GetVector(int32(rowIdIdx)))
	var bitmap *nulls.Nulls
	for i, rowId := range vs {
		blkid := rowId.CloneBlockID()
		segid := rowId.CloneSegmentID()
		blkOffset := rowId.GetBlockOffset()
		rowOffset := rowId.GetRowOffset()
		if blkOffset%uint16(deletion.Nbucket) != uint16(deletion.IBucket) {
			continue
		}
		if deletion.ctr.blockId_bitmap[blkid] == nil {
			deletion.ctr.blockId_bitmap[blkid] = nulls.NewWithSize(int(options.DefaultBlockMaxRows))
		}
		bitmap = deletion.ctr.blockId_bitmap[blkid]
		if bitmap.Contains(uint64(rowOffset)) {
			continue
		} else {
			bitmap.Add(uint64(rowOffset))
		}

		deletion.ctr.deleted_length += 1

		if deletion.SegmentMap[string(segid[:])] == colexec.TxnWorkSpaceIdType {
			deletion.ctr.blockId_type[blkid] = RawBatchOffset
		} else if deletion.SegmentMap[string(segid[:])] == colexec.CnBlockIdType {
			deletion.ctr.blockId_type[blkid] = CNBlockOffset
		} else {
			deletion.ctr.blockId_type[blkid] = RawRowIdBatch
		}

		if _, ok := deletion.ctr.partitionId_blockId_rowIdBatch[pIdx]; !ok {
			blockIdRowIdBatchMap := make(map[types.Blockid]*batch.Batch)
			var tmpBat *batch.Batch
			tmpBat = deletion.ctr.pool.get()
			if tmpBat == nil {
				tmpBat = batch.New(false, []string{catalog.Row_ID, "pk"})
				tmpBat.SetVector(0, proc.GetVector(types.T_Rowid.ToType()))
				tmpBat.SetVector(1, proc.GetVector(*destBatch.GetVector(int32(pkIdx)).GetType()))
			}
			blockIdRowIdBatchMap[blkid] = tmpBat
			deletion.ctr.partitionId_blockId_rowIdBatch[pIdx] = blockIdRowIdBatchMap
		} else {
			blockIdRowIdBatchMap := deletion.ctr.partitionId_blockId_rowIdBatch[pIdx]
			if _, ok := blockIdRowIdBatchMap[blkid]; !ok {
				var bat *batch.Batch
				bat = deletion.ctr.pool.get()
				if bat == nil {
					bat = batch.New(false, []string{catalog.Row_ID, "pk"})
					bat.SetVector(0, proc.GetVector(types.T_Rowid.ToType()))
					bat.SetVector(1, proc.GetVector(*destBatch.GetVector(int32(pkIdx)).GetType()))
				}
				blockIdRowIdBatchMap[blkid] = bat
			}
		}
		rbat := deletion.ctr.partitionId_blockId_rowIdBatch[pIdx][blkid]
		pk := getNonNullValue(destBatch.GetVector(int32(pkIdx)), uint32(i))
		vector.AppendFixed(rbat.GetVector(0), rowId, false, proc.GetMPool())
		vector.AppendAny(rbat.GetVector(1), pk, false, proc.GetMPool())

	}
	var batchSize int
	for _, bat := range deletion.ctr.partitionId_blockId_rowIdBatch[pIdx] {
		batchSize += bat.Size()
	}
	deletion.ctr.batch_size = uint32(batchSize)
}

func getNonNullValue(col *vector.Vector, row uint32) any {
	switch col.GetType().Oid {
	case types.T_bool:
		return vector.GetFixedAt[bool](col, int(row))
	case types.T_bit:
		return vector.GetFixedAt[uint64](col, int(row))
	case types.T_int8:
		return vector.GetFixedAt[int8](col, int(row))
	case types.T_int16:
		return vector.GetFixedAt[int16](col, int(row))
	case types.T_int32:
		return vector.GetFixedAt[int32](col, int(row))
	case types.T_int64:
		return vector.GetFixedAt[int64](col, int(row))
	case types.T_uint8:
		return vector.GetFixedAt[uint8](col, int(row))
	case types.T_uint16:
		return vector.GetFixedAt[uint16](col, int(row))
	case types.T_uint32:
		return vector.GetFixedAt[uint32](col, int(row))
	case types.T_uint64:
		return vector.GetFixedAt[uint64](col, int(row))
	case types.T_decimal64:
		return vector.GetFixedAt[types.Decimal64](col, int(row))
	case types.T_decimal128:
		return vector.GetFixedAt[types.Decimal128](col, int(row))
	case types.T_uuid:
		return vector.GetFixedAt[types.Uuid](col, int(row))
	case types.T_float32:
		return vector.GetFixedAt[float32](col, int(row))
	case types.T_float64:
		return vector.GetFixedAt[float64](col, int(row))
	case types.T_date:
		return vector.GetFixedAt[types.Date](col, int(row))
	case types.T_time:
		return vector.GetFixedAt[types.Time](col, int(row))
	case types.T_datetime:
		return vector.GetFixedAt[types.Datetime](col, int(row))
	case types.T_timestamp:
		return vector.GetFixedAt[types.Timestamp](col, int(row))
	case types.T_enum:
		return vector.GetFixedAt[types.Enum](col, int(row))
	case types.T_TS:
		return vector.GetFixedAt[types.TS](col, int(row))
	case types.T_Rowid:
		return vector.GetFixedAt[types.Rowid](col, int(row))
	case types.T_Blockid:
		return vector.GetFixedAt[types.Blockid](col, int(row))
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		return col.GetBytesAt(int(row))
	default:
		// return vector.ErrVecTypeNotSupport
		panic(any("No Support"))
	}
}
