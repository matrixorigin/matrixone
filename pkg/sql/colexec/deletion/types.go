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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Deletion)

// make it mutable in ut
var (
	flushThreshold = 5 * mpool.MB
)

// SetCNFlushDeletesThreshold update threshold to n MB
func SetCNFlushDeletesThreshold(n int) {
	flushThreshold = n * mpool.MB
}

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
	blockId_bitmap                       map[types.Blockid]*nulls.Nulls
	partitionId_blockId_rowIdBatch       map[int]map[types.Blockid]*batch.Batch // PartitionId -> blockId -> RowIdBatch
	partitionId_tombstoneObjectStatsBats map[int][]*batch.Batch                 // PartitionId -> tombstone object stats
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
	affectedRows     uint64
}
type Deletion struct {
	ctr       container
	DeleteCtx *DeleteCtx

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
	ctr := &deletion.ctr
	ctr.state = vm.Build
	if deletion.RemoteDelete {
		for k := range ctr.blockId_bitmap {
			delete(ctr.blockId_bitmap, k)
		}
		for pidx, blockidRowidbatch := range ctr.partitionId_blockId_rowIdBatch {
			for blkid, bat := range blockidRowidbatch {
				if bat != nil {
					bat.Clean(proc.GetMPool())
				}
				delete(blockidRowidbatch, blkid)
			}
			delete(ctr.partitionId_blockId_rowIdBatch, pidx)
		}

		for pIdx, bats := range ctr.partitionId_tombstoneObjectStatsBats {
			for _, bat := range bats {
				if bat != nil {
					bat.Clean(proc.GetMPool())
				}
			}
			delete(ctr.partitionId_tombstoneObjectStatsBats, pIdx)
		}

		for blkid := range ctr.blockId_type {
			delete(ctr.blockId_type, blkid)
		}
		for _, bat := range ctr.pool.pools {
			bat.Clean(proc.GetMPool())
		}
		ctr.pool.pools = ctr.pool.pools[:0]
	}

	if ctr.resBat != nil {
		ctr.resBat.CleanOnlyData()
	}

	ctr.partitionSources = nil
	ctr.source = nil

	ctr.batch_size = 0
	ctr.deleted_length = 0
}

// delete from t1 using t1 join t2 on t1.a = t2.a;
func (deletion *Deletion) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &deletion.ctr
	if deletion.RemoteDelete {
		deletion.SegmentMap = nil
		ctr.blockId_bitmap = nil
		ctr.partitionId_blockId_rowIdBatch = nil
		ctr.partitionId_tombstoneObjectStatsBats = nil
		ctr.blockId_type = nil
		ctr.pool = nil
	}

	if ctr.resBat != nil {
		ctr.resBat.Clean(proc.GetMPool())
		ctr.resBat = nil
	}

	ctr.partitionSources = nil
	ctr.source = nil
}

func (deletion *Deletion) AffectedRows() uint64 {
	return deletion.ctr.affectedRows
}

func (deletion *Deletion) SplitBatch(proc *process.Process, srcBat *batch.Batch) error {
	delCtx := deletion.DeleteCtx
	// If the target table is a partition table, group and split the batch data
	if len(deletion.ctr.partitionSources) != 0 {
		pkTyp := srcBat.Vecs[delCtx.PrimaryKeyIdx].GetType()
		deletion.ctr.resBat.SetVector(0, vector.NewVec(types.T_Rowid.ToType()))
		deletion.ctr.resBat.SetVector(1, vector.NewVec(*pkTyp))
		var err error

		for partIdx := range len(delCtx.PartitionTableIDs) {
			deletion.ctr.resBat.CleanOnlyData()
			expect := int32(partIdx)
			err = colexec.FillPartitionBatchForDelete(proc, srcBat, deletion.ctr.resBat, expect, delCtx.RowIdIdx, delCtx.PartitionIndexInBatch, delCtx.PrimaryKeyIdx)
			if err != nil {
				deletion.ctr.resBat.Clean(proc.Mp())
				return err
			}

			collectBatchInfo(proc, deletion, deletion.ctr.resBat, 0, partIdx, 1)
		}
		deletion.ctr.resBat.CleanOnlyData()
	} else {
		collectBatchInfo(proc, deletion, srcBat, deletion.DeleteCtx.RowIdIdx, 0, delCtx.PrimaryKeyIdx)
	}
	// we will flush all
	if deletion.ctr.batch_size >= uint32(flushThreshold) {
		size, err := deletion.ctr.flush(proc)
		if err != nil {
			return err
		}
		deletion.ctr.batch_size -= size
	}
	return nil
}

func (ctr *container) flush(proc *process.Process) (uint32, error) {
	resSize := uint32(0)
	for pidx, blockId_rowIdBatch := range ctr.partitionId_blockId_rowIdBatch {
		s3writer, err := colexec.NewS3TombstoneWriter()
		if err != nil {
			return 0, err
		}
		blkids := make([]types.Blockid, 0, len(blockId_rowIdBatch))
		for blkid := range blockId_rowIdBatch {
			//Don't flush rowids belong to uncommitted cn block and raw data batch in txn's workspace.
			if ctr.blockId_type[blkid] != RawRowIdBatch {
				continue
			}
			blkids = append(blkids, blkid)
		}
		slices.SortFunc(blkids, func(a, b types.Blockid) int {
			return a.Compare(&b)
		})
		for _, blkid := range blkids {
			bat := blockId_rowIdBatch[blkid]

			s3writer.StashBatch(proc, bat)
			resSize += uint32(bat.Size())
			bat.CleanOnlyData()
			ctr.pool.put(bat)
			delete(blockId_rowIdBatch, blkid)
		}

		_, stats, err := s3writer.SortAndSync(proc)
		if err != nil {
			return 0, err
		}

		bat := batch.New(false, []string{catalog.ObjectMeta_ObjectStats})
		bat.SetVector(0, vector.NewVec(types.T_text.ToType()))
		if err = vector.AppendBytes(
			bat.GetVector(0), stats.Marshal(), false, proc.GetMPool()); err != nil {
			return 0, err
		}

		bat.SetRowCount(bat.Vecs[0].Length())
		ctr.partitionId_tombstoneObjectStatsBats[pidx] =
			append(ctr.partitionId_tombstoneObjectStatsBats[pidx], bat)
	}
	return resSize, nil
}

// Collect relevant information about intermediate batche
func collectBatchInfo(proc *process.Process, deletion *Deletion, destBatch *batch.Batch, rowIdIdx int, pIdx int, pkIdx int) {
	vs := vector.MustFixedColWithTypeCheck[types.Rowid](destBatch.GetVector(int32(rowIdIdx)))
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
				tmpBat = makeDelBatch(*destBatch.GetVector(int32(pkIdx)).GetType())
			}
			blockIdRowIdBatchMap[blkid] = tmpBat
			deletion.ctr.partitionId_blockId_rowIdBatch[pIdx] = blockIdRowIdBatchMap
		} else {
			blockIdRowIdBatchMap := deletion.ctr.partitionId_blockId_rowIdBatch[pIdx]
			if _, ok := blockIdRowIdBatchMap[blkid]; !ok {
				var bat *batch.Batch
				bat = deletion.ctr.pool.get()
				if bat == nil {
					bat = makeDelBatch(*destBatch.GetVector(int32(pkIdx)).GetType())
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
		bat.SetRowCount(bat.Vecs[0].Length())
	}

	deletion.ctr.batch_size = uint32(batchSize)
}

func makeDelBatch(pkType types.Type) *batch.Batch {
	bat := batch.New(false, []string{catalog.Row_ID, "pk"})
	bat.SetVector(0, vector.NewVec(types.T_Rowid.ToType()))
	bat.SetVector(1, vector.NewVec(pkType))
	return bat
}

func makeDelRemoteBatch() *batch.Batch {
	bat := batch.NewWithSize(5)
	bat.Attrs = []string{
		catalog.BlockMeta_Delete_ID,
		catalog.BlockMeta_DeltaLoc,
		catalog.BlockMeta_Type,
		catalog.BlockMeta_Partition,
		catalog.BlockMeta_Deletes_Length,
	}
	bat.SetVector(0, vector.NewVec(types.T_text.ToType()))
	bat.SetVector(1, vector.NewVec(types.T_text.ToType()))
	bat.SetVector(2, vector.NewVec(types.T_int8.ToType()))
	bat.SetVector(3, vector.NewVec(types.T_int32.ToType()))
	//bat.Vecs[4] is constant
	return bat
}

func getNonNullValue(col *vector.Vector, row uint32) any {
	switch col.GetType().Oid {
	case types.T_bool:
		return vector.GetFixedAtNoTypeCheck[bool](col, int(row))
	case types.T_bit:
		return vector.GetFixedAtNoTypeCheck[uint64](col, int(row))
	case types.T_int8:
		return vector.GetFixedAtNoTypeCheck[int8](col, int(row))
	case types.T_int16:
		return vector.GetFixedAtNoTypeCheck[int16](col, int(row))
	case types.T_int32:
		return vector.GetFixedAtNoTypeCheck[int32](col, int(row))
	case types.T_int64:
		return vector.GetFixedAtNoTypeCheck[int64](col, int(row))
	case types.T_uint8:
		return vector.GetFixedAtNoTypeCheck[uint8](col, int(row))
	case types.T_uint16:
		return vector.GetFixedAtNoTypeCheck[uint16](col, int(row))
	case types.T_uint32:
		return vector.GetFixedAtNoTypeCheck[uint32](col, int(row))
	case types.T_uint64:
		return vector.GetFixedAtNoTypeCheck[uint64](col, int(row))
	case types.T_decimal64:
		return vector.GetFixedAtNoTypeCheck[types.Decimal64](col, int(row))
	case types.T_decimal128:
		return vector.GetFixedAtNoTypeCheck[types.Decimal128](col, int(row))
	case types.T_uuid:
		return vector.GetFixedAtNoTypeCheck[types.Uuid](col, int(row))
	case types.T_float32:
		return vector.GetFixedAtNoTypeCheck[float32](col, int(row))
	case types.T_float64:
		return vector.GetFixedAtNoTypeCheck[float64](col, int(row))
	case types.T_date:
		return vector.GetFixedAtNoTypeCheck[types.Date](col, int(row))
	case types.T_time:
		return vector.GetFixedAtNoTypeCheck[types.Time](col, int(row))
	case types.T_datetime:
		return vector.GetFixedAtNoTypeCheck[types.Datetime](col, int(row))
	case types.T_timestamp:
		return vector.GetFixedAtNoTypeCheck[types.Timestamp](col, int(row))
	case types.T_enum:
		return vector.GetFixedAtNoTypeCheck[types.Enum](col, int(row))
	case types.T_TS:
		return vector.GetFixedAtNoTypeCheck[types.TS](col, int(row))
	case types.T_Rowid:
		return vector.GetFixedAtNoTypeCheck[types.Rowid](col, int(row))
	case types.T_Blockid:
		return vector.GetFixedAtNoTypeCheck[types.Blockid](col, int(row))
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		return col.GetBytesAt(int(row))
	default:
		// return vector.ErrVecTypeNotSupport
		panic(any("No Support"))
	}
}
