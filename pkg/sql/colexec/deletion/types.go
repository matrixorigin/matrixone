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
	// blockId => rowId Batch
	blockId_rowIdBatch map[string]*batch.Batch
	blockId_metaLoc    map[string]*batch.Batch
	blockId_bitmap     map[string]*nulls.Nulls
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
	AffectedRows uint64
	Engine       engine.Engine

	// for delete filter below
	// mp[segmentName] = 1 => txnWorkSpace,mp[segmentName] = 2 => CN Block
	SegmentMap   map[string]int32
	RemoteDelete bool
	IBucket      uint32
	Nbucket      uint32
	ctr          *container
}

type DeleteCtx struct {
	CanTruncate bool

	DelSource []engine.Relation
	DelRef    []*plan.ObjectRef
	DelIdx    [][]int32

	IdxSource []engine.Relation
	IdxIdx    []int32

	OnRestrictIdx []int32

	OnCascadeSource []engine.Relation
	OnCascadeIdx    []int32

	OnSetSource       []engine.Relation
	OnSetUniqueSource [][]engine.Relation
	OnSetIdx          [][]int32
	OnSetRef          []*plan.ObjectRef
	OnSetTableDef     []*plan.TableDef
	OnSetUpdateCol    []map[string]int32
}

// delete from t1 using t1 join t2 on t1.a = t2.a;
func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
	if arg.RemoteDelete {
		for _, bat := range arg.ctr.blockId_rowIdBatch {
			bat.Clean(proc.GetMPool())
		}
		for _, bat := range arg.ctr.blockId_metaLoc {
			bat.Clean(proc.GetMPool())
		}
		arg.SegmentMap = nil
		arg.ctr.blockId_bitmap = nil
		arg.ctr.blockId_metaLoc = nil
		arg.ctr.blockId_rowIdBatch = nil
		arg.ctr.blockId_type = nil
		arg.ctr.pool = nil
	}
}

func (arg *Argument) SplitBatch(proc *process.Process, bat *batch.Batch) error {
	vs := vector.MustFixedCol[types.Rowid](bat.GetVector(0))
	var bitmap *nulls.Nulls
	arg.ctr.debug_len += uint32(len(vs))
	for _, rowId := range vs {
		blkid := rowId.GetBlockid()
		segid := rowId.GetSegid()
		blkOffset := rowId.GetBlockOffset()
		rowOffset := rowId.GetRowOffset()
		if blkOffset%uint16(arg.Nbucket) != uint16(arg.IBucket) {
			continue
		}
		arg.ctr.deleted_length += 1
		str := string(blkid[:])
		offsetFlag := false
		if arg.ctr.blockId_bitmap[str] == nil {
			arg.ctr.blockId_bitmap[str] = nulls.NewWithSize(int(options.DefaultBlockMaxRows))
		}
		bitmap = arg.ctr.blockId_bitmap[str]
		if bitmap.Contains(uint64(rowOffset)) {
			continue
		} else {
			bitmap.Np.Add(uint64(rowOffset))
		}
		if arg.SegmentMap[string(segid[:])] == colexec.TxnWorkSpaceIdType {
			arg.ctr.blockId_type[str] = RawBatchOffset
			offsetFlag = true
		} else if arg.SegmentMap[string(segid[:])] == colexec.CnBlockIdType {
			arg.ctr.blockId_type[str] = CNBlockOffset
			offsetFlag = true
		} else {
			arg.ctr.blockId_type[str] = RawRowIdBatch
		}
		if _, ok := arg.ctr.blockId_rowIdBatch[str]; !ok {
			if !offsetFlag {
				var bat *batch.Batch
				bat = arg.ctr.pool.get()
				if bat == nil {
					bat = batch.New(false, []string{catalog.Row_ID})
					bat.SetVector(0, vector.NewVec(types.T_Rowid.ToType()))
				}
				arg.ctr.blockId_rowIdBatch[str] = bat
			} else {
				bat := batch.New(false, []string{catalog.BlockMetaOffset})
				bat.SetVector(0, vector.NewVec(types.T_int64.ToType()))
				arg.ctr.blockId_rowIdBatch[str] = bat
			}
		}
		bat := arg.ctr.blockId_rowIdBatch[str]
		offset := rowId.GetRowOffset()
		if !offsetFlag {
			vector.AppendFixed(bat.GetVector(0), rowId, false, proc.GetMPool())
		} else {
			vector.AppendFixed(bat.GetVector(0), int64(offset), false, proc.GetMPool())
		}
		// add rowId size
		if offsetFlag {
			continue
		}
		arg.ctr.batch_size += 24
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
	s3writer := &colexec.S3Writer{}
	s3writer.SetSortIdx(-1)
	resSize := uint32(0)
	_, err = s3writer.GenerateWriter(proc)
	if err != nil {
		return 0, err
	}
	blkids := make([]string, 0, len(ctr.blockId_rowIdBatch))
	for blkid, bat := range ctr.blockId_rowIdBatch {
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
		delete(ctr.blockId_rowIdBatch, blkid)
	}
	metaLocs, err := s3writer.WriteEndBlocks(proc)
	if err != nil {
		return 0, err
	}
	for i, metaLoc := range metaLocs {
		if _, ok := ctr.blockId_metaLoc[blkids[i]]; !ok {
			bat := batch.New(false, []string{catalog.BlockMeta_MetaLoc})
			bat.SetVector(0, vector.NewVec(types.T_text.ToType()))
			ctr.blockId_metaLoc[blkids[i]] = bat
		}
		bat := ctr.blockId_metaLoc[blkids[i]]
		vector.AppendBytes(bat.GetVector(0), []byte(metaLoc), false, proc.GetMPool())
	}
	return resSize, nil
}
