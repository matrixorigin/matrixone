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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	flushThreshold = 32 * mpool.MB
)

// for now, we won't do compaction for cn block
type container struct {
	// blockId => rowId Batch
	blockId_rowIdBatch map[string]*batch.Batch
	blockId_metaLoc    map[string]*batch.Batch
	// don't flush cn block rowId and rawBatch
	// we just do compaction for cn block in the
	// future
	blockId_type map[string]int8
	// blockId_min        map[string]uint32
	// blockId_max        map[string]uint32
	batch_size     uint32
	deleted_length uint32
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
	for _, bat := range arg.ctr.blockId_rowIdBatch {
		bat.Clean(proc.GetMPool())
	}
	for _, bat := range arg.ctr.blockId_metaLoc {
		bat.Clean(proc.GetMPool())
	}
}

func (arg *Argument) SplitBatch(proc *process.Process, bat *batch.Batch) error {
	vs := vector.MustFixedCol[types.Rowid](bat.GetVector(0))
	arg.ctr.deleted_length += uint32(len(vs))
	for _, rowId := range vs {
		blkid := rowId.GetBlockid()
		str := (&blkid).String()
		offsetFlag := false
		if arg.SegmentMap[rowId.GetSegid().ToString()] == colexec.TxnWorkSpaceIdType {
			arg.ctr.blockId_type[str] = RawBatchOffset
			offsetFlag = true
		} else if arg.SegmentMap[rowId.GetSegid().ToString()] == colexec.CnBlockIdType {
			arg.ctr.blockId_type[str] = CNBlockOffset
			offsetFlag = true
		}
		if _, ok := arg.ctr.blockId_rowIdBatch[str]; !ok {
			if !offsetFlag {
				bat := batch.New(false, []string{catalog.Row_ID})
				bat.SetVector(0, vector.NewVec(types.T_Rowid.ToType()))
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
		if arg.SegmentMap[blkid.ObjectString()] == colexec.TxnWorkSpaceIdType {
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
	resSize := uint32(0)
	err = s3writer.GenerateWriter(proc)
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
	}
	metaLocs, err := s3writer.WriteEndBlocks(proc)
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
