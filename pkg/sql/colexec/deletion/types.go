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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	RemoteDelete = 1
	LocalDelete  = 2
	DeleteRows   = 2000
)

type container struct {
	// BlockId => offsets
	mp           pipeline.ArrayMap
	blkDeleteBat *batch.Batch
}

// assume that one blk will delete 2000 row in genernal
func (ctr *container) PutDeteteOffset(blkId string, offset int32) {
	if _, ok := ctr.mp.Mp[blkId]; !ok {
		ctr.mp.Mp[blkId] = &pipeline.Array{
			Array: make([]int32, DeleteRows),
		}
	}
	ctr.mp.Mp[blkId].Array = append(ctr.mp.Mp[blkId].Array, offset)
}

func (ctr *container) GetDeleteBatch(proc *process.Process) (*batch.Batch, error) {
	for blkId, offsets := range ctr.mp.Mp {
		vector.AppendBytes(ctr.blkDeleteBat.GetVector(0), []byte(blkId), false, proc.GetMPool())
		bytes, err := offsets.Marshal()
		if err != nil {
			return nil, err
		}
		vector.AppendBytes(ctr.blkDeleteBat.GetVector(1), bytes, false, proc.GetMPool())
	}
	return ctr.blkDeleteBat, nil
}

type Argument struct {
	Ts           uint64
	DeleteCtx    *DeleteCtx
	AffectedRows uint64
	Engine       engine.Engine

	DeleteType int
	ctr        *container
}

type DeleteCtx struct {
	CanTruncate bool

	DelSource []engine.Relation
	DelRef    []*plan.ObjectRef

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
	if arg.ctr != nil {
		arg.ctr.blkDeleteBat.Clean(proc.GetMPool())
	}
}
