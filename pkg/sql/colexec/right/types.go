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

package right

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	Build = iota
	Probe
	End
)

type evalVector struct {
	needFree bool
	vec      *vector.Vector
}

type container struct {
	state int

	inBuckets []uint8

	bat *batch.Batch

	evecs []evalVector
	vecs  []*vector.Vector

	mp *hashmap.JoinMap

	matched_sels []int64
}

type Argument struct {
	ctr        *container
	Ibucket    uint64
	Nbucket    uint64
	Result     []colexec.ResultPos
	Left_typs  []types.Type
	Right_typs []types.Type
	Cond       *plan.Expr
	Conditions [][]*plan.Expr

	Is_receiver bool
	Channel     chan *[]int64
	NumCPU      uint64
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
	ctr := arg.ctr
	if ctr != nil {
		mp := proc.Mp()
		ctr.cleanBatch(mp)
		ctr.cleanEvalVectors(mp)
		ctr.cleanHashMap()
	}
}

func (ctr *container) cleanBatch(mp *mpool.MPool) {
	if ctr.bat != nil {
		ctr.bat.Clean(mp)
		ctr.bat = nil
	}
}

func (ctr *container) cleanHashMap() {
	if ctr.mp != nil {
		ctr.mp.Free()
		ctr.mp = nil
	}
}

func (ctr *container) cleanEvalVectors(mp *mpool.MPool) {
	for i := range ctr.evecs {
		if ctr.evecs[i].needFree && ctr.evecs[i].vec != nil {
			ctr.evecs[i].vec.Free(mp)
			ctr.evecs[i].vec = nil
		}
	}
}
