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

package rightanti

import (
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	Build = iota
	Probe
	SendLast
	End
)

type evalVector struct {
	executor colexec.ExpressionExecutor
	vec      *vector.Vector
}

type container struct {
	colexec.ReceiverOperator

	state int

	inBuckets []uint8

	bat *batch.Batch

	expr colexec.ExpressionExecutor

	joinBat1 *batch.Batch
	cfs1     []func(*vector.Vector, *vector.Vector, int64, int) error

	joinBat2 *batch.Batch
	cfs2     []func(*vector.Vector, *vector.Vector, int64, int) error

	evecs []evalVector
	vecs  []*vector.Vector

	mp *hashmap.JoinMap

	matched *bitmap.Bitmap

	handledLast bool
}

type Argument struct {
	ctr        *container
	Ibucket    uint64
	Nbucket    uint64
	Result     []int32
	RightTypes []types.Type
	Cond       *plan.Expr
	Conditions [][]*plan.Expr

	IsMerger bool
	Channel  chan *bitmap.Bitmap
	NumCPU   uint64

	RuntimeFilterSpecs []*plan.RuntimeFilterSpec
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
	ctr := arg.ctr
	if ctr != nil {
		if !ctr.handledLast {
			if arg.NumCPU > 0 {
				if arg.IsMerger {
					for i := uint64(1); i < arg.NumCPU; i++ {
						<-arg.Channel
					}
				} else {
					arg.Channel <- ctr.matched
				}
			}
			ctr.handledLast = true
		}
		mp := proc.Mp()
		ctr.cleanBatch(mp)
		ctr.cleanEvalVectors()
		ctr.cleanHashMap()
		ctr.cleanExprExecutor()
		ctr.FreeAllReg()
	}
}

func (ctr *container) cleanExprExecutor() {
	if ctr.expr != nil {
		ctr.expr.Free()
	}
}

func (ctr *container) cleanBatch(mp *mpool.MPool) {
	if ctr.bat != nil {
		ctr.bat.Clean(mp)
		ctr.bat = nil
	}
	if ctr.joinBat1 != nil {
		ctr.joinBat1.Clean(mp)
		ctr.joinBat1 = nil
	}
	if ctr.joinBat2 != nil {
		ctr.joinBat2.Clean(mp)
		ctr.joinBat2 = nil
	}
}

func (ctr *container) cleanHashMap() {
	if ctr.mp != nil {
		ctr.mp.Free()
		ctr.mp = nil
	}
}

func (ctr *container) cleanEvalVectors() {
	for i := range ctr.evecs {
		if ctr.evecs[i].executor != nil {
			ctr.evecs[i].executor.Free()
		}
	}
}
