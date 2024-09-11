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

package semi

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(SemiJoin)

const (
	Build = iota
	Probe
	End
)

type container struct {
	state int

	rbat *batch.Batch

	expr colexec.ExpressionExecutor

	joinBat1 *batch.Batch
	cfs1     []func(*vector.Vector, *vector.Vector, int64, int) error

	joinBat2 *batch.Batch
	cfs2     []func(*vector.Vector, *vector.Vector, int64, int) error

	executor []colexec.ExpressionExecutor
	vecs     []*vector.Vector

	mp *message.JoinMap

	skipProbe     bool
	skipProbeRBat *batch.Batch //only ref to input batch, do not free this batch

	maxAllocSize int64
}

type SemiJoin struct {
	ctr        container
	Result     []int32
	Cond       *plan.Expr
	Conditions [][]*plan.Expr

	HashOnPK           bool
	IsShuffle          bool
	ShuffleIdx         int32
	RuntimeFilterSpecs []*plan.RuntimeFilterSpec
	JoinMapTag         int32
	vm.OperatorBase
	colexec.Projection
}

func (semiJoin *SemiJoin) GetOperatorBase() *vm.OperatorBase {
	return &semiJoin.OperatorBase
}

func init() {
	reuse.CreatePool[SemiJoin](
		func() *SemiJoin {
			return &SemiJoin{}
		},
		func(a *SemiJoin) {
			*a = SemiJoin{}
		},
		reuse.DefaultOptions[SemiJoin]().
			WithEnableChecker(),
	)
}

func (semiJoin SemiJoin) TypeName() string {
	return opName
}

func NewArgument() *SemiJoin {
	return reuse.Alloc[SemiJoin](nil)
}

func (semiJoin *SemiJoin) Release() {
	if semiJoin != nil {
		reuse.Free[SemiJoin](semiJoin, nil)
	}
}

func (semiJoin *SemiJoin) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &semiJoin.ctr

	ctr.resetExecutor()
	ctr.resetExprExecutor()
	ctr.cleanHashMap()
	ctr.state = Build
	ctr.skipProbe = false

	if semiJoin.ProjectList != nil {
		if semiJoin.OpAnalyzer != nil {
			semiJoin.OpAnalyzer.Alloc(semiJoin.ProjectAllocSize + semiJoin.ctr.maxAllocSize)
		}

		semiJoin.ctr.maxAllocSize = 0
		semiJoin.ResetProjection(proc)
	} else {
		if semiJoin.OpAnalyzer != nil {
			semiJoin.OpAnalyzer.Alloc(semiJoin.ctr.maxAllocSize)
		}
		semiJoin.ctr.maxAllocSize = 0
	}
}

func (semiJoin *SemiJoin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &semiJoin.ctr

	ctr.cleanExecutor()
	ctr.cleanExprExecutor()
	ctr.cleanBatch(proc)

	semiJoin.FreeProjection(proc)
}

func (ctr *container) resetExprExecutor() {
	if ctr.expr != nil {
		ctr.expr.ResetForNextQuery()
	}
}

func (ctr *container) cleanExprExecutor() {
	if ctr.expr != nil {
		ctr.expr.Free()
		ctr.expr = nil
	}
}

func (ctr *container) cleanBatch(proc *process.Process) {
	if ctr.rbat != nil {
		ctr.rbat.Clean(proc.Mp())
	}
	if ctr.joinBat1 != nil {
		ctr.joinBat1.Clean(proc.Mp())
	}
	if ctr.joinBat2 != nil {
		ctr.joinBat2.Clean(proc.Mp())
	}
}

func (ctr *container) cleanHashMap() {
	if ctr.mp != nil {
		ctr.mp.Free()
		ctr.mp = nil
	}
}

func (ctr *container) resetExecutor() {
	for i := range ctr.executor {
		if ctr.executor[i] != nil {
			ctr.executor[i].ResetForNextQuery()
		}
	}
}

func (ctr *container) cleanExecutor() {
	for i := range ctr.executor {
		if ctr.executor[i] != nil {
			ctr.executor[i].Free()
		}
	}
}
