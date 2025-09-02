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

package left

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(LeftJoin)

const (
	Build = iota
	Probe
	End
)

type probeState int

const (
	psNextBatch probeState = iota
	psSelsForOneRow
	psBatchRow
)

type container struct {
	state         int
	itr           hashmap.Iterator
	batchRowCount int64
	inbat         *batch.Batch
	rbat          *batch.Batch

	// fileds for pagination

	probeState probeState

	// at any time, lastRow is the index of not processed row in inbat
	lastRow int
	// process idx for zvs and vs, which returned by hashmap.Iterator.Find()
	// guarantee: vs[ctr.vsidx] is the result of inbat[ctr.lastRow]
	vsidx int
	zvs   []int64
	vs    []uint64
	// sels
	sels                    []int32
	anySelNEqMatchForOneRow bool

	expr colexec.ExpressionExecutor

	joinBats []*batch.Batch
	cfs1     []func(*vector.Vector, *vector.Vector, int64, int) error
	cfs2     []func(*vector.Vector, *vector.Vector, int64, int) error

	executor []colexec.ExpressionExecutor
	vecs     []*vector.Vector

	mp *message.JoinMap

	maxAllocSize int64
}

type LeftJoin struct {
	ctr        container
	Result     []colexec.ResultPos
	Typs       []types.Type
	Cond       *plan.Expr
	Conditions [][]*plan.Expr

	HashOnPK           bool
	IsShuffle          bool
	ShuffleIdx         int32
	RuntimeFilterSpecs []*plan.RuntimeFilterSpec
	JoinMapTag         int32

	vm.OperatorBase
}

func (leftJoin *LeftJoin) GetOperatorBase() *vm.OperatorBase {
	return &leftJoin.OperatorBase
}

func init() {
	reuse.CreatePool(
		func() *LeftJoin {
			return &LeftJoin{}
		},
		func(a *LeftJoin) {
			*a = LeftJoin{}
		},
		reuse.DefaultOptions[LeftJoin]().
			WithEnableChecker(),
	)
}

func (leftJoin LeftJoin) TypeName() string {
	return opName
}

func NewArgument() *LeftJoin {
	return reuse.Alloc[LeftJoin](nil)
}

func (leftJoin *LeftJoin) Release() {
	if leftJoin != nil {
		reuse.Free(leftJoin, nil)
	}
}

func (leftJoin *LeftJoin) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &leftJoin.ctr
	ctr.itr = nil
	ctr.resetExecutor()
	ctr.resetExprExecutor()
	ctr.cleanHashMap()
	ctr.inbat = nil
	ctr.lastRow = 0
	ctr.state = Build
	ctr.batchRowCount = 0
	ctr.probeState = psNextBatch
	ctr.anySelNEqMatchForOneRow = false

	if leftJoin.OpAnalyzer != nil {
		leftJoin.OpAnalyzer.Alloc(leftJoin.ctr.maxAllocSize)
	}
	leftJoin.ctr.maxAllocSize = 0
}

func (leftJoin *LeftJoin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &leftJoin.ctr

	ctr.cleanExecutor()
	ctr.cleanExprExecutor()
	ctr.cleanBatch(proc)

}

func (leftJoin *LeftJoin) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
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
	for i := range ctr.joinBats {
		if ctr.joinBats[i] != nil {
			ctr.joinBats[i].Clean(proc.Mp())
		}
		ctr.joinBats[i] = nil
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
