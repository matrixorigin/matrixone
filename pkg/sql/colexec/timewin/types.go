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

package timewin

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Argument)

const (
	initTag     = 0
	evalTag     = 1
	nextTag     = 2
	dataTag     = 3
	endTag      = 4
	evalLastCur = 5
	evalLastPre = 6
	resultTag   = 7
)

type preType int

const (
	withoutPre preType = iota
	hasPre
)

type curType int

const (
	withoutGrow curType = iota
	hasGrow
)

type container struct {
	colexec.ReceiverOperator

	rbat   *batch.Batch
	colCnt int

	bats []*batch.Batch

	aggExe []colexec.ExpressionExecutor
	aggVec [][]*vector.Vector

	tsExe colexec.ExpressionExecutor
	tsVec []*vector.Vector

	tsOid types.T
	tsTyp *types.Type

	status int32

	start     int64
	end       int64
	nextStart int64

	pre    preType
	preRow int
	preIdx int

	cur    curType
	curRow int
	curIdx int

	group int
	aggs  []aggexec.AggFuncExec

	wstart []int64
	wend   []int64

	calRes func(ctr *container, ap *Argument, proc *process.Process) (err error)
	eval   func(ctr *container, ap *Argument, proc *process.Process) (err error)
}

type Argument struct {
	ctr *container

	Types []types.Type
	Aggs  []aggexec.AggFuncExecExpression

	Interval *Interval
	Sliding  *Interval
	Ts       *plan.Expr

	WStart bool
	WEnd   bool

	vm.OperatorBase
}

func (arg *Argument) GetOperatorBase() *vm.OperatorBase {
	return &arg.OperatorBase
}

func init() {
	reuse.CreatePool[Argument](
		func() *Argument {
			return &Argument{}
		},
		func(a *Argument) {
			*a = Argument{}
		},
		reuse.DefaultOptions[Argument]().
			WithEnableChecker(),
	)
}

func (arg Argument) TypeName() string {
	return argName
}

func NewArgument() *Argument {
	return reuse.Alloc[Argument](nil)
}

func (arg *Argument) Release() {
	if arg != nil {
		reuse.Free[Argument](arg, nil)
	}
}

type Interval struct {
	Typ types.IntervalType
	Val int64
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := arg.ctr
	if ctr != nil {
		ctr.FreeMergeTypeOperator(pipelineFailed)
		ctr.cleanBatch(proc.Mp())
		ctr.cleanTsVector()
		ctr.cleanAggVector()
		ctr.cleanWin()
		arg.ctr = nil
	}
}

func (ctr *container) cleanBatch(mp *mpool.MPool) {
	if ctr.rbat != nil {
		ctr.rbat.Clean(mp)
	}
	for _, b := range ctr.bats {
		if b != nil {
			b.Clean(mp)
		}
	}
}

func (ctr *container) cleanTsVector() {
	if ctr.tsExe != nil {
		ctr.tsExe.Free()
	}
	ctr.tsVec = nil
	ctr.tsExe = nil
}

func (ctr *container) cleanAggVector() {
	for i := range ctr.aggExe {
		if ctr.aggExe[i] != nil {
			ctr.aggExe[i].Free()
		}
	}
	ctr.aggVec = nil
	ctr.aggExe = nil
}

func (ctr *container) cleanWin() {
	ctr.wstart = nil
	ctr.wend = nil
	ctr.aggs = nil
}
