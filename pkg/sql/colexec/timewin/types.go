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

var _ vm.Operator = new(TimeWin)

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
	rbat   *batch.Batch
	colCnt int

	i    int
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

	calRes func(ctr *container, ap *TimeWin, proc *process.Process) (err error)
	eval   func(ctr *container, ap *TimeWin, proc *process.Process) (err error)
}

type TimeWin struct {
	ctr container

	Types []types.Type
	Aggs  []aggexec.AggFuncExecExpression

	Interval *Interval
	Sliding  *Interval
	Ts       *plan.Expr

	WStart bool
	WEnd   bool

	vm.OperatorBase
}

func (timeWin *TimeWin) GetOperatorBase() *vm.OperatorBase {
	return &timeWin.OperatorBase
}

func init() {
	reuse.CreatePool[TimeWin](
		func() *TimeWin {
			return &TimeWin{}
		},
		func(a *TimeWin) {
			*a = TimeWin{}
		},
		reuse.DefaultOptions[TimeWin]().
			WithEnableChecker(),
	)
}

func (timeWin TimeWin) TypeName() string {
	return opName
}

func NewArgument() *TimeWin {
	return reuse.Alloc[TimeWin](nil)
}

func (timeWin *TimeWin) Release() {
	if timeWin != nil {
		reuse.Free[TimeWin](timeWin, nil)
	}
}

type Interval struct {
	Typ types.IntervalType
	Val int64
}

func (timeWin *TimeWin) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &timeWin.ctr
	ctr.resetExes()
	ctr.resetParam()
	ctr.resetWin()
}

func (timeWin *TimeWin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &timeWin.ctr
	ctr.freeBatch(proc.Mp())
	ctr.freeVector(proc.Mp())
	ctr.freeExes()
	ctr.freeAgg()
}

func (ctr *container) resetExes() {
	for _, exe := range ctr.aggExe {
		if exe != nil {
			exe.ResetForNextQuery()
		}
	}
	if ctr.tsExe != nil {
		ctr.tsExe.ResetForNextQuery()
	}
}

func (ctr *container) resetParam() {
	ctr.cur = withoutGrow
	ctr.pre = withoutPre
	ctr.status = initTag
	ctr.i = 0
	ctr.curIdx = 0
	ctr.curRow = 0
	ctr.preIdx = 0
	ctr.preRow = 0
	ctr.nextStart = 0
}

func (ctr *container) freeExes() {
	for _, exe := range ctr.aggExe {
		if exe != nil {
			exe.Free()
		}
	}
	if ctr.tsExe != nil {
		ctr.tsExe.Free()
	}
}

func (ctr *container) freeBatch(mp *mpool.MPool) {
	if ctr.rbat != nil {
		ctr.rbat.Clean(mp)
	}
	for _, b := range ctr.bats {
		if b != nil {
			b.Clean(mp)
		}
	}
}

func (ctr *container) freeAgg() {
	for _, a := range ctr.aggs {
		if a != nil {
			a.Free()
		}
	}
}

func (ctr *container) freeVector(mp *mpool.MPool) {
	for _, vec := range ctr.tsVec {
		if vec != nil {
			vec.Free(mp)
		}
	}
	ctr.tsVec = nil

	for _, vecs := range ctr.aggVec {
		for _, vec := range vecs {
			if vec != nil {
				vec.Free(mp)
			}
		}
	}
	ctr.aggVec = nil
}

func (ctr *container) resetWin() {
	ctr.wstart = nil
	ctr.wend = nil
	ctr.aggs = nil
}
