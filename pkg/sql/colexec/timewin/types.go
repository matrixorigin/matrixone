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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	receive     = 0
	fill        = 1
	end         = 2
	flush       = 3
	nextWindow  = 4
	nextBatch   = 5
	firstWindow = 6
	interval    = 7
)

type container struct {
	bat    *batch.Batch
	colCnt int
	i      int

	aggExe []colexec.ExpressionExecutor
	aggVec [][]*vector.Vector

	tsExe colexec.ExpressionExecutor
	tsVec []*vector.Vector
	tsOid types.T

	startExe colexec.ExpressionExecutor
	startVec *vector.Vector
	endExe   colexec.ExpressionExecutor
	endVec   *vector.Vector

	status int32
	end    bool

	group int
	aggs  []aggexec.AggFuncExec

	wStart []types.Datetime
	wEnd   []types.Datetime

	curVecIdx int
	curRowIdx int

	left  types.Datetime
	right types.Datetime

	preVecIdx int
	preRowIdx int

	nextLeft  types.Datetime
	nextRight types.Datetime

	withoutFill bool

	last    bool
	lastVal types.Datetime
}

type TimeWin struct {
	ctr container

	Types []types.Type
	Aggs  []aggexec.AggFuncExecExpression

	TsType  plan.Type
	Ts      *plan.Expr
	EndExpr *plan.Expr

	Interval types.Datetime
	Sliding  types.Datetime

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

func (timeWin *TimeWin) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &timeWin.ctr
	ctr.resetExes()
	ctr.resetParam(timeWin)
}

func (timeWin *TimeWin) MakeIntervalAndSliding(interval, sliding *plan.Expr) error {
	str := interval.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Sval).Sval
	typ, err := types.IntervalTypeOf(str)
	if err != nil {
		return err
	}
	val1 := interval.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
	timeWin.Interval, err = calcDatetime(val1, typ)
	if err != nil {
		return err
	}

	if sliding != nil {
		str = sliding.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Sval).Sval
		typ, err = types.IntervalTypeOf(str)
		if err != nil {
			return err
		}
		val2 := sliding.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
		timeWin.Sliding, err = calcDatetime(val2, typ)
		if err != nil {
			return err
		}
	}

	return nil
}

func calcDatetime(diff int64, iTyp types.IntervalType) (types.Datetime, error) {
	var num int64
	err := types.JudgeIntervalNumOverflow(diff, iTyp)
	if err != nil {
		return 0, err
	}
	switch iTyp {
	case types.Second:
		num = diff * types.MicroSecsPerSec
	case types.Minute:
		num = diff * types.SecsPerMinute * types.MicroSecsPerSec
	case types.Hour:
		num = diff * types.SecsPerHour * types.MicroSecsPerSec
	case types.Day:
		num = diff * types.SecsPerDay * types.MicroSecsPerSec
	default:
		return 0, moerr.NewNotSupportedNoCtx("Time Window aggregate only support SECOND, MINUTE, HOUR, DAY as the time unit")
	}
	return types.Datetime(num), nil
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
	if ctr.startExe != nil {
		ctr.startExe.ResetForNextQuery()
	}
	if ctr.endExe != nil {
		ctr.endExe.ResetForNextQuery()
	}
}

func (ctr *container) resetParam(timeWin *TimeWin) {
	if timeWin.EndExpr != nil {
		ctr.status = interval
	} else {
		ctr.status = receive
	}
	ctr.end = false
	ctr.group = -1
	ctr.wStart = nil
	ctr.wEnd = nil
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
	if ctr.startExe != nil {
		ctr.startExe.Free()
	}
	if ctr.endExe != nil {
		ctr.endExe.Free()
	}
}

func (ctr *container) freeBatch(mp *mpool.MPool) {
	if ctr.bat != nil {
		ctr.bat.Clean(mp)
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
